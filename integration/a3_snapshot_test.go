//go:build integration

package integration

import (
	"encoding/base64"
	json "encoding/json/v2"
	"fmt"
	"maps"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/librecosta/harvest/internal/execution"
)

func a3Enroll(t *testing.T, p *testPlane, stream, name, owner string) execution.ConsumerView {
	t.Helper()
	var c execution.ConsumerView
	p.call("POST", "/api/v1/consumers", "operator", "", execution.ConsumerEnrollment{Stream: stream, Name: name, Principal: owner}, &c, 200)
	if !execution.UUIDv7(c.ID) || !execution.UUIDv7(c.Epoch) {
		t.Fatal("consumer IDs must be UUIDv7")
	}
	return c
}

func a3Reset(p *testPlane, c execution.ConsumerView, owner string) execution.ResetView {
	p.t.Helper()
	var out execution.ResetView
	p.call("POST", "/api/v1/consumers/"+c.ID.String()+"/reset", owner, "", map[string]string{"stream": c.Stream}, &out, 200)
	return out
}

func a3Ack(p *testPlane, c execution.ConsumerView, owner, cursor string, token *uuid.UUID) execution.ConsumerView {
	p.t.Helper()
	var ack execution.ConsumerView
	p.call("POST", "/api/v1/consumers/"+c.ID.String()+"/ack", owner, "", execution.ConsumerAck{Stream: c.Stream, Cursor: cursor, Reset: token}, &ack, 200)
	return ack
}

func a3ChangesPath(c execution.ConsumerView, after string) string {
	return "/api/v1/changes?stream=" + c.Stream + "&consumer=" + c.ID.String() + "&after=" + after + "&limit=1"
}

type projectionValue struct {
	Position int64  `json:"position"`
	Data     string `json:"data,omitempty"`
	Deleted  bool   `json:"deleted,omitempty"`
}

func projectionKey(id execution.ResultIdentity) string {
	return fmt.Sprintf("%s/%s/%d/%s", id.TargetID, id.Capability, id.OutputSchema, id.ScopeKey)
}

func testA3Snapshot(t *testing.T, p *testPlane) {
	const stream = "snapshots"
	beforeCursor := p.register("fixture", "key", "snap-insert-before", stream)
	withdrawn := p.register("fixture", "key", "snap-withdrawn", stream)
	updated := p.register("fixture", "key", "snap-updated", stream)
	recreated := p.register("fixture", "key", "snap-recreated", stream)
	p.a3Worker("worker_a", detailOffer)
	p.a3Worker("worker_b", detailOffer)
	submit := func(target uuid.UUID, mode string, value int) uuid.UUID {
		return p.submit(uuid.NewV7().String(), a3Request(t, target, detailOffer, observationParams{Mode: mode, Value: value, Delay: 250})).ID
	}
	wait := func(jobs ...uuid.UUID) {
		for _, job := range jobs {
			p.waitJob(job, "real Worker observation completion", 12*time.Second, func(j jobDetail) bool { return j.Job.State == "succeeded" })
		}
	}
	wait(submit(withdrawn, "present", 1), submit(updated, "present", 1), submit(recreated, "present", 1))
	if workers := a3Count(t, p, `SELECT count(DISTINCT w.principal) FROM harvest.observations o
		JOIN harvest.attempts a ON a.id=o.attempt_id JOIN harvest.workers w ON w.id=a.worker_id WHERE o.stream=$1`, stream); workers != 2 {
		t.Fatalf("observations came from %d independent Worker principals", workers)
	}
	c := a3Enroll(t, p, stream, "snapshotter", "public_reader")
	if c2 := a3Enroll(t, p, stream, "snapshotter", "public_reader"); c2.ID != c.ID {
		t.Fatal("enrollment was not idempotent")
	}
	a3Problem(t, p, "POST", "/api/v1/consumers", "public_reader", execution.ConsumerEnrollment{Stream: stream, Name: "self", Principal: "public_reader"}, 403, "forbidden")
	a3Problem(t, p, "GET", "/api/v1/observations/current?stream=private", "public_reader", nil, 403, "forbidden")
	reset := a3Reset(p, c, "public_reader")
	snapshotPath := "/api/v1/snapshots/current?stream=" + stream + "&consumer=" + c.ID.String() + "&reset=" + reset.Reset.String() + "&limit=2"
	var page execution.CurrentView
	p.call("GET", snapshotPath, "public_reader", "", nil, &page, 200)
	if len(page.Items) != 2 || page.After == "" || page.Items[0].TargetID != withdrawn || page.Items[1].TargetID != withdrawn ||
		beforeCursor.String() >= withdrawn.String() {
		t.Fatal("fixture did not put the future insert before the page cursor")
	}
	staged := make(map[string]projectionValue)
	stage := func(page execution.CurrentView) {
		for _, item := range page.Items {
			staged[projectionKey(item.ResultIdentity)] = projectionValue{Position: item.Position, Data: string(item.Data)}
		}
	}
	stage(page)
	wait(submit(beforeCursor, "present", 6), submit(withdrawn, "withdraw", 0), submit(updated, "present", 2), submit(recreated, "withdraw", 0))
	wait(submit(updated, "present", 3), submit(recreated, "present", 7))
	for page.After != "" {
		next := page.After
		page = execution.CurrentView{}
		p.call("GET", snapshotPath+"&after="+next, "public_reader", "", nil, &page, 200)
		stage(page)
	}
	through := page.Head
	expected := a3ReadCurrent(t, p, stream)
	if expected.Head != through {
		t.Fatal("fixture head moved before the replay boundary was captured")
	}
	wait(submit(updated, "present", 4))
	var ignoredOlder, appliedWithdrawals, appliedMissed, replayed int
	apply := func(events []execution.ChangeView) {
		for _, event := range events {
			replayed++
			key := projectionKey(event.ResultIdentity)
			if event.Position <= staged[key].Position {
				if event.TargetID == updated || event.TargetID == recreated {
					ignoredOlder++
				}
				continue
			}
			if event.Action == "withdraw" {
				if len(event.TombstoneScope) == 0 || event.Observation != nil {
					t.Fatal("invalid tombstone")
				}
				staged[key] = projectionValue{Position: event.Position, Deleted: true}
				appliedWithdrawals++
			} else {
				if event.Observation == nil || event.ResultIdentity != event.Observation.ResultIdentity {
					t.Fatal("event lost immutable identity")
				}
				staged[key] = projectionValue{Position: event.Position, Data: string(event.Observation.Data)}
				if event.TargetID == beforeCursor {
					appliedMissed++
				}
			}
		}
	}
	next := reset.Baseline
	for pages := 0; ; pages++ {
		if pages > 64 {
			t.Fatal("change cursor stopped progressing")
		}
		var changes execution.ChangesView
		p.call("GET", a3ChangesPath(c, next)+"&reset="+reset.Reset.String()+"&through="+through, "public_reader", "", nil, &changes, 200)
		if len(changes.Items) > 1 || a3Position(t, changes.Next) < a3Position(t, next) {
			t.Fatal("invalid page")
		}
		apply(changes.Items)
		next = changes.Next
		if !changes.HasMore {
			break
		}
	}
	if next != through || ignoredOlder < 4 || appliedWithdrawals != 2 || appliedMissed != 2 {
		t.Fatalf("snapshot proof replay=%d skipped older=%d withdrawn=%d missed inserts=%d", replayed, ignoredOlder, appliedWithdrawals, appliedMissed)
	}
	compareProjection(t, staged, expected)

	// The consumer commits its projection and cursor together in its own
	// database, then separately acknowledges that durable cursor to Harvest.
	a3Exec(t, p, "CREATE DATABASE harvest_a3_consumer")
	cfg := p.pool.Config()
	cfg.ConnConfig.Database = "harvest_a3_consumer"
	clientDB, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer clientDB.Close()
	if _, err := clientDB.Exec(t.Context(), "CREATE TABLE published (id bool PRIMARY KEY, projection jsonb NOT NULL, cursor text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	publish := func(cursor string) {
		t.Helper()
		data, err := json.Marshal(staged)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := clientDB.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(t.Context())
		if _, err := tx.Exec(t.Context(), `INSERT INTO published VALUES (true,$1,$2)
			ON CONFLICT (id) DO UPDATE SET projection=EXCLUDED.projection,cursor=EXCLUDED.cursor`, data, cursor); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
		var durable string
		if err := clientDB.QueryRow(t.Context(), "SELECT cursor FROM published WHERE id").Scan(&durable); err != nil || durable != cursor {
			t.Fatalf("consumer cursor %q: %v", durable, err)
		}
	}
	if ack := a3Count(t, p, "SELECT ack_position FROM harvest.consumers WHERE id=$1", c.ID); ack != 0 {
		t.Fatal("reads implicitly ACKed")
	}
	publish(through)
	if ack := a3Ack(p, c, "public_reader", through, &reset.Reset); ack.ResetRequired || ack.ACK != through {
		t.Fatal("reset ACK failed")
	}
	a3Ack(p, c, "public_reader", through, &reset.Reset)
	ackPath := "/api/v1/consumers/" + c.ID.String() + "/ack"
	a3Problem(t, p, "POST", ackPath, "operator", execution.ConsumerAck{Stream: stream, Cursor: through}, 404, "not_found")
	a3Problem(t, p, "GET", a3ChangesPath(c, through), "operator", nil, 404, "not_found")
	var changes execution.ChangesView
	for pages := 0; ; pages++ {
		if pages > 8 {
			t.Fatal("incremental cursor stopped progressing")
		}
		p.call("GET", a3ChangesPath(c, next), "public_reader", "", nil, &changes, 200)
		apply(changes.Items)
		next = changes.Next
		if !changes.HasMore {
			break
		}
	}
	compareProjection(t, staged, a3ReadCurrent(t, p, stream))
	publish(next)
	a3Ack(p, c, "public_reader", next, nil)
	if ack := a3Ack(p, c, "public_reader", through, nil); ack.ACK != next {
		t.Fatal("lower ACK regressed cursor")
	}
	raw, _ := json.Marshal(map[string]any{"stream": stream, "epoch": c.Epoch, "position": a3Position(t, next) + 1})
	a3Problem(t, p, "POST", ackPath, "public_reader", execution.ConsumerAck{Stream: stream, Cursor: base64.RawURLEncoding.EncodeToString(raw)}, 400, "invalid_request")
	malformed, _ := json.Marshal(map[string]any{"stream": stream, "epoch": c.Epoch})
	a3Problem(t, p, "GET", a3ChangesPath(c, base64.RawURLEncoding.EncodeToString(malformed)), "public_reader", nil, 400, "invalid_request")
	if replayed < 14 {
		t.Fatal("fixture did not replay all required transitions")
	}
	t.Logf("converged through %d then %d, replayed %d events, skipped %d older versions", a3Position(t, through), a3Position(t, next), replayed, ignoredOlder)
}

func compareProjection(t *testing.T, staged map[string]projectionValue, current execution.CurrentView) {
	t.Helper()
	want := make(map[string]projectionValue, len(current.Items))
	got := make(map[string]projectionValue, len(staged))
	for _, item := range current.Items {
		want[projectionKey(item.ResultIdentity)] = projectionValue{Position: item.Position, Data: string(item.Data)}
	}
	for key, v := range staged {
		if !v.Deleted {
			got[key] = v
		}
	}
	if !maps.Equal(got, want) {
		t.Fatalf("projection diverged: got %v, want %v", got, want)
	}
}
