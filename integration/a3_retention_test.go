//go:build integration

package integration

import (
	json "encoding/json/v2"
	"errors"
	"testing"
	"time"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/execution"
)

func testA3Retention(t *testing.T, p *testPlane) {
	const stream = "retention"
	w := p.a3Register("worker_a")
	target := p.register("fixture", "key", "a3-retained", stream)
	complete := func(n int) {
		t.Helper()
		claim, payload := p.a3Claim("worker_a", w, target, sampleOffer, observationParams{Mode: "present", Value: n})
		p.call("POST", completePath(claim), "worker_a", "", payload, nil, 200)
	}
	c := a3Enroll(t, p, stream, "offline", "public_reader")
	zero := a3Reset(p, c, "public_reader")
	a3Ack(p, c, "public_reader", zero.Baseline, &zero.Reset)
	complete(1)
	one := a3ReadCurrent(t, p, stream).Head
	a3Ack(p, c, "public_reader", one, nil)
	pinner := a3Enroll(t, p, stream, "pin", "public_reader")
	pin := a3Reset(p, pinner, "public_reader")
	complete(2)
	complete(3)
	last := a3ReadCurrent(t, p, stream).Head
	caughtUp := a3Enroll(t, p, stream, "caught-up", "public_reader")
	final := a3Reset(p, caughtUp, "public_reader")
	a3Ack(p, caughtUp, "public_reader", final.Baseline, &final.Reset)
	held, heldPayload := p.a3Claim("worker_a", w, target, sampleOffer, observationParams{Mode: "present", Value: 4})
	a3Exec(t, p, "UPDATE harvest.observations SET received_at=clock_timestamp()-interval '40 days' WHERE stream=$1", stream)
	a3Exec(t, p, "UPDATE harvest.change_events SET committed_at=clock_timestamp()-interval '40 days' WHERE stream=$1", stream)
	cfg, err := config.Load(p.config)
	if err != nil {
		t.Fatal(err)
	}
	e := execution.New(p.pool, fixtureRegistry(), cfg.Principals, egress.NetworkConfig{}, cfg.Retention)
	prune := func(e *execution.Engine, floor int64) {
		t.Helper()
		a3Exec(t, p, "UPDATE harvest.change_heads SET last_pruned_at='epoch' WHERE stream=$1", stream)
		eventually(t, 4*time.Second, "retention floor", func() bool {
			if err := e.PruneResults(t.Context()); err != nil {
				t.Fatal(err)
			}
			return a3Count(t, p, "SELECT min_position FROM harvest.change_heads WHERE stream=$1", stream) == floor
		})
	}
	a3Exec(t, p, "UPDATE harvest.change_heads SET last_pruned_at='epoch' WHERE stream=$1", stream)
	eventually(t, 5*time.Second, "automatic retention", func() bool {
		return a3Count(t, p, "SELECT min_position FROM harvest.change_heads WHERE stream=$1", stream) == 2
	})
	if n := a3Count(t, p, "SELECT count(*) FROM harvest.observations WHERE stream=$1", stream); n != 2 {
		t.Fatalf("expected event and current pins after first prune, got %d", n)
	}
	a3Problem(t, p, "GET", a3ChangesPath(c, zero.Baseline), "public_reader", nil, 410, "cursor_expired")
	var changes execution.ChangesView
	p.call("GET", a3ChangesPath(c, one), "public_reader", "", nil, &changes, 200)
	if changes.MinAvailable != 2 || changes.MinAvailableCursor != one || len(changes.Items) != 1 || a3Position(t, changes.Next) != 2 || !changes.HasMore {
		t.Fatal("min-1 rejected")
	}
	p.call("GET", a3ChangesPath(pinner, pin.Baseline)+"&reset="+pin.Reset.String()+"&through="+last, "public_reader", "", nil, &changes, 200)

	a3Exec(t, p, "UPDATE harvest.change_events SET committed_at=clock_timestamp() WHERE stream=$1 AND position=3", stream)
	// Simulate pressure below the YAML production minimum against three rows.
	// The actual pruning SQL, reset invalidation and read path are unchanged.
	limits := cfg.Retention
	limits.EventRows = 1
	pressure := execution.New(p.pool, fixtureRegistry(), cfg.Principals, egress.NetworkConfig{}, limits)
	prune(pressure, 3)
	if n := a3Count(t, p, "SELECT count(*) FROM harvest.consumers WHERE stream=$1 AND reset_required AND reset_id IS NULL", stream); n != 2 {
		t.Fatal("prune floor moved without invalidating lagging consumer and reset")
	}
	a3Problem(t, p, "GET", a3ChangesPath(c, one), "public_reader", nil, 410, "cursor_expired")
	a3Problem(t, p, "GET", a3ChangesPath(pinner, pin.Baseline)+"&reset="+pin.Reset.String()+"&through="+last, "public_reader", nil, 410, "cursor_expired")
	a3Exec(t, p, "UPDATE harvest.change_events SET committed_at=clock_timestamp()-interval '40 days' WHERE stream=$1 AND position=3", stream)
	prune(e, 4)
	if n := a3Count(t, p, `SELECT count(*) FROM harvest.observations o JOIN harvest.current_results c
		ON c.observation_id=o.id WHERE o.stream=$1 AND o.data->>'value'='3'`, stream); n != 1 {
		t.Fatal("current pin removed by retention")
	}
	p.call("GET", a3ChangesPath(caughtUp, last), "public_reader", "", nil, &changes, 200)
	if len(changes.Items) != 0 || changes.Next != last || changes.MinAvailable != 4 || changes.HasMore {
		t.Fatal("empty retained stream broke valid floor cursor")
	}
	expired := a3Reset(p, pinner, "public_reader")
	a3Exec(t, p, "UPDATE harvest.consumers SET reset_deadline=clock_timestamp()-interval '1 second' WHERE id=$1", pinner.ID)
	a3Problem(t, p, "GET", "/api/v1/snapshots/current?stream="+stream+"&consumer="+pinner.ID.String()+"&reset="+expired.Reset.String(), "public_reader", nil, 410, "cursor_expired")
	fresh := a3Reset(p, pinner, "public_reader")
	changedGrant := config.Principal{Name: "public_reader", Role: "reader", Namespaces: []string{stream}, Capabilities: []string{detailOffer.Capability}}
	_, err = e.Snapshot(t.Context(), changedGrant, execution.SnapshotPage{
		ResultPage: execution.ResultPage{Stream: stream, Limit: 1}, Consumer: pinner.ID, Reset: fresh.Reset,
	})
	if !errors.Is(err, execution.ErrCursorExpired) {
		t.Fatalf("changed grant did not expire snapshot: %v", err)
	}
	a3Exec(t, p, "UPDATE harvest.consumers SET reset_bytes=reset_bytes-(33*1024*1024) WHERE id=$1", pinner.ID)
	a3Problem(t, p, "GET", "/api/v1/snapshots/current?stream="+stream+"&consumer="+pinner.ID.String()+"&reset="+fresh.Reset.String(), "public_reader", nil, 410, "cursor_expired")
	if n := a3Count(t, p, "SELECT count(*) FROM harvest.attempts WHERE id=$1 AND state='running' AND lease_until>now()", held.AttemptID); n != 1 {
		t.Fatal("result pruning disturbed live lease")
	}
	p.call("POST", completePath(held), "worker_a", "", heldPayload, nil, 200)
	if n := a3Count(t, p, `SELECT count(*) FROM harvest.quota_permits WHERE account_key='a' AND expires_at>now()`); n != 1 {
		t.Fatal("result pruning removed active quota")
	}

	restricted := a3Enroll(t, p, "public", "only-detail", "restricted")
	base := a3Reset(p, restricted, "restricted")
	a3Ack(p, restricted, "restricted", base.Baseline, &base.Reset)
	privateToGrant := p.register("fixture", "key", "a3-hidden-capability", "public")
	claim, payload := p.a3Claim("worker_a", w, privateToGrant, sampleOffer, observationParams{Mode: "present"})
	p.call("POST", completePath(claim), "worker_a", "", payload, nil, 200)
	p.call("GET", a3ChangesPath(restricted, base.Baseline), "restricted", "", nil, &changes, 200)
	if len(changes.Items) != 0 || changes.Next != changes.Head || changes.Next == base.Baseline {
		t.Fatal("capability filter leaked data or stalled the stream")
	}
	a3Problem(t, p, "GET", "/api/v1/observations/history?stream=public&capability=fixture.sample", "restricted", nil, 403, "forbidden")
	var status execution.Operations
	p.call("GET", "/api/v1/operations/status?namespace="+stream, "operator", "", nil, &status, 200)
	if status.Results.Stream == nil || status.Results.Stream.LastPrune == nil || status.Results.Stream.ExpiredConsumers < 2 {
		t.Fatalf("missing retention status: %+v", status.Results.Stream)
	}
	t.Log("retention preserved both pin types, active reset protection and live lease/permit; expired old cursor, deadline, changed grant and byte budget")
}

func TestA3ResultPageBytes(t *testing.T) {
	p := startFixture(t)
	p.a3Config()
	p.serve()
	w := p.a3Register("worker_a")
	for i := range 5 {
		target := p.register("fixture", "key", "byte-"+string(rune('a'+i)), "public")
		claim, payload := p.a3Claim("worker_a", w, target, sampleOffer, observationParams{Mode: "present", Padding: 450 << 10})
		p.call("POST", completePath(claim), "worker_a", "", payload, nil, 200)
	}
	path := "/api/v1/observations/current?stream=public&limit=200"
	status, body, err := p.request("GET", path, "public_reader", "", nil)
	if err != nil || status != 200 || len(body) > 2<<20 {
		t.Fatalf("result page status=%d bytes=%d err=%v", status, len(body), err)
	}
	var page execution.CurrentView
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 4 || page.After == "" {
		t.Fatalf("byte budget did not truncate page: %d items, %d bytes", len(page.Items), len(body))
	}
	after := page.After
	page = execution.CurrentView{}
	p.call("GET", path+"&after="+after, "public_reader", "", nil, &page, 200)
	if len(page.Items) != 1 || page.After != "" {
		t.Fatal("byte-budgeted keyset dropped last item")
	}
}
