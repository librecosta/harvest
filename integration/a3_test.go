//go:build integration

package integration

import (
	"bufio"
	"encoding/base64"
	json "encoding/json/v2"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/librecosta/harvest/db"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

func TestA3(t *testing.T) {
	p := newFixture(t)
	upgradeA2(t, p)
	p.a3Config()
	p.serve()
	for _, test := range []struct {
		name string
		run  func(*testing.T, *testPlane)
	}{
		{"atomic observations and fenced completion", testA3Results},
		{"serialized commit and rollback", testA3Commits},
		{"snapshot convergence with independent workers", testA3Snapshot},
		{"bounded pruning and explicit expiry", testA3Retention},
	} {
		if !t.Run(test.name, func(t *testing.T) { child := *p; child.t = t; test.run(t, &child) }) {
			break
		}
	}
}

func upgradeA2(t *testing.T, p *testPlane) {
	t.Helper()
	sqlDB := stdlib.OpenDBFromPool(p.pool)
	defer sqlDB.Close()
	m, err := goose.NewProvider(goose.DialectPostgres, sqlDB, db.Migrations, goose.WithTableName("public.goose_db_version"))
	if err != nil {
		t.Fatal(err)
	}
	if versions, err := m.UpTo(t.Context(), 2); err != nil || len(versions) != 2 {
		t.Fatalf("seed A2: %v %v", versions, err)
	}
	target, schedule, job, worker, attempt, permit := uuid.NewV7(), uuid.NewV7(), uuid.NewV7(), uuid.NewV7(), uuid.NewV7(), uuid.NewV7()
	hash := make([]byte, 32)
	a3Exec(t, p, "INSERT INTO harvest.targets (id,provider,resource_type,external_key,namespace) VALUES ($1,'fixture','key','upgrade','scheduler')", target)
	a3Exec(t, p, `INSERT INTO harvest.schedules
		(id,target_id,namespace,capability,request_schema,execution_key,params,scope,fingerprint,interval_seconds,cron,anchor_at,next_due_at,policy,enabled)
		VALUES ($1,$2,'scheduler','fixture.run',1,'fixture.run:1:1:fixture_v1','{}','{}',$3,60,'',now(),now(),'state_refresh',false)`, schedule, target, hash)
	a3Exec(t, p, `INSERT INTO harvest.jobs
		(id,target_id,namespace,capability,request_schema,execution_key,params,scope,fingerprint,cause,priority,available_at,deadline_at,state,attempt_count,generation,max_attempts)
		VALUES ($1,$2,'scheduler','fixture.run',1,'fixture.run:1:1:fixture_v1','{}','{}',$3,'manual',1,now(),now()+interval '1 day','running',1,1,2)`, job, target, hash)
	a3Exec(t, p, "INSERT INTO harvest.workers (id,principal,offers,egress_profiles,capacity,network_paths) VALUES ($1,'upgrade',ARRAY['fixture.run:1:1:fixture_v1'],ARRAY[]::text[],1,'[]')", worker)
	a3Exec(t, p, "INSERT INTO harvest.attempts (id,job_id,worker_id,generation,claimed_at,heartbeat_at,lease_until) VALUES ($1,$2,$3,1,now(),now(),now()+interval '1 day')", attempt, job, worker)
	a3Exec(t, p, "INSERT INTO harvest.quota_buckets (key,balance,refilled_at,issued) VALUES ('a3_upgrade',37,now(),11)")
	a3Exec(t, p, `INSERT INTO harvest.quota_permits (id,attempt_id,operation_id,request_hash,bucket_keys,account_key,max_bytes,expires_at)
		VALUES ($1,$2,$3,$4,ARRAY['g','p','a','n'],'a',4096,now()+interval '1 day')`, permit, attempt, uuid.NewV7(), hash)
	expectFailure(t, p.scratch, nil, p.bin, "schema is unavailable or incompatible", "fixture", "serve", "--local", "--config", p.config)
	if got := p.cli("migrate", "up", "--config", p.config); got != "Applied 2 Harvest migrations." {
		t.Fatal(got)
	}
	if got := p.cli("migrate", "status", "--config", p.config); got != "1 applied\n2 applied\n3 applied\n4 applied" {
		t.Fatal(got)
	}
	if n := a3Count(t, p, `SELECT count(*) FROM harvest.targets t JOIN harvest.schedules s ON s.target_id=t.id
		JOIN harvest.jobs j ON j.target_id=t.id JOIN harvest.attempts a ON a.job_id=j.id
		JOIN harvest.workers w ON w.id=a.worker_id JOIN harvest.quota_permits q ON q.attempt_id=a.id
		CROSS JOIN harvest.quota_buckets b WHERE t.id=$1 AND t.external_key='upgrade' AND s.id=$2 AND j.id=$3 AND
		j.state='running' AND w.id=$4 AND w.network_paths='[]' AND q.id=$5 AND q.max_bytes=4096 AND b.key='a3_upgrade' AND b.balance=37 AND b.issued=11`,
		target, schedule, job, worker, permit); n != 1 {
		t.Fatal("migration changed A2 rows")
	}
}

func a3Exec(t *testing.T, p *testPlane, sql string, args ...any) {
	t.Helper()
	if _, err := p.pool.Exec(t.Context(), sql, args...); err != nil {
		t.Fatal(err)
	}
}

func a3Count(t *testing.T, p *testPlane, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := p.pool.QueryRow(t.Context(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func a3Problem(t *testing.T, p *testPlane, method, path, who string, in any, want int, code string) {
	t.Helper()
	status, data, err := p.request(method, path, who, "", in)
	var problem struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(data, &problem)
	if err != nil || status != want || (code != "" && problem.Code != code) {
		t.Fatalf("%s %s: got %d %s (%v), wanted %d %s", method, path, status, data, err, want, code)
	}
}

func a3RejectLength(t *testing.T, p *testPlane, path, who string, length int) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", p.address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	// Let the server reject Content-Length before sending a body. Closing
	// a socket with unread body bytes can reset the peer on Linux.
	_, err = fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n", path, p.address, p.tokens[who], length)
	if err != nil {
		t.Fatal("could not write oversized request header")
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized %s returned %d", path, resp.StatusCode)
	}
}

func a3Position(t *testing.T, token string) int64 {
	t.Helper()
	data, err := base64.RawURLEncoding.DecodeString(token)
	var cursor struct {
		Position int64 `json:"position"`
	}
	if err != nil || json.Unmarshal(data, &cursor) != nil {
		t.Fatalf("bad cursor %q", token)
	}
	return cursor.Position
}

func a3ReadCurrent(t *testing.T, p *testPlane, stream string) execution.CurrentView {
	t.Helper()
	var out execution.CurrentView
	p.call("GET", "/api/v1/observations/current?stream="+stream, "operator", "", nil, &out, 200)
	return out
}

func testA3Results(t *testing.T, p *testPlane) {
	worker := p.a3Register("worker_a")
	target := p.register("fixture", "key", "a3-detail", "public")
	claim, first := p.a3Claim("worker_a", worker, target, detailOffer, observationParams{Mode: "present", Padding: 20 << 10})
	path := completePath(claim)
	bad := first
	bad.Result.Segments = append([]provider.Segment(nil), first.Result.Segments...)
	missingScope := first
	missingScope.Result.Segments = first.Result.Segments[:1]
	a3Problem(t, p, "POST", path, "worker_a", missingScope, 400, "invalid_request")
	bad.Result.Segments[1].Scope.Namespace = "private"
	a3Problem(t, p, "POST", path, "worker_a", bad, 400, "invalid_request")
	bad.Result.Segments[1] = first.Result.Segments[0]
	a3Problem(t, p, "POST", path, "worker_a", bad, 400, "invalid_request")
	bad.Result.Segments[1] = first.Result.Segments[1]
	bad.Result.Segments[1].OutputSchema = 2
	a3Problem(t, p, "POST", path, "worker_a", bad, 400, "invalid_request")
	bad.Result.Segments[1].OutputSchema = 1
	bad.Result.Segments[1].Source = "https://secret@example.test/token"
	a3Problem(t, p, "POST", path, "worker_a", bad, 400, "invalid_request")
	bad.Result.Segments[1].Source = "fixture"
	bad.Result.Segments[1].Data = []byte(`{"value":0,"unknown":true}`)
	a3Problem(t, p, "POST", path, "worker_a", bad, 400, "invalid_request")
	bad.Result.Segments[1].Data = []byte(fmt.Sprintf("{\"value\":0,\"padding\":%q}", strings.Repeat("x", provider.MaxSegmentData)))
	a3Problem(t, p, "POST", path, "worker_a", bad, 413, "result_too_large")
	a3RejectLength(t, p, path, "worker_a", provider.MaxCompletionBody+1)
	a3RejectLength(t, p, "/api/v1/jobs", "operator", execution.MaxBodySize+1)
	if a3Count(t, p, "SELECT count(*) FROM harvest.observations") != 0 || p.job(claim.JobID).Attempts[0].State != "running" {
		t.Fatal("validation mutated storage")
	}
	var receipt, replay execution.Receipt
	p.call("POST", path, "worker_a", "", first, &receipt, 200)
	first.Result.Segments[0], first.Result.Segments[1] = first.Result.Segments[1], first.Result.Segments[0]
	p.call("POST", path, "worker_a", "", first, &replay, 200)
	if receipt != replay {
		t.Fatalf("receipt changed: %+v %+v", receipt, replay)
	}
	first.Result.Segments[0].Data = []byte(`{"value":10}`)
	a3Problem(t, p, "POST", path, "worker_a", first, 409, "conflict")
	before := a3ReadCurrent(t, p, "public")
	if len(before.Items) != 2 || a3Position(t, before.Head) != 2 {
		t.Fatal("missing first results")
	}
	repeat, unchanged := p.a3Claim("worker_a", worker, target, detailOffer, observationParams{Mode: "present", Padding: 20 << 10})
	p.call("POST", completePath(repeat), "worker_a", "", unchanged, nil, 200)
	confirm, notModified := p.a3Claim("worker_a", worker, target, detailOffer, observationParams{Mode: "not_modified"})
	p.call("POST", completePath(confirm), "worker_a", "", notModified, nil, 200)
	after := a3ReadCurrent(t, p, "public")
	if after.Head != before.Head || a3Count(t, p, "SELECT count(*) FROM harvest.observations") != 2 {
		t.Fatal("confirmation duplicated detail history or changes")
	}
	for i, row := range after.Items {
		if row.ID != before.Items[i].ID || !row.ConfirmedAt.After(before.Items[i].ConfirmedAt) {
			t.Fatal("freshness did not advance")
		}
	}
	part, partial := p.a3Claim("worker_a", worker, target, detailOffer, observationParams{Mode: "partial", Value: 7})
	p.call("POST", completePath(part), "worker_a", "", partial, &receipt, 200)
	if receipt.State != "partial" || a3Count(t, p, "SELECT count(*) FROM harvest.observations WHERE state='unavailable' AND data IS NULL") != 1 {
		t.Fatal("partial result or diagnostic lost")
	}
	after = a3ReadCurrent(t, p, "public")
	var kept, changed int
	for i, row := range after.Items {
		if row.ID == before.Items[i].ID {
			kept++
		} else {
			changed++
		}
	}
	if kept != 1 || changed != 1 || a3Position(t, after.Head) != 3 {
		t.Fatal("scope failure destroyed last-known-good")
	}
	empty, verified := p.a3Claim("worker_a", worker, target, detailOffer, observationParams{Mode: "empty"})
	verified.Result.Segments[0].Evidence = ""
	a3Problem(t, p, "POST", completePath(empty), "worker_a", verified, 400, "invalid_request")
	verified.Result.Segments[0].Evidence = "complete"
	p.call("POST", completePath(empty), "worker_a", "", verified, nil, 200)
	if n := a3Count(t, p, `SELECT count(*) FROM harvest.current_results c JOIN harvest.observations o ON o.id=c.observation_id
		WHERE o.state='confirmed_empty' AND o.evidence='complete' AND o.data='{}'`); n != 2 {
		t.Fatal("confirmed empty lost")
	}
	for range 2 {
		sample, data := p.a3Claim("worker_a", worker, target, sampleOffer, observationParams{Mode: "present"})
		p.call("POST", completePath(sample), "worker_a", "", data, nil, 200)
	}
	missing, noValue := p.a3Claim("worker_a", worker, target, sampleOffer, observationParams{Mode: "missing"})
	p.call("POST", completePath(missing), "worker_a", "", noValue, nil, 200)
	p.call("POST", "/api/v1/jobs/"+missing.JobID.String()+"/cancel", "operator", "", nil, nil, 202)
	if n := a3Count(t, p, `SELECT count(*) FROM harvest.observations WHERE sample AND data->>'value'='0'`); n != 2 {
		t.Fatal("missing data manufactured a sample or repeated zero collapsed")
	}
	other := p.register("fixture", "key", "a3-without-good", "public")
	cancel, wrong := p.a3Claim("worker_a", worker, other, detailOffer, observationParams{Mode: "not_modified"})
	a3Problem(t, p, "POST", completePath(cancel), "worker_a", wrong, 409, "conflict")
	p.call("POST", "/api/v1/jobs/"+cancel.JobID.String()+"/cancel", "operator", "", nil, nil, 202)
	wrong.Result = a3Result(cancel.Task, observationParams{Mode: "present"}, time.Now())
	a3Problem(t, p, "POST", completePath(cancel), "worker_a", wrong, 409, "cancel_requested")
	wrong.Result = provider.Result{State: "cancelled", Code: "cancelled"}
	p.call("POST", completePath(cancel), "worker_a", "", wrong, nil, 200)
	stale, fenced := p.a3Claim("worker_a", worker, other, detailOffer, observationParams{Mode: "present"})
	a3Exec(t, p, "UPDATE harvest.attempts SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", stale.AttemptID)
	a3Problem(t, p, "POST", completePath(stale), "worker_a", fenced, 409, "lease_lost")
	p.call("POST", "/api/v1/jobs/"+stale.JobID.String()+"/cancel", "operator", "", nil, nil, 202)
	if a3Count(t, p, "SELECT count(*) FROM harvest.observations WHERE target_id=$1", other) != 0 {
		t.Fatal("invalid attempts published")
	}
	var history execution.HistoryView
	p.call("GET", "/api/v1/observations/history?stream=public&target="+target.String()+"&limit=1", "public_reader", "", nil, &history, 200)
	if len(history.Items) != 1 || history.After == "" {
		t.Fatal("missing bounded history")
	}
	firstHistory := history.Items[0].ID
	p.call("GET", "/api/v1/observations/history?stream=public&target="+target.String()+"&limit=1&after="+history.After, "public_reader", "", nil, &history, 200)
	if len(history.Items) != 1 || history.Items[0].ID == firstHistory {
		t.Fatal("history keyset repeated an observation")
	}
}
