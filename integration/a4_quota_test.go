//go:build integration

package integration

import (
	"context"
	json "encoding/json/v2"
	"testing"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

func a4LiveQuota(t *testing.T, p *testPlane, lab *networkLab, submit networkSubmit, cooldown time.Time) {
	t.Helper()
	const who = "net_wire_worker"
	reg := execution.Registration{ID: uuid.NewV7(), Offers: []provider.Offer{networkOffer}, Capacity: 1, Paths: []egress.Advertisement{
		{Profile: "local", Mode: egress.Direct, ID: egress.Direct, Kind: egress.Direct, Region: "cn", State: "healthy", Capacity: 1},
	}}
	p.call("POST", "/internal/v1/workers/register", who, "", reg, nil, 204)
	wire := submit("net_wire", "a4_wire", "ok", "cn")
	var claim execution.Claim
	p.call("POST", "/internal/v1/workers/"+reg.ID.String()+"/claim", who, "", nil, &claim, 200)
	if claim.JobID != wire.ID || claim.Network == nil {
		t.Fatal("missing wire claim")
	}
	permitPath := "/internal/v1/attempts/" + claim.AttemptID.String() + "/permit"
	in := execution.PermitRequest{Fence: claim.Fence, Intent: egress.Intent{
		OperationID: uuid.NewV7(), Profile: "local", PathID: egress.Direct, Origin: 0, Method: "GET", MaxBytes: 1024,
	}}
	var grant, duplicate egress.Grant
	p.call("POST", permitPath, who, "", in, &grant, 200)
	if !execution.UUIDv7(grant.ID) {
		t.Fatal("initial wire permit denied")
	}

	var before time.Time
	if err := p.pool.QueryRow(t.Context(), `UPDATE harvest.quota_buckets SET refilled_at=clock_timestamp()-interval '1 second'
		WHERE key='net_wire' RETURNING refilled_at`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	edit := a4Read(p)
	routing := edit.RoutingRevision
	cfg := a4Quota(edit.Policy, "net_wire")
	cfg.Burst, cfg.PerMinute, cfg.Concurrency = 10, 60, 2
	a4Quota(edit.Policy, "net403").Burst = 2
	shared := a4Quota(edit.Policy, "net_shared")
	shared.Burst, shared.PerMinute, shared.Concurrency = 4, 1200, 2
	start := time.Now()
	applied := a4Put(p, edit)
	t.Logf("numeric policy PUT with quota settlement: %v", time.Since(start))
	if applied.RoutingRevision != routing || applied.Revision != edit.Revision+1 {
		t.Fatal("numeric policy fenced compatible attempts")
	}
	var balance, issued int64
	var settled time.Time
	if err := p.pool.QueryRow(t.Context(), "SELECT balance,refilled_at,issued FROM harvest.quota_buckets WHERE key='net_wire'").Scan(&balance, &settled, &issued); err != nil {
		t.Fatal(err)
	}
	// The old rate was one token per minute, or one balance unit per µs.
	if balance != settled.Sub(before).Microseconds() || issued != 1 || balance >= 60_000_000 {
		t.Fatalf("old-rate settlement gifted balance: balance=%d elapsed=%v issued=%d", balance, settled.Sub(before), issued)
	}
	var stillCooling time.Time
	if err := p.pool.QueryRow(t.Context(), "SELECT cooldown_until FROM harvest.quota_buckets WHERE key='net403' AND issued=11 AND refused=3").Scan(&stillCooling); err != nil || !stillCooling.Equal(cooldown) {
		t.Fatalf("update reset cooldown or daily counters: %v %v", stillCooling, err)
	}
	p.call("POST", permitPath, who, "", in, &duplicate, 200)
	if duplicate.ID != grant.ID || !duplicate.ExpiresAt.Equal(grant.ExpiresAt) {
		t.Fatal("numeric update changed permit replay")
	}
	assertPermits(t, p, wire, 1)
	finish := execution.PermitFinish{Fence: claim.Fence, AttemptID: claim.AttemptID, Usage: egress.Usage{Outcome: "success", Bytes: 1}}
	first := submit("net_shared", "a4_hold_first", "held", "cn")
	second := submit("net_shared", "a4_hold_second", "held", "cn")
	eventually(t, 8*time.Second, "two independent Workers with held source responses", func() bool {
		return len(lab.hit("a4_hold_first")) == 1 && len(lab.hit("a4_hold_second")) == 1 &&
			a3Count(t, p, `SELECT count(DISTINCT w.principal) FROM harvest.quota_permits p
			JOIN harvest.attempts a ON a.id=p.attempt_id JOIN harvest.workers w ON w.id=a.worker_id
			WHERE p.account_key='net_shared' AND p.finished_at IS NULL AND p.expires_at>clock_timestamp()`) == 2
	})
	// Prepare a full shared balance to exercise clamping, and guarantee the
	// next wire operation is limited by global concurrency rather than its rate.
	a3Exec(t, p, "UPDATE harvest.quota_buckets SET balance=240000000,refilled_at=clock_timestamp() WHERE key IN ('net_shared','net_wire')")
	lower := a4Read(p)
	global := a4Quota(lower.Policy, "global_fixture")
	global.Burst, global.PerMinute, global.Concurrency = 2, 60, 1
	s := a4Quota(lower.Policy, "net_shared")
	s.Burst, s.PerMinute, s.Concurrency = 1, 15, 1
	in.OperationID = uuid.NewV7()
	lower, blocked := a4RaceLowering(t, p, lower, permitPath, who, in, reg, grant.ID, finish)
	if lower.RoutingRevision != routing || a3Count(t, p, "SELECT balance FROM harvest.quota_buckets WHERE key='net_shared'") != 60_000_000 {
		t.Fatal("burst decrease did not clamp existing balance")
	}
	if blocked.ID != uuid.Nil() || blocked.RetryMS < 1 ||
		a3Count(t, p, `SELECT count(*) FROM harvest.quota_permits WHERE account_key='net_shared'
			AND finished_at IS NULL AND expires_at>clock_timestamp()`) != 2 {
		t.Fatal("new concurrency ceiling overspent or revoked previously issued requests")
	}
	assertPermits(t, p, wire, 1)
	close(lab.releaseHeld)
	for _, job := range []testJob{first, second} {
		a4Succeeded(t, p, job)
		assertPermits(t, p, job, 1)
	}
	p.call("POST", permitPath, who, "", in, &grant, 200)
	if !execution.UUIDv7(grant.ID) {
		t.Fatal("compatible old claim could not spend under new ceiling")
	}
	p.call("POST", "/internal/v1/permits/"+grant.ID.String()+"/finish", who, "", finish, nil, 204)
	complete := execution.Completion{Fence: claim.Fence, OutputSchema: 1, Result: provider.Result{State: "succeeded", Data: []byte(`{"label":"a4_wire","count":2}`)}}
	p.call("POST", completePath(claim), who, "", complete, nil, 200)
	assertPermits(t, p, wire, 2)
	t.Log("two old active requests finished after the reduction, new permit deferred, and compatible claim resumed under new limits")
}

func a4RaceLowering(t *testing.T, p *testPlane, edit execution.PolicyView, path, who string, in execution.PermitRequest, reg execution.Registration, oldPermit uuid.UUID, finish execution.PermitFinish) (execution.PolicyView, egress.Grant) {
	t.Helper()
	const lock = int64(90404)
	conn, err := p.pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(t.Context(), "SELECT pg_advisory_lock($1)", lock); err != nil {
		t.Fatal(err)
	}
	defer conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", lock)
	a3Exec(t, p, `CREATE FUNCTION harvest.a4_pause() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_advisory_xact_lock(90404::bigint); RETURN NEW; END $$`)
	a3Exec(t, p, `CREATE TRIGGER a4_pause AFTER UPDATE ON harvest.operational_policies
		FOR EACH ROW EXECUTE FUNCTION harvest.a4_pause()`)
	type response struct {
		status int
		data   []byte
		err    error
	}
	updated, permit := make(chan response, 1), make(chan response, 1)
	go func() {
		s, b, err := p.request("PUT", a4PolicyPath, "operator", "", execution.PolicyUpdate{ExpectedRevision: edit.Revision, Policy: edit.Policy})
		updated <- response{s, b, err}
	}()
	eventually(t, time.Second, "policy SQL paused after quota settlement", func() bool {
		return a3Count(t, p, "SELECT count(*) FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND wait_event='advisory'", int32(conn.Conn().PgConn().PID())) == 1
	})
	go func() {
		s, b, err := p.request("POST", path, who, "", in)
		permit <- response{s, b, err}
	}()
	var read execution.PolicyView
	var ops execution.Operations
	calls := []struct {
		method, path, principal string
		in, out                 any
		want                    int
	}{
		{"GET", a4PolicyPath, "operator", nil, &read, 200},
		{"GET", "/api/v1/operations/status?namespace=net_wire", "operator", nil, &ops, 200},
		{"POST", "/internal/v1/workers/register", who, reg, nil, 204},
		{"POST", "/internal/v1/workers/" + reg.ID.String() + "/heartbeat", who, execution.Heartbeat{Paths: reg.Paths}, nil, 200},
		{"POST", "/internal/v1/workers/" + reg.ID.String() + "/claim", who, nil, nil, 204},
		{"POST", "/internal/v1/permits/" + oldPermit.String() + "/finish", who, finish, nil, 204},
	}
	concurrent := make([]chan response, len(calls))
	for i, call := range calls {
		concurrent[i] = make(chan response, 1)
		go func() {
			s, b, err := p.request(call.method, call.path, call.principal, "", call.in)
			concurrent[i] <- response{s, b, err}
		}()
	}
	select {
	case result := <-permit:
		t.Fatalf("permit escaped uncommitted policy barrier: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := conn.Exec(t.Context(), "SELECT pg_advisory_unlock($1)", lock); err != nil {
		t.Fatal(err)
	}
	var view execution.PolicyView
	var grant egress.Grant
	for _, res := range []struct {
		reply response
		out   any
	}{{<-updated, &view}, {<-permit, &grant}} {
		if res.reply.err != nil || res.reply.status != 200 || json.Unmarshal(res.reply.data, res.out) != nil {
			t.Fatalf("racing update/permit returned %d: %s %v", res.reply.status, res.reply.data, res.reply.err)
		}
	}
	for i, call := range calls {
		reply := <-concurrent[i]
		if reply.err != nil || reply.status != call.want || (call.out != nil && json.Unmarshal(reply.data, call.out) != nil) {
			t.Fatalf("concurrent %s %s returned %d: %s %v", call.method, call.path, reply.status, reply.data, reply.err)
		}
	}
	if read.PolicyStatus != view.PolicyStatus || ops.NetworkPolicy != view.PolicyStatus {
		t.Fatal("post-commit policy and metrics readers observed different versions")
	}
	a3Exec(t, p, "DROP TRIGGER a4_pause ON harvest.operational_policies")
	a3Exec(t, p, "DROP FUNCTION harvest.a4_pause()")
	return view, grant
}
