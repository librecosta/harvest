//go:build integration

package integration

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

type testJob struct {
	ID          uuid.UUID  `json:"id"`
	State       string     `json:"state"`
	Code        string     `json:"code"`
	Generation  int64      `json:"generation"`
	OriginJobID *uuid.UUID `json:"origin_job_id"`
}

type testAttempt struct {
	ID          uuid.UUID `json:"id"`
	WorkerID    uuid.UUID `json:"worker_id"`
	Generation  int64     `json:"generation"`
	State       string    `json:"state"`
	ClaimedAt   time.Time `json:"claimed_at"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
	LeaseUntil  time.Time `json:"lease_until"`
}

type jobDetail struct {
	Job      testJob       `json:"job"`
	Attempts []testAttempt `json:"attempts"`
	Waiting  string        `json:"waiting_reason"`
}

func requestFor(t *testing.T, id uuid.UUID, label, mode string, delay int32) execution.Request {
	t.Helper()
	params, err := json.Marshal(fixtureParams{label, delay, mode})
	if err != nil {
		t.Fatal(err)
	}
	return execution.Request{TargetID: id, Capability: fixtureOffer.Capability, RequestSchema: 1, Params: params}
}

func (p *testPlane) submit(key string, req execution.Request) testJob {
	p.t.Helper()
	var job testJob
	p.call("POST", "/api/v1/jobs", "operator", key, execution.Submit{Request: &req}, &job, 202)
	if !execution.UUIDv7(job.ID) {
		p.t.Fatalf("job ID is not UUIDv7: %s", job.ID)
	}
	return job
}

func (p *testPlane) job(id uuid.UUID) jobDetail {
	p.t.Helper()
	var job jobDetail
	p.call("GET", "/api/v1/jobs/"+id.String(), "operator", "", nil, &job, 200)
	return job
}

func (p *testPlane) waitJob(id uuid.UUID, description string, timeout time.Duration, want func(jobDetail) bool) jobDetail {
	p.t.Helper()
	var detail jobDetail
	eventually(p.t, timeout, description, func() bool {
		detail = p.job(id)
		return want(detail)
	})
	return detail
}

func (p *testPlane) registerWireWorker(who string) uuid.UUID {
	p.t.Helper()
	reg := execution.Registration{ID: uuid.NewV7(), Offers: []provider.Offer{fixtureOffer}, Capacity: 1}
	p.call("POST", "/internal/v1/workers/register", who, "", reg, nil, 204)
	return reg.ID
}

func TestA1(t *testing.T) {
	p := startFixture(t)
	t.Log("Fresh PG18 migration, anchored interval and UTC cron on concurrent database sessions")
	testSchedules(t, p)
	server := p.serve()

	t.Log("Canonical identities, scoped authentication, typed requests and idempotent submissions")
	target, private := testRegistryAndAuth(t, p)
	dedupe := requestFor(t, target, "dedupe", "success", 0)
	first := p.submit("dedupe-a", dedupe)
	reordered := dedupe
	reordered.Params = jsontext.Value(`{ "mode": "success", "label": "dedupe", "delay_ms": 0 }`)
	if next := p.submit("dedupe-b", reordered); next.ID != first.ID {
		t.Fatal("equivalent canonical requests created separate active jobs")
	}
	ids := make(chan uuid.UUID, 8)
	var submissions sync.WaitGroup
	for range 8 {
		submissions.Go(func() {
			status, data, err := p.request("POST", "/api/v1/jobs", "operator", "concurrent-dedupe", execution.Submit{Request: &reordered})
			var j testJob
			if err != nil || status != 202 || json.Unmarshal(data, &j) != nil {
				ids <- uuid.Nil()
				return
			}
			ids <- j.ID
		})
	}
	submissions.Wait()
	close(ids)
	for id := range ids {
		if id != first.ID {
			t.Fatal("concurrent idempotent submissions diverged")
		}
	}
	if detail := p.job(first.ID); detail.Waiting != "no_eligible_worker" {
		t.Fatalf("missing blocked-job diagnostic: %q", detail.Waiting)
	}
	reordered.Scope.Region = "CN"
	p.call("POST", "/api/v1/jobs", "operator", "dedupe-a", execution.Submit{Request: &reordered}, nil, 409)
	scoped := p.submit("different-scope", reordered)
	priv := p.submit("private-dedupe", requestFor(t, private, "dedupe", "success", 0))
	if first.ID == scoped.ID || first.ID == priv.ID {
		t.Fatal("active dedupe crossed a scope or namespace")
	}
	for _, job := range []testJob{first, scoped, priv} {
		p.call("POST", "/api/v1/jobs/"+job.ID.String()+"/cancel", "operator", "", nil, nil, 202)
	}
	if replay := p.submit("dedupe-b", dedupe); replay.ID != first.ID || replay.State != "cancelled" {
		t.Fatal("idempotency did not survive terminal cancellation")
	}

	t.Log("Two independent Workers contend, renew, recover after process crashes and replay receipts")
	children := map[string]*child{"worker_a": p.startWorker("worker_a"), "worker_b": p.startWorker("worker_b")}
	eventually(t, 10*time.Second, "both Worker registrations", func() bool {
		var ops execution.Operations
		p.call("GET", "/api/v1/operations/status?namespace=public", "operator", "", nil, &ops, 200)
		return len(ops.Workers) == 2
	})
	p.cli("healthcheck", "--worker", "--config", filepath.Join(p.scratch, "worker_a.yaml"))
	measureIdle(t, p, server, children)
	crash := p.submit("crash", requestFor(t, target, "crash", "success", 6500))
	running := p.waitJob(crash.ID, "one renewed lease", 12*time.Second, func(d jobDetail) bool {
		return d.Job.State == "running" && len(d.Attempts) == 1 && d.Attempts[0].HeartbeatAt.After(d.Attempts[0].ClaimedAt)
	})
	lost := running.Attempts[0]
	var owner string
	if err := p.pool.QueryRow(t.Context(), "SELECT principal FROM harvest.workers WHERE id = $1", lost.WorkerID).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	winner := children[owner]
	if winner == nil {
		t.Fatal("lease was not owned by one of the two subprocesses")
	}
	winner.kill(t)
	delete(children, owner)
	server.kill(t)
	server = p.serve()
	takeover := p.waitJob(crash.ID, "takeover by second Worker", 28*time.Second, func(d jobDetail) bool {
		return len(d.Attempts) == 2 && d.Job.State == "running"
	})
	active := takeover.Attempts[1]
	if takeover.Attempts[0].State != "expired" || lost.WorkerID == active.WorkerID || active.Generation != lost.Generation+1 {
		t.Fatalf("incorrect fenced takeover: %+v", takeover.Attempts)
	}
	stale := execution.Fence{JobID: crash.ID, WorkerID: lost.WorkerID, Generation: lost.Generation}
	p.call("POST", "/internal/v1/attempts/"+lost.ID.String()+"/renew", owner, "", stale, nil, 409)
	success := provider.Result{State: "succeeded", Data: jsontext.Value(`{"count":1,"label":"crash"}`)}
	p.call("POST", "/internal/v1/attempts/"+lost.ID.String()+"/complete", owner, "",
		execution.Completion{Fence: stale, OutputSchema: 1, Result: success}, nil, 409)
	terminal := p.waitJob(crash.ID, "durable success", 13*time.Second, func(d jobDetail) bool { return d.Job.State == "succeeded" })
	if len(terminal.Attempts) != 2 || terminal.Attempts[1].State != "succeeded" {
		t.Fatalf("unexpected attempt history: %+v", terminal)
	}
	var other string
	for name := range children {
		other = name
	}
	completeURL := "/internal/v1/attempts/" + active.ID.String() + "/complete"
	complete := execution.Completion{
		Fence:        execution.Fence{JobID: crash.ID, WorkerID: active.WorkerID, Generation: active.Generation},
		OutputSchema: 1, Result: success,
	}
	var receipt, replay execution.Receipt
	p.call("POST", completeURL, other, "", complete, &receipt, 200)
	p.call("POST", completeURL, other, "", complete, &replay, 200)
	if receipt != replay || receipt.AttemptID != active.ID || receipt.State != "succeeded" {
		t.Fatal("accepted attempt did not replay a stable receipt")
	}
	complete.Result.Data = jsontext.Value(`{"count":2,"label":"crash"}`)
	p.call("POST", completeURL, other, "", complete, nil, 409)

	// This leased cancellation deliberately receives no Worker acknowledgement.
	orphan := p.submit("orphan-cancel", requestFor(t, private, "orphan", "success", 0))
	orphanWorker := p.registerWireWorker("private_worker")
	var orphanClaim execution.Claim
	p.call("POST", "/internal/v1/workers/"+orphanWorker.String()+"/claim", "private_worker", "", nil, &orphanClaim, 200)
	if orphanClaim.JobID != orphan.ID {
		t.Fatal("the cancellation recovery fixture did not own its lease")
	}
	p.call("POST", "/api/v1/jobs/"+orphan.ID.String()+"/cancel", "operator", "", nil, nil, 202)

	t.Log("Retry budget, partial result, and immutable operator retry lineage")
	failed := p.submit("automatic-retry", requestFor(t, target, "retry", "retry", 0))
	history := p.waitJob(failed.ID, "three bounded attempts", 18*time.Second, func(d jobDetail) bool { return d.Job.State == "failed" })
	if len(history.Attempts) != 3 || history.Job.Code != "upstream_unavailable" ||
		history.Attempts[1].ClaimedAt.Sub(*attemptEnd(t, p, history.Attempts[0].ID)) < time.Second ||
		history.Attempts[2].ClaimedAt.Sub(*attemptEnd(t, p, history.Attempts[1].ID)) < 2*time.Second {
		t.Fatalf("retry budget or backoff mismatch: %+v", history)
	}
	var retry testJob
	p.call("POST", "/api/v1/jobs/"+failed.ID.String()+"/retry", "operator", "operator-retry", nil, &retry, 202)
	if retry.ID == failed.ID || retry.OriginJobID == nil || *retry.OriginJobID != failed.ID {
		t.Fatal("operator retry changed or lost the original job")
	}
	p.call("POST", "/api/v1/jobs/"+retry.ID.String()+"/cancel", "operator", "", nil, nil, 202)
	partial := p.submit("partial", requestFor(t, target, "partial", "partial", 0))
	p.waitJob(partial.ID, "typed partial result", 12*time.Second, func(d jobDetail) bool { return d.Job.State == "partial" })
	runningCancel := p.submit("running-cancel", requestFor(t, target, "cancel", "success", 10000))
	p.waitJob(runningCancel.ID, "running cancellation fixture", 12*time.Second, func(d jobDetail) bool { return d.Job.State == "running" })
	p.call("POST", "/api/v1/jobs/"+runningCancel.ID.String()+"/cancel", "operator", "", nil, nil, 202)
	cancelled := p.waitJob(runningCancel.ID, "Worker cancellation acknowledgement", 8*time.Second, func(d jobDetail) bool {
		return d.Job.State == "cancelled" && len(d.Attempts) == 1
	})
	if !attemptEnd(t, p, cancelled.Attempts[0].ID).Before(cancelled.Attempts[0].LeaseUntil) {
		t.Fatal("cancellation waited for lease recovery instead of Worker acknowledgement")
	}
	embedded := p.register("fixture", "key", "local", "embedded")
	local := p.submit("embedded", requestFor(t, embedded, "local", "success", 0))
	p.waitJob(local.ID, "embedded Worker through fenced HTTP protocol", 12*time.Second, func(d jobDetail) bool { return d.Job.State == "succeeded" })

	t.Log("Cancel versus complete under concurrent Job row locks")
	testCancellation(t, p, private)
	p.waitJob(orphan.ID, "unacknowledged cancellation after expiry", 15*time.Second, func(d jobDetail) bool {
		return d.Job.State == "cancelled" && len(d.Attempts) == 1 && d.Attempts[0].State == "cancelled"
	})
	t.Log("Pre-drain leaves the result path available to an active remote Worker")
	drain := p.submit("predrain", requestFor(t, target, "drain", "success", 3600))
	p.waitJob(drain.ID, "active pre-drain attempt", 12*time.Second, func(d jobDetail) bool { return d.Job.State == "running" })
	p.cli("stop")
	waitProbe(t, p.address, "/readyz", 503, "unavailable")
	p.call("POST", "/api/v1/jobs", "operator", "draining", execution.Submit{Request: &dedupe}, nil, 503)
	server.waitClean(t)
	server = p.serve()
	if detail := p.job(drain.ID); detail.Job.State != "succeeded" || len(detail.Attempts) != 1 {
		t.Fatalf("pre-drain did not preserve in-flight result: %+v", detail)
	}
	if runtime.GOOS != "windows" {
		signalJob := p.submit("worker-sigterm", requestFor(t, target, "worker-sigterm", "success", 2500))
		p.waitJob(signalJob.ID, "active Worker SIGTERM fixture", 12*time.Second, func(d jobDetail) bool {
			return d.Job.State == "running"
		})
		if err := children[other].cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		children[other].waitClean(t)
		if done := p.job(signalJob.ID); done.Job.State != "succeeded" || len(done.Attempts) != 1 {
			t.Fatalf("Worker SIGTERM abandoned its active attempt: %+v", done)
		}
	}
	p.cli("stop")
	server.waitClean(t)
	measurePlans(t, p)
}

func attemptEnd(t *testing.T, p *testPlane, id uuid.UUID) *time.Time {
	t.Helper()
	var end *time.Time
	if err := p.pool.QueryRow(t.Context(), "SELECT ended_at FROM harvest.attempts WHERE id = $1", id).Scan(&end); err != nil || end == nil {
		t.Fatalf("attempt end unavailable: %v", err)
	}
	return end
}

func testRegistryAndAuth(t *testing.T, p *testPlane) (uuid.UUID, uuid.UUID) {
	t.Helper()
	p.call("GET", "/api/v1/providers", "", "", nil, nil, 401)
	p.call("GET", "/api/v1/providers", "worker_a", "", nil, nil, 403)
	var providers []provider.Description
	p.call("GET", "/api/v1/providers", "operator", "", nil, &providers, 200)
	executable := 0
	for _, description := range providers {
		for _, cap := range description.Capabilities {
			if cap.Status == "executable" {
				executable++
				if cap.Capability != fixtureOffer.Capability {
					t.Fatal("unimplemented provider advertised execution")
				}
			}
		}
	}
	if executable != 1 {
		t.Fatal("fixture did not advertise its executable offer")
	}
	steam := p.register("steam", "app", "000570", "public")
	if p.register("steam", "app", "570", "public") != steam {
		t.Fatal("Steam canonical target identity duplicated")
	}
	if p.register("steam", "app", "570", "private") == steam {
		t.Fatal("private Steam target coalesced with a public target")
	}
	domain := p.register("web", "domain", "WWW.Bücher.DE.", "public")
	if p.register("web", "domain", "www.xn--bcher-kva.de", "public") != domain ||
		p.register("web", "origin", "https://www.bücher.de:443", "public") == domain {
		t.Fatal("web resource canonicalization or type isolation failed")
	}
	p.call("POST", "/api/v1/targets", "operator", "", execution.TargetInput{
		Provider: "steam", ResourceType: "app", ExternalKey: "4294967296", Namespace: "public",
	}, nil, 400)
	unsupported := execution.Request{TargetID: steam, Capability: "steam.players", RequestSchema: 1, Params: jsontext.Value("{}")}
	p.call("POST", "/api/v1/jobs", "operator", "no-provider", execution.Submit{Request: &unsupported}, nil, 409)
	target, private := p.register("fixture", "key", "ALPHA", "public"), p.register("fixture", "key", "alpha", "private")
	p.call("GET", "/api/v1/targets/"+private.String(), "public_reader", "", nil, nil, 404)
	p.call("GET", "/api/v1/jobs?namespace=private", "public_reader", "", nil, nil, 403)
	p.call("POST", "/api/v1/targets", "public_reader", "", execution.TargetInput{}, nil, 403)
	p.call("GET", "/api/v1/operations/status?namespace=public", "public_reader", "", nil, nil, 403)
	p.call("GET", "/api/v1/jobs?namespace=public&limit=201", "public_reader", "", nil, nil, 400)
	bad := requestFor(t, target, "bad", "success", 0)
	bad.Params = jsontext.Value(`{"label":"bad","mode":"success","secret":true}`)
	p.call("POST", "/api/v1/jobs", "operator", "bad-params", execution.Submit{Request: &bad}, nil, 400)
	reg := execution.Registration{ID: uuid.NewV7(), Offers: []provider.Offer{fixtureOffer}, Capacity: 1}
	p.call("POST", "/internal/v1/workers/register", "", "", reg, nil, 401)
	p.call("POST", "/internal/v1/workers/register", "operator", "", reg, nil, 403)
	reg.Offers[0].OutputSchema++
	p.call("POST", "/internal/v1/workers/register", "worker_a", "", reg, nil, 409)
	reg.Offers[0] = fixtureOffer
	reg.EgressProfiles = []string{"ungranted"}
	p.call("POST", "/internal/v1/workers/register", "worker_a", "", reg, nil, 403)
	return target, private
}

func testSchedules(t *testing.T, p *testPlane) {
	t.Helper()
	ctx := t.Context()
	registry := fixtureRegistry()
	e := execution.New(p.pool, registry, nil, config.DefaultRetention())
	second := execution.New(p.pool, registry, nil, config.DefaultRetention())
	auth := config.Principal{Name: "schedule_test", Role: "operator", Namespaces: []string{"scheduler"}}
	target, err := e.RegisterTarget(ctx, auth, execution.TargetInput{Provider: "fixture", ResourceType: "key", ExternalKey: "calendar", Namespace: "scheduler"})
	if err != nil {
		t.Fatal(err)
	}
	now, err := q.New(p.pool).DBNow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	anchor := now.UTC().Truncate(time.Second).Add(-3*time.Hour - 12*time.Second)
	refresh := execution.ScheduleInput{
		Request:    requestFor(t, target.ID, "refresh", "success", 0),
		Recurrence: execution.Recurrence{AnchorAt: anchor, IntervalSeconds: 3600, Policy: "state_refresh"},
	}
	s, err := e.CreateSchedule(ctx, auth, refresh)
	if err != nil {
		t.Fatal(err)
	}
	if copy, err := e.CreateSchedule(ctx, auth, refresh); err != nil || copy.ID != s.ID {
		t.Fatalf("schedule create replay failed: %v", err)
	}
	slot := now.UTC().Truncate(time.Minute).Add(-2 * time.Minute)
	point, err := e.CreateSchedule(ctx, auth, execution.ScheduleInput{
		Request:    requestFor(t, target.ID, "point", "success", 0),
		Recurrence: execution.Recurrence{AnchorAt: slot.Add(-48 * time.Hour), Cron: fmt.Sprintf("%d %d * * *", slot.Minute(), slot.Hour()), Policy: "point_in_time"},
	})
	if err != nil {
		t.Fatal(err)
	}
	reconciled := make(chan error, 2)
	go func() { reconciled <- e.Reconcile(ctx) }()
	go func() { reconciled <- second.Reconcile(ctx) }()
	for range 2 {
		if err := <-reconciled; err != nil {
			t.Fatal(err)
		}
	}
	e = execution.New(p.pool, registry, nil, config.DefaultRetention())
	if err := e.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	s, err = e.Schedule(ctx, auth, s.ID)
	if err != nil || !s.NextDueAt.Equal(anchor.Add(4*time.Hour)) || s.MissedCount != 3 {
		t.Fatalf("state refresh did not advance phase once: %+v %v", s, err)
	}
	point, err = e.Schedule(ctx, auth, point.ID)
	if err != nil || !point.NextDueAt.Equal(slot.Add(24*time.Hour)) || point.MissedCount != 3 {
		t.Fatalf("point-in-time cron did not count missed coverage: %+v %v", point, err)
	}
	jobs, err := e.Jobs(ctx, auth, execution.Page{Namespace: "scheduler", Size: 50})
	if err != nil || len(jobs) != 1 || jobs[0].ScheduledFor == nil || !jobs[0].ScheduledFor.Equal(anchor.Add(3*time.Hour)) {
		t.Fatalf("unexpected scheduled job lineage: %+v %v", jobs, err)
	}
	manual, err := e.SubmitJob(ctx, auth, "manual", execution.Submit{ScheduleID: &s.ID})
	if err != nil || manual.Cause != "manual" || manual.ScheduleID == nil || *manual.ScheduleID != s.ID {
		t.Fatalf("run now did not retain schedule lineage: %+v %v", manual, err)
	}
	enabled := false
	paused, err := e.EditSchedule(ctx, auth, s.ID, execution.SchedulePatch{Revision: s.Revision, Enabled: &enabled})
	if err != nil || paused.Enabled || !paused.NextDueAt.Equal(s.NextDueAt) || paused.Revision != s.Revision+1 {
		t.Fatalf("pause or Run Now changed phase incorrectly: %+v %v", paused, err)
	}
	if _, err := e.EditSchedule(ctx, auth, s.ID, execution.SchedulePatch{Revision: s.Revision, Enabled: &enabled}); err != execution.ErrConflict {
		t.Fatalf("stale schedule revision accepted: %v", err)
	}
	if saved, _, err := e.Job(ctx, auth, jobs[0].ID); err != nil || saved.State != "queued" {
		t.Fatalf("pausing schedule changed existing job: %+v %v", saved, err)
	}
}

func testCancellation(t *testing.T, p *testPlane, target uuid.UUID) {
	t.Helper()
	worker := p.registerWireWorker("private_worker")
	claimPath := "/internal/v1/workers/" + worker.String() + "/claim"
	publicWorker := p.registerWireWorker("worker_a")
	for n := range 6 {
		job := p.submit(fmt.Sprintf("cancel-%d", n), requestFor(t, target, fmt.Sprintf("cancel-%d", n), "success", 0))
		p.call("POST", "/internal/v1/workers/"+publicWorker.String()+"/claim", "worker_a", "", nil, nil, 204)
		p.call("POST", claimPath, "worker_a", "", nil, nil, 404)
		var claim execution.Claim
		p.call("POST", claimPath, "private_worker", "", nil, &claim, 200)
		if claim.JobID != job.ID {
			t.Fatal("private Worker claimed another namespace or job")
		}
		path := "/internal/v1/attempts/" + claim.AttemptID.String() + "/complete"
		if n == 0 {
			next := p.submit("capacity-wait", requestFor(t, target, "capacity", "success", 0))
			p.call("POST", claimPath, "private_worker", "", nil, nil, 204)
			p.call("POST", "/api/v1/jobs/"+next.ID.String()+"/cancel", "operator", "", nil, nil, 202)
		}
		cancelPath := "/api/v1/jobs/" + job.ID.String() + "/cancel"
		complete := execution.Completion{Fence: claim.Fence, OutputSchema: 1,
			Result: provider.Result{State: "succeeded", Data: jsontext.Value(`{"label":"race","count":1}`)}}
		var cancel, finish int
		var err1, err2 error
		var race sync.WaitGroup
		race.Go(func() { cancel, _, err1 = p.request("POST", cancelPath, "operator", "", nil) })
		race.Go(func() { finish, _, err2 = p.request("POST", path, "private_worker", "", complete) })
		race.Wait()
		if err1 != nil || err2 != nil {
			t.Fatalf("cancel/finish transport: %v %v", err1, err2)
		}
		switch {
		case cancel == 202 && finish == 409:
			ack := complete
			ack.Result = provider.Result{State: "cancelled", Code: "cancelled"}
			p.call("POST", path, "private_worker", "", ack, nil, 200)
			p.call("POST", path, "private_worker", "", complete, nil, 409)
			if detail := p.job(job.ID); detail.Job.State != "cancelled" || detail.Attempts[0].State != "cancelled" {
				t.Fatalf("cancel winner changed state: %+v", detail)
			}
		case cancel == 409 && finish == 200:
			if detail := p.job(job.ID); detail.Job.State != "succeeded" || detail.Attempts[0].State != "succeeded" {
				t.Fatalf("completion winner changed state: %+v", detail)
			}
		default:
			t.Fatalf("ambiguous race decision: cancel=%d complete=%d", cancel, finish)
		}
	}
}
