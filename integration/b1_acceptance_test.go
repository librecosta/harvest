//go:build integration

package integration

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

type b1Submit func(uuid.UUID, string, string) testJob
type b1Wait func(testJob, string, string) jobDetail
type b1Target func(string, string) uuid.UUID

func testB1PermitFence(t *testing.T, p *testPlane, target, other uuid.UUID) {
	t.Helper()
	worker := uuid.NewV7()
	p.call("POST", "/internal/v1/workers/register", "web_wire", "", execution.Registration{
		ID: worker, Offers: []provider.Offer{b1Offers[0]}, Capacity: 1,
		Paths: []egress.Advertisement{{Profile: "local", Mode: "direct", ID: "direct", Kind: "direct", Region: "cn", State: "healthy", Capacity: 1}},
	}, nil, 204)
	job := p.submit(uuid.NewV7().String(), b1Request(target, "web.http", "{}"))
	var claim execution.Claim
	p.call("POST", "/internal/v1/workers/"+worker.String()+"/claim", "web_wire", "", nil, &claim, 200)
	if claim.JobID != job.ID {
		t.Fatal("wrong wire claim")
	}
	path := "/internal/v1/attempts/" + claim.AttemptID.String() + "/permit"
	base := egress.Intent{OperationID: uuid.NewV7(), Profile: "local", PathID: "direct", Method: "GET", MaxBytes: 512 << 10,
		TargetID: target, Destination: "https://wire.example.com", RequestURI: "/"}
	for _, change := range []func(*egress.Intent){
		func(i *egress.Intent) { i.TargetID = other },
		func(i *egress.Intent) { i.Destination = "https://other.example.com" },
		func(i *egress.Intent) { i.Destination = "https://wire.example.com:443" },
		func(i *egress.Intent) { i.RequestURI = "/arbitrary" },
		func(i *egress.Intent) { i.RequestURI = "/?arbitrary=1" },
		func(i *egress.Intent) { i.RequestURI = "/arbitrary"; i.Previous = uuid.NewV7() },
		func(i *egress.Intent) { i.Method = "HEAD" },
	} {
		intent := base
		change(&intent)
		p.call("POST", path, "web_wire", "", execution.PermitRequest{Fence: claim.Fence, Intent: intent}, nil, 403)
	}
	assertPermits(t, p, job, 0)
	var grant egress.Grant
	p.call("POST", path, "web_wire", "", execution.PermitRequest{Fence: claim.Fence, Intent: base}, &grant, 200)
	p.call("POST", "/internal/v1/permits/"+grant.ID.String()+"/finish", "web_wire", "", execution.PermitFinish{
		Fence: claim.Fence, AttemptID: claim.AttemptID, Usage: egress.Usage{Outcome: "success", RedirectDigest: egress.RedirectDigest("/reported")},
	}, nil, 204)
	redirect := base
	redirect.OperationID, redirect.Previous, redirect.RequestURI = uuid.NewV7(), grant.ID, "/forged"
	p.call("POST", path, "web_wire", "", execution.PermitRequest{Fence: claim.Fence, Intent: redirect}, nil, 403)
	redirect.RequestURI = "/reported"
	var first, replay egress.Grant
	body := execution.PermitRequest{Fence: claim.Fence, Intent: redirect}
	p.call("POST", path, "web_wire", "", body, &first, 200)
	p.call("POST", path, "web_wire", "", body, &replay, 200)
	if first.ID != replay.ID {
		t.Fatal("redirect permit replay debited twice")
	}
	redirect.OperationID = uuid.NewV7()
	p.call("POST", path, "web_wire", "", execution.PermitRequest{Fence: claim.Fence, Intent: redirect}, nil, 403)
	p.call("POST", "/internal/v1/permits/"+first.ID.String()+"/finish", "web_wire", "", execution.PermitFinish{
		Fence: claim.Fence, AttemptID: claim.AttemptID, Usage: egress.Usage{Outcome: "transport"},
	}, nil, 204)
	assertPermits(t, p, job, 2)
	cancelJob(p, job)
	p.call("POST", completePath(claim), "web_wire", "", execution.Completion{Fence: claim.Fence, OutputSchema: 1,
		Result: provider.Result{State: "cancelled", Code: "cancelled"}}, nil, 200)
}

func testB1Semantic(t *testing.T, p *testPlane, lab *webLab, content, tlsTarget, domain uuid.UUID,
	submit b1Submit, wait b1Wait, current func(uuid.UUID, string, string, string) string, events func(uuid.UUID, string) int64,
	history func(uuid.UUID, string, string) int64) {
	t.Helper()
	t.Log("Scoped current results, Change Stream, snapshot and acknowledgement")
	c := a3Enroll(t, p, "web_direct", "web_reader", "public_reader")
	reset := a3Reset(p, c, "public_reader")
	var snapshot execution.CurrentView
	snapshotPath := "/api/v1/snapshots/current?stream=web_direct&consumer=" + c.ID.String() + "&reset=" + reset.Reset.String() + "&limit=2"
	p.call("GET", snapshotPath, "public_reader", "", nil, &snapshot, 200)
	if len(snapshot.Items) != 2 || snapshot.After != "" || snapshot.Head != reset.Baseline {
		t.Fatalf("bad Web snapshot: %+v", snapshot)
	}
	cursor := a3Ack(p, c, "public_reader", snapshot.Head, &reset.Reset)
	if cursor.ResetRequired {
		t.Fatal("Web consumer reset did not complete")
	}

	lab.site.Store(1)
	wait(submit(content, "web.http", "{}"), "succeeded", "")
	if events(content, "http") != 1 || events(content, "html") != 1 {
		t.Fatal("a changing footer produced semantic events")
	}
	lab.site.Store(2)
	wait(submit(content, "web.http", "{}"), "succeeded", "")
	if events(content, "http") != 1 || events(content, "html") != 2 {
		t.Fatal("title change did not produce exactly one HTML event")
	}
	lab.site.Store(6)
	wait(submit(content, "web.http", "{}"), "succeeded", "")
	title := current(content, "web.http", "html", "")
	if events(content, "http") != 1 || events(content, "html") != 3 ||
		!strings.Contains(title, "Updated title") || !strings.Contains(title, `"id": "ghost"`) {
		t.Fatalf("technology change did not produce exactly one HTML event: %s", title)
	}
	for _, mode := range []int32{3, 4, 5} {
		lab.site.Store(mode)
		wait(submit(content, "web.http", "{}"), "partial", "incomplete")
		if s := current(content, "web.http", "html", ""); s != title {
			t.Fatal("degraded page changed last-known-good HTML")
		}
	}
	if events(content, "html") != 3 || history(content, "html", "body_incomplete") != 1 {
		t.Fatal("truncated page emitted a change or lost its diagnostic")
	}
	if s := current(content, "web.http", "http", ""); !strings.Contains(s, `"truncated": true`) || !strings.Contains(s, `"body_bytes": 524288`) {
		t.Fatal(s)
	}

	projection := make(map[string]string)
	for _, item := range snapshot.Items {
		projection[projectionKey(item.ResultIdentity)] = string(item.Data)
	}
	var seen int
	next := snapshot.Head
	for {
		var changes execution.ChangesView
		p.call("GET", a3ChangesPath(c, next), "public_reader", "", nil, &changes, 200)
		for _, change := range changes.Items {
			if change.Action != "upsert" || change.Observation == nil {
				t.Fatal("unexpected Web stream event")
			}
			projection[projectionKey(change.ResultIdentity)] = string(change.Observation.Data)
			seen++
		}
		next = changes.Next
		if !changes.HasMore {
			break
		}
	}
	if seen != 5 {
		t.Fatalf("consumer saw %d new changes instead of 5", seen)
	}
	a3Ack(p, c, "public_reader", next, nil)
	for _, item := range a3ReadCurrent(t, p, "web_direct").Items {
		if projection[projectionKey(item.ResultIdentity)] != string(item.Data) {
			t.Fatal("Web consumer did not converge")
		}
	}

	before := current(tlsTarget, "web.tls", "tls", "")
	lab.revision.Store(1)
	// Each explicit HEAD closes the connection, so it negotiates the new certificate.
	wait(submit(tlsTarget, "web.tls", "{}"), "succeeded", "")
	if events(tlsTarget, "tls") != 2 || current(tlsTarget, "web.tls", "tls", "") == before {
		t.Fatal("certificate renewal did not produce exactly one TLS event")
	}

	const params = `{"types":["a","mx"]}`
	lab.dns.Store(1)
	wait(submit(domain, "web.dns", params), "succeeded", "")
	if events(domain, "dns") != 8 {
		t.Fatal("DNS TTL or AD changed semantic results")
	}
	lab.dns.Store(2)
	wait(submit(domain, "web.dns", params), "succeeded", "")
	if events(domain, "dns") != 9 || !strings.Contains(current(domain, "web.dns", "dns", "a"), "8.8.8.9") {
		t.Fatal("DNS record update did not publish exactly once")
	}
	mx := current(domain, "web.dns", "dns", "mx")
	lab.dns.Store(3)
	job := submit(domain, "web.dns", params)
	a3Exec(t, p, "UPDATE harvest.jobs SET max_attempts=1 WHERE id=$1", job.ID)
	wait(job, "partial", "resolver_servfail")
	assertPermits(t, p, job, 2)
	if current(domain, "web.dns", "dns", "mx") != mx || events(domain, "dns") != 10 ||
		history(domain, "dns", "resolver_servfail") != 1 {
		t.Fatal("DNS partial failure did not preserve MX LKG and commit the independent A result")
	}
	lab.dns.Store(4)
	wait(submit(domain, "web.dns", params), "partial", "resolver_refused")
	if current(domain, "web.dns", "dns", "mx") != mx || history(domain, "dns", "resolver_refused") != 1 {
		t.Fatal("REFUSED evidence lost")
	}
	lab.dns.Store(0)
}

func testB1Policy(t *testing.T, p *testPlane, lab *webLab, a, b *fakeProxy, target b1Target, submit b1Submit, wait b1Wait) {
	t.Helper()
	t.Log("Live routing revision, global concurrency, cooldown, cancellation and proxy outage")
	change := func(edit func(*egress.NetworkConfig)) {
		view := a4Read(p)
		edit(view.Policy)
		a4Put(p, view)
	}
	awaitHold := func() {
		t.Helper()
		select {
		case <-lab.held:
		case <-time.After(6 * time.Second):
			t.Fatal("Web origin did not receive held request")
		}
	}
	hold := target("web_hold", "https://held.example.com")
	active := submit(hold, "web.http", "{}")
	awaitHold()
	change(func(c *egress.NetworkConfig) {
		for i := range c.Sources {
			if c.Sources[i].Namespace == "web_hold" {
				c.Sources[i].Profile = "socks"
			}
		}
	})
	lab.release <- struct{}{}
	awaitHold()
	lab.release <- struct{}{}
	d := wait(active, "succeeded", "")
	assertPermits(t, p, active, 3)
	hits := lab.network.hit("held.example.com")
	if len(d.Attempts) != 2 || len(hits) != 3 || hits[0].proxy != a.name ||
		hits[1].proxy != b.name || hits[2].proxy != b.name || hits[2].mode != "GET /page" {
		t.Fatalf("live route did not fence old path: %+v / %+v", d.Attempts, hits)
	}
	change(func(c *egress.NetworkConfig) { a4Quota(c, "web_global").Concurrency = 1 })
	active = submit(hold, "web.http", "{}")
	awaitHold()
	backpressured := submit(target("web_direct", "http://backpressure.example.com"), "web.http", "{}")
	eventually(t, 3*time.Second, "database global quota deferral", func() bool {
		return a3Count(t, p, "SELECT deferred FROM harvest.quota_buckets WHERE key='web_global'") > 0
	})
	if len(lab.network.hit("backpressure.example.com")) != 0 {
		t.Fatal("global quota allowed a second Worker to send")
	}
	cancelJob(p, active)
	wait(active, "cancelled", "cancelled")
	assertFinished(t, p, active)
	wait(backpressured, "succeeded", "")
	assertPermits(t, p, backpressured, 1)
	change(func(c *egress.NetworkConfig) { a4Quota(c, "web_global").Concurrency = 8 })

	throttle := submit(target("web_throttle", "http://throttle.example.com"), "web.http", "{}")
	wait(throttle, "partial", "upstream_throttled")
	assertPermits(t, p, throttle, 1)
	paused := submit(target("web_throttle", "http://paused.example.com"), "web.http", "{}")
	p.waitJob(paused.ID, "persisted Web source cooldown", 4*time.Second, func(d jobDetail) bool { return d.Waiting == "upstream_cooldown" })
	assertPermits(t, p, paused, 0)
	cancelJob(p, paused)
	_ = a.ln.Close()
	outage := submit(target("web_connect", "https://outage.example.com"), "web.http", "{}")
	blocked := p.waitJob(outage.ID, "required proxy failure without fallback", 12*time.Second, func(d jobDetail) bool {
		return d.Job.State == "retry_wait" || d.Job.State == "failed" || d.Waiting == "no_healthy_egress"
	})
	if len(lab.network.hit("outage.example.com")) != 0 {
		t.Fatal("required proxy outage escaped to origin")
	}
	if blocked.Job.State != "failed" {
		cancelJob(p, outage)
	}
}

func testB1Crash(t *testing.T, p *testPlane, lab *webLab, worker *child, config string, target b1Target, submit b1Submit, wait b1Wait) *child {
	t.Helper()
	t.Log("Crash after an actual Web request and reclaim under a new lease")
	await := func(deadline time.Duration) {
		t.Helper()
		select {
		case <-lab.held:
		case <-time.After(deadline):
			t.Fatal("held Web request did not reach origin")
		}
	}
	job := submit(target("web_hold", "https://held.example.com"), "web.http", "{}")
	await(6 * time.Second)
	worker.kill(t)
	worker = p.start("worker", "--config", config)
	await(18 * time.Second)
	lab.release <- struct{}{}
	d := wait(job, "succeeded", "")
	assertPermits(t, p, job, 3)
	if len(d.Attempts) != 2 || a3Count(t, p, "SELECT count(*) FROM harvest.attempts WHERE job_id=$1 AND state='expired'", job.ID) != 1 {
		t.Fatalf("old Web attempt was not fenced: %+v", d.Attempts)
	}
	waitNoActivePermits(t, p)
	return worker
}

func measureB1(t *testing.T, p *testPlane, server, workerA, workerB *child, control *permitMetrics,
	target b1Target, submit b1Submit, wait b1Wait) {
	t.Helper()
	a3Exec(t, p, "CREATE EXTENSION pg_stat_statements")
	type stats struct {
		calls, inserted, body int64
		lsn                   string
	}
	get := func() stats {
		var s stats
		err := p.pool.QueryRow(t.Context(), `SELECT coalesce(sum(calls),0)::bigint,
		coalesce(sum(calls) FILTER (WHERE query LIKE '%INSERT INTO harvest.quota_permits%'),0)::bigint,
		(SELECT body_bytes FROM harvest.quota_buckets WHERE key='web_global'),pg_current_wal_insert_lsn()::text
		FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
		AND query NOT LIKE '%pg_stat_statements%'`).Scan(&s.calls, &s.inserted, &s.body, &s.lsn)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	const waves = 8
	var targets [waves][4]uuid.UUID
	caps := []string{"web.http", "web.dns", "web.tls", "web.http"}
	for i := range waves {
		targets[i] = [4]uuid.UUID{
			target("web_direct", fmt.Sprintf("http://bench%d.example.com", i)),
			target("web_socks", fmt.Sprintf("bench%d.example.com", i)),
			target("web_socks", fmt.Sprintf("https://bench%d.example.com", i)),
			target("web_socks", fmt.Sprintf("http://bench%d.example.com", i)),
		}
	}
	before := get()
	baseHeap := sampleHeap(p, server, workerA, workerB)
	control.mu.Lock()
	control.permits, control.bytes = nil, 0
	control.mu.Unlock()
	start := time.Now()
	var duration [4][]time.Duration
	var response [2][]time.Duration
	for _, row := range targets {
		var jobs [4]testJob
		for i, id := range row {
			jobs[i] = submit(id, caps[i], "{}")
		}
		for i, j := range jobs {
			d := wait(j, "succeeded", "")
			var elapsed float64
			err := p.pool.QueryRow(t.Context(), `SELECT extract(epoch FROM (ended_at-claimed_at)) FROM harvest.attempts WHERE job_id=$1`, d.Job.ID).Scan(&elapsed)
			if err != nil {
				t.Fatal(err)
			}
			duration[i] = append(duration[i], time.Duration(elapsed*float64(time.Second)))
			if i == 0 || i == 3 {
				var ms int64
				err := p.pool.QueryRow(t.Context(), `SELECT (data->>'latency_ms')::bigint FROM harvest.observations
				WHERE job_id=$1 AND scope->'dimensions'->>'section'='http'`, j.ID).Scan(&ms)
				if err != nil {
					t.Fatal(err)
				}
				response[i/3] = append(response[i/3], time.Duration(ms)*time.Millisecond)
			}
		}
	}
	after := get()
	var wal int64
	if err := p.pool.QueryRow(t.Context(), "SELECT pg_wal_lsn_diff($1::text::pg_lsn,$2::text::pg_lsn)::bigint", after.lsn, before.lsn).Scan(&wal); err != nil {
		t.Fatal(err)
	}
	control.mu.Lock()
	latency, controlBytes := slices.Clone(control.permits), control.bytes
	control.mu.Unlock()
	if after.inserted-before.inserted != waves*11 || len(latency) < waves*11 {
		t.Fatalf("B1 measure permit mismatch: %d / %d", after.inserted-before.inserted, len(latency))
	}
	p95 := func(values []time.Duration) time.Duration {
		slices.Sort(values)
		return values[(len(values)*95+99)/100-1]
	}
	t.Logf("B1 mixed %d targets, %d permit grants in %.2fs: SQL=%d, WAL=%d B, body=%d B, permit/finish request=%d B",
		waves*4, after.inserted-before.inserted, time.Since(start).Seconds(), after.calls-before.calls, wal, after.body-before.body, controlBytes)
	t.Logf("B1 p95 attempt HTTP direct=%v DoH(8)=%v TLS=%v HTTP SOCKS5=%v, permit RPC=%v. Heap (server / Worker A / Worker B) before=%v after=%v B",
		p95(duration[0]), p95(duration[1]), p95(duration[2]), p95(duration[3]), p95(latency), baseHeap, sampleHeap(p, server, workerA, workerB))
	t.Logf("B1 HTTP response-header p95 direct=%v SOCKS5=%v", p95(response[0]), p95(response[1]))
}

// Keep the fixed-provider DNS timeout assertion on the guarded path.
func testB1DNSTimeout(t *testing.T, p *testPlane, target b1Target, submit b1Submit, wait b1Wait) {
	t.Helper()
	job := submit(target("web_socks", "slow.example.com"), "web.dns", `{"types":["a"]}`)
	a3Exec(t, p, "UPDATE harvest.jobs SET max_attempts=1 WHERE id=$1", job.ID)
	wait(job, "failed", "timeout")
	assertPermits(t, p, job, 1)
}
