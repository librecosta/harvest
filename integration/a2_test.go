//go:build integration

package integration

import (
	"crypto/rand"
	json "encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/librecosta/harvest/db"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

type fixtureRoute struct {
	profile, mode string
	direct        bool
	endpoints     []*fakeProxy
}

var networkNamespaces = []string{
	"net_direct", "net_http", "net_socks", "net_shared", "net_fallback", "net_no_fallback",
	"net_wire", "net401", "net403", "net429",
}

func setupA2(t *testing.T, p *testPlane, a, backup, b *fakeProxy) (string, string) {
	t.Helper()
	existing, err := os.ReadFile(p.config)
	if err != nil {
		t.Fatal(err)
	}
	content := strings.Replace(string(existing), "public, private, scheduler, embedded",
		"public, private, scheduler, embedded, "+strings.Join(networkNamespaces, ", "), 1)
	var grants, network strings.Builder
	policies := []struct{ who, namespaces, profiles string }{
		{"net_worker_a", "net_direct, net_http, net_shared, net_fallback, net_no_fallback, net401, net403, net429",
			"local, web_edge, shared, prefer_edge, prefer_closed"},
		{"net_worker_b", "net_socks, net_shared", "socks_edge, shared"},
		{"net_wire_worker", "net_wire", "local"},
	}
	for _, g := range policies {
		token := rand.Text() + rand.Text()
		p.tokens[g.who] = token
		fmt.Fprintf(&grants, "  - name: %q\n    role: worker\n    token: %q\n    namespaces: [%s]\n    capabilities: [fixture.network]\n    egress_profiles: [%s]\n    max_slots: 1\n",
			g.who, token, g.namespaces, g.profiles)
	}
	content = strings.Replace(content, "local_worker:", grants.String()+"local_worker:", 1)
	routes := []fixtureRoute{
		{"local", egress.Direct, false, nil},
		{"web_edge", egress.RequireProxy, false, []*fakeProxy{a, backup}},
		{"socks_edge", egress.RequireProxy, false, []*fakeProxy{b}},
		{"shared", egress.RequireProxy, false, []*fakeProxy{a, b}},
		{"prefer_edge", egress.PreferProxy, true, []*fakeProxy{a, backup}},
		{"prefer_closed", egress.PreferProxy, false, []*fakeProxy{a, backup}},
	}
	routeForNamespace := []string{"local", "web_edge", "socks_edge", "shared", "prefer_edge", "prefer_closed", "local", "web_edge", "web_edge", "web_edge"}
	fmt.Fprint(&network, "network:\n  global_bucket: global_fixture\n  routes:\n")
	for _, r := range routes {
		fmt.Fprintf(&network, "    - profile: %s\n      mode: %s\n      allow_direct: %t\n      paths:\n", r.profile, r.mode, r.direct)
		for _, end := range r.endpoints {
			fmt.Fprintf(&network, "        - id: %s\n          kind: %s\n          region: cn\n          bucket: %s\n", end.name, end.scheme, end.name)
		}
		if r.mode == egress.Direct || r.direct {
			fmt.Fprint(&network, "        - id: direct\n          kind: direct\n          region: cn\n          bucket: direct_edge\n")
		}
	}
	fmt.Fprint(&network, "  sources:\n")
	for i, ns := range networkNamespaces {
		fmt.Fprintf(&network, "    - capability: fixture.network\n      namespace: %s\n      profile: %s\n      provider_bucket: fixture_provider\n      account_bucket: %s\n", ns, routeForNamespace[i], ns)
	}
	fmt.Fprint(&network, "  quotas:\n")
	quotaIDs := append([]string{"global_fixture", "fixture_provider", "direct_edge", a.name, backup.name, b.name}, networkNamespaces...)
	for _, id := range quotaIDs {
		burst, rate, concurrency := 64, 1200, 16
		switch id {
		case "net_shared":
			burst, rate, concurrency = 1, 15, 1
		case "net_wire":
			burst, rate, concurrency = 1, 1, 1
		}
		fmt.Fprintf(&network, "    - id: %s\n      burst: %d\n      per_minute: %d\n      concurrency: %d\n", id, burst, rate, concurrency)
	}
	if err := os.WriteFile(p.config, []byte(content+network.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return networkWorkerYAML(t, p, "net_worker_a", routes, []*fakeProxy{a, backup}),
		networkWorkerYAML(t, p, "net_worker_b", routes, []*fakeProxy{b})
}

func networkWorkerYAML(t *testing.T, p *testPlane, who string, routes []fixtureRoute, endpoints []*fakeProxy) string {
	t.Helper()
	var data strings.Builder
	fmt.Fprintf(&data, "control_url: %q\ntoken: %q\nconcurrency: 1\noffers:\n  - capability: fixture.network\n    request_schema: 1\n    output_schema: 1\n    implementation: network_v1\negress:\n  endpoints:\n", "http://"+p.address, p.tokens[who])
	for _, end := range endpoints {
		fmt.Fprintf(&data, "    - id: %s\n      url: %q\n      enabled: true\n      region: cn\n      concurrency: 1\n      cooldown_seconds: 2\n      bytes_per_minute: 131072\n", end.name, end.address())
	}
	fmt.Fprint(&data, "  profiles:\n")
	for _, r := range routes {
		var names []string
		for _, end := range r.endpoints {
			if slices.Contains(endpoints, end) {
				names = append(names, end.name)
			}
		}
		if (r.mode == egress.Direct && who != "net_worker_a") || (r.mode != egress.Direct && len(names) == 0) {
			continue
		}
		fmt.Fprintf(&data, "    - name: %s\n      mode: %s\n      allow_direct: %t\n      endpoints: [%s]\n", r.profile, r.mode, r.direct, strings.Join(names, ", "))
		if r.mode == egress.Direct || r.direct {
			fmt.Fprint(&data, "      direct_region: cn\n")
		}
	}
	path := filepath.Join(p.scratch, who+".yaml")
	if err := os.WriteFile(path, []byte(data.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func upgradeA1(t *testing.T, p *testPlane) {
	t.Helper()
	db1 := stdlib.OpenDBFromPool(p.pool)
	defer db1.Close()
	m, err := goose.NewProvider(goose.DialectPostgres, db1, db.Migrations, goose.WithTableName("public.goose_db_version"))
	if err != nil {
		t.Fatal(err)
	}
	if versions, err := m.UpTo(t.Context(), 1); err != nil || len(versions) != 1 {
		t.Fatalf("seed A1: %v %v", versions, err)
	}
	id := uuid.NewV7()
	_, err = p.pool.Exec(t.Context(), "INSERT INTO harvest.targets (id, provider, resource_type, external_key, namespace) VALUES ($1,'steam','app','123','public')", id)
	if err != nil {
		t.Fatal(err)
	}
	expectFailure(t, p.scratch, nil, p.bin, "schema is unavailable or incompatible", "fixture", "serve", "--local", "--config", p.config)
	if got := p.cli("migrate", "up", "--config", p.config); got != "Applied 4 Harvest migrations." {
		t.Fatalf("A1 upgrade: %s", got)
	}
	var key string
	if err := p.pool.QueryRow(t.Context(), "SELECT external_key FROM harvest.targets WHERE id=$1", id).Scan(&key); err != nil || key != "123" {
		t.Fatalf("A1 data was not preserved: %q %v", key, err)
	}
}

func TestA2(t *testing.T) {
	p := newFixture(t)
	upgradeA1(t, p)
	lab := newNetworkLab(t)
	p.origins = []string{lab.upstream.URL, lab.second.URL}
	secret := rand.Text()
	a := lab.proxy("http_a", "http", secret)
	backup := lab.proxy("http_backup", "http", "")
	b := lab.proxy("socks_b", "socks5", "")
	configA, configB := setupA2(t, p, a, backup, b)
	p.cli("policy", "init", "--config", p.config)
	control := measureControl(t, p, configA, configB)
	server := p.serve()
	workerA := p.start("worker", "--config", configA)
	workerB := p.start("worker", "--config", configB)
	workers := make(map[string]uuid.UUID)
	eventually(t, 15*time.Second, "two registered network Workers", func() bool {
		rows, err := p.pool.Query(t.Context(), "SELECT principal, id FROM harvest.workers WHERE principal IN ('net_worker_a','net_worker_b') AND NOT draining")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			var id uuid.UUID
			if err := rows.Scan(&name, &id); err != nil {
				t.Fatal(err)
			}
			workers[name] = id
		}
		return len(workers) == 2
	})
	targets := make(map[string]uuid.UUID)
	for _, ns := range networkNamespaces {
		targets[ns] = p.register("fixture", "key", ns, ns)
	}
	labels := make(map[uuid.UUID]string)
	submit := func(ns, label, mode, region string) testJob {
		data, _ := json.Marshal(networkParams{Label: label, Mode: mode})
		job := p.submit(label, execution.Request{TargetID: targets[ns], Capability: networkOffer.Capability,
			RequestSchema: networkOffer.RequestSchema, Params: data, Scope: provider.Scope{EgressRegion: region}})
		labels[job.ID] = label
		return job
	}
	state := func(job testJob, desired string) jobDetail {
		return p.waitJob(job.ID, labels[job.ID]+" "+desired, 25*time.Second, func(d jobDetail) bool {
			if desired == "succeeded" && (d.Job.State == "failed" || d.Job.State == "cancelled") {
				t.Fatalf("%s ended in %s (%s), attempts=%+v", labels[job.ID], d.Job.State, d.Job.Code, d.Attempts)
			}
			return d.Job.State == desired
		})
	}
	assertRoute := func(label, path string, count int) []networkHit {
		t.Helper()
		hits := lab.hit(label)
		if len(hits) != count || slices.ContainsFunc(hits, func(h networkHit) bool { return h.proxy != path || h.proxyAuth != "" }) {
			t.Fatalf("unexpected network path for %s: got %d hits, want %d through %q", label, len(hits), count, path)
		}
		return hits
	}
	t.Log("Direct, authenticated CONNECT and SOCKS5 on independent Workers")
	state(submit("net_direct", "direct_ok", "ok", "cn"), "succeeded")
	assertRoute("direct_ok", "", 1)
	state(submit("net_http", "http_ok", "ok", "cn"), "succeeded")
	assertRoute("http_ok", a.name, 1)
	state(submit("net_socks", "socks_ok", "ok", "cn"), "succeeded")
	assertRoute("socks_ok", b.name, 1)
	noRegion, _ := json.Marshal(networkParams{Label: "no_region", Mode: "ok"})
	p.call("POST", "/api/v1/jobs", "operator", "no_region", execution.Submit{Request: &execution.Request{
		TargetID: targets["net_http"], Capability: networkOffer.Capability, RequestSchema: 1, Params: noRegion,
	}}, nil, 400)
	blocked := submit("net_http", "wrong_region", "ok", "us")
	p.waitJob(blocked.ID, "unserved region", 4*time.Second, func(d jobDetail) bool { return d.Waiting == "no_healthy_egress" })
	assertRoute("wrong_region", "", 0)
	cancelJob(p, blocked)
	bad := execution.Registration{ID: uuid.NewV7(), Offers: []provider.Offer{networkOffer}, Capacity: 1, Paths: []egress.Advertisement{
		{Profile: "web_edge", Mode: egress.RequireProxy, ID: a.name, Kind: a.scheme, Region: "cn", State: "healthy", Capacity: 1},
	}}
	p.call("POST", "/internal/v1/workers/register", "net_worker_b", "", bad, nil, 403)
	t.Log("Per-roundtrip SDK replay, cross-origin redirect and transport reuse")
	sdk := submit("net_http", "sdk_replay", "sdk", "cn")
	state(sdk, "succeeded")
	hits := assertRoute("sdk_replay", a.name, 2)
	if hits[0].remote != hits[1].remote {
		t.Fatal("HTTP proxy connection was not reused")
	}
	assertPermits(t, p, sdk, 2)
	redirect := submit("net_http", "redirect_ok", "redirect", "cn")
	state(redirect, "succeeded")
	hits = assertRoute("redirect_ok", a.name, 2)
	if hits[1].authorization != "" || hits[1].cookie != "" {
		t.Fatal("sensitive headers followed cross-origin redirect")
	}
	assertPermits(t, p, redirect, 2)
	unsafe := submit("net_http", "unsafe_source", "unsafe", "cn")
	state(unsafe, "failed")
	assertPermits(t, p, unsafe, 0)
	private := submit("net_http", "private_redirect", "private", "cn")
	state(private, "failed")
	assertRoute("private_redirect", a.name, 1)
	assertPermits(t, p, private, 1)
	large := submit("net_direct", "body_limit", "large", "cn")
	state(large, "failed")
	assertPermits(t, p, large, 1)
	t.Log("429 and authorization refusal cool only the source account, with no proxy rotation")
	for _, status := range []string{"429", "401", "403"} {
		ns, label := "net"+status, "refused_"+status
		before := backup.connects.Load()
		job := submit(ns, label, "status"+status, "cn")
		d := state(job, "failed")
		if len(d.Attempts) != 1 || backup.connects.Load() != before {
			t.Fatal("origin refusal rotated or retried")
		}
		assertRoute(label, a.name, 1)
		assertPermits(t, p, job, 1)
		var cooling bool
		minSeconds := 1
		if status == "429" {
			minSeconds = 70
		}
		err := p.pool.QueryRow(t.Context(), "SELECT cooldown_until > clock_timestamp()+make_interval(secs=>$2) FROM harvest.quota_buckets WHERE key=$1", ns, float64(minSeconds)).Scan(&cooling)
		if err != nil || !cooling {
			t.Fatalf("source account cooldown missing: %v", err)
		}
		paused := submit(ns, "after_"+status, "ok", "cn")
		d = p.waitJob(paused.ID, "persisted source cooldown", 3*time.Second, func(d jobDetail) bool { return d.Waiting == "upstream_cooldown" })
		if len(d.Attempts) != 0 {
			t.Fatal("cooling source was claimed")
		}
		assertRoute("after_"+status, "", 0)
		cancelJob(p, paused)
	}
	t.Log("Transport failure rotates once, require_proxy remains closed, prefer_proxy needs explicit fallback")
	// Force a fresh connection by using the second approved origin for the
	// redirect destination, after disposing idle connections in the fixtures.
	lab.upstream.CloseClientConnections()
	time.Sleep(100 * time.Millisecond)
	a.status.Store(502)
	state(submit("net_http", "failover", "ok", "cn"), "succeeded")
	assertRoute("failover", backup.name, 1)
	lab.upstream.CloseClientConnections()
	backup.status.Store(502)
	strict := submit("net_http", "strict_down", "ok", "cn")
	eventually(t, 12*time.Second, "proxy attempts failed or deferred", func() bool {
		d := p.job(strict.ID)
		return len(d.Attempts) > 0 && d.Job.State != "running" || d.Waiting == "no_healthy_egress"
	})
	assertRoute("strict_down", "", 0)
	if p.job(strict.ID).Job.State != "failed" {
		cancelJob(p, strict)
	}
	fallback := submit("net_fallback", "prefer_fallback", "ok", "cn")
	state(fallback, "succeeded")
	assertRoute("prefer_fallback", "", 1)
	noFallback := submit("net_no_fallback", "prefer_closed", "ok", "cn")
	time.Sleep(1300 * time.Millisecond)
	assertRoute("prefer_closed", "", 0)
	if p.job(noFallback.ID).Job.State != "failed" {
		cancelJob(p, noFallback)
	}
	a.status.Store(0)
	backup.status.Store(0)
	// A restored listener can still be in the Worker's local cooldown.
	// Require a fresh eligible advertisement before exercising recovery.
	var restoredAt time.Time
	if err := p.pool.QueryRow(t.Context(), "SELECT clock_timestamp()").Scan(&restoredAt); err != nil {
		t.Fatal(err)
	}
	eventually(t, 3*execution.HeartbeatInterval, "restored proxy advertisement", func() bool {
		var ready bool
		err := p.pool.QueryRow(t.Context(), `SELECT last_seen > $2 AND EXISTS (
			SELECT 1 FROM jsonb_array_elements(network_paths) AS path
			WHERE path->>'profile'='web_edge' AND path->>'state' IN ('healthy','degraded'))
			FROM harvest.workers WHERE id=$1`, workers["net_worker_a"], restoredAt).Scan(&ready)
		return err == nil && ready
	})
	recovered := submit("net_http", "recovered", "ok", "cn")
	state(recovered, "succeeded")
	if got := lab.hit("recovered"); len(got) != 1 || got[0].proxy == "" {
		t.Fatal("proxy did not recover without going direct")
	}
	t.Log("Two processes share one global concurrency quota and release after body close")
	lab.maxConcurrent.Store(0)
	var issuedBefore, issuedAfter int64
	if err := p.pool.QueryRow(t.Context(), "SELECT issued FROM harvest.quota_buckets WHERE key='global_fixture'").Scan(&issuedBefore); err != nil {
		t.Fatal(err)
	}
	first := submit("net_shared", "shared_first", "hold", "cn")
	eventually(t, 10*time.Second, "first shared response started", func() bool { return len(lab.hit("shared_first")) == 1 })
	second := submit("net_shared", "shared_second", "hold", "cn")
	state(first, "succeeded")
	state(second, "succeeded")
	h1, h2 := lab.hit("shared_first"), lab.hit("shared_second")
	var deferred int64
	err := p.pool.QueryRow(t.Context(), "SELECT deferred FROM harvest.quota_buckets WHERE key='net_shared'").Scan(&deferred)
	if err != nil || deferred < 1 || len(h1) != 1 || len(h2) != 1 || lab.maxConcurrent.Load() != 1 {
		t.Fatalf("shared cap did not serialize both Workers: %d %d %d %v", len(h1), len(h2), deferred, err)
	}
	if err := p.pool.QueryRow(t.Context(), "SELECT issued FROM harvest.quota_buckets WHERE key='global_fixture'").Scan(&issuedAfter); err != nil {
		t.Fatal(err)
	}
	if h2[0].at.Sub(h1[0].at) < 3800*time.Millisecond || issuedAfter-issuedBefore != 2 {
		t.Fatal("shared rate or atomic debit was bypassed")
	}
	firstDetail, secondDetail := p.job(first.ID), p.job(second.ID)
	if !slices.ContainsFunc(secondDetail.Attempts, func(a testAttempt) bool { return a.WorkerID != firstDetail.Attempts[0].WorkerID }) {
		t.Fatal("two independent Workers did not contend on the shared bucket")
	}
	assertWireQuota(t, p, submit, state)
	t.Log("Cancellation, leaked body, database outage and shutdown")
	cancelled := submit("net_direct", "body_cancel", "leak", "cn")
	eventually(t, 8*time.Second, "leaked body started", func() bool { return len(lab.hit("body_cancel")) == 1 })
	// Returning from the provider without Close still terminates the body.
	state(cancelled, "succeeded")
	assertFinished(t, p, cancelled)
	eventually(t, 3*time.Second, "leaked response connection closed", func() bool { return lab.concurrent.Load() == 0 })
	waitNoActivePermits(t, p)
	cancelled = submit("net_direct", "cancel_live", "hold_long", "cn")
	eventually(t, 8*time.Second, "live body started", func() bool { return len(lab.hit("cancel_live")) == 1 })
	cancelJob(p, cancelled)
	state(cancelled, "cancelled")
	assertFinished(t, p, cancelled)
	eventually(t, 3*time.Second, "cancelled response connection closed", func() bool { return lab.concurrent.Load() == 0 })
	waitNoActivePermits(t, p)
	delayed := submit("net_direct", "offline_gate", "delayed", "cn")
	state(delayed, "running")
	p.compose("stop", "-t", "1", "postgres")
	waitProbe(t, p.address, "/readyz", 503, "unavailable")
	if err := os.WriteFile(filepath.Join(p.scratch, "offline_gate.release"), []byte("release"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1600 * time.Millisecond)
	assertRoute("offline_gate", "", 0)
	p.compose("up", "-d", "--wait", "--wait-timeout", "45", "postgres")
	waitProbe(t, p.address, "/readyz", 200, "ok")
	state(delayed, "succeeded")
	assertRoute("offline_gate", "", 1)
	measureA2(t, p, lab, control, submit, state, server, workerA, workerB)
	t.Log("An unacknowledged on-wire request expires, then a different Worker recovers the Job")
	crashed := submit("net_shared", "crash_live", "hold_long", "cn")
	eventually(t, 8*time.Second, "active request before Worker crash", func() bool { return len(lab.hit("crash_live")) == 1 })
	firstWorker := p.job(crashed.ID).Attempts[0].WorkerID
	victim := workerA
	if firstWorker == workers["net_worker_b"] {
		victim = workerB
	}
	victim.kill(t)
	bypass := submit("net_shared", "after_permit_expiry", "ok", "cn")
	state(bypass, "succeeded")
	detail := state(crashed, "succeeded")
	if len(detail.Attempts) != 2 || detail.Attempts[0].State != "expired" || detail.Attempts[1].WorkerID == firstWorker {
		t.Fatal("live request did not recover on the surviving Worker after lease loss")
	}
	assertPermits(t, p, crashed, 2)
	if victim == workerA {
		workerA = p.start("worker", "--config", configA)
	} else {
		workerB = p.start("worker", "--config", configB)
	}
	active := submit("net_direct", "server_drain", "hold", "cn")
	eventually(t, 8*time.Second, "active request before drain", func() bool { return len(lab.hit("server_drain")) == 1 })
	p.cli("stop")
	server.waitClean(t)
	server = p.serve()
	state(active, "succeeded")
	assertFinished(t, p, active)
	waitNoActivePermits(t, p)
	if runtime.GOOS != "windows" {
		active = submit("net_http", "worker_term", "hold", "cn")
		eventually(t, 8*time.Second, "active request before Worker SIGTERM", func() bool { return len(lab.hit("worker_term")) == 1 })
		if err := workerA.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		workerA.waitClean(t)
		state(active, "succeeded")
		eventually(t, 3*time.Second, "drained Worker closed idle CONNECT tunnels", func() bool { return a.openCount()+backup.openCount() == 0 })
		assertFinished(t, p, active)
		waitNoActivePermits(t, p)
	}
	var snapshot execution.Operations
	_, raw, err := p.request("GET", "/api/v1/operations/status?namespace=net_http", "operator", "", nil)
	if err != nil || strings.Contains(string(raw), secret) || strings.Contains(string(raw), a.address()) ||
		json.Unmarshal(raw, &snapshot) != nil || len(snapshot.Quota) == 0 || snapshot.DBPressure.Max != 4 {
		t.Fatalf("invalid sanitized operations snapshot: %v", err)
	}
	p.cli("stop")
	server.waitClean(t)
}

func cancelJob(p *testPlane, job testJob) {
	p.t.Helper()
	p.call("POST", "/api/v1/jobs/"+job.ID.String()+"/cancel", "operator", "", nil, nil, 202)
}

func assertPermits(t *testing.T, p *testPlane, job testJob, want int) {
	t.Helper()
	var count int
	err := p.pool.QueryRow(t.Context(), `SELECT count(*) FROM harvest.quota_permits p JOIN harvest.attempts a ON a.id=p.attempt_id WHERE a.job_id=$1`, job.ID).Scan(&count)
	if err != nil || count != want {
		t.Fatalf("permit count for %s: %d, want %d: %v", job.ID, count, want, err)
	}
}

func assertFinished(t *testing.T, p *testPlane, job testJob) {
	t.Helper()
	eventually(t, 3*time.Second, "permit finish acknowledgement", func() bool {
		var total, finished int
		err := p.pool.QueryRow(t.Context(), `SELECT count(*),count(q.finished_at) FROM harvest.quota_permits q JOIN harvest.attempts a ON a.id=q.attempt_id WHERE a.job_id=$1`, job.ID).Scan(&total, &finished)
		if err != nil {
			t.Fatal(err)
		}
		return total > 0 && total == finished
	})
}

func waitNoActivePermits(t *testing.T, p *testPlane) {
	t.Helper()
	eventually(t, 6*time.Second, "no active permit", func() bool {
		var count int
		err := p.pool.QueryRow(t.Context(), `SELECT count(*) FROM harvest.quota_permits WHERE finished_at IS NULL AND expires_at > clock_timestamp()`).Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		return count == 0
	})
}
