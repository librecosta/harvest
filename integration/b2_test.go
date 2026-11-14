//go:build integration

package integration

import (
	json "encoding/json/v2"
	"fmt"
	"os"
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

type b2Run struct {
	t    *testing.T
	p    *testPlane
	lab  *discoveryLab
	a, b *fakeProxy
}

func TestB2(t *testing.T) {
	p := newFixture(t)
	legacy := upgradeB1Discovery(t, p)
	lab, a, b := newDiscoveryLab(t, p)
	cfgA, cfgB := configureWeb(t, p, a, b, b2Offers())
	p.cli("policy", "init", "--config", p.config)
	control := measureControl(t, p, cfgA, cfgB)
	server := p.serve()
	workerA, workerB := p.start("worker", "--config", cfgA), p.start("worker", "--config", cfgB)
	eventually(t, 15*time.Second, "two discovery Worker processes", func() bool {
		return a3Count(t, p, "SELECT count(*) FROM harvest.workers WHERE principal IN ('web_a','web_b') AND NOT draining") == 2
	})
	b2 := b2Run{t, p, lab, a, b}
	var old execution.CurrentView
	p.call("GET", "/api/v1/observations/current?stream=public", "public_reader", "", nil, &old, 200)
	if len(old.Items) != 1 || old.Items[0].TargetID != legacy || !strings.Contains(string(old.Items[0].Data), "B1 page") {
		t.Fatal("B1 current result changed during upgrade")
	}
	b2.invalidAsset(legacy, old.Items[0].ID, "icon", 0, provider.Scope{EgressRegion: "cn"}, 409)
	b2.admissionCases()

	// An invalid IANA refresh must never authorize a registry request.
	lab.bootstrap.Store(1)
	b2.wait(b2.once(b2.domain("web_connect", "bad-bootstrap.example.com"), "web.rdap", "{}"), "failed", "rdap_bootstrap_unavailable", 1)
	lab.bootstrap.Store(0)

	t.Log("Robots availability, origin redirect cap, and conservative gated paths")
	b2.robotsCases()
	t.Log("Homepage RSS/Atom and assets bind to accepted scoped data")
	b2.assetsCases()
	t.Log("Sitemap XML bounds, stable semantics and zero listed-page fetches")
	b2.sitemapCases()
	t.Log("IANA bootstrap, two independent Worker routes and metered DoH")
	b2.rdapCases()
	doH := b2.submit(b2.domain("web_direct", "dns-check.example.com"), "web.dns", `{"types":["A"]}`)
	b2.wait(doH, "succeeded", "", 1)
	b2.hits("dns.google", " GET /dns-query?", true)
	b2.policyCases(cfgB, &workerB)

	if os.Getenv("HARVEST_B2_MEASURE") == "1" {
		b2.measure(server, workerA, workerB, control)
	}
	throttled := b2.submit(b2.domain("web_socks", "quota-example.io"), "web.rdap", "{}")
	b2.wait(throttled, "failed", "upstream_throttled", 1)
	if a3Count(t, p, "SELECT count(*) FROM harvest.quota_buckets WHERE key='rdap_account' AND cooldown_until > clock_timestamp()") != 1 {
		t.Fatal("RDAP cooldown missing")
	}
	waitNoActivePermits(t, p)
	if got := len(lab.network.hit("data.iana.org")); got != 2 {
		t.Fatalf("expected failed and successful IANA refresh only, got %d", got)
	}
	for _, host := range []string{"outside.example.com", "elsewhere.example.com", "private-registry.example.com", "untrusted.example.com"} {
		if len(lab.network.hit(host)) != 0 {
			t.Fatalf("unapproved host reached: %s", host)
		}
	}
	if runtime.GOOS != "windows" {
		b2.gracefulWorker(workerA)
	}
	p.cli("stop")
	server.waitClean(t)
}

func (b b2Run) origin(ns, host string) uuid.UUID {
	return b.p.register("web", "origin", "https://"+host, ns)
}
func (b b2Run) domain(ns, host string) uuid.UUID { return b.p.register("web", "domain", host, ns) }
func (b b2Run) submit(id uuid.UUID, cap, params string) testJob {
	return b.p.submit(uuid.NewV7().String(), b1Request(id, cap, params))
}
func (b b2Run) once(id uuid.UUID, cap, params string) testJob {
	job := b.submit(id, cap, params)
	a3Exec(b.t, b.p, "UPDATE harvest.jobs SET max_attempts=1 WHERE id=$1", job.ID)
	return job
}
func (b b2Run) wait(job testJob, state, code string, permits int) jobDetail {
	b.t.Helper()
	d := b.p.waitJob(job.ID, "B2 "+state, 34*time.Second, func(d jobDetail) bool {
		return slices.Contains([]string{"succeeded", "failed", "cancelled", "partial"}, d.Job.State)
	})
	if d.Job.State != state || d.Job.Code != code {
		b.t.Fatalf("%s/%s, want %s/%s, attempts=%+v", d.Job.State, d.Job.Code, state, code, d.Attempts)
	}
	if permits >= 0 {
		assertPermits(b.t, b.p, job, permits)
	}
	return d
}
func (b b2Run) current(id uuid.UUID, cap, section, kind string) (uuid.UUID, string) {
	b.t.Helper()
	var obs uuid.UUID
	var data string
	err := b.p.pool.QueryRow(b.t.Context(), `SELECT o.id,o.data::text FROM harvest.current_results c JOIN harvest.observations o ON o.id=c.observation_id
WHERE c.target_id=$1 AND c.capability=$2 AND o.scope->'dimensions'->>'section'=$3
AND coalesce(o.scope->'dimensions'->>'asset','')=$4`, id, cap, section, kind).Scan(&obs, &data)
	if err != nil {
		b.t.Fatal(err)
	}
	if strings.Contains(data, "never-store") {
		b.t.Fatalf("privacy leak: %s", data)
	}
	return obs, data
}
func (b b2Run) hits(host, hit string, prefix bool) {
	b.t.Helper()
	if !slices.ContainsFunc(b.lab.network.hit(host), func(h networkHit) bool {
		value := h.proxy + " " + h.mode
		return value == hit || prefix && strings.HasPrefix(value, hit)
	}) {
		b.t.Fatalf("missing source hit %s %q: %+v", host, hit, b.lab.network.hit(host))
	}
}
func (b b2Run) invalidAsset(id, obs uuid.UUID, kind string, index int, scope provider.Scope, code int) {
	b.t.Helper()
	r := b1Request(id, "web.assets", assetParams(obs, kind, index))
	r.Scope = scope
	b.p.call("POST", "/api/v1/jobs", "operator", uuid.NewV7().String(), execution.Submit{Request: &r}, nil, code)
}
func assetParams(id uuid.UUID, kind string, index int) string {
	data, _ := json.Marshal(provider.AssetParams{Kind: kind, ObservationID: &id, Index: index})
	return string(data)
}

func (b b2Run) admissionCases() {
	b.t.Helper()
	const principal = "web_wire"
	worker := uuid.NewV7()
	offers := b2Offers()[len(b1Offers):]
	b.p.call("POST", "/internal/v1/workers/register", principal, "", execution.Registration{
		ID: worker, Offers: offers, Capacity: 1,
		Paths: []egress.Advertisement{{Profile: "local", Mode: "direct", ID: "direct", Kind: "direct", Region: "cn", State: "healthy", Capacity: 1}},
	}, nil, 204)
	origin := b.origin(principal, "wire.example.com")
	domain := b.domain(principal, "wire.example.com")
	for _, tc := range []struct {
		id      uuid.UUID
		cap     string
		minimum int32
		intent  egress.Intent
	}{
		{origin, "web.robots", 24 * 3600, egress.Intent{MaxBytes: 512 << 10, Destination: "https://wire.example.com", RequestURI: "/arbitrary"}},
		{domain, "web.rdap", 7 * 24 * 3600, egress.Intent{MaxBytes: egress.RDAPMaxBytes, Destination: "https://elsewhere.example.com", RDAPStage: egress.RDAPLookup}},
	} {
		req := b1Request(tc.id, tc.cap, "{}")
		schedule := execution.ScheduleInput{Request: req, Recurrence: execution.Recurrence{
			AnchorAt: time.Now().UTC().Add(24 * time.Hour), IntervalSeconds: tc.minimum - 1, Policy: "state_refresh",
		}}
		b.p.call("POST", "/api/v1/schedules", "operator", "", schedule, nil, 400)
		schedule.Recurrence.IntervalSeconds++
		b.p.call("POST", "/api/v1/schedules", "operator", "", schedule, nil, 201)
		job := b.submit(tc.id, tc.cap, "{}")
		var claim execution.Claim
		b.p.call("POST", "/internal/v1/workers/"+worker.String()+"/claim", principal, "", nil, &claim, 200)
		if claim.JobID != job.ID {
			b.t.Fatal("wrong B2 wire claim")
		}
		in := tc.intent
		in.OperationID, in.TargetID = uuid.NewV7(), tc.id
		in.Profile, in.PathID, in.Method = "local", "direct", "GET"
		b.p.call("POST", "/internal/v1/attempts/"+claim.AttemptID.String()+"/permit", principal, "", execution.PermitRequest{Fence: claim.Fence, Intent: in}, nil, 403)
		assertPermits(b.t, b.p, job, 0)
		cancelJob(b.p, job)
		b.p.call("POST", completePath(claim), principal, "", execution.Completion{Fence: claim.Fence, OutputSchema: 1,
			Result: provider.Result{State: "cancelled", Code: "cancelled"}}, nil, 200)
	}
}

func (b b2Run) robotsCases() {
	id := b.origin("web_socks", "gates.example.com")
	b.wait(b.once(id, "web.sitemap", "{}"), "failed", "robots_unavailable", 0)
	b.wait(b.submit(id, "web.robots", "{}"), "succeeded", "", 1)
	_, first := b.current(id, "web.robots", "robots", "")
	if !strings.Contains(first, "HarvestBot") || !strings.Contains(first, "https://outside.example.com/sitemap.xml") {
		b.t.Fatal(first)
	}
	b.wait(b.submit(id, "web.sitemap", "{}"), "succeeded", "", 1)
	b.lab.robots.Store(1)
	b.wait(b.once(id, "web.robots", "{}"), "failed", "upstream_unavailable", 1)
	if _, data := b.current(id, "web.robots", "robots", ""); data != first {
		b.t.Fatal("robots failure overwrote LKG")
	}
	b.wait(b.once(id, "web.sitemap", "{}"), "failed", "robots_unavailable", 0)
	b.lab.robots.Store(2)
	b.wait(b.submit(id, "web.robots", "{}"), "succeeded", "", 1)
	b.wait(b.submit(id, "web.sitemap", "{}"), "failed", "robots_blocked", 0)
	b.lab.robots.Store(0)

	for _, tc := range []struct {
		ns, host, state, code string
		permits               int
	}{
		{"web_connect", "redirect", "succeeded", "", 2},
		{"web_connect", "cross", "failed", "source_redirect", 1},
		{"web_connect", "large", "failed", "source_incomplete", 1},
		{"web_connect", "slow", "failed", "timeout", 1},
		{"web_tls", "forbidden", "failed", "upstream_auth", 1},
		{"web_tls403", "throttle", "failed", "upstream_throttled", 1},
	} {
		host := tc.host + ".example.com"
		target := b.origin(tc.ns, host)
		job := b.once(target, "web.robots", "{}")
		b.wait(job, tc.state, tc.code, tc.permits)
		if tc.host == "cross" && a3Count(b.t, b.p, `SELECT count(*) FROM harvest.observations WHERE job_id=$1 AND state='unavailable' AND evidence='source_redirect' AND data='{"url":"https://elsewhere.example.com/robots.txt","verified":false}'::jsonb`, job.ID) != 1 {
			b.t.Fatal("sanitized unresolved redirect missing from history")
		}
		if got := len(b.lab.network.hit(host)); got != tc.permits {
			b.t.Fatalf("robots %s wire count %d", host, got)
		}
	}
	missing := b.origin("web_socks", "notfound.example.com")
	b.wait(b.submit(missing, "web.robots", "{}"), "succeeded", "", 1)
	if _, data := b.current(missing, "web.robots", "robots", ""); !strings.Contains(data, `"status": "missing"`) {
		b.t.Fatal(data)
	}
	b.wait(b.submit(missing, "web.assets", "{}"), "succeeded", "", 1)
	b.hits("notfound.example.com", "web_socks GET /favicon.ico", false)
}

func (b b2Run) assetsCases() {
	id := b.origin("web_connect", "assets.example.com")
	b.wait(b.submit(id, "web.http", "{}"), "succeeded", "", 1)
	obs, html := b.current(id, "web.http", "html", "")
	for _, value := range []string{"/updates/feed.xml", "application/rss+xml", "application/atom+xml", "discovered", `"verified": false`, "asset_candidates"} {
		if !strings.Contains(html, value) {
			b.t.Fatal(html)
		}
	}
	if got := len(b.lab.network.hit("assets.example.com")); got != 1 {
		b.t.Fatal("discovery issued another request")
	}
	b.invalidAsset(id, obs, "icon", 2, provider.Scope{EgressRegion: "cn"}, 409)
	b.invalidAsset(id, obs, "icon", 3, provider.Scope{EgressRegion: "cn"}, 409)
	b.invalidAsset(id, obs, "icon", 0, provider.Scope{EgressRegion: "us"}, 409)
	b.invalidAsset(b.origin("web_socks", "assets.example.com"), obs, "icon", 0, provider.Scope{EgressRegion: "cn"}, 409)
	r := b1Request(id, "web.assets", `{"path":"/private"}`)
	b.p.call("POST", "/api/v1/jobs", "operator", uuid.NewV7().String(), execution.Submit{Request: &r}, nil, 400)
	b.wait(b.once(id, "web.assets", assetParams(obs, "manifest", 0)), "failed", "robots_unavailable", 0)
	b.wait(b.submit(id, "web.robots", "{}"), "succeeded", "", 1)
	b.wait(b.submit(id, "web.assets", assetParams(obs, "icon", 0)), "succeeded", "", 1)
	if _, data := b.current(id, "web.assets", "assets", "icon"); !strings.Contains(data, `"width": 16`) || !strings.Contains(data, `"height": 24`) || !strings.Contains(data, "sha256") || !strings.Contains(data, "16x24") {
		b.t.Fatal(data)
	}
	b.wait(b.submit(id, "web.assets", assetParams(obs, "icon", 1)), "succeeded", "", 1)
	if _, data := b.current(id, "web.assets", "assets", "icon"); !strings.Contains(data, "image/svg+xml") || strings.Contains(data, "dimensions") {
		b.t.Fatal(data)
	}
	manifest := assetParams(obs, "manifest", 0)
	b.wait(b.submit(id, "web.assets", manifest), "succeeded", "", 1)
	manifestID, data := b.current(id, "web.assets", "assets", "manifest")
	if !strings.Contains(data, "Fixture App") || !strings.Contains(data, "16x24") || strings.Contains(data, "ignored") {
		b.t.Fatal(data)
	}
	b.lab.manifest.Store(1)
	b.wait(b.submit(id, "web.assets", manifest), "succeeded", "", 1)
	b.lab.manifest.Store(2)
	b.wait(b.submit(id, "web.assets", manifest), "failed", "source_incomplete", 1)
	if after, _ := b.current(id, "web.assets", "assets", "manifest"); after != manifestID {
		b.t.Fatal("manifest reorder or truncation replaced LKG")
	}
	b.lab.manifest.Store(0)

	c := a3Enroll(b.t, b.p, "web_connect", "b2-reader", "public_reader")
	reset := a3Reset(b.p, c, "public_reader")
	var page execution.CurrentView
	path := "/api/v1/snapshots/current?stream=web_connect&consumer=" + c.ID.String() + "&reset=" + reset.Reset.String() + "&limit=100"
	b.p.call("GET", path, "public_reader", "", nil, &page, 200)
	if len(page.Items) < 5 || page.After != "" {
		b.t.Fatal("snapshot incomplete")
	}
	a3Ack(b.p, c, "public_reader", page.Head, &reset.Reset)
	b.lab.feed.Store(1)
	b.wait(b.submit(id, "web.http", "{}"), "succeeded", "", 1)
	var changes execution.ChangesView
	b.p.call("GET", a3ChangesPath(c, page.Head), "public_reader", "", nil, &changes, 200)
	if len(changes.Items) != 1 || changes.HasMore || changes.Items[0].Observation == nil ||
		!strings.Contains(string(changes.Items[0].Observation.Data), "/updates/new.xml") {
		b.t.Fatalf("RSS link change did not produce exactly one event: %+v", changes)
	}
	a3Ack(b.p, c, "public_reader", changes.Next, nil)
	b.lab.feed.Store(0)
}

func (b b2Run) sitemapCases() {
	id := b.origin("web_socks", "sitemap.example.com")
	b.wait(b.submit(id, "web.robots", "{}"), "succeeded", "", 1)
	b.wait(b.submit(id, "web.sitemap", "{}"), "succeeded", "", 1)
	original, data := b.current(id, "web.sitemap", "sitemap", "")
	for _, v := range []string{`"total": 3`, `"same_origin": 1`, `"external": 1`, `"invalid": 1`, "2024-01-02", `"lastmod_state": "invalid"`} {
		if !strings.Contains(data, v) {
			b.t.Fatal(data)
		}
	}
	b.lab.sitemap.Store(7)
	b.wait(b.submit(id, "web.sitemap", "{}"), "succeeded", "", 1)
	for _, tc := range []struct {
		mode int32
		code string
	}{{2, "source_invalid"}, {3, "source_incomplete"}, {4, "source_invalid"}, {5, "source_incomplete"}} {
		b.lab.sitemap.Store(tc.mode)
		b.wait(b.submit(id, "web.sitemap", "{}"), "failed", tc.code, 1)
		if after, _ := b.current(id, "web.sitemap", "sitemap", ""); after != original {
			b.t.Fatal("incomplete or reordered sitemap replaced LKG")
		}
	}
	b.lab.sitemap.Store(1)
	b.wait(b.submit(id, "web.sitemap", "{}"), "succeeded", "", 1)
	if _, data := b.current(id, "web.sitemap", "sitemap", ""); !strings.Contains(data, "sitemapindex") || !strings.Contains(data, "child.xml") {
		b.t.Fatal(data)
	}
	b.lab.sitemap.Store(6)
	b.wait(b.submit(id, "web.sitemap", "{}"), "succeeded", "", 1)
	if _, data := b.current(id, "web.sitemap", "sitemap", ""); !strings.Contains(data, `"sample_truncated": true`) || !strings.Contains(data, `"total": 60`) {
		b.t.Fatal(data)
	}
	b.lab.sitemap.Store(0)
	for _, hit := range b.lab.network.hit("sitemap.example.com") {
		if hit.mode != "GET /robots.txt" && hit.mode != "GET /sitemap.xml" {
			b.t.Fatalf("listed sitemap resource fetched: %+v", hit)
		}
	}
}

func (b b2Run) rdapCases() {
	one := b.domain("web_connect", "one.example.com")
	two := b.domain("web_socks", "two.example.com")
	j1, j2 := b.submit(one, "web.rdap", "{}"), b.submit(two, "web.rdap", "{}")
	b.wait(j1, "succeeded", "", -1)
	b.wait(j2, "succeeded", "", -1)
	if n := a3Count(b.t, b.p, `SELECT count(*) FROM harvest.quota_permits q JOIN harvest.attempts a ON a.id=q.attempt_id WHERE a.job_id IN ($1,$2)`, j1.ID, j2.ID); n != 3 {
		b.t.Fatalf("cold cache grants=%d, expected 1 bootstrap + 2 registries", n)
	}
	b.hits("registry.example.com", "web_connect GET /rdap/domain/example.com", false)
	b.hits("registry.example.com", "web_socks GET /rdap/domain/example.com", false)
	for _, id := range []uuid.UUID{one, two} {
		_, data := b.current(id, "web.rdap", "rdap", "")
		if !strings.Contains(data, "Fixture Registrar") || !strings.Contains(data, "ns1.example.com") ||
			!strings.Contains(data, "2020-01-02") || strings.Contains(data, "expires_at") || strings.Contains(data, "vcardArray") {
			b.t.Fatal(data)
		}
	}
	for _, tc := range []struct {
		domain, code string
		permits      int
	}{
		{"no-registry.org", "rdap_registry_missing", 0}, {"private-example.net", "policy_denied", 1},
		{"redirect-example.dev", "source_redirect", 1}, {"large-example.info", "source_incomplete", 1},
		{"wrong-example.biz", "source_invalid", 1}, {"invalid-registry.app", "rdap_registry_missing", 0},
	} {
		b.wait(b.submit(b.domain("web_socks", tc.domain), "web.rdap", "{}"), "failed", tc.code, tc.permits)
	}
	bad := b1Request(b.domain("web_socks", "co.uk"), "web.rdap", "{}")
	b.p.call("POST", "/api/v1/jobs", "operator", uuid.NewV7().String(), execution.Submit{Request: &bad}, nil, 400)
}

func (b b2Run) policyCases(configB string, workerB **child) {
	b.t.Helper()
	p := b.p
	t := b.t
	held := b.origin("web_hold", "held.example.com")
	await := func(timeout time.Duration) {
		t.Helper()
		select {
		case <-b.lab.held:
		case <-time.After(timeout):
			t.Fatal("B2 held origin not reached")
		}
	}
	first := b.submit(held, "web.robots", "{}")
	await(6 * time.Second)
	policy := a4Read(p)
	for i := range policy.Policy.Sources {
		if policy.Policy.Sources[i].Namespace == "web_hold" {
			policy.Policy.Sources[i].Profile = "socks"
		}
	}
	a4Put(p, policy)
	b.lab.release <- struct{}{}
	await(6 * time.Second)
	b.lab.release <- struct{}{}
	if d := b.wait(first, "succeeded", "", 3); len(d.Attempts) != 2 {
		t.Fatal("routing edit did not fence the first attempt")
	}
	b.hits("held.example.com", "web_connect GET /robots.txt", false)
	b.hits("held.example.com", "web_socks GET /robots-final.txt", false)

	cancelled := b.submit(held, "web.robots", "{}")
	await(6 * time.Second)
	cancelJob(p, cancelled)
	b.wait(cancelled, "cancelled", "cancelled", 1)
	assertFinished(t, p, cancelled)

	crashed := b.submit(held, "web.robots", "{}")
	await(6 * time.Second)
	(*workerB).kill(t)
	*workerB = p.start("worker", "--config", configB)
	await(18 * time.Second)
	b.lab.release <- struct{}{}
	b.wait(crashed, "succeeded", "", 3)
	if a3Count(t, p, "SELECT count(*) FROM harvest.attempts WHERE job_id=$1 AND state='expired'", crashed.ID) != 1 {
		t.Fatal("crashed lease was not fenced")
	}
	waitNoActivePermits(t, p)
}

func (b b2Run) gracefulWorker(worker *child) {
	b.t.Helper()
	job := b.submit(b.origin("web_connect", "shutdown.example.com"), "web.robots", "{}")
	select {
	case <-b.lab.held:
	case <-time.After(8 * time.Second):
		b.t.Fatal("B2 request did not reach origin before SIGTERM")
	}
	if err := worker.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		b.t.Fatal(err)
	}
	b.lab.release <- struct{}{}
	worker.waitClean(b.t)
	b.wait(job, "succeeded", "", 1)
	assertFinished(b.t, b.p, job)
	waitNoActivePermits(b.t, b.p)
}

func upgradeB1Discovery(t *testing.T, p *testPlane) uuid.UUID {
	t.Helper()
	sql := stdlib.OpenDBFromPool(p.pool)
	defer sql.Close()
	m, err := goose.NewProvider(goose.DialectPostgres, sql, db.Migrations, goose.WithTableName("public.goose_db_version"))
	if err != nil {
		t.Fatal(err)
	}
	if v, err := m.UpTo(t.Context(), 5); err != nil || len(v) != 5 {
		t.Fatalf("seed B1: %v %v", v, err)
	}
	id, obs := uuid.NewV7(), uuid.NewV7()
	a3Exec(t, p, "INSERT INTO harvest.targets (id,provider,resource_type,external_key,namespace) VALUES ($1,'web','origin','https://legacy.example.com','public')", id)
	a3Exec(t, p, `INSERT INTO harvest.change_heads (stream,epoch,last_position,event_bytes,total_event_bytes,history_rows,history_bytes)
VALUES ('public',$1,1,0,0,1,512)`, uuid.NewV7())
	a3Exec(t, p, `INSERT INTO harvest.observations
(id,stream,target_id,capability,output_schema,scope_key,scope,state,observed_at,received_at,data,content_hash,source,evidence,sample,job_id,attempt_id,stored_bytes)
VALUES ($1,'public',$2,'web.http',1,repeat('0',64),'{"namespace":"public","egress_region":"cn","dimensions":{"section":"html"}}','present',
now(),now(),'{"title":"B1 page","icons":["https://legacy.example.com/favicon.ico"],"ruleset_version":"web-passive-1","semantic_sha256":"retained"}',$3,'web','',false,$4,$5,512)`,
		obs, id, make([]byte, 32), uuid.NewV7(), uuid.NewV7())
	a3Exec(t, p, `INSERT INTO harvest.current_results (stream,target_id,capability,output_schema,scope_key,observation_id,content_hash,position,confirmed_at)
VALUES ('public',$1,'web.http',1,repeat('0',64),$2,$3,1,now())`, id, obs, make([]byte, 32))
	if got := p.cli("migrate", "up", "--config", p.config); got != "Applied 2 Harvest migrations." {
		t.Fatal(got)
	}
	return id
}

func (b b2Run) measure(server, a, workerB *child, control *permitMetrics) {
	b.t.Helper()
	type counters struct {
		calls, grants, bytes int64
		lsn                  string
	}
	a3Exec(b.t, b.p, "CREATE EXTENSION pg_stat_statements")
	read := func() counters {
		var c counters
		err := b.p.pool.QueryRow(b.t.Context(), `SELECT coalesce(sum(calls),0)::bigint,
coalesce(sum(calls) FILTER (WHERE query LIKE '%INSERT INTO harvest.quota_permits%'),0)::bigint,
(SELECT body_bytes FROM harvest.quota_buckets WHERE key='web_global'),pg_current_wal_insert_lsn()::text
FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND query NOT LIKE '%pg_stat_statements%'`).
			Scan(&c.calls, &c.grants, &c.bytes, &c.lsn)
		if err != nil {
			b.t.Fatal(err)
		}
		return c
	}
	const n = 6
	var origins, domains [n]uuid.UUID
	for i := range n {
		origins[i] = b.origin("web_connect", fmt.Sprintf("bench%d.example.com", i))
		domains[i] = b.domain("web_socks", fmt.Sprintf("bench%d.example.com", i))
	}
	before := read()
	baseHeap := sampleHeap(b.p, server, a, workerB)
	control.mu.Lock()
	control.permits, control.bytes = nil, 0
	control.mu.Unlock()
	caps := []string{"web.robots", "web.sitemap", "web.assets", "web.rdap"}
	var latency [4][]time.Duration
	start := time.Now()
	for i := range n {
		r := b.submit(origins[i], caps[0], "{}")
		d := b.submit(domains[i], caps[3], "{}")
		for j, job := range []testJob{r, d} {
			b.wait(job, "succeeded", "", 1)
			idx := 0
			if j == 1 {
				idx = 3
			}
			latency[idx] = append(latency[idx], b2AttemptLatency(b, job))
		}
		for j := 1; j <= 2; j++ {
			job := b.submit(origins[i], caps[j], "{}")
			b.wait(job, "succeeded", "", 1)
			latency[j] = append(latency[j], b2AttemptLatency(b, job))
		}
	}
	after := read()
	var wal int64
	if err := b.p.pool.QueryRow(b.t.Context(), "SELECT pg_wal_lsn_diff($1::text::pg_lsn,$2::text::pg_lsn)::bigint", after.lsn, before.lsn).Scan(&wal); err != nil {
		b.t.Fatal(err)
	}
	if after.grants-before.grants != n*4 {
		b.t.Fatal("B2 measured grants diverged")
	}
	p95 := func(v []time.Duration) time.Duration { slices.Sort(v); return v[(len(v)*95+99)/100-1] }
	control.mu.Lock()
	permit, controlBytes := p95(control.permits), control.bytes
	control.mu.Unlock()
	b.t.Logf("B2 %d origin/domain pairs, %d grants in %.2fs: SQL=%d WAL=%d B body=%d B control=%d B",
		n, after.grants-before.grants, time.Since(start).Seconds(), after.calls-before.calls, wal, after.bytes-before.bytes, controlBytes)
	b.t.Logf("B2 Attempt p95 robots=%v sitemap=%v assets=%v RDAP warm=%v permit=%v heap server/A/B %v -> %v",
		p95(latency[0]), p95(latency[1]), p95(latency[2]), p95(latency[3]), permit, baseHeap, sampleHeap(b.p, server, a, workerB))
}

func b2AttemptLatency(b b2Run, job testJob) time.Duration {
	b.t.Helper()
	var elapsed float64
	if err := b.p.pool.QueryRow(b.t.Context(), "SELECT extract(epoch FROM (ended_at-claimed_at)) FROM harvest.attempts WHERE job_id=$1", job.ID).Scan(&elapsed); err != nil {
		b.t.Fatal(err)
	}
	return time.Duration(elapsed * float64(time.Second))
}
