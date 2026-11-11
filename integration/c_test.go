//go:build integration

package integration

import (
	"fmt"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

type steamRun struct {
	t   *testing.T
	p   *testPlane
	lab *steamLab
}

func TestC(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	p := newFixture(t)
	p.cli("migrate", "up", "--config", p.config)
	lab, a, b := newSteamLab(t, p)
	cfgA, cfgB := configureWeb(t, p, a, b, steamOffers())
	p.cli("policy", "init", "--config", p.config)
	control := measureControl(t, p, cfgA, cfgB)
	server := p.serve()
	workerA, workerB := p.start("worker", "--config", cfgA), p.start("worker", "--config", cfgB)
	eventually(t, 15*time.Second, "two Steam Worker processes", func() bool {
		for _, worker := range []*child{workerA, workerB} {
			select {
			case <-worker.done:
				t.Fatalf("Steam Worker exited: %v\n%s", worker.err, worker.log.String())
			default:
			}
		}
		return a3Count(t, p, "SELECT count(*) FROM harvest.workers WHERE principal IN ('web_a','web_b') AND NOT draining") == 2
	})
	s := steamRun{t, p, lab}
	connect := s.app("web_connect", "000440")
	socks := s.app("web_socks", "440")
	s.wireCases()
	t.Log("Six explicit Store scopes, global Storefront rate and connection reuse")
	s.scopeCases(connect, socks, server, workerA, workerB, control)
	t.Log("Store and API failures preserve independent observations")
	s.storeCases(connect)
	s.newsCases(connect)
	s.playerCases(socks)
	pjob := s.submit(b1Request(s.app("web_direct", "440"), "steam.players", "{}"))
	s.wait(pjob, "succeeded", "", 1)
	if hits := lab.hits(); hits[len(hits)-1].proxy != "" || hits[len(hits)-1].host != "api.steampowered.com" {
		t.Fatal("direct API request took an unexpected route")
	}
	t.Log("Shared UTC daily quota, policy restart and Worker lifecycle")
	server = s.dailyCases(connect, socks, server)
	s.lifecycleCases(connect, workerA, workerB, cfgB)
	if runtime.GOOS != "windows" {
		eventually(t, 5*time.Second, "Worker shutdown closed its CONNECT pool", func() bool { return a.openCount() == 0 })
	}
	waitNoActivePermits(t, p)
	p.cli("stop")
	server.waitClean(t)
}

func (s steamRun) app(ns, id string) uuid.UUID { return s.p.register("steam", "app", id, ns) }

func steamDetailsRequest(id uuid.UUID, lang, country string) execution.Request {
	return b1Request(id, "steam.details", fmt.Sprintf(`{"language":%q,"store_country":%q}`, lang, country))
}

func (s steamRun) submit(req execution.Request) testJob {
	return s.p.submit(uuid.NewV7().String(), req)
}

func (s steamRun) once(req execution.Request) testJob {
	job := s.submit(req)
	a3Exec(s.t, s.p, "UPDATE harvest.jobs SET max_attempts=1 WHERE id=$1", job.ID)
	return job
}

func (s steamRun) wait(job testJob, state, code string, permits int) jobDetail {
	s.t.Helper()
	d := s.p.waitJob(job.ID, "Steam "+state, 35*time.Second, func(d jobDetail) bool {
		return slices.Contains([]string{"succeeded", "failed", "cancelled", "partial"}, d.Job.State)
	})
	if d.Job.State != state || d.Job.Code != code {
		s.t.Fatalf("Steam %s/%s, expected %s/%s, attempts %+v", d.Job.State, d.Job.Code, state, code, d.Attempts)
	}
	assertPermits(s.t, s.p, job, permits)
	return d
}

func (s steamRun) current(id uuid.UUID, cap, section, locale, country string) (uuid.UUID, string) {
	s.t.Helper()
	var obs uuid.UUID
	var data string
	err := s.p.pool.QueryRow(s.t.Context(), `SELECT o.id,o.data::text FROM harvest.current_results c JOIN harvest.observations o ON o.id=c.observation_id
WHERE c.target_id=$1 AND c.capability=$2 AND o.scope->'dimensions'->>'section'=$3 AND
coalesce(o.scope->>'locale','')=$4 AND coalesce(o.scope->>'region','')=$5`, id, cap, section, locale, country).Scan(&obs, &data)
	if err != nil {
		s.t.Fatal(err)
	}
	for _, secret := range []string{"never-store", "external.example.com", "<script>", "cookie", "proxy-authorization"} {
		if strings.Contains(strings.ToLower(data), secret) {
			s.t.Fatalf("unexpected source material in %s", section)
		}
	}
	return obs, data
}

func (s steamRun) wireCases() {
	t, p := s.t, s.p
	worker := uuid.NewV7()
	p.call("POST", "/internal/v1/workers/register", "web_wire", "", execution.Registration{
		ID: worker, Offers: steamOffers(), Capacity: 1,
		Paths: []egress.Advertisement{{Profile: "local", Mode: "direct", ID: "direct", Kind: "direct", Region: "cn", State: "healthy", Capacity: 1}},
	}, nil, 204)
	target := s.app("web_wire", "440")
	request := steamDetailsRequest(target, "en-US", "us")
	request.Scope.Locale = "zh-CN"
	p.call("POST", "/api/v1/jobs", "operator", uuid.NewV7().String(), execution.Submit{Request: &request}, nil, 400)
	request.Scope.Locale = ""
	job := s.submit(request)
	var claim execution.Claim
	p.call("POST", "/internal/v1/workers/"+worker.String()+"/claim", "web_wire", "", nil, &claim, 200)
	if claim.JobID != job.ID || claim.Task.Scope.Locale != "en" || claim.Task.Scope.Region != "us" || claim.Task.Scope.EgressRegion != "cn" {
		t.Fatal("Steam claim did not normalize three independent scopes")
	}
	path := "/internal/v1/attempts/" + claim.AttemptID.String() + "/permit"
	in := egress.Intent{OperationID: uuid.NewV7(), Profile: "local", PathID: "direct", Method: "GET", MaxBytes: 512 << 10, TargetID: target}
	for _, uri := range []string{"/api/appdetails?appids=441&cc=US&l=english", "/api/appdetails?appids=440&cc=HK&l=english",
		"/api/appdetails?appids=440&cc=US&key=fixture&l=english", "/app/440"} {
		in.RequestURI = uri
		p.call("POST", path, "web_wire", "", execution.PermitRequest{Fence: claim.Fence, Intent: in}, nil, 403)
	}
	assertPermits(t, p, job, 0)
	policy := a4Read(p)
	for i := range policy.Policy.Sources {
		if policy.Policy.Sources[i].Namespace == "web_hold" {
			policy.Policy.Sources[i].Profile = "socks"
		}
	}
	a4Put(p, policy)
	in.RequestURI = "/api/appdetails?appids=440&cc=US&l=english"
	p.call("POST", path, "web_wire", "", execution.PermitRequest{Fence: claim.Fence, Intent: in}, nil, 409)
	assertPermits(t, p, job, 0)
	cancelJob(p, job)
	p.call("POST", completePath(claim), "web_wire", "", execution.Completion{Fence: claim.Fence, OutputSchema: 1,
		Result: provider.Result{State: "cancelled", Code: "cancelled"}}, nil, 200)

	job = s.submit(b1Request(target, "steam.players", "{}"))
	claim = execution.Claim{}
	p.call("POST", "/internal/v1/workers/"+worker.String()+"/claim", "web_wire", "", nil, &claim, 200)
	if claim.JobID != job.ID {
		t.Fatal("unexpected claim for official Permit replay")
	}
	path = "/internal/v1/attempts/" + claim.AttemptID.String() + "/permit"
	in.OperationID, in.MaxBytes = uuid.NewV7(), 4<<10
	in.RequestURI = "/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid=440"
	var first, replay egress.Grant
	p.call("POST", path, "web_wire", "", execution.PermitRequest{Fence: claim.Fence, Intent: in}, &first, 200)
	p.call("POST", path, "web_wire", "", execution.PermitRequest{Fence: claim.Fence, Intent: in}, &replay, 200)
	if first.ID == uuid.Nil() || first.ID != replay.ID || first.LeaseMS <= 0 || replay.LeaseMS <= 0 {
		t.Fatal("replayed official Permit did not preserve its receipt")
	}
	finish := execution.PermitFinish{Fence: claim.Fence, AttemptID: claim.AttemptID, Usage: egress.Usage{Outcome: "policy"}}
	for range 2 {
		p.call("POST", "/internal/v1/permits/"+first.ID.String()+"/finish", "web_wire", "", finish, nil, 204)
	}
	assertPermits(t, p, job, 1)
	if a3Count(t, p, "SELECT count(*) FROM harvest.quota_buckets WHERE key='steam_official' AND issued=1 AND failed=1") != 1 {
		t.Fatal("replay changed official account counters")
	}
	cancelJob(p, job)
	p.call("POST", completePath(claim), "web_wire", "", execution.Completion{Fence: claim.Fence, OutputSchema: 1,
		Result: provider.Result{State: "cancelled", Code: "cancelled"}}, nil, 200)
}

func (s steamRun) scopeCases(id, other uuid.UUID, server, a, b *child, metrics *permitMetrics) {
	t, p := s.t, s.p
	a3Exec(t, p, "CREATE EXTENSION pg_stat_statements")
	var heaps []uint64
	eventually(t, 5*time.Second, "initial Steam heap samples", func() bool {
		heaps = sampleHeap(p, server, a, b)
		return !slices.Contains(heaps, 0)
	})
	before := readSteamMetrics(t, p)
	start := time.Now()
	var jobs []testJob
	for _, country := range []string{"CN", "US", "HK"} {
		for _, language := range []string{"english", "schinese"} {
			jobs = append(jobs, s.submit(steamDetailsRequest(id, language, country)))
		}
	}
	for _, job := range jobs {
		s.wait(job, "succeeded", "", 1)
	}
	recordSteamMetrics(t, p, before, metrics, time.Since(start), heaps, server, a, b)
	var ops execution.Operations
	p.call("GET", "/api/v1/operations/status?namespace=web_connect", "operator", "", nil, &ops, 200)
	if len(ops.SteamSources) != 3 || !slices.Contains(ops.SteamSources, execution.SteamSourceStatus{
		Capability: "steam.details", Class: "storefront", ProviderQuota: "steam_store", AccountQuota: "steam_store_account", RecentPermits: 6,
	}) || !slices.Contains(ops.SteamSources, execution.SteamSourceStatus{
		Capability: "steam.players", Class: "official_api", ProviderQuota: "steam_api", AccountQuota: "steam_official", RecentPermits: 0,
	}) {
		t.Fatalf("Steam operations did not report scoped Permit counts: %+v", ops.SteamSources)
	}
	if got := a3Count(t, p, "SELECT count(*) FROM harvest.current_results WHERE target_id=$1 AND capability='steam.details'", id); got != 18 {
		t.Fatalf("six scoped Store results produced %d current segments", got)
	}
	for _, tc := range []struct{ locale, country, name, price string }{
		{"en", "us", "english US", "priced"}, {"zh-cn", "cn", "schinese CN", "priced"}, {"zh-cn", "hk", "schinese HK", "free"},
	} {
		_, data := s.current(id, "steam.details", "details", tc.locale, tc.country)
		if !strings.Contains(data, tc.name) || !strings.Contains(data, "Coming 2027") || strings.Contains(data, `"mac"`) {
			t.Fatal(data)
		}
		_, data = s.current(id, "steam.details", "price", tc.locale, tc.country)
		if !strings.Contains(data, tc.price) {
			t.Fatal(data)
		}
	}
	hits := s.lab.hits()
	if len(hits) != 6 {
		t.Fatalf("six Store jobs reached wire %d times", len(hits))
	}
	for i, h := range hits {
		if h.proxy != "web_connect" || h.app != "440" || h.peer != hits[0].peer {
			t.Fatalf("Steam SDK did not reuse the CONNECT tunnel: %+v", hits)
		}
		if i > 0 && h.at.Sub(hits[i-1].at) < 1850*time.Millisecond {
			t.Fatalf("Storefront burst exceeded: %+v", hits)
		}
	}
	// Both processes share the same Storefront provider bucket.
	left := s.submit(steamDetailsRequest(id, "english", "US"))
	right := s.submit(steamDetailsRequest(other, "english", "US"))
	s.wait(left, "succeeded", "", 1)
	s.wait(right, "succeeded", "", 1)
	hits = s.lab.hits()
	if len(hits) != 8 || !slices.ContainsFunc(hits[6:], func(h steamHit) bool { return h.proxy == "web_socks" }) ||
		hits[7].at.Sub(hits[6].at) < 1850*time.Millisecond {
		t.Fatalf("Storefront cross-Worker quota or route failed: %+v", hits)
	}
}

func (s steamRun) storeCases(id uuid.UUID) {
	req := steamDetailsRequest(id, "english", "US")
	prior, _ := s.current(id, "steam.details", "price", "en", "us")
	for _, mode := range []int32{1, 13} {
		s.lab.store.Store(mode)
		s.wait(s.submit(req), "partial", "incomplete", 1)
		if after, _ := s.current(id, "steam.details", "price", "en", "us"); after != prior {
			s.t.Fatal("missing price replaced prior good value")
		}
	}
	s.lab.store.Store(2)
	s.wait(s.submit(req), "succeeded", "", 1)
	if _, data := s.current(id, "steam.details", "price", "en", "us"); !strings.Contains(data, "zero_price") || !strings.Contains(data, `"final": 0`) {
		s.t.Fatal(data)
	}
	prior, _ = s.current(id, "steam.details", "price", "zh-cn", "cn")
	for _, tc := range []struct {
		mode int32
		code string
	}{
		{3, "steam_unavailable"}, {4, "steam_unavailable"}, {5, "steam_unavailable"}, {6, "source_invalid"},
		{7, "upstream_throttled"}, {8, "upstream_auth"}, {9, "upstream_blocked"}, {10, "source_redirect"}, {11, "upstream_unavailable"}, {12, "source_incomplete"},
	} {
		s.lab.store.Store(tc.mode)
		before := len(s.lab.hits())
		s.wait(s.once(steamDetailsRequest(id, "schinese", "CN")), "failed", tc.code, 1)
		if len(s.lab.hits()) != before+1 {
			s.t.Fatal("SDK retried, rotated or followed a failed request")
		}
		if tc.mode >= 7 && tc.mode <= 9 {
			if a3Count(s.t, s.p, "SELECT count(*) FROM harvest.quota_buckets WHERE key='steam_store_account' AND cooldown_until > clock_timestamp()") != 1 {
				s.t.Fatal("Steam block did not cool the account")
			}
			a3Exec(s.t, s.p, "UPDATE harvest.quota_buckets SET cooldown_until='epoch' WHERE key='steam_store_account'")
		}
		if after, _ := s.current(id, "steam.details", "price", "zh-cn", "cn"); after != prior {
			s.t.Fatal("invalid storefront result replaced an unrelated price")
		}
	}
	s.lab.store.Store(0)
}

func (s steamRun) newsCases(id uuid.UUID) {
	req := b1Request(id, "steam.news", `{"count":2}`)
	bad := b1Request(id, "steam.news", `{"count":11}`)
	s.p.call("POST", "/api/v1/jobs", "operator", uuid.NewV7().String(), execution.Submit{Request: &bad}, nil, 400)
	s.wait(s.submit(req), "succeeded", "", 1)
	original, data := s.current(id, "steam.news", "news", "", "")
	if strings.Count(data, `"gid"`) != 1 || !strings.Contains(data, "2023-11-14") || !strings.Contains(data, "Fixture Publisher") || len(data) > 1500 {
		s.t.Fatal(data)
	}
	s.wait(s.submit(req), "succeeded", "", 1)
	for _, mode := range []int32{2, 3, 4} {
		s.lab.news.Store(mode)
		s.wait(s.submit(req), "failed", "source_invalid", 1)
		if after, _ := s.current(id, "steam.news", "news", "", ""); after != original {
			s.t.Fatal("malformed news replaced prior page")
		}
	}
	c := a3Enroll(s.t, s.p, "web_connect", "steam-reader", "public_reader")
	reset := a3Reset(s.p, c, "public_reader")
	var snapshot execution.CurrentView
	s.p.call("GET", "/api/v1/snapshots/current?stream=web_connect&consumer="+c.ID.String()+"&reset="+reset.Reset.String()+"&limit=100", "public_reader", "", nil, &snapshot, 200)
	if !slices.ContainsFunc(snapshot.Items, func(o execution.CurrentItem) bool { return o.ID == original }) {
		s.t.Fatal("Steam news missing from snapshot")
	}
	a3Ack(s.p, c, "public_reader", snapshot.Head, &reset.Reset)
	s.lab.news.Store(1)
	s.wait(s.submit(req), "succeeded", "", 1)
	var changes execution.ChangesView
	s.p.call("GET", a3ChangesPath(c, snapshot.Head), "public_reader", "", nil, &changes, 200)
	if len(changes.Items) != 1 || changes.Items[0].Observation == nil || changes.Items[0].Observation.State != "confirmed_empty" {
		s.t.Fatal("empty News page did not produce one confirmed-empty page event")
	}
	a3Ack(s.p, c, "public_reader", changes.Next, nil)
	if a3Count(s.t, s.p, "SELECT count(*) FROM harvest.observations WHERE id=$1", original) != 1 {
		s.t.Fatal("empty News page withdrew the historical observation")
	}
	s.lab.news.Store(0)
}

func (s steamRun) playerCases(id uuid.UUID) {
	req := b1Request(id, "steam.players", "{}")
	for range 2 {
		s.wait(s.submit(req), "succeeded", "", 1)
	}
	for _, tc := range []struct {
		mode int32
		code string
	}{{1, "steam_unavailable"}, {2, "source_invalid"}, {3, "source_invalid"}, {4, "source_invalid"}, {5, "timeout"}} {
		s.lab.players.Store(tc.mode)
		s.wait(s.once(req), "failed", tc.code, 1)
	}
	if n := a3Count(s.t, s.p, `SELECT count(*) FROM harvest.observations WHERE target_id=$1 AND capability='steam.players'
AND sample AND data->>'player_count'='0' AND data->>'result'='1'`, id); n != 2 {
		s.t.Fatalf("expected two independent successful zeros, got %d", n)
	}
	s.lab.players.Store(0)
	policy := execution.ScheduleInput{Request: req, Recurrence: execution.Recurrence{AnchorAt: time.Now().UTC().Add(-65 * time.Minute), IntervalSeconds: 1800, Policy: "state_refresh"}}
	s.p.call("POST", "/api/v1/schedules", "operator", "", policy, nil, 400)
	policy.Recurrence.Policy = "point_in_time"
	var scheduled struct {
		ID uuid.UUID `json:"id"`
	}
	s.p.call("POST", "/api/v1/schedules", "operator", "", policy, &scheduled, 201)
	eventually(s.t, 8*time.Second, "missed point-in-time sample slots", func() bool {
		return a3Count(s.t, s.p, "SELECT count(*) FROM harvest.schedules WHERE id=$1 AND missed_count=3", scheduled.ID) == 1
	})
	if a3Count(s.t, s.p, "SELECT count(*) FROM harvest.jobs WHERE schedule_id=$1", scheduled.ID) != 0 {
		s.t.Fatal("missed players slots were backfilled")
	}
}

func (s steamRun) dailyCases(a, b uuid.UUID, server *child) *child {
	p, t := s.p, s.t
	policy := a4Read(p)
	var used int64
	if err := p.pool.QueryRow(t.Context(), "SELECT issued FROM harvest.quota_buckets WHERE key='steam_official'").Scan(&used); err != nil {
		t.Fatal(err)
	}
	for i := range policy.Policy.Quotas {
		if policy.Policy.Quotas[i].ID == "steam_official" {
			policy.Policy.Quotas[i].DailyLimit = used + 1
		}
	}
	a4Put(p, policy)
	before := len(s.lab.hits())
	jobs := []testJob{s.submit(b1Request(a, "steam.players", "{}")), s.submit(b1Request(b, "steam.news", "{}"))}
	eventually(t, 12*time.Second, "one final grant across both Workers", func() bool {
		d0, d1 := p.job(jobs[0].ID), p.job(jobs[1].ID)
		return d0.Job.State == "succeeded" && d1.Waiting == "daily_quota_exhausted" || d1.Job.State == "succeeded" && d0.Waiting == "daily_quota_exhausted"
	})
	if len(s.lab.hits()) != before+1 {
		t.Fatal("official daily cap allowed more than the final request")
	}
	p.cli("stop")
	server.waitClean(t)
	server = p.serve()
	var pending testJob
	for _, job := range jobs {
		if p.job(job.ID).Job.State != "succeeded" {
			pending = job
		}
	}
	eventually(t, 5*time.Second, "persisted daily quota after restart", func() bool { return p.job(pending.ID).Waiting == "daily_quota_exhausted" })
	if len(s.lab.hits()) != before+1 {
		t.Fatal("restart reset the daily limit")
	}
	// Advance only the fixture ledger window. The next Permit uses the
	// PostgreSQL UTC date, resets counters, and debits the first new grant.
	a3Exec(t, p, "UPDATE harvest.quota_buckets SET metrics_from=metrics_from - interval '1 day' WHERE key='steam_official'")
	s.wait(pending, "succeeded", "", 1)
	if a3Count(t, p, "SELECT count(*) FROM harvest.quota_buckets WHERE key='steam_official' AND issued=1 AND metrics_from=date_trunc('day',clock_timestamp(),'UTC')") != 1 {
		t.Fatal("UTC day rollover did not start a fresh budget")
	}
	return server
}

func (s steamRun) lifecycleCases(id uuid.UUID, workerA, workerB *child, cfgB string) {
	p, t := s.p, s.t
	s.lab.players.Store(6)
	wait := func(timeout time.Duration) {
		t.Helper()
		select {
		case <-s.lab.held:
		case <-time.After(timeout):
			t.Fatal("Steam origin did not enter the held request")
		}
	}
	job := s.submit(b1Request(id, "steam.players", "{}"))
	wait(8 * time.Second)
	cancelJob(p, job)
	s.wait(job, "cancelled", "cancelled", 1)
	assertFinished(t, p, job)

	socks := s.app("web_socks", "570")
	job = s.submit(b1Request(socks, "steam.players", "{}"))
	wait(8 * time.Second)
	workerB.kill(t)
	p.start("worker", "--config", cfgB)
	wait(18 * time.Second)
	s.lab.release <- struct{}{}
	s.wait(job, "succeeded", "", 2)
	if a3Count(t, p, "SELECT count(*) FROM harvest.attempts WHERE job_id=$1 AND state='expired'", job.ID) != 1 {
		t.Fatal("crashed Steam attempt was not fenced")
	}
	if runtime.GOOS != "windows" {
		job = s.submit(b1Request(id, "steam.players", "{}"))
		wait(8 * time.Second)
		if err := workerA.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		s.lab.release <- struct{}{}
		workerA.waitClean(t)
		s.wait(job, "succeeded", "", 1)
	}
	s.lab.players.Store(0)
}
