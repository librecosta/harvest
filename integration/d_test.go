//go:build integration

package integration

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

const dStream = "web_hold"

func TestD(t *testing.T) {
	p := newFixture(t)
	currentBinary := p.bin
	if baseline := os.Getenv("HARVEST_D_C_FIXTURE"); baseline != "" {
		if !filepath.IsAbs(baseline) {
			t.Fatal("HARVEST_D_C_FIXTURE must be an absolute path to the C integration test binary")
		}
		p.bin = baseline
		t.Log("D upgrade: seeding and serving with the C fixture binary")
	}
	p.cli("migrate", "up", "--config", p.config)
	steam := &steamLab{t: t}
	web, connect, socks := newWebLab(t, p, steam)
	offers := append(slices.Clone(b1Offers), provider.Offer{Capability: "web.robots", RequestSchema: 1, OutputSchema: 1, Implementation: "web_v1"})
	offers = append(offers, steamOffers()...)
	cfgA, cfgB := configureWeb(t, p, connect, socks, offers)
	p.a3Config()
	raw, err := os.ReadFile(p.config)
	if err != nil {
		t.Fatal(err)
	}
	cfg := strings.ReplaceAll(string(raw), "namespaces: [public, snapshots, retention]", "namespaces: [public, snapshots, retention, web_hold]")
	// A serves Storefront and Web HTTP while B publishes player samples into the same stream.
	cfg = strings.Replace(cfg, "capability: steam.players, namespace: web_hold, profile: connect,", "capability: steam.players, namespace: web_hold, profile: socks,", 1)
	if err := os.WriteFile(p.config, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	p.cli("policy", "init", "--config", p.config)
	a3Exec(t, p, "CREATE EXTENSION pg_stat_statements")
	clientCfg := newConsumerDB(t, p)
	metrics := measureControl(t, p, cfgA, cfgB)
	server := p.serve()
	workerA, workerB := p.start("worker", "--config", cfgA), p.start("worker", "--config", cfgB)
	eventually(t, 15*time.Second, "both D Workers", func() bool {
		return a3Count(t, p, "SELECT count(*) FROM harvest.workers WHERE principal IN ('web_a','web_b')") == 2
	})

	// Register the future insert first so its UUIDv7 sorts behind the first
	// snapshot cursor when the result arrives between pages.
	future := p.register("web", "origin", "https://future.example.com", dStream)
	origin := p.register("web", "origin", "https://content.example.com", dStream)
	domain := p.register("web", "domain", "content.example.com", "web_socks")
	sockSite := p.register("web", "origin", "https://content.example.com", "web_socks")
	direct := p.register("web", "origin", "https://content.example.com", "web_direct")
	app := p.register("steam", "app", "440", dStream)
	tombstone := p.register("fixture", "key", "withdrawable", dStream)
	wire := p.a3Register("worker_a")
	dBytePressure(t, p, wire)
	fixture := func(mode string) {
		c, completion := p.a3Claim("worker_a", wire, tombstone, detailOffer, observationParams{Mode: mode, Value: 1})
		p.call("POST", completePath(c), "worker_a", "", completion, nil, 200)
	}
	submit := func(req execution.Request) testJob { return p.submit(uuid.NewV7().String(), req) }
	run := func(reqs ...execution.Request) []testJob {
		t.Helper()
		jobs := make([]testJob, 0, len(reqs))
		for _, req := range reqs {
			jobs = append(jobs, submit(req))
		}
		for _, j := range jobs {
			p.waitJob(j.ID, "mixed D Job", 25*time.Second, func(d jobDetail) bool {
				if slices.Contains([]string{"cancelled", "failed"}, d.Job.State) {
					t.Fatalf("mixed Job failed: %+v", d)
				}
				return d.Job.State == "succeeded" || d.Job.State == "partial"
			})
		}
		return jobs
	}
	httpReq := b1Request(origin, "web.http", "{}")
	french := httpReq
	french.Scope.Locale = "fr"
	newWeb := b1Request(future, "web.http", "{}")
	english := steamDetailsRequest(app, "english", "US")
	chinese := steamDetailsRequest(app, "schinese", "CN")
	news := b1Request(app, "steam.news", "{}")
	players := b1Request(app, "steam.players", "{}")
	run(httpReq, french, english, chinese, news, players, b1Request(direct, "web.http", "{}"))
	fixture("present")
	enrolled := a3Enroll(t, p, dStream, "durable_projection", "public_reader")
	a3Problem(t, p, "GET", "/internal/v1/readyz", "public_reader", nil, 403, "forbidden")
	request := func(method, path string, in any) (int, []byte, error) {
		return p.request(method, path, "public_reader", "", in)
	}
	client := openDConsumer(t, clientCfg, request, dStream, enrolled.ID)
	ctx := t.Context()
	if err := client.reset(ctx, func() {
		web.site.Store(2)
		steam.store.Store(2)
		steam.news.Store(1)
		run(newWeb, httpReq, english, chinese, news, players)
		fixture("withdraw")
	}); err != nil {
		t.Fatal(err)
	}
	// Simulate consumer exit after publication and before the first reset ACK.
	client.db.Close()
	client = openDConsumer(t, clientCfg, request, dStream, enrolled.ID)
	if s, err := client.state(ctx, nil, false); err != nil || s.Reset == nil {
		t.Fatalf("missing durable reset receipt: %+v %v", s, err)
	}
	if err := client.drain(ctx); err != nil {
		t.Fatal(err)
	}
	assertDProjection(t, p, client)
	if n := dProjectionCount(t, client, "value IS NULL"); n != 2 {
		t.Fatalf("expected both scoped withdrawal tombstones, got %d", n)
	}
	if n := dProjectionCount(t, client, "provider='steam' AND external_key='440' AND capability='steam.details' AND value->'data'->>'state'='zero_price'"); n != 2 {
		t.Fatalf("expected both scoped Steam price updates, got %d", n)
	}
	if n := dProjectionCount(t, client, "capability='steam.news' AND value->>'state'='confirmed_empty'"); n != 1 {
		t.Fatal("News page did not update the durable projection")
	}
	if n := dProjectionCount(t, client, "capability='steam.players' AND value->'data'->>'player_count'='0'"); n != 1 {
		t.Fatal("consumer did not keep a single latest zero sample")
	}

	s, err := client.state(ctx, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	run(players)
	page, err := client.changes(s.Cursor, "", nil)
	if err != nil || len(page.Items) != 1 || page.Items[0].Action != "sample" {
		t.Fatalf("expected one real Steam sample event: %+v %v", page, err)
	}
	for range 2 {
		if err := client.apply(ctx, s.Cursor, page); err != nil {
			t.Fatal(err)
		}
	}
	client.db.Close()
	client = openDConsumer(t, clientCfg, request, dStream, enrolled.ID)
	if err := client.drain(ctx); err != nil {
		t.Fatal(err)
	}
	web.site.Store(3)
	steam.store.Store(1)
	run(httpReq, english)
	if err := client.drain(ctx); err != nil {
		t.Fatal(err)
	}
	if n := dProjectionCount(t, client, "capability='steam.details' AND value->'data'->>'state'='zero_price'"); n != 2 {
		t.Fatal("partial Steam price erased last known good")
	}
	if n := dProjectionCount(t, client, "capability='web.http' AND value->'data'->>'title'='Updated title'"); n != 1 {
		t.Fatalf("partial Web result erased last known good: %d", n)
	}
	if n := dProjectionCount(t, client, "capability='web.http' AND scope->>'locale'='fr' AND value->'data'->>'title'='First title'"); n != 1 {
		t.Fatalf("Web locale update crossed an independent scope: %d", n)
	}
	assertDProjection(t, p, client)
	web.site.Store(0)
	steam.store.Store(0)
	steam.news.Store(0)

	dMixedLoad(t, p, metrics, client, []execution.Request{
		httpReq, b1Request(domain, "web.dns", `{"types":["a","aaaa"]}`), b1Request(sockSite, "web.tls", "{}"),
		b1Request(sockSite, "web.robots", "{}"), english, news, players,
	}, run, []*fakeProxy{connect, socks}, server, workerA, workerB)
	a3Problem(t, p, "GET", "/api/v1/observations/current?stream=private", "public_reader", nil, 403, "forbidden")

	p.call("POST", "/api/v1/schedules", "operator", "", execution.ScheduleInput{Request: players,
		Recurrence: execution.Recurrence{AnchorAt: time.Now().Add(time.Hour), IntervalSeconds: 1800, Policy: "point_in_time"}}, nil, 201)
	beforeRestart := dDurableCounts(t, p)
	p.cli("stop")
	server.waitClean(t)
	p.bin = currentBinary
	p.cli("migrate", "up", "--config", p.config)
	server = p.serve()
	if after := dDurableCounts(t, p); after != beforeRestart {
		t.Fatalf("C-schema restart changed durable data: %v -> %v", beforeRestart, after)
	}
	run(players, news)
	run(players)
	dPrune(t, p, client)
	if err := client.reset(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := client.drain(ctx); err != nil {
		t.Fatal(err)
	}
	assertDProjection(t, p, client)
	waitNoActivePermits(t, p)
	if n := a3Count(t, p, "SELECT count(*) FROM harvest.jobs WHERE state IN ('queued','running','retry_wait','cancel_requested')"); n != 0 {
		t.Fatalf("drain left %d orphaned Jobs", n)
	}
	p.cli("stop")
	server.waitClean(t)
	t.Log("consumer converged after restart, duplicate replay, withdrawal, retention 410 and full reset")
}

// Force the same storage-pruning queries below the YAML minima with three
// representative rows. Age-based pruning is exercised separately on D's stream.
func dBytePressure(t *testing.T, p *testPlane, worker uuid.UUID) {
	t.Helper()
	const stream = "retention"
	target := p.register("fixture", "key", "d-byte-pressure", stream)
	for i := range 3 {
		claim, completion := p.a3Claim("worker_a", worker, target, sampleOffer, observationParams{Mode: "present", Value: i, Padding: 16 << 10})
		p.call("POST", completePath(claim), "worker_a", "", completion, nil, 200)
	}
	cfg, err := config.Load(p.config)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Retention.EventBytes = a3Count(t, p, "SELECT stored_bytes FROM harvest.change_events WHERE stream=$1 AND position=3", stream)
	cfg.Retention.HistoryBytes = a3Count(t, p, "SELECT stored_bytes FROM harvest.observations WHERE stream=$1 ORDER BY received_at DESC LIMIT 1", stream)
	engine := execution.New(p.pool, fixtureRegistry(), cfg.Principals, cfg.Retention)
	if err := engine.PruneResults(t.Context()); err != nil {
		t.Fatal(err)
	}
	if a3Count(t, p, "SELECT min_position FROM harvest.change_heads WHERE stream=$1", stream) != 3 ||
		a3Count(t, p, "SELECT count(*) FROM harvest.observations WHERE stream=$1", stream) != 1 ||
		a3Count(t, p, "SELECT count(*) FROM harvest.current_results WHERE stream=$1", stream) != 1 {
		t.Fatal("estimated-byte pruning did not preserve the newest event and current result")
	}
	t.Log("D estimated-byte pressure pruned two events and observations and preserved both pins")
}

func dDurableCounts(t *testing.T, p *testPlane) [9]int64 {
	t.Helper()
	var s [9]int64
	err := p.pool.QueryRow(t.Context(), `SELECT
 (SELECT count(*) FROM harvest.targets), (SELECT count(*) FROM harvest.schedules), (SELECT count(*) FROM harvest.jobs),
 (SELECT count(*) FROM harvest.attempts), (SELECT count(*) FROM harvest.observations),
 (SELECT count(*) FROM harvest.current_results), (SELECT count(*) FROM harvest.change_events),
 (SELECT coalesce(sum(issued),0)::bigint FROM harvest.quota_buckets),
 (SELECT revision FROM harvest.operational_policies)`).Scan(&s[0], &s[1], &s[2], &s[3], &s[4], &s[5], &s[6], &s[7], &s[8])
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func dProjectionCount(t *testing.T, c dConsumer, condition string) int {
	t.Helper()
	var n int
	if err := c.db.QueryRow(t.Context(), "SELECT count(*) FROM projection p JOIN consumer_cursor c USING (stream,generation) WHERE "+condition).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func assertDProjection(t *testing.T, p *testPlane, c dConsumer) {
	t.Helper()
	ctx := t.Context()
	items := a3ReadCurrent(t, p, c.stream)
	state, err := c.state(ctx, nil, false)
	if err != nil || state.Cursor != items.Head || state.Reset != nil {
		t.Fatalf("durable cursor did not converge to API head: %+v %v", state, err)
	}
	if n := dProjectionCount(t, c, "value IS NOT NULL"); n != len(items.Items) {
		t.Fatalf("projection contains %d current values, API contains %d", n, len(items.Items))
	}
	for _, item := range items.Items {
		var position int64
		var id uuid.UUID
		err := c.db.QueryRow(ctx, `SELECT position,(value->>'id')::uuid FROM projection
WHERE stream=$1 AND generation=$2 AND target_id=$3 AND capability=$4 AND output_schema=$5 AND scope_key=$6`,
			c.stream, state.Generation, item.TargetID, item.Capability, item.OutputSchema, item.ScopeKey).Scan(&position, &id)
		if err != nil || position != item.Position || id != item.ID {
			t.Fatalf("stale projection for %s: %v", item.Capability, err)
		}
	}
}

func dPrune(t *testing.T, p *testPlane, c dConsumer) {
	t.Helper()
	ctx := t.Context()
	s, err := c.state(ctx, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	ack := a3Position(t, s.Cursor)
	a3Exec(t, p, "UPDATE harvest.change_events SET committed_at=now()-interval '13 days 12 hours' WHERE stream=$1 AND position=$2", c.stream, ack+1)
	var status execution.Operations
	p.call(http.MethodGet, "/api/v1/operations/status?namespace="+c.stream, "operator", "", nil, &status, 200)
	if status.Results.Stream == nil || status.Results.Stream.Warning != "consumer_replay_window_low" || status.Results.Stream.ReplayAgeCutoff == nil {
		t.Fatalf("operator did not receive expiry warning: %+v", status.Results.Stream)
	}
	a3Exec(t, p, "UPDATE harvest.change_events SET committed_at=now()-interval '15 days' WHERE stream=$1 AND position<(SELECT last_position FROM harvest.change_heads WHERE stream=$1)", c.stream)
	a3Exec(t, p, "UPDATE harvest.observations SET received_at=now()-interval '31 days' WHERE stream=$1", c.stream)
	a3Exec(t, p, "UPDATE harvest.jobs SET updated_at=now()-interval '31 days' WHERE state IN ('succeeded','partial','failed','cancelled')")
	a3Exec(t, p, "UPDATE harvest.change_heads SET last_pruned_at='epoch' WHERE stream=$1", c.stream)
	cfg, err := config.Load(p.config)
	if err != nil {
		t.Fatal(err)
	}
	engine := execution.New(p.pool, fixtureRegistry(), cfg.Principals, cfg.Retention)
	before := dDurableCounts(t, p)
	start := time.Now()
	for range 10 {
		if err := engine.Prune(ctx); err != nil {
			t.Fatal(err)
		}
		a3Exec(t, p, "UPDATE harvest.change_heads SET last_pruned_at='epoch' WHERE stream=$1", c.stream)
		if err := engine.PruneResults(ctx); err != nil {
			t.Fatal(err)
		}
	}
	after := dDurableCounts(t, p)
	if after[2] != 0 || after[3] != 0 || after[4] >= before[4] || after[5] != before[5] || after[6] >= before[6] {
		t.Fatalf("retention failed to prune aged execution/history or preserve current pins: %v -> %v", before, after)
	}
	if n := a3Count(t, p, `SELECT count(*) FROM harvest.change_events e LEFT JOIN harvest.observations o ON o.id=e.observation_id
WHERE e.observation_id IS NOT NULL AND o.id IS NULL`); n != 0 {
		t.Fatalf("%d retained events lost pinned observations", n)
	}
	if _, err := c.changes(s.Cursor, "", nil); !errors.Is(err, errProjectionExpired) {
		t.Fatalf("aged cursor did not produce HTTP 410: %v", err)
	}
	t.Logf("D prune %s jobs=%d→%d attempts=%d→%d observations=%d→%d current=%d events=%d→%d",
		time.Since(start), before[2], after[2], before[3], after[3], before[4], after[4], after[5], before[6], after[6])
	vacuum := time.Now()
	a3Exec(t, p, "VACUUM (ANALYZE) harvest.jobs, harvest.attempts, harvest.observations, harvest.change_events")
	t.Logf("D VACUUM ANALYZE %s; %s", time.Since(vacuum), fmt.Sprintf("%+v", dDatabase(t, p)))
}
