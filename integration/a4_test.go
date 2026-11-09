//go:build integration

package integration

import (
	"crypto/rand"
	json "encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

const a4PolicyPath = "/api/v1/operations/network-policy"

func a4Read(p *testPlane) execution.PolicyView {
	p.t.Helper()
	var out execution.PolicyView
	p.call("GET", a4PolicyPath, "operator", "", nil, &out, 200)
	return out
}

func a4Quota(c *egress.NetworkConfig, id string) *egress.QuotaConfig {
	return &c.Quotas[slices.IndexFunc(c.Quotas, func(q egress.QuotaConfig) bool { return q.ID == id })]
}

func a4Source(c *egress.NetworkConfig, ns string) *egress.SourceConfig {
	return &c.Sources[slices.IndexFunc(c.Sources, func(s egress.SourceConfig) bool { return s.Namespace == ns })]
}

func a4Put(p *testPlane, current execution.PolicyView) execution.PolicyView {
	p.t.Helper()
	var out execution.PolicyView
	p.call("PUT", a4PolicyPath, "operator", "", execution.PolicyUpdate{ExpectedRevision: current.Revision, Policy: current.Policy}, &out, 200)
	return out
}

func TestA4(t *testing.T) {
	p := newFixture(t)
	legacyPermit, legacyFinish := upgradeA3(t, p)
	lab := newNetworkLab(t)
	p.origins = []string{lab.upstream.URL, lab.second.URL}
	a := lab.proxy("edge_a", "http", rand.Text())
	backup := lab.proxy("edge_backup", "http", "")
	b := lab.proxy("socks_b", "socks5", "")
	workerA, workerB := setupA2(t, p, a, backup, b)
	yaml, err := os.ReadFile(p.config)
	if err != nil {
		t.Fatal(err)
	}
	staleYAML := strings.Replace(string(yaml), "namespaces: [net_socks, net_shared]", "namespaces: [net_socks, net_shared, net_http]", 1)
	for _, role := range []string{"writer", "operator"} {
		name := "a4_" + role
		p.tokens[name] = rand.Text() + rand.Text()
		entry := fmt.Sprintf("  - name: %s\n    role: %s\n    token: %q\n    namespaces: [public]\n", name, role, p.tokens[name])
		staleYAML = strings.Replace(staleYAML, "local_worker:", entry+"local_worker:", 1)
	}
	p.tokens["upgrade"] = rand.Text() + rand.Text()
	upgradeGrant := fmt.Sprintf("  - name: upgrade\n    role: worker\n    token: %q\n    namespaces: [scheduler]\n    capabilities: [fixture.run]\n    max_slots: 1\n", p.tokens["upgrade"])
	staleYAML = strings.Replace(staleYAML, "local_worker:", upgradeGrant+"local_worker:", 1)
	if err := os.WriteFile(p.config, []byte(staleYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	server := p.serve()
	empty := a4Read(p)
	if empty.Status != "not_configured" || empty.Revision != 0 || empty.Policy != nil || a3Count(t, p, "SELECT count(*) FROM harvest.operational_policies") != 0 {
		t.Fatal("serve auto-imported legacy YAML")
	}
	p.call("GET", "/internal/v1/readyz", "net_worker_a", "", nil, nil, 503)
	a3Problem(t, p, "PUT", a4PolicyPath, "operator", execution.PolicyUpdate{ExpectedRevision: 1, Policy: &egress.NetworkConfig{}}, 409, "policy_not_configured")
	p.cli("stop")
	server.waitClean(t)
	if got := p.cli("policy", "init", "--config", p.config); got != "Initialized network policy revision 1." {
		t.Fatal(got)
	}
	env := append(os.Environ(), "HARVEST_TEST_ORIGINS="+strings.Join(p.origins, ","))
	expectFailure(t, p.scratch, env, p.bin, "already initialized", "fixture", "policy", "init", "--config", p.config)
	server = p.serve()
	old := a4Read(p)
	if old.Status != "active" || old.Revision != 1 || old.RoutingRevision != 1 || old.UpdatedBy != "cli_init" || len(old.Checksum) != 64 {
		t.Fatalf("missing initial policy metadata: %+v", old.PolicyStatus)
	}
	var cooldown time.Time
	if err := p.pool.QueryRow(t.Context(), "SELECT cooldown_until FROM harvest.quota_buckets WHERE key='net403' AND balance=12345 AND issued=11 AND refused=3").Scan(&cooldown); err != nil {
		t.Fatal(err)
	}

	p.call("POST", "/internal/v1/permits/"+legacyPermit.String()+"/finish", "upgrade", "", legacyFinish, nil, 204)
	if a3Count(t, p, "SELECT count(*) FROM harvest.quota_permits WHERE id=$1 AND finished_at IS NOT NULL", legacyPermit) != 1 {
		t.Fatal("legacy permit could not finish")
	}

	for _, who := range []string{"public_reader", "a4_writer", "a4_operator", "net_worker_a"} {
		a3Problem(t, p, "GET", a4PolicyPath, who, nil, 403, "forbidden")
		a3Problem(t, p, "PUT", a4PolicyPath, who, execution.PolicyUpdate{ExpectedRevision: 1, Policy: old.Policy}, 403, "forbidden")
	}
	status, body, err := p.request("GET", a4PolicyPath, "operator", "", nil)
	if err != nil || status != 200 || strings.Contains(string(body), "http://") || strings.Contains(string(body), "password") {
		t.Fatal("policy read leaked proxy credentials or URL")
	}
	a3Problem(t, p, "PUT", a4PolicyPath, "operator", map[string]any{"expected_revision": 1, "policy": map[string]any{"proxy_url": "http://user:secret@localhost:1"}}, 400, "invalid_request")
	invalid := a4Read(p)
	a4Source(invalid.Policy, "net_http").Profile = "missing_profile"
	a3Problem(t, p, "PUT", a4PolicyPath, "operator", execution.PolicyUpdate{ExpectedRevision: 1, Policy: invalid.Policy}, 400, "invalid_network_policy")
	invalid = a4Read(p)
	a4Source(invalid.Policy, "net_http").Capability = "fixture.run"
	a3Problem(t, p, "PUT", a4PolicyPath, "operator", execution.PolicyUpdate{ExpectedRevision: 1, Policy: invalid.Policy}, 400, "invalid_network_policy")
	invalid = a4Read(p)
	a4Source(invalid.Policy, "net_http").AccountBucket = "net403"
	a3Problem(t, p, "PUT", a4PolicyPath, "operator", execution.PolicyUpdate{ExpectedRevision: 1, Policy: invalid.Policy}, 409, "policy_quota_binding_conflict")

	slices.Reverse(old.Policy.Quotas)
	slices.Reverse(old.Policy.Routes)
	slices.Reverse(old.Policy.Sources)
	for i := range old.Policy.Routes {
		slices.Reverse(old.Policy.Routes[i].Paths)
	}
	if noop := a4Put(p, old); noop.Revision != old.Revision || noop.Checksum != old.Checksum || !noop.UpdatedAt.Equal(old.UpdatedAt) {
		t.Fatal("canonical no-op created a revision")
	}

	// Competing full replacements use the same expected revision.
	type response struct {
		status int
		data   []byte
		err    error
	}
	cas := make(chan response, 2)
	var contenders sync.WaitGroup
	casStart := time.Now()
	for _, burst := range []int64{4, 5} {
		doc := a4Read(p)
		a4Quota(doc.Policy, "net_shared").Burst = burst
		contenders.Go(func() {
			status, data, err := p.request("PUT", a4PolicyPath, "operator", "", execution.PolicyUpdate{ExpectedRevision: 1, Policy: doc.Policy})
			cas <- response{status, data, err}
		})
	}
	contenders.Wait()
	close(cas)
	wins, conflicts := 0, 0
	for r := range cas {
		switch {
		case r.err == nil && r.status == 200:
			wins++
		case r.err == nil && r.status == 409 && strings.Contains(string(r.data), "policy_revision_conflict"):
			conflicts++
		default:
			t.Fatalf("unexpected CAS status %d: %v %s", r.status, r.err, r.data)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("CAS accepted %d writers with %d conflicts", wins, conflicts)
	}
	current := a4Read(p)
	if current.Revision != 2 || current.RoutingRevision != 1 || current.UpdatedBy != "operator" {
		t.Fatal("CAS policy metadata incorrect")
	}
	t.Logf("competing online replacements resolved in %v", time.Since(casStart))

	// Force failure at COMMIT, after settlement and UPDATE have executed.
	a3Exec(t, p, `CREATE FUNCTION harvest.a4_fail_commit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'test policy commit failure'; END $$`)
	a3Exec(t, p, `CREATE CONSTRAINT TRIGGER a4_fail_commit AFTER UPDATE ON harvest.operational_policies
		DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION harvest.a4_fail_commit()`)
	beforeBalance := a3Count(t, p, "SELECT balance FROM harvest.quota_buckets WHERE key='net_shared'")
	a4Quota(current.Policy, "net_shared").PerMinute = 42
	a3Problem(t, p, "PUT", a4PolicyPath, "operator", execution.PolicyUpdate{ExpectedRevision: current.Revision, Policy: current.Policy}, 503, "internal_unavailable")
	a3Exec(t, p, "DROP TRIGGER a4_fail_commit ON harvest.operational_policies")
	a3Exec(t, p, "DROP FUNCTION harvest.a4_fail_commit()")
	if got := a4Read(p); got.Revision != current.Revision || got.Checksum != current.Checksum ||
		a3Count(t, p, "SELECT balance FROM harvest.quota_buckets WHERE key='net_shared'") != beforeBalance {
		t.Fatal("failed commit partially applied policy or quota settlement")
	}

	p.start("worker", "--config", workerA)
	p.start("worker", "--config", workerB)
	eventually(t, 12*time.Second, "two network Worker registrations", func() bool {
		return a3Count(t, p, "SELECT count(DISTINCT principal) FROM harvest.workers WHERE principal IN ('net_worker_a','net_worker_b')") == 2
	})
	targets := make(map[string]uuid.UUID)
	for _, ns := range []string{"net_shared", "net_wire", "net_http"} {
		targets[ns] = p.register("fixture", "key", ns, ns)
	}
	submit := func(ns, label, mode, region string) testJob {
		data, _ := json.Marshal(networkParams{Label: label, Mode: mode})
		return p.submit(label, execution.Request{TargetID: targets[ns], Capability: networkOffer.Capability, RequestSchema: 1, Params: data, Scope: provider.Scope{EgressRegion: region}})
	}
	a4LiveQuota(t, p, lab, submit, cooldown)
	a4RouteChange(t, p, lab, b, submit)

	// Preserve the original migration-input YAML and keep both Worker processes alive.
	final := a4Read(p)
	original, err := os.ReadFile(p.config)
	if err != nil || string(original) != staleYAML {
		t.Fatal("legacy fixture input was rewritten")
	}
	p.cli("stop")
	server.waitClean(t)
	p.serve()
	got := a4Read(p)
	if got.Revision != final.Revision || got.RoutingRevision != final.RoutingRevision || got.Checksum != final.Checksum ||
		a4Source(got.Policy, "net_http").Profile != "socks_edge" || a4Quota(got.Policy, "global_fixture").Concurrency != 1 {
		t.Fatal("restart replaced the effective DB policy with YAML")
	}
	restarted := submit("net_http", "a4_restart", "ok", "cn")
	a4Succeeded(t, p, restarted)
	if hits := lab.hit("a4_restart"); len(hits) != 1 || hits[0].proxy != b.name {
		t.Fatal("stale YAML re-enabled the original route")
	}
	var ops execution.Operations
	p.call("GET", "/api/v1/operations/status?namespace=net_http", "operator", "", nil, &ops, 200)
	if ops.NetworkPolicy != got.PolicyStatus {
		t.Fatal("operations policy status disagrees with effective policy")
	}
}

func a4Succeeded(t *testing.T, p *testPlane, job testJob) jobDetail {
	t.Helper()
	return p.waitJob(job.ID, "policy fixture completion", 20*time.Second, func(detail jobDetail) bool {
		if detail.Job.State == "failed" || detail.Job.State == "cancelled" {
			t.Fatalf("policy fixture job: %+v", detail)
		}
		return detail.Job.State == "succeeded"
	})
}

func a4RouteChange(t *testing.T, p *testPlane, lab *networkLab, b *fakeProxy, submit networkSubmit) {
	t.Helper()
	job := submit("net_http", "a4_old_route", "delayed", "cn")
	p.waitJob(job.ID, "old CONNECT claim", 8*time.Second, func(d jobDetail) bool { return d.Job.State == "running" })
	next := a4Read(p)
	originalRouting := next.RoutingRevision
	a4Source(next.Policy, "net_http").Profile = "socks_edge"
	switched := a4Put(p, next)
	if switched.RoutingRevision <= originalRouting {
		t.Fatal("routing epoch did not advance")
	}
	if err := os.WriteFile(filepath.Join(p.scratch, "a4_old_route.release"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	d := a4Succeeded(t, p, job)
	hits := lab.hit("a4_old_route")
	if len(d.Attempts) != 2 || d.Attempts[0].WorkerID == d.Attempts[1].WorkerID || len(hits) != 1 || hits[0].proxy != b.name ||
		a3Count(t, p, `SELECT count(*) FROM harvest.attempts WHERE id=$1 AND error_code='network_policy_changed'`, d.Attempts[0].ID) != 1 {
		t.Fatalf("old routing attempt contacted an origin or failed to retry safely: attempts=%+v hits=%+v", d.Attempts, hits)
	}
	assertPermits(t, p, job, 1)
	routeIndex := slices.IndexFunc(switched.Policy.Routes, func(r egress.RouteConfig) bool { return r.Profile == "socks_edge" })
	known := switched.Policy.Routes[routeIndex].Paths
	switched.Policy.Routes[routeIndex].Paths = []egress.PathConfig{{ID: "missing_endpoint", Kind: "socks5", Region: "cn", Bucket: b.name}}
	waitingPolicy := a4Put(p, switched)
	waiting := submit("net_http", "a4_no_path", "ok", "cn")
	d = p.waitJob(waiting.ID, "no active Worker offer for edited route", 5*time.Second, func(d jobDetail) bool { return d.Waiting == "no_healthy_egress" })
	if len(d.Attempts) != 0 || len(lab.hit("a4_no_path")) != 0 {
		t.Fatal("missing required proxy fell back or burned an attempt")
	}
	waitingPolicy.Policy.Routes[routeIndex].Paths = known
	a4Put(p, waitingPolicy)
	a4Succeeded(t, p, waiting)
	if hits := lab.hit("a4_no_path"); len(hits) != 1 || hits[0].proxy != b.name {
		t.Fatal("restored path was bypassed")
	}
}
