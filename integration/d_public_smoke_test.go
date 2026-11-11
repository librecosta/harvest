//go:build integration && harvest_public_smoke

package integration

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/provider"
)

func TestPublicSmoke(t *testing.T) {
	if os.Getenv("HARVEST_PUBLIC_SMOKE_CONFIRM") != "authorized" {
		t.Skip("public smoke requires explicit authorization")
	}
	cfg, err := config.LoadWorker(os.Getenv("HARVEST_PUBLIC_SMOKE_WORKER"))
	if err != nil {
		t.Fatal("a valid explicit proxy Worker YAML is required")
	}
	if len(cfg.Egress.Profiles) != 1 || len(cfg.Egress.Endpoints) != 1 {
		t.Fatal("smoke requires a single regional proxy")
	}
	profile, endpoint := cfg.Egress.Profiles[0], cfg.Egress.Endpoints[0]
	if profile.Mode != "require_proxy" || profile.AllowDirect || len(profile.Endpoints) != 1 ||
		profile.Endpoints[0] != endpoint.ID || endpoint.Region == "" || !endpoint.Enabled {
		t.Fatal("smoke proxy must be enabled and require_proxy with a region")
	}
	kind, target := os.Getenv("HARVEST_PUBLIC_SMOKE_SOURCE"), os.Getenv("HARVEST_PUBLIC_SMOKE_TARGET")
	var offer provider.Offer
	var params, resource string
	var ceiling int64
	switch kind {
	case "web":
		u, err := url.Parse(target)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil ||
			(u.Port() != "" && u.Port() != "443") || (u.Path != "" && u.Path != "/") ||
			u.RawQuery != "" || u.ForceQuery || strings.Contains(target, "#") {
			t.Fatal("supply an authorized HTTPS origin at its root")
		}
		offer, params, resource, ceiling = b1Offers[0], "{}", "origin", 4
	case "steam":
		app, err := strconv.ParseUint(target, 10, 32)
		language, country := os.Getenv("HARVEST_PUBLIC_SMOKE_LANGUAGE"), os.Getenv("HARVEST_PUBLIC_SMOKE_COUNTRY")
		if err != nil || app == 0 || (language != "english" && language != "schinese") ||
			(country != "CN" && country != "US" && country != "HK") {
			t.Fatal("supply an authorized AppID, supported language and store country")
		}
		offer, resource, ceiling = steamOffers()[0], "app", 1
		params = fmt.Sprintf(`{"language":%q,"store_country":%q}`, language, country)
	default:
		t.Fatal("choose one authorized source: web or steam")
	}

	runPublicSmoke(t, newFixture(t), endpoint, kind, resource, target, offer, params, ceiling)
}

func TestLocalSmokeHarness(t *testing.T) {
	p := newFixture(t)
	lab, proxy, _ := newSteamLab(t, p)
	endpoint := egress.EndpointConfig{URL: proxy.address(), Region: "cn"}
	runPublicSmoke(t, p, endpoint, "steam", "app", "440", steamOffers()[0], `{"language":"english","store_country":"US"}`, 1)
	if hits := lab.hits(); len(hits) != 1 || hits[0].proxy != "web_connect" {
		t.Fatal("smoke harness did not use exactly one guarded request on the fixture proxy")
	}
}

func runPublicSmoke(t *testing.T, p *testPlane, endpoint egress.EndpointConfig, kind, resource, target string, offer provider.Offer, params string, ceiling int64) {
	t.Helper()
	proxy, _ := url.Parse(endpoint.URL)
	p.cli("migrate", "up", "--config", p.config)
	raw, err := os.ReadFile(p.config)
	if err != nil {
		t.Fatal(err)
	}
	grant := fmt.Sprintf("    capabilities: [%s]\n    max_slots: 1\n    egress_profiles: [smoke]", offer.Capability)
	server := strings.Replace(string(raw), "    capabilities: [fixture.run]\n    max_slots: 1\n    egress_profiles: [test_direct]", grant, 1)
	server += fmt.Sprintf(`
network:
  global_bucket: smoke_global
  routes:
    - profile: smoke
      mode: require_proxy
      paths:
        - {id: smoke_proxy, kind: %s, region: %s, bucket: smoke_exit}
  sources:
    - {capability: %s, namespace: public, profile: smoke, provider_bucket: smoke_provider, account_bucket: smoke_account}
  quotas:
    - {id: smoke_global, burst: 1, per_minute: 30, concurrency: 1, daily_limit: %d}
    - {id: smoke_exit, burst: 1, per_minute: 30, concurrency: 1}
    - {id: smoke_provider, burst: 1, per_minute: 30, concurrency: 1}
    - {id: smoke_account, burst: 1, per_minute: 30, concurrency: 1}
`, proxy.Scheme, endpoint.Region, offer.Capability, ceiling)
	if err := os.WriteFile(p.config, []byte(server), 0o600); err != nil {
		t.Fatal(err)
	}
	worker := fmt.Sprintf(`control_url: %q
token: %q
concurrency: 1
offers:
  - {capability: %s, request_schema: 1, output_schema: 1, implementation: %s}
egress:
  profiles:
    - {name: smoke, mode: require_proxy, endpoints: [smoke_proxy]}
  endpoints:
    - id: smoke_proxy
      url: %q
      enabled: true
      region: %q
      concurrency: 1
      cooldown_seconds: 30
      bytes_per_minute: 4194304
`, "http://"+p.address, p.tokens["worker_a"], offer.Capability, offer.Implementation, endpoint.URL, endpoint.Region)
	path := filepath.Join(p.scratch, "smoke-worker.yaml")
	if err := os.WriteFile(path, []byte(worker), 0o600); err != nil {
		t.Fatal(err)
	}
	p.cli("policy", "init", "--config", p.config)
	process := p.serve()
	id := p.register(kind, resource, target, "public")
	request := b1Request(id, offer.Capability, params)
	request.Scope.EgressRegion = endpoint.Region
	job := p.submit(uuid.NewV7().String(), request)
	a3Exec(t, p, "UPDATE harvest.jobs SET max_attempts=1, deadline_at=clock_timestamp()+interval '40 seconds' WHERE id=$1", job.ID)
	start := time.Now()
	p.start("worker", "--config", path)
	result := p.waitJob(job.ID, "public smoke completion", 45*time.Second, func(d jobDetail) bool {
		return d.Job.State == "succeeded" || d.Job.State == "partial" || d.Job.State == "failed"
	})
	permits := a3Count(t, p, "SELECT issued FROM harvest.quota_buckets WHERE key='smoke_global'")
	if permits > ceiling || len(result.Attempts) > 1 {
		t.Fatal("public smoke exceeded its configured budget")
	}
	waitNoActivePermits(t, p)
	p.cli("stop")
	process.waitClean(t)
	t.Logf("public smoke %s state=%s code=%s permits=%d/%d elapsed=%s", offer.Capability, result.Job.State, result.Job.Code, permits, ceiling, time.Since(start))
	if result.Job.State != "succeeded" || permits == 0 {
		t.Fatal("public smoke did not produce a complete observation")
	}
}
