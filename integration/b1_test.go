//go:build integration

package integration

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

func configureB1(t *testing.T, p *testPlane, a, b *fakeProxy) (string, string) {
	t.Helper()
	raw, err := os.ReadFile(p.config)
	if err != nil {
		t.Fatal(err)
	}
	namespaces := []string{"web_direct", "web_connect", "web_socks", "web_hold", "web_wire", "web_throttle", "web_tls", "web_tls403"}
	var grants strings.Builder
	for _, g := range []struct{ name, namespaces, profiles string }{
		{"web_a", "web_direct, web_connect, web_hold, web_throttle", "local, connect"},
		{"web_b", "web_socks, web_hold, web_tls, web_tls403", "socks"},
		{"web_wire", "web_wire", "local"},
	} {
		token := rand.Text() + rand.Text()
		p.tokens[g.name] = token
		fmt.Fprintf(&grants, "  - name: %q\n    role: worker\n    token: %q\n    namespaces: [%s]\n    capabilities: [web.http, web.tls, web.dns]\n    egress_profiles: [%s]\n    max_slots: 1\n",
			g.name, token, g.namespaces, g.profiles)
	}
	server := strings.Replace(string(raw), "public, private, scheduler, embedded", "public, private, scheduler, embedded, "+strings.Join(namespaces, ", "), 1)
	server = strings.Replace(server, "    namespaces: [public]\n  - name: \"worker_a\"", "    namespaces: [public, "+strings.Join(namespaces, ", ")+"]\n  - name: \"worker_a\"", 1)
	server = strings.Replace(server, "local_worker:", grants.String()+"local_worker:", 1)
	var network strings.Builder
	fmt.Fprintln(&network, `network:
  global_bucket: web_global
  routes:
    - profile: local
      mode: direct
      paths:
        - {id: direct, kind: direct, region: cn, bucket: web_local}
    - profile: connect
      mode: require_proxy
      paths:
        - {id: web_connect, kind: http, region: cn, bucket: web_connect_path}
    - profile: socks
      mode: require_proxy
      paths:
        - {id: web_socks, kind: socks5, region: cn, bucket: web_socks_path}
  sources:`)
	for i, ns := range namespaces {
		profile := []string{"local", "connect", "socks", "connect", "local", "connect", "socks", "socks"}[i]
		for _, offer := range b1Offers {
			fmt.Fprintf(&network, "    - {capability: %s, namespace: %s, profile: %s, provider_bucket: web_provider, account_bucket: %s}\n",
				offer.Capability, ns, profile, ns)
		}
	}
	fmt.Fprintln(&network, "  quotas:")
	for _, bucket := range append([]string{"web_global", "web_provider", "web_local", "web_connect_path", "web_socks_path"}, namespaces...) {
		fmt.Fprintf(&network, "    - {id: %s, burst: 64, per_minute: 1200, concurrency: 8}\n", bucket)
	}
	if err := os.WriteFile(p.config, []byte(server+network.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	worker := func(name, profile string, end *fakeProxy, direct bool) string {
		var data strings.Builder
		fmt.Fprintf(&data, "control_url: %q\ntoken: %q\nconcurrency: 1\noffers:\n", "http://"+p.address, p.tokens[name])
		for _, offer := range b1Offers {
			fmt.Fprintf(&data, "  - {capability: %s, request_schema: 1, output_schema: 1, implementation: web_v1}\n", offer.Capability)
		}
		fmt.Fprintf(&data, `egress:
  endpoints:
    - {id: %s, url: %q, enabled: true, region: cn, concurrency: 1, cooldown_seconds: 2, bytes_per_minute: 33554432}
  profiles:
    - {name: %s, mode: require_proxy, endpoints: [%s]}
`, end.name, end.address(), profile, end.name)
		if direct {
			fmt.Fprintln(&data, "    - {name: local, mode: direct, direct_region: cn}")
		}
		path := filepath.Join(p.scratch, name+".yaml")
		if err := os.WriteFile(path, []byte(data.String()), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	return worker("web_a", "connect", a, true), worker("web_b", "socks", b, false)
}

func b1Request(target uuid.UUID, capability, params string) execution.Request {
	return execution.Request{TargetID: target, Capability: capability, RequestSchema: 1, Params: []byte(params),
		Scope: provider.Scope{EgressRegion: "cn"}}
}

func TestB1(t *testing.T) {
	p := startFixture(t)
	lab, a, b := newWebLab(t, p)
	configA, configB := configureB1(t, p, a, b)
	p.cli("policy", "init", "--config", p.config)
	control := measureControl(t, p, configA, configB)
	server := p.serve()
	workerA, workerB := p.start("worker", "--config", configA), p.start("worker", "--config", configB)
	eventually(t, 15*time.Second, "two remote Web Workers", func() bool {
		return a3Count(t, p, "SELECT count(*) FROM harvest.workers WHERE principal IN ('web_a','web_b') AND NOT draining") == 2
	})
	target := func(namespace, key string) uuid.UUID {
		resource := "domain"
		if strings.Contains(key, "://") {
			resource = "origin"
		}
		return p.register("web", resource, key, namespace)
	}
	submit := func(id uuid.UUID, capability, params string) testJob {
		return p.submit(uuid.NewV7().String(), b1Request(id, capability, params))
	}
	wait := func(job testJob, want, code string) jobDetail {
		t.Helper()
		d := p.waitJob(job.ID, "Web "+want, 36*time.Second, func(d jobDetail) bool {
			return slices.Contains([]string{"succeeded", "partial", "failed", "cancelled", "expired"}, d.Job.State)
		})
		if d.Job.State != want || d.Job.Code != code {
			t.Fatalf("Web job got %s/%s, want %s/%s, attempts=%+v", d.Job.State, d.Job.Code, want, code, d.Attempts)
		}
		return d
	}
	assertHits := func(host string, expected ...string) {
		t.Helper()
		hits := lab.network.hit(host)
		var actual []string
		for _, h := range hits {
			actual = append(actual, h.proxy+" "+h.mode)
		}
		if !slices.Equal(actual, expected) {
			t.Fatalf("%s hits %q, expected %q", host, actual, expected)
		}
	}
	current := func(id uuid.UUID, cap, section, rrtype string) string {
		t.Helper()
		var data string
		err := p.pool.QueryRow(t.Context(), `SELECT o.data::text FROM harvest.current_results c JOIN harvest.observations o ON o.id=c.observation_id
WHERE c.target_id=$1 AND c.capability=$2 AND o.scope->'dimensions'->>'section'=$3
AND coalesce(o.scope->'dimensions'->>'rrtype','')=$4`, id, cap, section, rrtype).Scan(&data)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(data, "never-store") || strings.Contains(data, "http2_supported") || strings.Contains(data, "adoption") {
			t.Fatalf("unsafe Web observation: %s", data)
		}
		return data
	}
	history := func(id uuid.UUID, section, evidence string) int64 {
		return a3Count(t, p, `SELECT count(*) FROM harvest.observations WHERE target_id=$1 AND scope->'dimensions'->>'section'=$2 AND evidence=$3`, id, section, evidence)
	}
	events := func(id uuid.UUID, section string) int64 {
		return a3Count(t, p, `SELECT count(*) FROM harvest.change_events e JOIN harvest.observations o ON o.id=e.observation_id
WHERE e.target_id=$1 AND o.scope->'dimensions'->>'section'=$2`, id, section)
	}
	t.Log("HTTP root, metered same-origin redirect, authenticated CONNECT and SOCKS5")
	content := target("web_direct", "http://content.example.com")
	connect := target("web_connect", "https://redirect.example.com")
	socks := target("web_socks", "https://redirect-socks.example.com")
	directJob, connectJob, socksJob := submit(content, "web.http", "{}"), submit(connect, "web.http", "{}"), submit(socks, "web.http", "{}")
	for _, j := range []testJob{directJob, connectJob, socksJob} {
		wait(j, "succeeded", "")
		assertFinished(t, p, j)
	}
	assertPermits(t, p, directJob, 1)
	assertPermits(t, p, connectJob, 2)
	assertPermits(t, p, socksJob, 2)
	assertHits("content.example.com", " GET /")
	assertHits("redirect.example.com", "web_connect GET /", "web_connect GET /page?sid=never-store")
	assertHits("redirect-socks.example.com", "web_socks GET /", "web_socks GET /page?sid=never-store")
	metadata := current(content, "web.http", "html", "")
	for _, value := range []string{"First title", "Preview", "summary", "Fixture Site", "wordpress", "semantic_sha256", "site.webmanifest"} {
		if !strings.Contains(metadata, value) {
			t.Fatalf("metadata missing %q: %s", value, metadata)
		}
	}
	for _, value := range []string{"body_sha256", "response_headers", "connection_protocol", "nginx", "/page"} {
		if s := current(connect, "web.http", "http", ""); !strings.Contains(s, value) {
			t.Fatalf("HTTP evidence missing %q: %s", value, s)
		}
	}
	if got := a3Count(t, p, `SELECT count(*) FROM harvest.quota_permits child JOIN harvest.quota_permits parent ON child.previous_permit=parent.id
WHERE parent.web_hop=0 AND child.web_hop=1 AND parent.redirect_digest<>''
AND child.attempt_id=parent.attempt_id`); got != 2 {
		t.Fatalf("got %d chained redirects", got)
	}
	for _, tc := range []struct {
		host    string
		permits int
	}{
		{"away.example.com", 1}, {"downgrade.example.com", 1}, {"limit.example.com", 4},
	} {
		id := target("web_connect", "https://"+tc.host)
		j := submit(id, "web.http", "{}")
		wait(j, "partial", "incomplete")
		assertPermits(t, p, j, tc.permits)
		if hits := lab.network.hit(tc.host); len(hits) != tc.permits || slices.ContainsFunc(hits, func(h networkHit) bool { return h.proxy != "web_connect" }) {
			t.Fatalf("unexpected redirect traffic %+v", hits)
		}
		if s := current(id, "web.http", "http", ""); !strings.Contains(s, `"followed": false`) {
			t.Fatalf("blocked redirect not reported: %s", s)
		}
	}
	assertHits("elsewhere.example.com")
	t.Log("Attempt-bound permit validation, SSRF, rebinding and HTTPS validation")
	testB1PermitFence(t, p, target("web_wire", "https://wire.example.com"), target("web_wire", "https://other.example.com"))
	for _, tc := range []struct {
		host          string
		permits, hits int
	}{
		{"mixed.example.com", 1, 0}, {"metadata.example.com", 1, 0}, {"rebind.example.com", 2, 1},
	} {
		j := submit(target("web_connect", "http://"+tc.host), "web.http", "{}")
		wait(j, "failed", "policy_denied")
		assertPermits(t, p, j, tc.permits)
		if len(lab.network.hit(tc.host)) != tc.hits {
			t.Fatalf("unexpected traffic to %s", tc.host)
		}
	}
	invalid := b1Request(target("web_direct", "https://ports.example.com:8443"), "web.http", "{}")
	p.call("POST", "/api/v1/jobs", "operator", uuid.NewV7().String(), execution.Submit{Request: &invalid}, nil, 400)
	invalid = b1Request(content, "web.tls", "{}")
	p.call("POST", "/api/v1/jobs", "operator", uuid.NewV7().String(), execution.Submit{Request: &invalid}, nil, 400)
	invalid = b1Request(content, "web.http", `{"url":"http://169.254.169.254/"}`)
	p.call("POST", "/api/v1/jobs", "operator", uuid.NewV7().String(), execution.Submit{Request: &invalid}, nil, 400)
	tlsTarget := target("web_tls", "https://valid.example.com")
	head := submit(tlsTarget, "web.tls", "{}")
	wait(head, "succeeded", "")
	assertPermits(t, p, head, 1)
	assertHits("valid.example.com", "web_socks HEAD /")
	if s := current(tlsTarget, "web.tls", "tls", ""); !strings.Contains(s, `"verified": true`) || !strings.Contains(s, "Harvest B1 fixture CA") {
		t.Fatal(s)
	}
	forbidden := submit(target("web_tls403", "https://valid403.example.com"), "web.tls", "{}")
	wait(forbidden, "succeeded", "")
	assertPermits(t, p, forbidden, 1)
	assertHits("valid403.example.com", "web_socks HEAD /")
	for host, code := range map[string]string{"wrong.example.com": "tls_name_mismatch", "expired.example.com": "tls_expired", "untrusted.example.com": "tls_untrusted"} {
		id := target("web_tls", "https://"+host)
		j := submit(id, "web.tls", "{}")
		wait(j, "failed", code)
		assertHits(host)
		assertPermits(t, p, j, 1)
		if events(id, "tls") != 0 || history(id, "tls", code) != 1 {
			t.Fatal("failed verification published a TLS fact")
		}
	}
	t.Log("Recursive DoH types, accurate empty answers, redaction and partial LKG")
	domain := target("web_socks", "records.example.com")
	dnsJob := submit(domain, "web.dns", "{}")
	wait(dnsJob, "succeeded", "")
	assertPermits(t, p, dnsJob, 8)
	for _, rr := range []string{"a", "aaaa", "caa", "cname", "mx", "ns", "soa", "txt"} {
		s := current(domain, "web.dns", "dns", rr)
		if !strings.Contains(s, `"authenticated_data": true`) || !strings.Contains(s, "google-public-dns") || !strings.Contains(s, `"egress_kind": "socks5"`) {
			t.Fatal(s)
		}
		if (rr == "txt" || rr == "caa") && !strings.Contains(s, "value_sha256") {
			t.Fatal(s)
		}
	}
	if hits := lab.network.hit("dns.google"); len(hits) != 8 || slices.ContainsFunc(hits, func(h networkHit) bool { return h.proxy != "web_socks" }) {
		t.Fatalf("DoH route: %+v", hits)
	}
	for _, entry := range []struct{ key, rcode, status string }{
		{"missing.example.com", "nxdomain", "nxdomain"}, {"empty.example.com", "noerror", "nodata"},
	} {
		id := target("web_socks", entry.key)
		j := submit(id, "web.dns", `{"types":["A","a"]}`)
		wait(j, "succeeded", "")
		assertPermits(t, p, j, 1)
		s := current(id, "web.dns", "dns", "a")
		if !strings.Contains(s, `"rcode": "`+entry.rcode+`"`) || !strings.Contains(s, `"status": "`+entry.status+`"`) {
			t.Fatal(s)
		}
	}
	testB1DNSTimeout(t, p, target, submit, wait)
	testB1Semantic(t, p, lab, content, tlsTarget, domain, submit, wait, current, events, history)
	testB1Policy(t, p, lab, a, b, target, submit, wait)
	workerB = testB1Crash(t, p, lab, workerB, configB, target, submit, wait)
	if os.Getenv("HARVEST_B1_MEASURE") == "1" {
		measureB1(t, p, server, workerA, workerB, control, target, submit, wait)
	}
	waitNoActivePermits(t, p)
}
