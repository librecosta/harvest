//go:build integration

package integration

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"uuid"

	"github.com/librecosta/harvest/internal/provider"
)

const fixtureCatalogKey = "a920b029bb19982830080c13d82478a1"

func (l *steamLab) serveCatalog(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if r.Header.Get("X-WebAPI-Key") != fixtureCatalogKey || q.Get("include_games") != "true" ||
		q.Get("include_dlc") != "false" || q.Get("include_software") != "false" || q.Get("include_hardware") != "false" ||
		q.Get("include_videos") != "false" || q.Get("max_results") != "2" || q.Get("key") != "" {
		l.t.Error("Steam catalog request shape or authentication")
	}
	switch l.catalog.Load() {
	case 1:
		w.WriteHeader(429)
		return
	case 2:
		_, _ = io.WriteString(w, strings.Repeat("x", 1<<18))
		return
	case 3:
		_, _ = io.WriteString(w, `{"response":{"apps":[]}}`)
		return
	case 4:
		w.WriteHeader(401)
		return
	}
	appid := uint32(42)
	if q.Get("last_appid") == "42" {
		appid = 43
	}
	if q.Get("last_appid") == "43" {
		appid = 43
	} // Deliberately repeats the cursor.
	_, _ = fmt.Fprintf(w, `{"response":{"apps":[{"appid":%d,"name":"Fox Town","last_modified":1700000000,"price_change_number":17}],"have_more_results":%t,"last_appid":%d}}`, appid, appid == 42, appid)
}

func TestSteamCatalog(t *testing.T) {
	p := newFixture(t)
	p.cli("migrate", "up", "--config", p.config)
	lab, a, b := newSteamLab(t, p)
	cfgA, cfgB := configureWeb(t, p, a, b, []provider.Offer{{Capability: "steam.catalog", RequestSchema: 1, OutputSchema: 1, Implementation: "steam_v1"}})
	p.cli("policy", "init", "--config", p.config)
	server := p.serve()
	aWorker := p.start("worker", "--config", cfgA)
	bWorker := p.start("worker", "--config", cfgB)
	target := p.register("steam", "catalog", "public", "web_connect")
	if again := p.register("steam", "catalog", "public", "web_connect"); again != target {
		t.Fatal("duplicated singleton")
	}
	s := steamRun{t, p, lab}
	req := b1Request(target, "steam.catalog", `{"if_modified_since":0,"last_appid":0,"max_results":2}`)
	p.call("POST", "/api/v1/jobs", "operator", "bad-"+uuid.NewV7().String(),
		map[string]any{"request": b1Request(target, "steam.catalog", `{"max_results":2,"key":"forbidden"}`)}, nil, 400)
	first := s.submit(req)
	firstDone := s.wait(first, "succeeded", "", 1)
	second := s.submit(req)
	secondDone := s.wait(second, "succeeded", "", 1)
	if first.ID == second.ID || len(firstDone.Results) != 1 || len(secondDone.Results) != 1 ||
		firstDone.Results[0].ID != secondDone.Results[0].ID || secondDone.Results[0].JobID != first.ID {
		t.Fatal("repeat Job did not prove confirmation of the same immutable result")
	}
	other := p.register("steam", "catalog", "public", "web_socks")
	for _, c := range []struct {
		mode        int32
		after       int
		state, code string
	}{
		{0, 42, "succeeded", ""}, {0, 43, "failed", "source_invalid"}, {2, 42, "failed", "source_incomplete"},
		{3, 42, "failed", "source_invalid"}, {4, 42, "failed", "upstream_auth"}, {1, 42, "failed", "upstream_throttled"},
	} {
		lab.catalog.Store(c.mode)
		job := s.once(b1Request(other, "steam.catalog", fmt.Sprintf(`{"if_modified_since":0,"last_appid":%d,"max_results":2}`, c.after)))
		s.wait(job, c.state, c.code, 1)
		if c.mode == 4 {
			if n := a3Count(t, p, "SELECT count(*) FROM harvest.quota_buckets WHERE key='steam_catalog_account' AND cooldown_until>now()"); n != 1 {
				t.Fatal("auth failure did not cool the account")
			}
			a3Exec(t, p, "UPDATE harvest.quota_buckets SET cooldown_until=now() WHERE cooldown_until>now()")
		}
	}
	if n := a3Count(t, p, "SELECT count(*) FROM harvest.observations WHERE capability='steam.catalog' AND state='present'"); n != 2 {
		t.Fatalf("invalid or unchanged pages changed history: %d", n)
	}
	if n := a3Count(t, p, "SELECT count(*) FROM harvest.quota_permits p JOIN harvest.attempts a ON a.id=p.attempt_id JOIN harvest.jobs j ON j.id=a.job_id WHERE j.capability='steam.catalog' AND p.finished_at IS NOT NULL"); n != 8 {
		t.Fatalf("unexpected catalog permits: %d", n)
	}
	if n := a3Count(t, p, "SELECT count(*) FROM harvest.jobs WHERE params::text LIKE $1", "%"+fixtureCatalogKey+"%"); n != 0 {
		t.Fatal("catalog key persisted in a Job")
	}
	for _, proc := range []*child{server, aWorker, bWorker} {
		if strings.Contains(proc.log.String(), fixtureCatalogKey) {
			t.Fatal("catalog key logged")
		}
	}
	for _, hit := range lab.hits() {
		if hit.host != "partner.steam-api.com" || hit.proxy == "" {
			t.Fatal("catalog did not use the authorized proxy")
		}
	}
	t.Logf("catalog Jobs=%d, two material pages, one unchanged proof; fixed TLS route through two proxy Workers", len(lab.hits()))
	waitNoActivePermits(t, p)
}
