package egress

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/netip"
	"net/url"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

const (
	diagnosticInterval = 10 * time.Minute
	diagnosticSpacing  = 5 * time.Second
	diagnosticBudget   = 3 * time.Second
)

type healthKey struct{ profile, id, kind, region, check string }

type HealthReport struct {
	Profile           string `json:"profile"`
	PathID            string `json:"path_id"`
	Kind              string `json:"kind"`
	Region            string `json:"region"`
	CheckType         string `json:"check_type"`
	LastOutcome       string `json:"last_outcome"`
	Successes         int64  `json:"successes"`
	Failures          int64  `json:"failures"`
	TransportFailures int64  `json:"transport_failures"`
	LatencyMS         int64  `json:"latency_ms"`
	LastLatencyMS     int32  `json:"last_latency_ms"`
	TrailingFailures  int64  `json:"trailing_failures"`
	Recoveries        int64  `json:"recoveries"`
}

func (p *Pool) recordHealth(key healthKey, outcome string, latency time.Duration, recovered bool) {
	row := p.health[key]
	// Bound each batch even if the control plane remains unreachable.
	if row.Successes+row.Failures >= 1_000_000 {
		row = HealthReport{}
	}
	row.Profile, row.PathID, row.Kind, row.Region, row.CheckType = key.profile, key.id, key.kind, key.region, key.check
	row.LastOutcome = outcome
	row.LastLatencyMS = int32(min(5000, max(0, latency.Milliseconds())))
	row.LatencyMS += int64(row.LastLatencyMS)
	if outcome == "ok" {
		row.Successes++
		row.TrailingFailures = 0
	} else {
		row.Failures++
		row.TrailingFailures++
		if outcome == "transport_failure" {
			row.TransportFailures++
		}
	}
	if recovered {
		row.Recoveries++
	}
	p.health[key] = row
}

func (p *Pool) TakeHealth() []HealthReport {
	p.mu.Lock()
	defer p.mu.Unlock()
	rows := make([]HealthReport, 0, len(p.health))
	for _, report := range p.health {
		rows = append(rows, report)
	}
	clear(p.health)
	return rows
}

func (p *Pool) nextCheck(now time.Time) (healthKey, *endpoint) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.draining || now.Before(p.nextDiagnosis) {
		return healthKey{}, nil
	}
	var best healthKey
	var proxy *endpoint
	for _, profile := range p.profiles {
		consider := func(key healthKey, e *endpoint) {
			for _, check := range []string{"google_doh", "steam"} {
				key.check = check
				last := p.diagnosed[key]
				if now.Sub(last) >= diagnosticInterval && (best.check == "" || last.Before(p.diagnosed[best])) {
					best, proxy = key, e
				}
			}
		}
		if profile.Mode == Direct || profile.AllowDirect {
			consider(healthKey{profile.Name, Direct, Direct, profile.DirectRegion, ""}, nil)
		}
		for _, id := range profile.Endpoints {
			e := p.endpoints[id]
			if (e.state == "healthy" && e.inflight < e.cfg.Concurrency) || (e.state == "degraded" && e.inflight == 0) {
				consider(healthKey{profile.Name, id, e.url.Scheme, e.cfg.Region, ""}, e)
			}
		}
	}
	if best.check != "" {
		p.diagnosed[best] = now
		p.nextDiagnosis = now.Add(diagnosticSpacing)
		if proxy != nil {
			proxy.inflight++
		}
	}
	return best, proxy
}

// Diagnose performs at most one compiled, read-only connectivity check. It
// never retries, follows a redirect, takes a different egress path, or changes
// the existing proxy circuit breaker based on an upstream's behavior.
func (p *Pool) Diagnose(ctx context.Context) {
	start := time.Now()
	key, proxy := p.nextCheck(start)
	if key.check == "" {
		return
	}
	var endpointURL *url.URL
	if proxy != nil {
		endpointURL = proxy.url
	}
	result := diagnose(ctx, key.check, endpointURL)
	p.mu.Lock()
	defer p.mu.Unlock()
	if proxy != nil {
		proxy.inflight--
	}
	if ctx.Err() == nil {
		if proxy == nil {
			p.directChecks[key.profile] = time.Now().UTC()
		}
		p.recordHealth(key, result, time.Since(start), false)
	}
}

func diagnose(ctx context.Context, check string, endpoint *url.URL) string {
	method, target := http.MethodHead, "https://store.steampowered.com/"
	var bootstrap *FixtureNetwork
	if check == "google_doh" {
		method = http.MethodGet
		query := dnsmessage.Message{Header: dnsmessage.Header{RecursionDesired: true}, Questions: []dnsmessage.Question{
			{Name: dnsmessage.MustNewName("example.com."), Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET},
		}}
		data, err := query.Pack()
		if err != nil {
			return "upstream_failure"
		}
		target = "https://dns.google/dns-query?dns=" + base64.RawURLEncoding.EncodeToString(data)
		// The compiled resolver's public bootstrap address is used only for
		// this check. TLS still verifies dns.google. No local DNS fallback.
		bootstrap = &FixtureNetwork{Lookup: func(_ context.Context, _, host string) ([]netip.Addr, error) {
			if host != "dns.google." {
				return nil, errors.New("invalid diagnostic host")
			}
			return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
		}}
	} else if check != "steam" {
		return "upstream_failure"
	}
	checkCtx, cancel := context.WithTimeout(ctx, diagnosticBudget)
	defer cancel()
	req, err := http.NewRequestWithContext(checkCtx, method, target, nil)
	if err != nil {
		return "upstream_failure"
	}
	if check == "google_doh" {
		req.Header.Set("Accept", "application/dns-message")
	}
	transport := routeTransport(endpoint, false, bootstrap)
	defer transport.CloseIdleConnections()
	return diagnosticResult(transport, req, check)
}

func diagnosticResult(transport http.RoundTripper, req *http.Request, check string) string {
	res, err := transport.RoundTrip(req)
	if err != nil {
		return "transport_failure"
	}
	defer res.Body.Close()
	if check == "steam" {
		if res.StatusCode >= 200 && res.StatusCode < 400 {
			return "ok"
		}
		return "upstream_failure"
	}
	media, _, _ := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if res.StatusCode != http.StatusOK || media != "application/dns-message" {
		return "upstream_failure"
	}
	var answer dnsmessage.Message
	data, err := io.ReadAll(io.LimitReader(res.Body, 2049))
	if err != nil {
		return "transport_failure"
	}
	if len(data) > 2048 || answer.Unpack(data) != nil || !answer.Response || answer.ID != 0 || answer.Truncated ||
		answer.RCode != dnsmessage.RCodeSuccess || len(answer.Questions) != 1 || answer.Questions[0] !=
		(dnsmessage.Question{Name: dnsmessage.MustNewName("example.com."), Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}) {
		return "upstream_failure"
	}
	for _, record := range answer.Answers {
		if record.Header.Type == dnsmessage.TypeA && record.Header.Class == dnsmessage.ClassINET {
			return "ok"
		}
	}
	return "upstream_failure"
}
