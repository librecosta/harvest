package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/librecosta/harvest/internal/egress"
)

const (
	webBodyLimit = 512 << 10
	dnsBodyLimit = 16 << 10
	resolverID   = "google-public-dns"
	resolverPath = "/dns-query"
)

var webDiagnostics = []string{
	"policy_denied", "proxy_unavailable", "transport_error", "timeout", "network_quota_deferred", "network_budget_exhausted",
	"tls_name_mismatch", "tls_expired", "tls_untrusted", "tls_verification_failed",
	"body_incomplete", "html_invalid", "unsupported_encoding",
	"resolver_unavailable", "resolver_servfail", "resolver_refused", "dns_invalid",
	"upstream_throttled", "upstream_auth",
}

// WebManifest uses a fixed resolver and the OS trust store in production.
// Only the integration binary supplies a fixture network.
func WebManifest(fixture *egress.FixtureNetwork) Manifest {
	return Manifest{
		Name:      "web",
		Resources: []Resource{{"domain", domain}, {"origin", origin}},
		Capabilities: []Capability{
			{
				Offer: Offer{"web.http", 1, 1, "web_v1"}, Resource: "origin",
				MinInterval: 15 * time.Minute, Timeout: 45 * time.Second, Partial: true,
				NormalizeParams: Normalize[struct{}], Execute: acquireHTTP,
				Network: &egress.Source{WebMethod: http.MethodGet, AllowDirect: true, RequireRegion: true,
					MaxRequests: 1 + egress.MaxWebRedirects, MaxBodyBytes: webBodyLimit, ContentTypes: []string{"text/html", "application/xhtml+xml"}, FixtureNetwork: fixture},
				Observation: webPolicy(httpScopes, ""),
			},
			{
				Offer: Offer{"web.tls", 1, 1, "web_v1"}, Resource: "origin",
				MinInterval: 6 * time.Hour, Timeout: 15 * time.Second,
				NormalizeParams: Normalize[struct{}], Execute: acquireTLS,
				Network: &egress.Source{WebMethod: http.MethodHead, AllowDirect: true, RequireRegion: true,
					MaxRequests: 1, MaxBodyBytes: 1024, ContentTypes: []string{"text/html"}, FixtureNetwork: fixture},
				Observation: webPolicy(func(t Task) []SegmentScope { return []SegmentScope{webScope(t, "tls")} }, ""),
			},
			{
				Offer: Offer{"web.dns", 1, 1, "web_v1"}, Resource: "domain",
				MinInterval: time.Hour, Timeout: 90 * time.Second, Partial: true,
				NormalizeParams: normalizeDNSParams, Execute: acquireDNS,
				Network: &egress.Source{Origins: []string{"https://dns.google"}, FixedPath: resolverPath,
					AllowDirect: true, RequireRegion: true, MaxRequests: 8, MaxBodyBytes: dnsBodyLimit,
					ContentTypes: []string{"application/dns-message"}, FixtureNetwork: fixture},
				Observation: webPolicy(dnsScopes, "dns_response"),
			},
		},
	}
}

func webPolicy(scopes func(Task) []SegmentScope, empty string) *ObservationPolicy {
	return &ObservationPolicy{
		Scopes: scopes, Source: "web", MaxDataBytes: 32 << 10, EmptyEvidence: empty,
		DiagnosticEvidence: webDiagnostics, NormalizeScopedData: normalizeWebData,
	}
}

func webScope(t Task, kind string) SegmentScope {
	return SegmentScope{Namespace: t.Namespace, Scope: t.Scope, Dimensions: map[string]string{"section": kind}}
}

func httpScopes(t Task) []SegmentScope {
	scopes := []SegmentScope{webScope(t, "http"), webScope(t, "html")}
	if strings.HasPrefix(t.ExternalKey, "https://") {
		scopes = append(scopes, webScope(t, "tls"))
	}
	return scopes
}

func webSegment(scope SegmentScope, state, evidence string, data any) Segment {
	seg := Segment{Scope: scope, OutputSchema: 1, Source: "web", State: state, Evidence: evidence, ObservedAt: time.Now().UTC()}
	if data != nil {
		value, err := json.Marshal(data, json.Deterministic(true))
		if err != nil || len(value) > 32<<10 {
			seg.State, seg.Evidence = "invalid", "html_invalid"
		} else {
			seg.Data = value
		}
	}
	return seg
}

func webResult(scopes []SegmentScope, code string, segments ...Segment) Result {
	if len(segments) == 0 {
		for _, scope := range scopes {
			segments = append(segments, webSegment(scope, "unavailable", code, nil))
		}
	}
	valid := 0
	for _, seg := range segments {
		if seg.State == "present" || seg.State == "confirmed_empty" {
			valid++
		}
	}
	state := "partial"
	switch valid {
	case len(scopes):
		state, code = "succeeded", ""
	case 0:
		state = "failed"
	}
	if state != "succeeded" && code == "" {
		code = "incomplete"
	}
	return Result{State: state, Code: code, Segments: segments}
}

func webNetworkCode(err error) string {
	for _, known := range []error{egress.ErrTLSName, egress.ErrTLSExpired, egress.ErrTLSUntrusted, egress.ErrTLSVerify, egress.ErrProxy} {
		if errors.Is(err, known) {
			return known.Error()
		}
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "timeout"
	case errors.Is(err, egress.ErrPolicy):
		return "policy_denied"
	case errors.Is(err, egress.ErrDeferred):
		return "network_quota_deferred"
	case errors.Is(err, egress.ErrBudget):
		return "network_budget_exhausted"
	default:
		return "transport_error"
	}
}

func hashWeb(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func webDigest(s string) bool {
	digest, err := hex.DecodeString(s)
	return err == nil && len(digest) == sha256.Size && hex.EncodeToString(digest) == s
}

func cleanText(value string, limit int) string {
	var out strings.Builder
	var space bool
	for _, r := range value {
		if unicode.IsSpace(r) {
			space = out.Len() > 0
			continue
		}
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			continue
		}
		if out.Len()+len(string(r))+1 > limit {
			break
		}
		if space {
			out.WriteByte(' ')
			space = false
		}
		out.WriteRune(r)
	}
	return out.String()
}

func normalizeWebData(task Task, scope SegmentScope, state string, value jsontext.Value) (jsontext.Value, jsontext.Value, error) {
	switch scope.Dimensions["section"] {
	case "http":
		return normalizeHTTP(task, state, value)
	case "html":
		return normalizeHTML(state, value)
	case "tls":
		return normalizeTLS(task, state, value)
	case "dns":
		return normalizeDNS(task, scope, state, value)
	default:
		return nil, nil, errors.New("unknown Web observation section")
	}
}

func webData[T any](valid bool, data T, semantic any) (jsontext.Value, jsontext.Value, error) {
	if !valid {
		return nil, nil, errors.New("invalid Web observation")
	}
	encoded, err := json.Marshal(data, json.Deterministic(true))
	if semantic == nil || err != nil {
		return encoded, encoded, err
	}
	projected, err := json.Marshal(semantic, json.Deterministic(true))
	return encoded, projected, err
}

func validText(value string, n int) bool {
	return cleanText(value, n) == value
}

func webStatusCode(status int) string {
	switch status {
	case http.StatusTooManyRequests:
		return "upstream_throttled"
	case http.StatusUnauthorized, http.StatusForbidden:
		return "upstream_auth"
	}
	return ""
}

func validVantage(kind string) bool {
	return kind == "" || kind == egress.Direct || egress.ProxyKind(kind)
}

func sortedUnique(values []string) []string {
	slices.Sort(values)
	return slices.Compact(values)
}
