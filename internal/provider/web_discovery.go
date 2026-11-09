package provider

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/librecosta/harvest/internal/egress"
)

const (
	robotsLimit   = 512 << 10
	sitemapLimit  = 512 << 10
	assetLimit    = 128 << 10
	manifestLimit = 32 << 10
)

var discoveryDiagnostics = []string{
	"robots_unavailable", "robots_blocked", "candidate_unavailable", "rdap_bootstrap_unavailable", "rdap_registry_missing",
	"source_missing", "source_redirect", "source_invalid", "source_incomplete", "upstream_unavailable",
}

func discoveryCapabilities(fixture *egress.FixtureNetwork) []Capability {
	var result []Capability
	for _, spec := range []struct {
		name, resource, path string
		interval             time.Duration
		limit                int64
		execute              func(context.Context, Task, egress.Client) Result
		normalize            func(Task, string, jsontext.Value) (jsontext.Value, jsontext.Value, error)
	}{
		{"robots", "origin", egress.RobotsPath, 24 * time.Hour, robotsLimit, acquireRobots, normalizeRobots},
		{"sitemap", "origin", egress.SitemapPath, 24 * time.Hour, sitemapLimit, acquireSitemap, normalizeSitemap},
		{"assets", "origin", egress.FaviconPath, 24 * time.Hour, assetLimit, acquireAssets, normalizeAssets},
		{"rdap", "domain", "", 7 * 24 * time.Hour, egress.RDAPMaxBytes, acquireRDAP, normalizeRDAP},
	} {
		name := spec.name
		source := &egress.Source{AllowDirect: true, RequireRegion: true, WebMethod: http.MethodGet, WebPath: spec.path,
			MaxRequests: 3, MaxBodyBytes: spec.limit, ContentTypes: []string{"application/octet-stream"}, FixtureNetwork: fixture}
		params := Normalize[struct{}]
		var empty string
		switch name {
		case "robots":
			empty = "robots_missing"
		case "assets":
			params = normalizeAssetParams
			source.Candidate, source.Robots = true, true
		case "sitemap":
			source.Robots = true
		case "rdap":
			source.RDAP, source.WebMethod, source.MaxRequests = true, "", 2
		}
		policy := webPolicy(func(t Task) []SegmentScope { return []SegmentScope{discoveryScope(t, name)} }, empty)
		policy.NormalizeDiagnosticData = normalizeDiscoveryDiagnostic
		policy.DiagnosticEvidence = append(slices.Clone(policy.DiagnosticEvidence), discoveryDiagnostics...)
		policy.NormalizeScopedData = func(t Task, _ SegmentScope, state string, data jsontext.Value) (jsontext.Value, jsontext.Value, error) {
			return spec.normalize(t, state, data)
		}
		result = append(result, Capability{Offer: Offer{"web." + name, 1, 1, "web_v1"}, Resource: spec.resource, MinInterval: spec.interval,
			Timeout: 30 * time.Second, NormalizeParams: params, Execute: spec.execute, Network: source, Observation: policy})
	}
	return result
}

func discoveryScope(t Task, name string) SegmentScope {
	scope := webScope(t, name)
	if name == "assets" {
		params, _ := Object[AssetParams](t.Params)
		scope.Dimensions["asset"] = params.Kind
		scope.Dimensions["selection"] = "conventional"
		if params.ObservationID != nil {
			scope.Dimensions["selection"] = "discovered"
		}
	}
	return scope
}

func discoveryRequest(ctx context.Context, t Task, client egress.Client, path, accept string) (*http.Response, error) {
	if !egress.DiscoveryPath(path) {
		return nil, egress.ErrPolicy
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, t.ExternalKey+path, nil)
	if err != nil {
		return nil, egress.ErrPolicy
	}
	r.Header.Set("Accept", accept)
	r.Header.Set("User-Agent", egress.DiscoveryUserAgent)
	return client.Do(r)
}

func discoveryResult(scope SegmentScope, code string, data any) Result {
	scopes := []SegmentScope{scope}
	if data == nil {
		state := "unavailable"
		if code == "source_invalid" || code == "source_incomplete" {
			state = "invalid"
		}
		return webResult(scopes, code, webSegment(scope, state, code, nil))
	}
	return webResult(scopes, "", webSegment(scope, "present", "", data))
}

func discoveryNetworkCode(err error) string {
	for _, known := range []error{egress.ErrRobotsUnknown, egress.ErrRobotsBlocked, egress.ErrCandidate,
		egress.ErrBootstrapUnavailable, egress.ErrRegistryMissing} {
		if errors.Is(err, known) {
			return known.Error()
		}
	}
	return webNetworkCode(err)
}

func discoveryStatus(status int) string {
	if code := webStatusCode(status); code != "" {
		return code
	}
	switch {
	case status == http.StatusOK:
		return ""
	case status == http.StatusNotFound || status == http.StatusGone:
		return "source_missing"
	case status >= 300 && status < 400:
		return "source_redirect"
	case status >= 500:
		return "upstream_unavailable"
	default:
		return "source_invalid"
	}
}

type redirectHint struct {
	URL      string `json:"url"`
	Verified bool   `json:"verified"`
}

func discoveryFailure(scope SegmentScope, res *http.Response, code string) Result {
	if code == "source_redirect" {
		if target, err := res.Location(); err == nil {
			if link := pageURL(nil, target.String()); link != "" {
				return webResult([]SegmentScope{scope}, code, webSegment(scope, "unavailable", code, redirectHint{URL: link}))
			}
		}
	}
	return discoveryResult(scope, code, nil)
}

func normalizeDiscoveryDiagnostic(code string, data jsontext.Value) (jsontext.Value, error) {
	v, err := ObservationObject[redirectHint](data)
	if err != nil || code != "source_redirect" || v.Verified || v.URL == "" || v.URL != pageURL(nil, v.URL) {
		return nil, errors.New("invalid unresolved redirect")
	}
	clean, _, err := webData(true, v, nil)
	return clean, err
}

func discoveryBody(res *http.Response, limit int64, gzipFile bool) ([]byte, string) {
	body, err := egress.ReadBounded(res, limit, gzipFile)
	switch {
	case errors.Is(err, egress.ErrBudget):
		return nil, "source_incomplete"
	case errors.Is(err, egress.ErrResponse):
		return nil, "source_invalid"
	case err != nil:
		return nil, webNetworkCode(err)
	}
	return body, ""
}

func mediaType(res *http.Response) string {
	media, _, _ := mime.ParseMediaType(res.Header.Get("Content-Type"))
	return media
}

func finalURL(res *http.Response) *url.URL {
	if res.Request == nil {
		return nil
	}
	return res.Request.URL
}

func siteURL(raw, origin string) bool {
	return raw != "" && strings.HasPrefix(raw, origin+"/") && safeWebURL(nil, raw) == raw
}
