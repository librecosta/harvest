package egress

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"unicode"

	"golang.org/x/net/publicsuffix"
)

const (
	RobotsPath               = "/robots.txt"
	SitemapPath              = "/sitemap.xml"
	FaviconPath              = "/favicon.ico"
	DiscoveryUserAgent       = "HarvestBot/0.1 (+https://github.com/librecosta/harvest)"
	BootstrapURL             = "https://data.iana.org/rdap/dns.json"
	RDAPMaxBytes             = 256 << 10
	RDAPBootstrap      int16 = 1
	RDAPLookup         int16 = 2
)

var (
	ErrRobotsUnknown        = errors.New("robots_unavailable")
	ErrRobotsBlocked        = errors.New("robots_blocked")
	ErrCandidate            = errors.New("candidate_unavailable")
	ErrBootstrapRequired    = errors.New("rdap_bootstrap_required")
	ErrBootstrapReady       = errors.New("rdap_bootstrap_ready")
	ErrBootstrapUnavailable = errors.New("rdap_bootstrap_unavailable")
	ErrRegistryMissing      = errors.New("rdap_registry_missing")
)

// DiscoveryPath excludes query data, encoded separators, controls, and dot
// segments. The registered origin and authority are checked separately.
func DiscoveryPath(uri string) bool {
	if len(uri) > 512 || !WebRequestURI(uri) || strings.ContainsAny(uri, "?#") {
		return false
	}
	u, err := url.ParseRequestURI(uri)
	if err != nil || strings.ContainsAny(u.Path, "%\\") || strings.Contains(u.Path, "//") ||
		strings.Contains(strings.ToLower(uri), "%2f") {
		return false
	}
	for part := range strings.SplitSeq(u.Path, "/") {
		if part == "." || part == ".." {
			return false
		}
		for _, r := range part {
			if r <= 32 || unicode.IsControl(r) {
				return false
			}
		}
	}
	return true
}

func RegistrableDomain(name string) (string, error) {
	if !ValidHostPort(&url.URL{Host: name}) {
		return "", ErrPolicy
	}
	_, icann := publicsuffix.PublicSuffix(name)
	domain, err := publicsuffix.EffectiveTLDPlusOne(name)
	if !icann || err != nil {
		return "", ErrPolicy
	}
	return domain, nil
}

// Bootstrap is a bounded RFC 9224 DNS service map. HTTPS, public-looking
// names, standard port and a clean path are mandatory for every chosen base.
type Bootstrap map[string]string

func ParseBootstrap(raw jsontext.Value) (Bootstrap, error) {
	var doc struct {
		Version  string       `json:"version"`
		Services [][][]string `json:"services"`
	}
	if len(raw) == 0 || len(raw) > RDAPMaxBytes || json.Unmarshal(raw, &doc) != nil ||
		doc.Version != "1.0" || len(doc.Services) == 0 || len(doc.Services) > 2048 {
		return nil, ErrBootstrapUnavailable
	}
	result := make(Bootstrap)
	seen := make(map[string]bool)
	for _, entry := range doc.Services {
		if len(entry) != 2 || len(entry[0]) == 0 || len(entry[1]) == 0 || len(entry[1]) > 32 {
			return nil, ErrBootstrapUnavailable
		}
		var base string
		for _, candidate := range entry[1] {
			if u, err := rdapURL(candidate); err == nil && strings.HasSuffix(u.EscapedPath(), "/") {
				base = u.String()
				break
			}
		}
		for _, suffix := range entry[0] {
			if !ValidHostPort(&url.URL{Host: suffix}) || strings.ContainsAny(suffix, ":[]") ||
				seen[suffix] || len(seen) == 4096 {
				return nil, ErrBootstrapUnavailable
			}
			seen[suffix] = true
			if base != "" {
				result[suffix] = base
			}
		}
	}
	if len(result) == 0 {
		return nil, ErrBootstrapUnavailable
	}
	return result, nil
}

func (b Bootstrap) Endpoint(name string) string {
	for suffix := name; suffix != ""; {
		if base := b[suffix]; base != "" {
			return base + "domain/" + name
		}
		_, suffix, _ = strings.Cut(suffix, ".")
	}
	return ""
}

func rdapURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Opaque != "" || u.User != nil || strings.Contains(raw, "#") ||
		u.Port() != "" || !ValidHostPort(u) || !strings.Contains(u.Hostname(), ".") ||
		!DiscoveryPath(u.RequestURI()) || u.String() != raw {
		return nil, ErrPolicy
	}
	if _, err := netip.ParseAddr(u.Hostname()); err == nil {
		return nil, ErrPolicy
	}
	last := u.Hostname()[strings.LastIndexByte(u.Hostname(), '.')+1:]
	switch last {
	case "local", "localhost", "internal", "test", "invalid", "example", "onion", "arpa", "lan":
		return nil, ErrPolicy
	}
	return u, nil
}

func RegistryEndpoint(raw, domain string) (*url.URL, error) {
	u, err := rdapURL(raw)
	if err != nil || !strings.HasSuffix(u.EscapedPath(), "/domain/"+domain) {
		return nil, ErrPolicy
	}
	return u, nil
}

// RegistryClient exposes the single compiled RDAP operation without giving a
// Provider a destination selector. The control plane supplies the registry URL.
type RegistryClient interface {
	LookupRDAP(context.Context) (*http.Response, error)
}

type rdapStageKey struct{}

func (c scopedClient) LookupRDAP(ctx context.Context) (*http.Response, error) {
	s := c.session
	if !s.source.RDAP {
		return nil, ErrPolicy
	}
	run := func(stage int16) (*http.Response, error) {
		req, err := http.NewRequestWithContext(context.WithValue(ctx, rdapStageKey{}, stage), http.MethodGet, BootstrapURL, nil)
		if err != nil {
			return nil, ErrPolicy
		}
		req.Header.Set("Accept", "application/rdap+json, application/json")
		req.Header.Set("User-Agent", DiscoveryUserAgent)
		res, err := s.RoundTrip(req)
		if err != nil {
			return nil, safeError(err)
		}
		return res, nil
	}
	res, err := run(RDAPLookup)
	if !errors.Is(err, ErrBootstrapRequired) {
		return res, err
	}
	res, err = run(RDAPBootstrap)
	if err != nil && !errors.Is(err, ErrBootstrapReady) {
		return nil, err
	}
	if res != nil {
		_ = res.Body.Close()
		switch res.StatusCode {
		case http.StatusOK:
		case http.StatusTooManyRequests, http.StatusForbidden, http.StatusUnauthorized:
			return res, nil
		default:
			return nil, ErrBootstrapUnavailable
		}
	}
	res, err = run(RDAPLookup)
	if errors.Is(err, ErrBootstrapRequired) {
		return nil, ErrBootstrapUnavailable
	}
	return res, err
}
