// Package egress implements the Worker-owned network boundary and its wire policy.
package egress

import (
	"errors"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/idna"
)

const (
	Direct        = "direct"
	PreferProxy   = "prefer_proxy"
	RequireProxy  = "require_proxy"
	MaxPaths      = 32
	MaxPermitTime = 5 * time.Second
)

var (
	ErrPolicy      = errors.New("network_policy_denied")
	ErrUnavailable = errors.New("network_unavailable")
	ErrBudget      = errors.New("network_budget_exhausted")
	ErrDeferred    = errors.New("network_quota_deferred")
)

type Path struct {
	ID     string
	Kind   string
	Region string
}

// Path is always sent without any network address or credential.
type Advertisement struct {
	Profile  string `json:"profile"`
	Mode     string `json:"mode"`
	ID       string `json:"id"`
	Kind     string `json:"kind"`
	Region   string `json:"region"`
	State    string `json:"state"`
	Capacity int32  `json:"capacity"`
}

func (a Advertisement) Path() Path { return Path{a.ID, a.Kind, a.Region} }

type Plan struct {
	Profile     string          `json:"profile"`
	Mode        string          `json:"mode"`
	AllowDirect bool            `json:"allow_direct,omitempty"`
	Paths       []Advertisement `json:"paths"`
}

func Mode(s string) bool      { return s == Direct || s == PreferProxy || s == RequireProxy }
func ProxyKind(s string) bool { return s == "http" || s == "https" || s == "socks5" }

// Source is fixed to compiled origins or the canonical Web Target.
// Fixture is set only by the integration test registry.
type Source struct {
	Origins        []string
	WebMethod      string // GET for Web acquisition or HEAD for HTTPS certificate evidence.
	WebPath        string // Empty means the registered origin root.
	Candidate      bool   // An immutable homepage Observation can authorize the first path.
	Robots         bool   // The control plane checks current Robots policy before issuing a permit.
	RDAP           bool
	Steam          bool
	FixedPath      string // Exact fixed-source path. Redirects are disabled.
	FixtureNetwork *FixtureNetwork
	AllowDirect    bool
	RequireRegion  bool
	Replay         bool
	MaxRequests    int32
	MaxBodyBytes   int64
	ContentTypes   []string
	Fixture        bool
}

func (s Source) Valid() bool {
	if len(s.Origins) > 8 || s.MaxRequests < 1 || s.MaxRequests > 32 ||
		s.MaxBodyBytes < 1 || s.MaxBodyBytes > 2<<20 || len(s.ContentTypes) < 1 || len(s.ContentTypes) > 8 {
		return false
	}
	if s.Steam && (s.RDAP || s.WebMethod != "" || s.FixedPath == "" || len(s.Origins) != 1 || !strings.HasPrefix(s.Origins[0], "https://") || s.Fixture || s.Replay || s.MaxRequests != 1) {
		return false
	}
	if s.RDAP {
		return s.WebMethod == "" && s.WebPath == "" && !s.Candidate && !s.Robots && len(s.Origins) == 0 &&
			s.FixedPath == "" && !s.Fixture && !s.Replay && s.MaxRequests == 2 && s.MaxBodyBytes == RDAPMaxBytes
	}
	if s.WebMethod != "" {
		return len(s.Origins) == 0 && s.FixedPath == "" && !s.Fixture && !s.Replay && s.MaxRequests <= 4 &&
			(s.WebMethod == "GET" && (s.WebPath == "" || DiscoveryPath(s.WebPath)) && (!s.Candidate || s.Robots) ||
				s.WebMethod == "HEAD" && s.WebPath == "" && !s.Candidate && !s.Robots && s.MaxRequests == 1)
	}
	if s.WebPath != "" || s.Candidate || s.Robots {
		return false
	}
	if len(s.Origins) == 0 || (s.FixedPath != "" && !WebRequestURI(s.FixedPath)) {
		return false
	}
	for i, origin := range s.Origins {
		u, err := url.Parse(origin)
		if err != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
			origin != u.Scheme+"://"+u.Host || s.Origin(u) != i {
			return false
		}
	}
	return true
}

func (s Source) Origin(u *url.URL) int {
	if u == nil || u.Opaque != "" || u.User != nil || u.Fragment != "" ||
		(u.Scheme != "http" && u.Scheme != "https") || !ValidHostPort(u) {
		return -1
	}
	if ip, err := netip.ParseAddr(u.Hostname()); err == nil && !allowedIP(ip, s.Fixture) {
		return -1
	}
	for i, origin := range s.Origins {
		if u.Scheme+"://"+u.Host == origin {
			return i
		}
	}
	return -1
}

func ValidHostPort(u *url.URL) bool {
	host := u.Hostname()
	if host == "" || strings.ContainsAny(u.Host, "%\\ \r\n\t") {
		return false
	}
	if p := u.Port(); p != "" {
		port, err := strconv.ParseUint(p, 10, 16)
		if err != nil || port == 0 || u.Host != net.JoinHostPort(host, p) {
			return false
		}
	} else if u.Host != host && u.Host != "["+host+"]" {
		return false
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		return ip.Zone() == ""
	}
	name, err := idna.Lookup.ToASCII(host)
	if err != nil || name != host || len(host) > 253 || host != strings.ToLower(host) {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	return true
}

var reserved = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("168.63.129.16/32"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"), netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"), netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001::/23"), netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"), netip.MustParsePrefix("3fff::/20"),
}
var publicV6 = netip.MustParsePrefix("2000::/3")

func allowedIP(ip netip.Addr, fixture bool) bool {
	ip = ip.Unmap()
	if fixture && ip.IsLoopback() {
		return true
	}
	if !ip.IsGlobalUnicast() || ip.Zone() != "" || ip.IsPrivate() ||
		(ip.Is6() && !publicV6.Contains(ip)) {
		return false
	}
	for _, p := range reserved {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}
