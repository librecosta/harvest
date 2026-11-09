package egress

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
)

const MaxWebRedirects = 3

var (
	ErrTLSName      = errors.New("tls_name_mismatch")
	ErrTLSExpired   = errors.New("tls_expired")
	ErrTLSUntrusted = errors.New("tls_untrusted")
	ErrTLSVerify    = errors.New("tls_verification_failed")
	ErrProxy        = errors.New("proxy_unavailable")
	ErrResponse     = errors.New("invalid_source_response")
)

// WebTarget permits the canonical Target's standard HTTP(S) endpoint only.
// A literal IP is checked here as well as at the connect boundary.
func (s Source) WebTarget(key string) bool {
	u, err := url.Parse(key)
	return err == nil && s.WebMethod != "" && u.Path == "" && u.RawQuery == "" &&
		!u.ForceQuery && key == u.Scheme+"://"+u.Host && s.webURL(u, key)
}

func (s Source) webURL(u *url.URL, destination string) bool {
	if u == nil || u.Opaque != "" || u.User != nil || u.Fragment != "" ||
		(u.Scheme != "https" && (u.Scheme != "http" || s.WebMethod == http.MethodHead)) ||
		u.Scheme+"://"+u.Host != destination || !ValidHostPort(u) || u.Port() != "" {
		return false
	}
	if ip, err := netip.ParseAddr(u.Hostname()); err == nil {
		return allowedIP(ip, false)
	}
	return strings.Contains(u.Hostname(), ".")
}

func WebRequestURI(uri string) bool {
	if len(uri) > 2048 || !strings.HasPrefix(uri, "/") || strings.ContainsAny(uri, "#\\") {
		return false
	}
	u, err := url.ParseRequestURI(uri)
	return err == nil && u.RequestURI() == uri && !strings.HasPrefix(u.Path, "//") &&
		!strings.ContainsAny(u.Path, "\\\x00\r\n")
}

func RedirectDigest(uri string) string {
	sum := sha256.Sum256([]byte(uri))
	return hex.EncodeToString(sum[:])
}

func redirectStatus(code int) bool {
	switch code {
	case 301, 302, 303, 307, 308:
		return true
	}
	return false
}

// webDo follows only responses produced by this session. Using RoundTrip
// directly also preserves valid response/TLS evidence on malformed Location.
func (s *Session) webDo(r *http.Request) (*http.Response, error) {
	for hop := 0; ; hop++ {
		res, err := s.RoundTrip(r)
		if err != nil {
			return nil, safeError(err)
		}
		if hop >= int(s.source.MaxRequests)-1 || !redirectStatus(res.StatusCode) {
			return res, nil
		}
		next, err := res.Location()
		if err != nil || !s.webRedirect(next) {
			return res, nil
		}
		_ = res.Body.Close()
		follow := r.Clone(r.Context())
		follow.URL, follow.Host, follow.Response = next, "", res
		r = follow
	}
}

func (s *Session) firstPath() string {
	if s.source.Candidate && s.target.Path != "" {
		return s.target.Path
	}
	return s.source.InitialPath()
}

func (s Source) InitialPath() string {
	if s.WebPath == "" {
		return "/"
	}
	return s.WebPath
}

func (s *Session) webRedirect(u *url.URL) bool {
	return s.source.WebMethod == http.MethodGet && s.source.webURL(u, s.target.Origin) &&
		WebRequestURI(u.RequestURI()) && (s.source.WebPath == "" || DiscoveryPath(u.RequestURI()))
}
