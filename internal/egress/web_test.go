package egress

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"testing"
	"uuid"
)

func TestWebViaHTTPSConnect(t *testing.T) {
	const origin = "http://source.example.com"
	const pinned = "93.184.215.14"
	proxy := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect || r.Host != pinned+":80" || r.Header.Get("Proxy-Authorization") == "" {
			t.Errorf("expected authenticated numeric CONNECT, got %s %q", r.Method, r.Host)
			w.WriteHeader(403)
			return
		}
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		_, _ = rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
		_ = rw.Flush()
		next, err := http.ReadRequest(rw.Reader)
		if err != nil || next.Host != "source.example.com" || next.Method != http.MethodGet || next.URL.Path != "/" ||
			next.Header.Get("Proxy-Authorization") != "" {
			t.Errorf("bad tunneled dynamic Web request: %v", err)
			return
		}
		_, _ = rw.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
		_ = rw.Flush()
	}))
	defer proxy.Close()
	u, _ := url.Parse(proxy.URL)
	u.User = url.UserPassword("fixture", "secret")
	roots := x509.NewCertPool()
	roots.AddCert(proxy.Certificate())
	pool := NewPool(Config{
		Profiles:  []ProfileConfig{{Name: "strict", Mode: RequireProxy, Endpoints: []string{"tls-edge"}}},
		Endpoints: []EndpointConfig{{ID: "tls-edge", URL: u.String(), Concurrency: 1, Enabled: true, BytesPerMinute: 1 << 20}},
	})
	defer pool.Close()
	gate := new(testGate)
	source := Source{WebMethod: http.MethodGet, AllowDirect: true, MaxRequests: 1, MaxBodyBytes: 1024, ContentTypes: []string{"text/html"},
		FixtureNetwork: &FixtureNetwork{RootCAs: roots, Lookup: func(_ context.Context, network, host string) ([]netip.Addr, error) {
			if network != "ip" || host != "source.example.com." {
				t.Errorf("unexpected lookup %q %q", network, host)
			}
			return []netip.Addr{netip.MustParseAddr(pinned)}, nil
		}},
	}
	session := pool.Session(t.Context(), Plan{Profile: "strict", Mode: RequireProxy, Paths: pool.Advertise(1)}, source,
		"scope", "web.http", gate, WebDestination{ID: uuid.NewV7(), Origin: origin})
	defer session.Close()
	for _, test := range []struct{ url, host, header string }{
		{origin + "/other", "", ""}, {origin + "/", "", "Authorization"}, {origin + "/", "elsewhere.example.com", ""},
		{"http://elsewhere.example.com/", "", ""}, {origin + ":8080/", "", ""},
	} {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, test.url, nil)
		req.Host = test.host
		if test.header != "" {
			req.Header.Set(test.header, "test")
		}
		if resp, err := session.Client().Do(req); !errors.Is(err, ErrPolicy) {
			if resp != nil {
				resp.Body.Close()
			}
			t.Fatalf("dynamic route allowed invalid request: %+v / %v", test, err)
		}
	}
	if gate.acquired.Load() != 0 {
		t.Fatal("invalid request acquired a permit")
	}
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, origin+"/", nil)
	res, err := session.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if err != nil || string(data) != "ok" || ResponsePath(res).Kind != "https" || gate.acquired.Load() != 1 || gate.finished.Load() != 1 {
		t.Fatalf("HTTPS CONNECT dynamic chain not correctly metered: %s / %v", data, err)
	}
}
