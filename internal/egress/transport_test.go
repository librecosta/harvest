package egress

import (
	"bufio"
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
	"uuid"
)

func TestCheckedAddress(t *testing.T) {
	const public = "93.184.215.14"
	for _, value := range []string{"10.1.1.1", "127.0.0.1", "169.254.169.254", "168.63.129.16",
		"100.100.100.200", "::ffff:127.0.0.1", "fd00:ec2::254", "2002:a9fe:a9fe::1", "64:ff9b::a9fe:a9fe"} {
		t.Run(value, func(t *testing.T) {
			lookup := func(context.Context, string, string) ([]netip.Addr, error) {
				return []netip.Addr{netip.MustParseAddr(public), netip.MustParseAddr(value)}, nil
			}
			if _, err := checkedAddress(t.Context(), lookup, "allowed.example:443", false); !errors.Is(err, ErrPolicy) {
				t.Fatalf("mixed DNS result admitted: %v", err)
			}
		})
	}
	lookup := func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr(public)}, nil
	}
	if dest, err := checkedAddress(t.Context(), lookup, "allowed.example:443", false); err != nil || dest != public+":443" {
		t.Fatalf("verified address was not pinned: %q %v", dest, err)
	}
}

func TestHTTPSConnectProxy(t *testing.T) {
	const dest = "93.184.215.14:80"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect || r.Host != dest || r.Header.Get("Proxy-Authorization") == "" {
			t.Error("invalid TLS-protected CONNECT")
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
		if err != nil || next.Method != http.MethodGet || next.Header.Get("Proxy-Authorization") != "" {
			t.Errorf("bad tunneled source request: %v", err)
			return
		}
		_, _ = rw.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
		_ = rw.Flush()
	}))
	defer server.Close()
	proxy, _ := url.Parse(server.URL)
	proxy.User = url.UserPassword("fixture", "local_secret")
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	dial := &net.Dialer{}
	if conn, err := connectTunnel(ctx, dial, proxy, dest, x509.NewCertPool()); !errors.Is(err, errConnect) {
		if conn != nil {
			conn.Close()
		}
		t.Fatalf("untrusted proxy certificate accepted: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	conn, err := connectTunnel(ctx, dial, proxy, dest, roots)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	req, _ := http.NewRequest(http.MethodGet, "http://source.example/", nil)
	if err := req.Write(conn); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	content, err := io.ReadAll(resp.Body)
	if err != nil || string(content) != "ok" {
		t.Fatalf("missing TLS proxy payload: %v", err)
	}
}

func TestDisabledEndpointCannotSend(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("disabled proxy leaked traffic to origin")
	}))
	defer origin.Close()
	pool := NewPool(Config{
		Profiles:  []ProfileConfig{{Name: "strict", Mode: RequireProxy, Endpoints: []string{"disabled"}}},
		Endpoints: []EndpointConfig{{ID: "disabled", URL: "http://127.0.0.1:1", Concurrency: 1}},
	})
	defer pool.Close()
	pool.Probe(t.Context())
	paths := pool.Advertise(1)
	if len(paths) != 1 || paths[0].State != "disabled" {
		t.Fatal("disabled path was not advertised as disabled")
	}
	gate := new(testGate)
	s := pool.Session(t.Context(), Plan{Profile: "strict", Mode: RequireProxy, Paths: paths}, Source{
		Origins: []string{origin.URL}, AllowDirect: true, MaxRequests: 1, MaxBodyBytes: 1024, ContentTypes: []string{"text/plain"}, Fixture: true,
	}, "scope", "fixture.network", gate)
	defer s.Close()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, origin.URL, nil)
	if resp, err := s.Client().Do(req); !errors.Is(err, ErrUnavailable) {
		if resp != nil {
			resp.Body.Close()
		}
		t.Fatalf("disabled path was selected: %v", err)
	}
	if gate.acquired.Load() != 0 {
		t.Fatal("disabled path acquired a permit")
	}
}

type testGate struct{ acquired, finished atomic.Int32 }

func (g *testGate) Acquire(context.Context, Intent) (Grant, error) {
	g.acquired.Add(1)
	return Grant{ID: uuid.NewV7(), LeaseMS: 2000}, nil
}

func (g *testGate) Finish(context.Context, uuid.UUID, Usage) { g.finished.Add(1) }

func TestTransportDoesNotReplayReusedConnection(t *testing.T) {
	var hits atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 2 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "ok")
	}))
	defer origin.Close()
	pool := NewPool(Config{Profiles: []ProfileConfig{{Name: "direct", Mode: Direct}}})
	defer pool.Close()
	gate := new(testGate)
	s := pool.Session(t.Context(), Plan{Profile: "direct", Mode: Direct, Paths: pool.Advertise(1)}, Source{
		Origins: []string{origin.URL}, AllowDirect: true, Replay: true, MaxRequests: 3, MaxBodyBytes: 32, ContentTypes: []string{"text/plain"}, Fixture: true,
	}, "scope", "fixture.network", gate)
	defer s.Close()
	get := func() (*http.Response, error) {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, origin.URL, nil)
		return s.Client().Do(req)
	}
	resp, err := get()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp, err := get(); err == nil {
		resp.Body.Close()
		t.Fatal("transport silently replayed a sent GET")
	}
	if hits.Load() != 2 || gate.acquired.Load() != 2 || gate.finished.Load() != 2 {
		t.Fatalf("wire hits, grants and finished operations differ: %d/%d/%d", hits.Load(), gate.acquired.Load(), gate.finished.Load())
	}
}
