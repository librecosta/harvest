package egress

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

type probeTransport func(*http.Request) (*http.Response, error)

func (f probeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestDiagnosticDistinguishesProxyAndUpstream(t *testing.T) {
	ctx := t.Context()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	proxyURL := "http://" + listener.Addr().String()
	pool := NewPool(Config{
		Profiles:  []ProfileConfig{{Name: "home", Mode: Direct, DirectRegion: "cn"}, {Name: "overseas", Mode: RequireProxy, Endpoints: []string{"intl"}}},
		Endpoints: []EndpointConfig{{ID: "intl", URL: proxyURL, Region: "intl", Enabled: true, Concurrency: 1, CooldownSeconds: 2}},
	})
	defer pool.Close()
	pool.Probe(ctx)
	rows := pool.TakeHealth()
	if len(rows) != 1 || rows[0].CheckType != "proxy_connection" || rows[0].Successes != 1 {
		t.Fatalf("TCP connection did not register: %+v", rows)
	}
	now := time.Now()
	direct, selected := pool.nextCheck(now)
	if direct.kind != Direct || direct.region != "cn" || selected != nil {
		t.Fatalf("unexpected direct check %+v", direct)
	}
	pool.nextCheck(now.Add(diagnosticSpacing))
	overseas, selected := pool.nextCheck(now.Add(2 * diagnosticSpacing))
	if overseas.region != "intl" || selected == nil || overseas.kind != "http" {
		t.Fatalf("diagnostic did not pick declared overseas proxy: %+v", overseas)
	}
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://dns.google/dns-query", nil)
	unreachable := probeTransport(func(*http.Request) (*http.Response, error) { return nil, errors.New("timeout") })
	if got := diagnosticResult(unreachable, request, "google_doh"); got != "transport_failure" {
		t.Fatalf("DoH failure was classified as %s", got)
	}
	answer := dnsmessage.Message{Header: dnsmessage.Header{Response: true}, Questions: []dnsmessage.Question{
		{Name: dnsmessage.MustNewName("example.com."), Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET},
	}, Answers: []dnsmessage.Resource{{
		Header: dnsmessage.ResourceHeader{Name: dnsmessage.MustNewName("example.com."), Class: dnsmessage.ClassINET},
		Body:   &dnsmessage.AResource{A: [4]byte{192, 0, 2, 10}},
	}}}
	data, err := answer.Pack()
	if err != nil {
		t.Fatal(err)
	}
	success := probeTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/dns-message"}}, Body: io.NopCloser(bytes.NewReader(data))}, nil
	})
	if got := diagnosticResult(success, request, "google_doh"); got != "ok" {
		t.Fatalf("valid DoH response returned %s", got)
	}
	failedUpstream := probeTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("unavailable"))}, nil
	})
	if got := diagnosticResult(failedUpstream, request, "steam"); got != "upstream_failure" {
		t.Fatalf("Steam HTTP failure returned %s", got)
	}

	// The proxy breaker still belongs to the existing active TCP probe.
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	endpoint := pool.endpoints["intl"]
	endpoint.nextProbe = time.Time{}
	pool.Probe(ctx)
	if rows := pool.TakeHealth(); endpoint.state != "cooldown" || len(rows) != 1 || rows[0].TransportFailures != 1 {
		t.Fatalf("proxy outage did not enter cooldown: %s %+v", endpoint.state, rows)
	}
	restored, err := net.Listen("tcp4", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	endpoint.nextProbe = time.Time{}
	pool.Probe(ctx)
	if rows := pool.TakeHealth(); endpoint.state != "degraded" || len(rows) != 1 || rows[0].Recoveries != 1 {
		t.Fatalf("proxy recovery did not take the half-open path: %s %+v", endpoint.state, rows)
	}
}
