//go:build integration

package integration

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/provider"
)

var networkOffer = provider.Offer{Capability: "fixture.network", RequestSchema: 1, OutputSchema: 1, Implementation: "network_v1"}

type networkParams struct {
	Label string `json:"label"`
	Mode  string `json:"mode"`
}

func fixtureNetwork(origins []string) provider.Capability {
	return provider.Capability{
		Offer: networkOffer, Resource: "key", MinInterval: time.Second, Timeout: 24 * time.Second,
		Network: &egress.Source{Origins: origins, AllowDirect: true, RequireRegion: true, Replay: true, MaxRequests: 16,
			MaxBodyBytes: 1024, ContentTypes: []string{"text/plain"}, Fixture: true},
		NormalizeParams: func(data jsontext.Value) (jsontext.Value, error) {
			p, err := provider.Object[networkParams](data)
			if err != nil || !provider.Identifier(p.Label) || !slices.Contains([]string{
				"ok", "sdk", "redirect", "private", "unsafe", "hold", "hold_long", "leak", "large", "bench",
				"status401", "status403", "status429", "delayed", "held",
			}, p.Mode) {
				return nil, errors.New("invalid network fixture")
			}
			return json.Marshal(p)
		},
		NormalizeOutput: provider.Normalize[fixtureOutput],
		Execute: func(ctx context.Context, task provider.Task, network egress.Client) provider.Result {
			p, _ := provider.Object[networkParams](task.Params)
			if p.Mode == "delayed" {
				ticker := time.NewTicker(50 * time.Millisecond)
				defer ticker.Stop()
				for {
					if _, err := os.Stat(p.Label + ".release"); err == nil {
						break
					}
					select {
					case <-ctx.Done():
						return provider.Result{State: "failed", Code: "timeout"}
					case <-ticker.C:
					}
				}
			}
			count := 1
			if p.Mode == "sdk" {
				count = 2
			}
			if p.Mode == "bench" {
				count = 12
			}
			for i := range count {
				address := origins[0] + "/" + p.Mode + "/" + p.Label
				if p.Mode == "unsafe" {
					address = "http://169.254.169.254/latest"
				}
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
				req.Header.Set("Authorization", "Bearer fixture-origin-private")
				req.Header.Set("Cookie", "private=fixture-origin-private")
				res, err := network.Do(req)
				if err != nil {
					return networkFailure(err)
				}
				if p.Mode == "leak" {
					break // Intentionally leave the bounded body open.
				}
				_, err = io.Copy(io.Discard, res.Body)
				_ = res.Body.Close()
				if err != nil {
					return networkFailure(err)
				}
				switch res.StatusCode {
				case 429:
					return provider.Result{State: "failed", Code: "upstream_throttled"}
				case 401, 403:
					return provider.Result{State: "failed", Code: "upstream_auth"}
				case 503:
					if p.Mode == "sdk" && i == 0 {
						continue
					}
					return provider.Result{State: "failed", Code: "upstream_unavailable"}
				case 200:
				default:
					return provider.Result{State: "failed", Code: "policy_denied"}
				}
			}
			output, _ := json.Marshal(fixtureOutput{p.Label, int32(count)})
			return provider.Result{State: "succeeded", Data: output}
		},
	}
}

func networkFailure(err error) provider.Result {
	code := "transport_error"
	switch {
	case errors.Is(err, egress.ErrPolicy) || errors.Is(err, egress.ErrBudget):
		code = "policy_denied"
	case errors.Is(err, egress.ErrDeferred):
		code = "network_quota_deferred"
	}
	return provider.Result{State: "failed", Code: code}
}

type networkHit struct {
	mode, label, proxy, remote       string
	authorization, cookie, proxyAuth string
	at                               time.Time
}

type networkLab struct {
	t             *testing.T
	mu            sync.Mutex
	hits          []networkHit
	peers         map[string]string
	upstream      *httptest.Server
	second        *httptest.Server
	concurrent    atomic.Int32
	maxConcurrent atomic.Int32
	releaseHeld   chan struct{}
}

func newNetworkLab(t *testing.T) *networkLab {
	l := &networkLab{t: t, peers: make(map[string]string), releaseHeld: make(chan struct{})}
	l.second = httptest.NewServer(http.HandlerFunc(l.handle))
	l.upstream = httptest.NewServer(http.HandlerFunc(l.handle))
	t.Cleanup(l.second.Close)
	t.Cleanup(l.upstream.Close)
	return l
}

func (l *networkLab) hit(label string) []networkHit {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []networkHit
	for _, h := range l.hits {
		if h.label == label {
			out = append(out, h)
		}
	}
	return out
}

func (l *networkLab) handle(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(parts) != 2 {
		w.WriteHeader(400)
		return
	}
	l.mu.Lock()
	previous := slices.ContainsFunc(l.hits, func(h networkHit) bool { return h.label == parts[1] })
	l.hits = append(l.hits, networkHit{parts[0], parts[1], l.peers[r.RemoteAddr], r.RemoteAddr,
		r.Header.Get("Authorization"), r.Header.Get("Cookie"), r.Header.Get("Proxy-Authorization"), time.Now()})
	l.mu.Unlock()
	now := l.concurrent.Add(1)
	for old := l.maxConcurrent.Load(); old < now && !l.maxConcurrent.CompareAndSwap(old, now); old = l.maxConcurrent.Load() {
	}
	defer l.concurrent.Add(-1)
	w.Header().Set("Content-Type", "text/plain")
	switch parts[0] {
	case "redirect":
		w.Header().Set("Location", l.second.URL+"/ok/"+parts[1])
		w.WriteHeader(302)
		return
	case "private":
		w.Header().Set("Location", "http://169.254.169.254/latest")
		w.WriteHeader(302)
		return
	case "sdk":
		if !previous {
			w.WriteHeader(503)
			return
		}
	case "status401":
		w.WriteHeader(401)
		return
	case "status403":
		w.WriteHeader(403)
		return
	case "status429":
		w.Header().Set("Retry-After", "75")
		w.WriteHeader(429)
		return
	case "large":
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, strings.Repeat("x", 2048))
		return
	case "held":
		_, _ = io.WriteString(w, "a")
		w.(http.Flusher).Flush()
		select {
		case <-l.releaseHeld:
		case <-r.Context().Done():
			return
		}
	case "hold", "hold_long", "leak":
		if parts[0] == "hold_long" && previous {
			break
		}
		_, _ = io.WriteString(w, "a")
		w.(http.Flusher).Flush()
		timer := time.NewTimer(2400 * time.Millisecond)
		if parts[0] != "hold" {
			timer.Reset(15 * time.Second)
		}
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
		}
	}
	_, _ = io.WriteString(w, "ok")
}

type fakeProxy struct {
	lab      *networkLab
	name     string
	ln       net.Listener
	scheme   string
	secret   string
	status   atomic.Int32
	connects atomic.Int64
	mu       sync.Mutex
	open     map[net.Conn]struct{}
	wg       sync.WaitGroup
}

func (l *networkLab) proxy(name, scheme, secret string) *fakeProxy {
	l.t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		l.t.Fatal(err)
	}
	p := &fakeProxy{lab: l, name: name, ln: ln, scheme: scheme, secret: secret, open: make(map[net.Conn]struct{})}
	p.wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			p.open[c] = struct{}{}
			p.mu.Unlock()
			p.wg.Go(func() {
				defer func() {
					c.Close()
					p.mu.Lock()
					delete(p.open, c)
					p.mu.Unlock()
				}()
				_ = c.SetDeadline(time.Now().Add(45 * time.Second))
				if p.scheme == "socks5" {
					p.socks(c)
				} else {
					p.http(c)
				}
			})
		}
	})
	l.t.Cleanup(func() {
		ln.Close()
		p.mu.Lock()
		for c := range p.open {
			c.Close()
		}
		p.mu.Unlock()
		p.wg.Wait()
	})
	return p
}

func (p *fakeProxy) openCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.open)
}

func (p *fakeProxy) address() string {
	u := url.URL{Scheme: p.scheme, Host: p.ln.Addr().String()}
	if p.secret != "" {
		u.User = url.UserPassword("fixture", p.secret)
	}
	return u.String()
}

func (p *fakeProxy) target(address string) net.Conn {
	ip, err := netip.ParseAddrPort(address)
	if err != nil || !ip.Addr().IsLoopback() ||
		("http://"+address != p.lab.upstream.URL && "http://"+address != p.lab.second.URL) {
		p.lab.t.Error("proxy was asked to resolve or connect to a non-fixture address")
		return nil
	}
	dest, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		return nil
	}
	p.lab.mu.Lock()
	p.lab.peers[dest.LocalAddr().String()] = p.name
	p.lab.mu.Unlock()
	return dest
}

func tunnel(client, dest net.Conn, fromClient io.Reader) {
	defer dest.Close()
	var wg sync.WaitGroup
	wg.Go(func() {
		_, _ = io.Copy(dest, fromClient)
		dest.Close()
	})
	_, _ = io.Copy(client, dest)
	client.Close()
	wg.Wait()
}

func (p *fakeProxy) socks(c net.Conn) {
	head := make([]byte, 2)
	if _, err := io.ReadFull(c, head); err != nil || head[0] != 5 {
		return
	}
	if _, err := io.ReadFull(c, make([]byte, int(head[1]))); err != nil {
		return
	}
	if _, err := c.Write([]byte{5, 0}); err != nil {
		return
	}
	b := make([]byte, 10)
	if _, err := io.ReadFull(c, b[:4]); err != nil {
		return
	}
	if b[0] != 5 || b[1] != 1 || b[3] != 1 {
		p.lab.t.Error("SOCKS5 proxy expected a numeric IPv4 CONNECT")
		return
	}
	if _, err := io.ReadFull(c, b[4:]); err != nil {
		return
	}
	p.connects.Add(1)
	address := net.JoinHostPort(net.IP(b[4:8]).String(), strconv.Itoa(int(binary.BigEndian.Uint16(b[8:]))))
	dest := p.target(address)
	if dest == nil {
		return
	}
	if _, err := c.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0}); err != nil {
		dest.Close()
		return
	}
	tunnel(c, dest, c)
}

func (p *fakeProxy) http(c net.Conn) {
	reader := bufio.NewReader(c)
	req, err := http.ReadRequest(reader)
	if err != nil {
		return
	}
	p.connects.Add(1)
	if req.Method != http.MethodConnect || !p.authorized(req) {
		p.lab.t.Error("proxy CONNECT missing expected authentication or method")
		_, _ = io.WriteString(c, "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
		return
	}
	if status := p.status.Load(); status != 0 {
		_, _ = fmt.Fprintf(c, "HTTP/1.1 %d Failed\r\nContent-Length: 0\r\n\r\n", status)
		return
	}
	dest := p.target(req.Host)
	if dest == nil {
		return
	}
	if _, err := io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		dest.Close()
		return
	}
	tunnel(c, dest, reader)
}

func (p *fakeProxy) authorized(req *http.Request) bool {
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("fixture:"+p.secret))
	return p.secret == "" || req.Header.Get("Proxy-Authorization") == want
}
