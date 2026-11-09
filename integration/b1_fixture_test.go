//go:build integration

package integration

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	json "encoding/json/v2"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/provider"
)

const webPin = "93.184.216.34"
const dohPin = "8.8.8.8"

type webFixtureConfig struct {
	Targets    map[string]string
	CA         string
	RebindFile string
}

func webFixtureNetwork() *egress.FixtureNetwork {
	raw, err := os.ReadFile(os.Getenv("HARVEST_TEST_WEB"))
	var fixture webFixtureConfig
	if err != nil || json.Unmarshal(raw, &fixture) != nil {
		panic("invalid B1 fixture")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(fixture.CA)) {
		panic("invalid B1 fixture trust store")
	}
	return &egress.FixtureNetwork{
		RootCAs: roots,
		Lookup: func(_ context.Context, _, name string) ([]netip.Addr, error) {
			switch {
			case name == "dns.google.":
				return []netip.Addr{netip.MustParseAddr(dohPin)}, nil
			case name == "data.iana.org.":
				return []netip.Addr{netip.MustParseAddr(webPin)}, nil
			case name == "private-registry.example.com.":
				return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
			case name == "mixed.example.com.":
				return []netip.Addr{netip.MustParseAddr(webPin), netip.MustParseAddr("127.0.0.1")}, nil
			case name == "metadata.example.com.":
				return []netip.Addr{netip.MustParseAddr("169.254.169.254")}, nil
			case name == "rebind.example.com.":
				if _, err := os.Stat(fixture.RebindFile); err == nil {
					return []netip.Addr{netip.MustParseAddr("10.0.0.1")}, nil
				}
			case !strings.HasSuffix(name, ".example.com."):
				return nil, errors.New("not a fixture name")
			}
			return []netip.Addr{netip.MustParseAddr(webPin)}, nil
		},
		DialDirect: func(ctx context.Context, network, address string) (net.Conn, error) {
			if mapped := fixture.Targets[address]; mapped != "" {
				return (&net.Dialer{}).DialContext(ctx, network, mapped)
			}
			return nil, errors.New("not a fixture address")
		},
	}
}

type webLab struct {
	t             *testing.T
	network       *networkLab
	site          atomic.Int32
	dns           atomic.Int32
	revision      atomic.Int32
	rebindFile    string
	held, release chan struct{}
}

func newWebLab(t *testing.T, p *testPlane) (*webLab, *fakeProxy, *fakeProxy) {
	t.Helper()
	w := &webLab{t: t, network: &networkLab{t: t, peers: make(map[string]string)},
		rebindFile: filepath.Join(p.scratch, "rebind"), held: make(chan struct{}, 1), release: make(chan struct{}, 1)}
	certPEM, cert := testWebCA(t)
	base := cert(2, []string{"*.example.com", "dns.google"}, false, false)
	renewed := cert(3, []string{"*.example.com", "dns.google"}, false, false)
	wrong := cert(4, []string{"different.example.com"}, false, false)
	expired := cert(5, []string{"expired.example.com"}, true, false)
	unknown := cert(6, []string{"untrusted.example.com"}, false, true)
	tlsSite := httptest.NewUnstartedServer(http.HandlerFunc(w.serve))
	tlsSite.TLS = &tls.Config{GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		switch hello.ServerName {
		case "wrong.example.com":
			return &wrong, nil
		case "expired.example.com":
			return &expired, nil
		case "untrusted.example.com":
			return &unknown, nil
		}
		if w.revision.Load() == 1 {
			return &renewed, nil
		}
		return &base, nil
	}}
	tlsSite.StartTLS()
	plain := httptest.NewServer(http.HandlerFunc(w.serve))
	t.Cleanup(tlsSite.Close)
	t.Cleanup(plain.Close)
	w.network.pinnedTargets = map[string]string{
		webPin + ":80":  plain.Listener.Addr().String(),
		webPin + ":443": tlsSite.Listener.Addr().String(),
		dohPin + ":443": tlsSite.Listener.Addr().String(),
	}
	data, err := json.Marshal(webFixtureConfig{w.network.pinnedTargets, string(certPEM), w.rebindFile})
	if err != nil {
		t.Fatal(err)
	}
	p.webFixture = filepath.Join(p.scratch, "web-fixture.json")
	if err := os.WriteFile(p.webFixture, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return w, w.network.proxy("web_connect", "http", rand.Text()), w.network.proxy("web_socks", "socks5", "")
}

func testWebCA(t *testing.T) ([]byte, func(int64, []string, bool, bool) tls.Certificate) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Harvest B1 fixture CA"},
		NotBefore: now.Add(-72 * time.Hour), NotAfter: now.Add(72 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), func(serial int64, names []string, expired, self bool) tls.Certificate {
		t.Helper()
		pub, leafKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{
			SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: names[0]}, DNSNames: names,
			NotBefore: now.Add(-48 * time.Hour), NotAfter: now.Add(48 * time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		if expired {
			leaf.NotAfter = now.Add(-time.Hour)
		}
		issuer, signer := ca, key
		if self {
			issuer, signer = leaf, leafKey
		}
		raw, err := x509.CreateCertificate(rand.Reader, leaf, issuer, pub, signer)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{raw}, PrivateKey: leafKey}
	}
}

func (l *webLab) serve(w http.ResponseWriter, r *http.Request) {
	if r.TLS != nil && r.TLS.ServerName != r.Host {
		l.t.Errorf("Host and SNI differ: %q / %q", r.Host, r.TLS.ServerName)
	}
	if r.Header.Get("Proxy-Authorization") != "" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
		l.t.Error("unexpected credentials at Web origin")
	}
	l.network.mu.Lock()
	l.network.hits = append(l.network.hits, networkHit{mode: r.Method + " " + r.URL.RequestURI(), label: r.Host,
		proxy: l.network.peers[r.RemoteAddr], at: time.Now()})
	l.network.mu.Unlock()
	if r.Host == "dns.google" {
		l.serveDNS(w, r)
		return
	}
	if r.Method == http.MethodHead {
		w.Header().Set("Location", "/%ZZ")
		w.Header().Set("Connection", "close")
		if r.Host == "valid403.example.com" {
			w.WriteHeader(http.StatusForbidden)
		} else {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	switch {
	case r.Host == "away.example.com":
		http.Redirect(w, r, "https://elsewhere.example.com/x?token=never-store", 302)
		return
	case r.Host == "downgrade.example.com":
		http.Redirect(w, r, "http://downgrade.example.com/", 302)
		return
	case r.Host == "limit.example.com":
		http.Redirect(w, r, r.URL.Path+"step/", 302)
		return
	case r.Host == "rebind.example.com" && r.URL.Path == "/":
		w.Header().Set("Connection", "close")
		_ = os.WriteFile(l.rebindFile, nil, 0o600)
		http.Redirect(w, r, "/page", 302)
		return
	case r.Host == "held.example.com" && r.URL.Path == "/":
		select {
		case l.held <- struct{}{}:
		default:
		}
		select {
		case <-l.release:
		case <-r.Context().Done():
			return
		}
		http.Redirect(w, r, "/page", 302)
		return
	case strings.HasPrefix(r.Host, "redirect") && r.URL.Path == "/":
		http.Redirect(w, r, "/page?sid=never-store", 302)
		return
	case r.Host == "throttle.example.com":
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(429)
		return
	}
	mode := l.site.Load()
	if r.Host != "content.example.com" {
		mode = 0
	}
	switch mode {
	case 3:
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "{}")
		return
	case 4:
		w.WriteHeader(304)
		return
	case 5:
		_, _ = io.WriteString(w, strings.Repeat("x", 540<<10))
		return
	}
	title := "First title"
	if mode == 2 || mode == 6 {
		title = "Updated title"
	}
	generator, asset := "WordPress 6", "/wp-content/main.js"
	if mode == 6 {
		generator, asset = "Ghost 5", "/assets/built/ghost-sdk.js"
	}
	w.Header().Set("Server", "nginx/1.2.3")
	w.Header().Set("Content-Security-Policy", "script-src 'nonce-never-store'")
	w.Header().Set("Set-Cookie", "auth=never-store")
	fmt.Fprintf(w, `<!doctype html><html lang="en"><head><title>%s</title>
<meta name="description" content="Fixture page"><meta property="og:title" content="Preview">
<meta name="twitter:card" content="summary"><meta name="generator" content="%s">
<link rel="canonical" href="/home?secret=never-store"><link rel="icon" href="/favicon.svg">
<link rel="manifest" href="/site.webmanifest"><script type="application/ld+json">{"@graph":[{"@type":"WebSite","name":"Fixture Site","url":"/?token=never-store"}]}</script>
</head><body><script src="%s"></script><footer>nonce=%d</footer></body></html>`, title, generator, asset, mode)
}

func (l *webLab) serveDNS(w http.ResponseWriter, r *http.Request) {
	var q dnsmessage.Message
	wire, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get("dns"))
	if err != nil || q.Unpack(wire) != nil || len(q.Questions) != 1 || q.Response ||
		r.URL.Path != "/dns-query" || r.Method != http.MethodGet || r.Header.Get("Accept") != "application/dns-message" {
		l.t.Error("unexpected DoH request")
		w.WriteHeader(400)
		return
	}
	question := q.Questions[0]
	name := question.Name.String()
	reply := dnsmessage.Message{Header: dnsmessage.Header{ID: q.ID, Response: true, RecursionAvailable: true, AuthenticData: l.dns.Load() != 1},
		Questions: q.Questions}
	addr := byte(8)
	if l.dns.Load() == 2 {
		addr = 9
	}
	ttl := uint32(300)
	if l.dns.Load() == 1 {
		ttl = 42
	}
	host := dnsmessage.MustNewName("answer.example.com.")
	var answer dnsmessage.ResourceBody
	switch question.Type {
	case dnsmessage.TypeA:
		answer = &dnsmessage.AResource{A: [4]byte{8, 8, 8, addr}}
	case dnsmessage.TypeAAAA:
		answer = &dnsmessage.AAAAResource{AAAA: [16]byte{0x26, 0x06, 0x47, 0x00, 0x47, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0x11, 0x11}}
	case dnsmessage.TypeCNAME:
		answer = &dnsmessage.CNAMEResource{CNAME: host}
	case dnsmessage.TypeNS:
		answer = &dnsmessage.NSResource{NS: host}
	case dnsmessage.TypeMX:
		answer = &dnsmessage.MXResource{Pref: 10, MX: host}
	case dnsmessage.TypeSOA:
		answer = &dnsmessage.SOAResource{NS: host, MBox: dnsmessage.MustNewName("dns.example.com."), Serial: 1, Refresh: 60, Retry: 30, Expire: 3600, MinTTL: 30}
	case dnsmessage.TypeTXT:
		answer = &dnsmessage.TXTResource{TXT: []string{"never-store", "second-chunk"}}
	case 257:
		answer = &dnsmessage.UnknownResource{Type: 257, Data: append([]byte{0, 5}, []byte("issueca-secret-never-store")...)}
	}
	switch {
	case strings.HasPrefix(name, "missing."):
		reply.RCode, answer = dnsmessage.RCodeNameError, nil
	case strings.HasPrefix(name, "empty."):
		answer = nil
	case strings.HasPrefix(name, "slow."):
		<-r.Context().Done()
		return
	case l.dns.Load() == 3 && question.Type == dnsmessage.TypeMX:
		reply.RCode, answer = dnsmessage.RCodeServerFailure, nil
	case l.dns.Load() == 4 && question.Type == dnsmessage.TypeMX:
		reply.RCode, answer = dnsmessage.RCodeRefused, nil
	}
	if answer != nil {
		reply.Answers = []dnsmessage.Resource{{Header: dnsmessage.ResourceHeader{Name: question.Name, Type: question.Type, Class: question.Class, TTL: ttl}, Body: answer}}
	}
	data, err := reply.Pack()
	if err != nil {
		l.t.Error(err)
		w.WriteHeader(500)
		return
	}
	w.Header().Set("Content-Type", "application/dns-message")
	_, _ = w.Write(data)
}

var b1Offers = []provider.Offer{
	{Capability: "web.http", RequestSchema: 1, OutputSchema: 1, Implementation: "web_v1"},
	{Capability: "web.tls", RequestSchema: 1, OutputSchema: 1, Implementation: "web_v1"},
	{Capability: "web.dns", RequestSchema: 1, OutputSchema: 1, Implementation: "web_v1"},
}
