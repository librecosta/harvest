//go:build integration

package integration

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	json "encoding/json/v2"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/provider"
)

type discoveryLab struct {
	t               *testing.T
	network         *networkLab
	doh             *webLab
	robots          atomic.Int32
	sitemap         atomic.Int32
	manifest        atomic.Int32
	feed            atomic.Int32
	bootstrap       atomic.Int32
	held, release   chan struct{}
	icon, ico, bomb []byte
}

func newDiscoveryLab(t *testing.T, p *testPlane) (*discoveryLab, *fakeProxy, *fakeProxy) {
	t.Helper()
	l := &discoveryLab{t: t, network: &networkLab{t: t, peers: make(map[string]string)}, doh: &webLab{t: t},
		held: make(chan struct{}, 1), release: make(chan struct{}, 1)}
	ca, cert := testWebCA(t)
	c := cert(10, []string{"*.example.com", "data.iana.org", "dns.google"}, false, false)
	tlsSite := httptest.NewUnstartedServer(http.HandlerFunc(l.serve))
	tlsSite.TLS = &tls.Config{Certificates: []tls.Certificate{c}}
	tlsSite.StartTLS()
	t.Cleanup(tlsSite.Close)
	l.network.pinnedTargets = map[string]string{
		webPin + ":443": tlsSite.Listener.Addr().String(),
		dohPin + ":443": tlsSite.Listener.Addr().String(),
	}
	data, err := json.Marshal(webFixtureConfig{Targets: l.network.pinnedTargets, CA: string(ca)})
	if err != nil {
		t.Fatal(err)
	}
	p.webFixture = filepath.Join(p.scratch, "b2-fixture.json")
	if err := os.WriteFile(p.webFixture, data, 0o600); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 16, 24))
	img.Set(1, 1, color.RGBA{0x90, 0x20, 0x10, 0xff})
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	l.icon = bytes.Clone(buf.Bytes())
	l.ico = make([]byte, 22+len(l.icon))
	copy(l.ico, []byte{0, 0, 1, 0, 1, 0, 16, 24})
	binary.LittleEndian.PutUint32(l.ico[14:], uint32(len(l.icon)))
	binary.LittleEndian.PutUint32(l.ico[18:], 22)
	copy(l.ico[22:], l.icon)
	buf.Reset()
	gz := gzip.NewWriter(&buf)
	_, _ = io.WriteString(gz, strings.Repeat(" ", 530<<10))
	_ = gz.Close()
	l.bomb = bytes.Clone(buf.Bytes())
	return l, l.network.proxy("web_connect", "http", rand.Text()), l.network.proxy("web_socks", "socks5", "")
}

func (l *discoveryLab) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || r.TLS == nil || r.TLS.ServerName != r.Host ||
		r.Header.Get("Proxy-Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Authorization") != "" {
		l.t.Errorf("unexpected B2 source request: %s %s", r.Method, r.Host)
	}
	l.network.mu.Lock()
	l.network.hits = append(l.network.hits, networkHit{mode: r.Method + " " + r.URL.RequestURI(), label: r.Host, proxy: l.network.peers[r.RemoteAddr]})
	l.network.mu.Unlock()
	switch r.Host {
	case "dns.google":
		l.doh.serveDNS(w, r)
		return
	case "data.iana.org":
		l.serveBootstrap(w, r)
		return
	case "registry.example.com":
		l.serveRegistry(w, r)
		return
	}
	switch r.URL.Path {
	case "/":
		w.Header().Set("Content-Type", "text/html")
		feed := "feed.xml"
		if l.feed.Load() == 1 {
			feed = "new.xml"
		}
		fmt.Fprintf(w, `<!doctype html><html><head><title>Discovery fixture</title>
<link rel="alternate" type="application/rss+xml" title="News" href="%s?token=never-store">
<base href="/updates/"><link rel="alternate" type="application/atom+xml" href="https://elsewhere.example.com/atom#never-store">
<link rel="icon" sizes="16x24" href="/icon.png">
<link rel="icon" href="/icon.svg"><link rel="icon" href="/query.png?token=never-store">
<link rel="icon" href="https://outside.example.com/icon.png">
<link rel="manifest" href="/site.webmanifest"></head><body>Fixture</body></html>`, feed)
	case "/robots.txt", "/robots-final.txt":
		l.serveRobots(w, r)
	case "/sitemap.xml", "/sitemap.xml.gz":
		l.serveSitemap(w, r)
	case "/favicon.ico":
		w.Header().Set("Content-Type", "image/x-icon")
		_, _ = w.Write(l.ico)
	case "/icon.png":
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(l.icon)
	case "/icon.svg":
		w.Header().Set("Content-Type", "image/svg+xml")
		_, _ = io.WriteString(w, `<svg xmlns="http://www.w3.org/2000/svg"><script>never-store</script><image href="https://elsewhere.example.com/image"/></svg>`)
	case "/site.webmanifest":
		w.Header().Set("Content-Type", "application/manifest+json")
		switch l.manifest.Load() {
		case 1:
			_, _ = io.WriteString(w, `{"ignored":"other","icons":[{"src":"/icon.png","sizes":"16x24","type":"image/png"}],"start_url":"/?secret=never-store","name":"Fixture App"}`)
		case 2:
			_, _ = io.WriteString(w, strings.Repeat("x", 35<<10))
		default:
			_, _ = io.WriteString(w, `{"name":"Fixture App","start_url":"/?secret=never-store","ignored":"never-store","icons":[{"type":"image/png","src":"/icon.png","sizes":"16x24"}]}`)
		}
	default:
		l.t.Errorf("unexpected B2 visit to %s%s", r.Host, r.URL.Path)
		w.WriteHeader(404)
	}
}

func (l *discoveryLab) serveRobots(w http.ResponseWriter, r *http.Request) {
	if r.UserAgent() != egress.DiscoveryUserAgent {
		l.t.Error("robots user agent mismatch")
	}
	w.Header().Set("Content-Type", "text/plain")
	if r.URL.Path == "/robots.txt" {
		switch r.Host {
		case "notfound.example.com":
			w.WriteHeader(404)
			return
		case "forbidden.example.com":
			w.WriteHeader(403)
			return
		case "throttle.example.com":
			w.Header().Set("Retry-After", "60")
			w.WriteHeader(429)
			return
		case "slow.example.com":
			<-r.Context().Done()
			return
		case "large.example.com":
			_, _ = io.WriteString(w, "User-agent: *\n#"+strings.Repeat("x", 513<<10))
			return
		case "redirect.example.com":
			http.Redirect(w, r, "/robots-final.txt", 302)
			return
		case "cross.example.com":
			http.Redirect(w, r, "https://elsewhere.example.com/robots.txt?token=never-store", 302)
			return
		case "held.example.com", "shutdown.example.com":
			select {
			case l.held <- struct{}{}:
			default:
			}
			select {
			case <-l.release:
			case <-r.Context().Done():
				return
			}
			if r.Host == "held.example.com" {
				http.Redirect(w, r, "/robots-final.txt", 302)
				return
			}
		}
	}
	switch l.robots.Load() {
	case 1:
		w.WriteHeader(503)
		return
	case 2:
		_, _ = io.WriteString(w, "User-agent: HarvestBot\nDisallow: /site.webmanifest\nDisallow: /sitemap.xml\n")
		return
	default:
		_, _ = io.WriteString(w, "User-agent: *\nDisallow: /\n\nUser-agent: HarvestBot\nDisallow: /private\nAllow: /private/public$\nSitemap: https://outside.example.com/sitemap.xml?token=never-store\n")
	}
}

const sitemapStart = `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`

func (l *discoveryLab) serveSitemap(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/xml")
	switch l.sitemap.Load() {
	case 1:
		_, _ = io.WriteString(w, `<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><sitemap><loc>https://sitemap.example.com/child.xml</loc></sitemap></sitemapindex>`)
	case 2:
		_, _ = io.WriteString(w, sitemapStart+"<url>")
	case 3:
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(l.bomb)
	case 4:
		_, _ = io.WriteString(w, `<!DOCTYPE urlset [<!ENTITY remote SYSTEM "file:///secret">]>`+sitemapStart+`<url><loc>&remote;</loc></url></urlset>`)
	case 5:
		_, _ = io.WriteString(w, strings.Repeat(" ", 520<<10))
	case 6:
		_, _ = io.WriteString(w, sitemapStart)
		for i := range 60 {
			fmt.Fprintf(w, "<url><loc>https://%s/never-visit/%d</loc></url>", r.Host, i)
		}
		_, _ = io.WriteString(w, "</urlset>")
	case 7:
		fmt.Fprintf(w, sitemapStart+`<url><loc>https://%s/listed?token=never-store</loc><lastmod>2024-01-02</lastmod></url>
<url><loc>https://elsewhere.example.com/page</loc><lastmod>yesterday</lastmod></url><url><loc>/relative</loc></url></urlset>`, r.Host)
	default:
		fmt.Fprintf(w, sitemapStart+`<url><loc>/relative</loc></url><url><loc>https://elsewhere.example.com/page</loc><lastmod>yesterday</lastmod></url>
<url><loc>https://%s/listed?token=never-store</loc><lastmod>2024-01-02</lastmod></url></urlset>`, r.Host)
	}
}

func (l *discoveryLab) serveBootstrap(w http.ResponseWriter, r *http.Request) {
	if r.URL.RequestURI() != "/rdap/dns.json" {
		l.t.Error("invalid IANA path")
	}
	w.Header().Set("Content-Type", "application/json")
	if l.bootstrap.Load() == 1 {
		_, _ = io.WriteString(w, `{"version":"1.0","services":[["invalid"]]}`)
		return
	}
	fmt.Fprintf(w, `{"version":"1.0","description":%q,"services":[
[["com","dev","info","biz","io"],["http://untrusted.example.com/","https://registry.example.com/rdap/"]],
[["net"],["https://private-registry.example.com/rdap/"]],
[["app"],["https://localhost/rdap/"]]
]}`, strings.Repeat("x", 20<<10)) // Exercise the dedicated finish payload limit.
}

func (l *discoveryLab) serveRegistry(w http.ResponseWriter, r *http.Request) {
	name, ok := strings.CutPrefix(r.URL.Path, "/rdap/domain/")
	if !ok || name == "" || r.URL.RawQuery != "" {
		l.t.Errorf("invalid registry request %s", r.URL.Path)
		w.WriteHeader(400)
		return
	}
	w.Header().Set("Content-Type", "application/rdap+json")
	switch {
	case strings.HasSuffix(name, ".dev"):
		http.Redirect(w, r, "https://elsewhere.example.com/registry", 302)
		return
	case strings.HasSuffix(name, ".info"):
		_, _ = io.WriteString(w, strings.Repeat("x", 270<<10))
		return
	case strings.HasSuffix(name, ".io"):
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(429)
		return
	case strings.HasSuffix(name, ".biz"):
		name = "wrong.example.com"
	}
	fmt.Fprintf(w, `{"objectClassName":"domain","ldhName":%q,"status":["active","client transfer prohibited"],
"events":[{"eventAction":"registration","eventDate":"2020-01-02T00:00:00Z"},{"eventAction":"last changed","eventDate":"2024-01-02T00:00:00Z"}],
"nameservers":[{"ldhName":"NS1.Example.com."}],
"entities":[{"roles":["registrant"],"vcardArray":["vcard",[["fn",{},"text","never-store"],["email",{},"text","never-store@example.com"]]]},
{"roles":["registrar"],"publicIds":[{"type":"IANA Registrar ID","identifier":"25"}],"vcardArray":["vcard",[["fn",{},"text","Fixture Registrar"],["adr",{},"text","never-store"]]]}]}`, name)
}

func b2Offers() []provider.Offer {
	out := append([]provider.Offer{}, b1Offers...)
	for _, name := range []string{"robots", "sitemap", "assets", "rdap"} {
		out = append(out, provider.Offer{Capability: "web." + name, RequestSchema: 1, OutputSchema: 1, Implementation: "web_v1"})
	}
	return out
}
