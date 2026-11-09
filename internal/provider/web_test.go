package provider

import (
	"bytes"
	"compress/gzip"
	"encoding/json/jsontext"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
)

func TestWebPassiveFingerprintFixtures(t *testing.T) {
	base, _ := url.Parse("https://fixture.example.com/")
	for _, tc := range []struct {
		id         string
		html       string
		headers    http.Header
		confidence string
	}{
		{"wordpress", `<meta name="generator" content="WordPress 6.0"><script src="/wp-content/theme.js"></script>`, nil, "high"},
		{"ghost", `<meta name="generator" content="Ghost 5.0">`, nil, "high"},
		{"drupal", `<script src="/core/misc/drupal.js"></script>`, http.Header{"X-Drupal-Cache": {"HIT"}}, "high"},
		{"discourse", `<meta id="data-discourse-setup" data-template="safe">`, nil, "medium"},
		{"xenforo", `<script src="/js/xf/core-compiled.js"></script>`, nil, "medium"},
		{"mediawiki", `<script src="/w/load.php?modules=startup"></script>`, nil, "medium"},
		{"nextjs", `<script id="__NEXT_DATA__" type="application/json">{}</script><script src="/_next/static/main.js"></script>`, nil, "high"},
		{"nuxt", `<script id="__NUXT_DATA__" type="application/json">{}</script><script src="/_nuxt/main.js"></script>`, nil, "high"},
		{"astro", `<astro-island></astro-island>`, nil, "medium"},
		{"vercel", "", http.Header{"Server": {"Vercel"}, "X-Vercel-Id": {"redacted"}}, "high"},
		{"netlify", "", http.Header{"X-Nf-Request-Id": {"redacted"}}, "medium"},
		{"github-pages", "", http.Header{"Server": {"GitHub.com"}, "X-Github-Request-Id": {"redacted"}}, "high"},
		{"cloudflare", "", http.Header{"Cf-Ray": {"redacted"}, "Server": {"cloudflare"}}, "high"},
		{"fastly", "", http.Header{"Fastly-Request-Id": {"redacted"}, "Via": {"1.1 cache.fastly.net"}}, "high"},
		{"cloudfront", "", http.Header{"X-Amz-Cf-Id": {"redacted"}, "Via": {"1.1 a.cloudfront.net (CloudFront)"}}, "high"},
		{"nginx", "", http.Header{"Server": {"nginx/1.0"}}, "low"},
		{"apache", "", http.Header{"Server": {"Apache/2.4.1"}}, "low"},
		{"caddy", "", http.Header{"Server": {"Caddy"}}, "low"},
	} {
		t.Run(tc.id, func(t *testing.T) {
			value, err := parsePage([]byte("<html><head>"+tc.html+"</head></html>"), "text/html", base, headerSignals(tc.headers))
			if err != nil || len(value.Technology) != 1 || value.Technology[0].ID != tc.id ||
				value.Technology[0].Confidence != tc.confidence || !validTechnology(value.Technology) {
				t.Fatalf("unexpected evidence: %+v, %v", value.Technology, err)
			}
			if slices.Contains(value.Technology[0].Evidence, "redacted") {
				t.Fatal("raw marker value exposed")
			}
		})
	}
	value, err := parsePage([]byte(`<html><head><meta name="generator" content="WordPress-ish">
		<script src="https://outside.example.com/wp-content/main.js"></script>
		<script src="/_astro/ambiguous.js"></script><script src="/react.js"></script>
		</head><body>WordPress and Drupal support</body></html>`),
		"text/html", base, headerSignals(http.Header{"X-Github-Request-Id": {"shared"}, "Server": {"notnginx"}}))
	if err != nil || len(value.Technology) != 0 {
		t.Fatalf("ambiguous evidence promoted: %+v / %v", value.Technology, err)
	}
}

func TestWebHTMLLimitsAndRedaction(t *testing.T) {
	base, _ := url.Parse("https://fixture.example.com/page")
	long := "https://fixture.example.com/" + strings.Repeat("a", 2047)
	if safeWebURL(nil, long) != long || pageURL(nil, long) != "" {
		t.Fatal("HTTP final URLs and stored metadata links did not use separate bounds")
	}
	input := []byte("<html><head><title>Caf\xe9 &amp; tea</title>" +
		`<meta name="description" content="  A   page  ">
		<meta property="og:image" content="/image?token=private#hash">
		<link rel="canonical" href="https://user:private@fixture.example.com/">
		<link rel="icon" href="javascript:alert(1)">
		<link rel="manifest" href="/manifest#private">
		<script type="application/ld+json">{"@graph":[{"@type":"Organization","name":"Fixture","url":"/org"},
		{"@type":["WebSite","Thing"],"name":"Site","url":"/?token=private"},{"name":"missing type","url":"/skip"},
		{"@type":"Person","name":"private"}]}</script></head></html>`)
	value, err := parsePage(input, "text/html; charset=windows-1252", base, make(webSignals))
	if err != nil || value.Title != "Café & tea" || value.Description != "A page" || value.Canonical != "" ||
		len(value.Icons) != 0 || len(value.LinkedData) != 2 || value.OpenGraph["image"] != "https://fixture.example.com/image" ||
		value.Manifest != "https://fixture.example.com/manifest" || strings.Contains(value.LinkedData[1].URL, "private") {
		t.Fatalf("wrong bounded metadata: %+v %v", value, err)
	}
	if _, err := parsePage([]byte("<html><script>"+strings.Repeat("x", 65<<10)+"</script></html>"), "text/html", base, make(webSignals)); err == nil {
		t.Fatal("HTML tokenizer accepted an oversized token")
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, _ = io.WriteString(writer, strings.Repeat("x", webBodyLimit+1))
	_ = writer.Close()
	res := &http.Response{StatusCode: 200, Header: http.Header{"Content-Encoding": {"gzip"}}}
	seg := parseHTTPPage(SegmentScope{}, res, compressed.Bytes(), false, make(webSignals))
	if seg.State != "unavailable" || seg.Evidence != "body_incomplete" {
		t.Fatal("gzip expansion was accepted")
	}
	for _, input := range []jsontext.Value{[]byte(`{"types":["AXFR"]}`), []byte(`{"resolver":"http://127.0.0.1"}`)} {
		if _, err := normalizeDNSParams(input); err == nil {
			t.Fatalf("DNS parameters accepted %s", input)
		}
	}
}

func BenchmarkWebPage(b *testing.B) {
	base, _ := url.Parse("https://fixture.example.com/")
	for _, size := range []struct {
		label   string
		repeats int
	}{{"8KiB", 32}, {"256KiB", 1024}} {
		b.Run(size.label, func(b *testing.B) {
			body := []byte(`<html><head><title>Benchmark</title><meta name="generator" content="WordPress 6.0"></head><body>` +
				strings.Repeat("<p>"+strings.Repeat("x", 249)+"</p>", size.repeats) + "</body></html>")
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			for b.Loop() {
				if _, err := parsePage(body, "text/html; charset=utf-8", base, make(webSignals)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
