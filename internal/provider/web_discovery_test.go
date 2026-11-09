package provider

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"net/url"
	"strings"
	"testing"
)

func TestHarvestRobotsPolicy(t *testing.T) {
	base, _ := url.Parse("https://example.com/robots.txt")
	value, err := parseRobots([]byte(`# Ignore this comment.
User-agent: *
Disallow: /
User-agent: Harvest
Disallow: /lower-priority
USER-AGENT: hArVeStBoT
Disallow: /private # Comments do not enter the path.
Allow: /private/open
Disallow: /private/open$
User-agent: Other
Disallow: /
User-agent: HarvestBot
Disallow: /icons/*/old$
Disallow: /%7Euser
Disallow: /encoded%2Fslash
Disallow: /café
`), base)
	if err != nil || value.Match != "harvestbot" {
		t.Fatalf("policy = %+v, %v", value, err)
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]bool{
		"/": true, "/lower-priority": true, "/private/closed": false, "/private/open": true,
		"/private/open/child": true, "/icons/app/old": false, "/icons/app/older": true,
		"/~user": false, "/encoded%2fslash": false, "/encoded/slash": true, "/caf%C3%A9": false,
	} {
		got, err := AllowsRobots(data, path)
		if err != nil || got != want {
			t.Errorf("%s: got %t, %v, want %t", path, got, err, want)
		}
	}
	for _, raw := range []string{"<html>error</html>", "User-agent: *\nDisallow: /broken%xx", "User-agent: *\n" + strings.Repeat("Disallow: /x\n", 257)} {
		if _, err := parseRobots([]byte(raw), base); err == nil {
			t.Errorf("accepted invalid or incomplete robots file")
		}
	}
	if _, err := parseRobots([]byte("# comment only\n"), base); err != nil {
		t.Fatal(err)
	}
}

func TestUnresolvedRedirect(t *testing.T) {
	for _, data := range []jsontext.Value{
		[]byte(`{"url":"https://other.example.com/redirect?secret=x","verified":false}`),
		[]byte(`{"url":"https://other.example.com/redirect","verified":true}`),
		[]byte(`{"url":"http://user:secret@other.example.com/redirect","verified":false}`),
	} {
		if _, err := normalizeDiscoveryDiagnostic("source_redirect", data); err == nil {
			t.Errorf("accepted untrusted diagnostic: %s", data)
		}
	}
}

func BenchmarkWebDiscovery(b *testing.B) {
	const origin = "https://example.com"
	const rdap = `{"objectClassName":"domain","ldhName":"example.com","status":["active","client transfer prohibited"],"nameservers":[{"ldhName":"ns1.example.com"}],"events":[{"eventAction":"expiration","eventDate":"2028-02-03T00:00:00Z"}],"entities":[{"roles":["registrar"],"publicIds":[{"type":"IANA Registrar ID","identifier":"25"}],"vcardArray":["vcard",[["fn",{},"text","Registrar"]]]}]}`
	robotsURL, _ := url.Parse(origin + "/robots.txt")
	robots := []byte("User-agent: *\nDisallow: /\nUser-agent: HarvestBot\nDisallow: /private\nAllow: /private/public$\nSitemap: " + origin + "/sitemap.xml\n")
	sitemapURL, _ := url.Parse(origin + "/sitemap.xml")
	sitemap := []byte(`<urlset xmlns="` + sitemapNS + `">` + strings.Repeat(`<url><loc>https://example.com/page</loc><lastmod>2026-01-01</lastmod></url>`, 200) + "</urlset>")
	rdapData := []byte(rdap)
	manifestURL, _ := url.Parse(origin + "/manifest.json")
	manifest := []byte(`{"name":"Example","short_name":"App","start_url":"/","icons":[{"src":"/icon.png","type":"image/png","sizes":"48x48 96x96 192x192"}]}`)
	for _, test := range []struct {
		name string
		size int
		run  func() error
	}{
		{"robots", len(robots), func() error { _, err := parseRobots(robots, robotsURL); return err }},
		{"sitemap_200", len(sitemap), func() error { _, err := parseSitemap(sitemap, sitemapURL, origin); return err }},
		{"rdap", len(rdapData), func() error {
			_, err := parseRDAP(rdapData, origin+"/rdap/domain/example.com", "example.com")
			return err
		}},
		{"manifest", len(manifest), func() error { _, err := parseManifest(manifest, manifestURL, origin); return err }},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.SetBytes(int64(test.size))
			b.ReportAllocs()
			for b.Loop() {
				if err := test.run(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
