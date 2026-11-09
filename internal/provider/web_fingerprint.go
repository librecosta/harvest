package provider

import (
	"net/http"
	"net/url"
	"slices"
	"strings"
)

const webRuleset = "web-passive-1"

type technology struct {
	ID         string   `json:"id"`
	Category   string   `json:"category"`
	Confidence string   `json:"confidence"`
	Evidence   []string `json:"evidence"`
}

type webSignals map[string]bool

type matcher struct {
	evidence string
	weight   int
}

type techRule struct {
	id, category string
	matchers     []matcher
}

var webRules = []techRule{
	{"wordpress", "cms", []matcher{{"generator:wordpress", 3}, {"asset:wp-content", 2}, {"asset:wp-includes", 2}}},
	{"ghost", "cms", []matcher{{"generator:ghost", 3}, {"asset:ghost-sdk", 2}}},
	{"drupal", "cms", []matcher{{"generator:drupal", 3}, {"header:drupal-cache", 2}, {"asset:drupal-core", 2}}},
	{"discourse", "community", []matcher{{"generator:discourse", 3}, {"marker:discourse-setup", 2}}},
	{"xenforo", "community", []matcher{{"generator:xenforo", 3}, {"asset:xenforo-core", 2}}},
	{"mediawiki", "cms", []matcher{{"generator:mediawiki", 3}, {"asset:mediawiki-loader", 2}}},
	{"nextjs", "framework", []matcher{{"marker:next-data", 2}, {"asset:next-static", 2}, {"header:nextjs", 2}}},
	{"nuxt", "framework", []matcher{{"marker:nuxt-data", 2}, {"asset:nuxt-build", 2}}},
	{"astro", "framework", []matcher{{"generator:astro", 3}, {"marker:astro-island", 2}, {"asset:astro-build", 1}}},
	{"vercel", "edge_hosting", []matcher{{"header:vercel-id", 2}, {"server:vercel", 1}}},
	{"netlify", "edge_hosting", []matcher{{"header:netlify-request-id", 2}, {"server:netlify", 1}}},
	{"github-pages", "edge_hosting", []matcher{{"server:github.com", 2}, {"header:github-request-id", 1}}},
	{"cloudflare", "edge_hosting", []matcher{{"header:cf-ray", 2}, {"server:cloudflare", 1}}},
	{"fastly", "edge_hosting", []matcher{{"header:fastly-request-id", 2}, {"header:fastly-via", 1}}},
	{"cloudfront", "edge_hosting", []matcher{{"header:cloudfront-id", 2}, {"header:cloudfront-via", 2}}},
	{"nginx", "server", []matcher{{"server:nginx", 1}}},
	{"apache", "server", []matcher{{"server:apache", 1}}},
	{"caddy", "server", []matcher{{"server:caddy", 1}}},
}

func identify(signals webSignals) []technology {
	var matches []technology
	for _, rule := range webRules {
		var evidence []string
		weight := 0
		for _, m := range rule.matchers {
			if signals[m.evidence] {
				weight += m.weight
				evidence = append(evidence, m.evidence)
			}
		}
		// GitHub request IDs are shared across GitHub services. Astro's
		// generic asset directory alone is insufficient to identify Astro.
		if weight == 0 || (rule.id == "github-pages" || rule.id == "astro") && weight == 1 {
			continue
		}
		confidence := "low"
		if weight >= 3 {
			confidence = "high"
		} else if weight == 2 {
			confidence = "medium"
		}
		matches = append(matches, technology{rule.id, rule.category, confidence, evidence})
	}
	return matches
}

func validTechnology(values []technology) bool {
	if len(values) > len(webRules) {
		return false
	}
	signals := make(webSignals)
	for _, v := range values {
		if len(v.Evidence) > 3 {
			return false
		}
		for _, evidence := range v.Evidence {
			signals[evidence] = true
		}
	}
	return slices.EqualFunc(values, identify(signals), func(a, b technology) bool {
		return a.ID == b.ID && a.Category == b.Category && a.Confidence == b.Confidence && slices.Equal(a.Evidence, b.Evidence)
	})
}

func product(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if cut := strings.IndexAny(v, "/ (\t"); cut >= 0 {
		v = v[:cut]
	}
	return v
}

func headerSignals(h http.Header) webSignals {
	s := make(webSignals)
	switch token := product(h.Get("Server")); token {
	case "github.com", "cloudflare", "vercel", "netlify", "nginx", "apache", "caddy":
		s["server:"+token] = true
	}
	for name, evidence := range map[string]string{
		"X-Vercel-Id": "header:vercel-id", "X-Nf-Request-Id": "header:netlify-request-id",
		"X-Github-Request-Id": "header:github-request-id", "Cf-Ray": "header:cf-ray",
		"Fastly-Request-Id": "header:fastly-request-id", "X-Amz-Cf-Id": "header:cloudfront-id",
		"X-Drupal-Cache": "header:drupal-cache",
	} {
		if v := h.Get(name); len(v) >= 2 && len(v) <= 256 {
			s[evidence] = true
		}
	}
	if product(h.Get("X-Powered-By")) == "next.js" {
		s["header:nextjs"] = true
	}
	for _, via := range h.Values("Via") {
		for entry := range strings.SplitSeq(strings.ToLower(via), ",") {
			parts := strings.Fields(entry)
			if len(parts) < 2 {
				continue
			}
			s["header:cloudfront-via"] = s["header:cloudfront-via"] ||
				(strings.HasSuffix(parts[1], ".cloudfront.net") && slices.Contains(parts[2:], "(cloudfront)"))
			s["header:fastly-via"] = s["header:fastly-via"] || strings.HasSuffix(parts[1], ".fastly.net")
		}
	}
	return s
}

func (s webSignals) generator(value string) {
	switch token := product(value); token {
	case "wordpress", "ghost", "drupal", "discourse", "xenforo", "mediawiki", "astro":
		s["generator:"+token] = true
	}
}

func (s webSignals) asset(base *url.URL, raw string) {
	if len(raw) > 1024 {
		return
	}
	u, err := base.Parse(raw)
	if err != nil || u.User != nil || u.Scheme != base.Scheme || u.Host != base.Host {
		return
	}
	path := strings.ToLower(u.Path)
	for prefix, evidence := range map[string]string{
		"/wp-content/": "wp-content", "/wp-includes/": "wp-includes", "/assets/built/ghost-sdk": "ghost-sdk",
		"/core/misc/drupal.js": "drupal-core", "/js/xf/core": "xenforo-core",
		"/_next/static/": "next-static", "/_nuxt/": "nuxt-build", "/_astro/": "astro-build",
	} {
		if strings.HasPrefix(path, prefix) {
			s["asset:"+evidence] = true
		}
	}
	if (path == "/w/load.php" || path == "/load.php") && u.Query().Has("modules") {
		s["asset:mediawiki-loader"] = true
	}
}
