package provider

import (
	"mime"
	"net/url"
	"slices"
	"strings"
	"uuid"

	"github.com/librecosta/harvest/internal/egress"
)

type feedHint struct {
	URL      string `json:"url"`
	MIME     string `json:"mime"`
	Title    string `json:"title,omitempty"`
	Source   string `json:"source"`
	Status   string `json:"status"`
	Verified bool   `json:"verified"`
}

type assetHint struct {
	URL       string   `json:"url"`
	Kind      string   `json:"kind"`
	Sizes     []string `json:"sizes,omitempty"`
	Fetchable bool     `json:"fetchable"`
}

type rawHint struct{ href, kind, mime, title, sizes string }

type homepageLinks struct {
	base          *url.URL
	feeds, assets []rawHint
}

func (l *homepageLinks) readBase(doc *url.URL, raw string) {
	if l.base != nil || raw == "" {
		return
	}
	u, err := doc.Parse(strings.TrimSpace(raw))
	if err == nil && u.User == nil && u.Opaque == "" && (u.Scheme == "https" || u.Scheme == "http") && egress.ValidHostPort(u) {
		l.base = u
	}
}

func (l *homepageLinks) readLink(attrs map[string]string) {
	href := attrs["href"]
	if href == "" {
		return
	}
	for rel := range strings.FieldsSeq(strings.ToLower(attrs["rel"])) {
		switch rel {
		case "alternate":
			media, _, _ := mime.ParseMediaType(attrs["type"])
			if feedType(media) && len(l.feeds) < 12 {
				l.feeds = append(l.feeds, rawHint{href: href, mime: media, title: attrs["title"]})
			}
		case "icon", "apple-touch-icon", "mask-icon", "manifest":
			kind := "icon"
			if rel == "manifest" {
				kind = "manifest"
			}
			if len(l.assets) < 12 {
				l.assets = append(l.assets, rawHint{href: href, kind: kind, sizes: attrs["sizes"]})
			}
		}
	}
}

func (l homepageLinks) apply(out *pageFacts, doc *url.URL) {
	base := l.base
	if base == nil {
		base = doc
	}
	for _, hint := range l.feeds {
		if link := pageURL(base, hint.href); link != "" {
			out.Feeds = append(out.Feeds, feedHint{URL: link, MIME: hint.mime, Title: cleanText(hint.title, 128),
				Source: "html_alternate", Status: "discovered"})
		}
	}
	slices.SortFunc(out.Feeds, func(a, b feedHint) int {
		if c := strings.Compare(a.URL, b.URL); c != 0 {
			return c
		}
		if c := strings.Compare(a.MIME, b.MIME); c != 0 {
			return c
		}
		return strings.Compare(a.Title, b.Title)
	})
	out.Feeds = slices.CompactFunc(out.Feeds, func(a, b feedHint) bool { return a.URL == b.URL && a.MIME == b.MIME })
	out.Feeds = out.Feeds[:min(len(out.Feeds), 4)]
	for _, hint := range l.assets {
		u, err := base.Parse(strings.TrimSpace(hint.href))
		link := pageURL(base, hint.href)
		if err != nil || link == "" {
			continue
		}
		ok := u.User == nil && u.Scheme == doc.Scheme && u.Host == doc.Host && u.Fragment == "" && !strings.Contains(hint.href, "#") && u.RequestURI() != "/" && egress.DiscoveryPath(u.RequestURI())
		out.AssetCandidates = append(out.AssetCandidates, assetHint{link, hint.kind, parseSizes(hint.sizes), ok})
	}
	slices.SortFunc(out.AssetCandidates, func(a, b assetHint) int {
		if c := strings.Compare(a.Kind, b.Kind); c != 0 {
			return c
		}
		if c := strings.Compare(a.URL, b.URL); c != 0 {
			return c
		}
		if a.Fetchable != b.Fetchable {
			if a.Fetchable {
				return -1
			}
			return 1
		}
		return slices.Compare(a.Sizes, b.Sizes)
	})
	out.AssetCandidates = slices.CompactFunc(out.AssetCandidates, func(a, b assetHint) bool { return a.Kind == b.Kind && a.URL == b.URL })
	var icons, manifests int
	out.AssetCandidates = slices.DeleteFunc(out.AssetCandidates, func(hint assetHint) bool {
		if hint.Kind == "icon" {
			icons++
			return icons > 4
		}
		manifests++
		return manifests > 1
	})
}

func feedType(s string) bool { return s == "application/rss+xml" || s == "application/atom+xml" }

type AssetParams struct {
	Kind          string     `json:"kind"`
	ObservationID *uuid.UUID `json:"observation_id,omitempty"`
	Index         int        `json:"index,omitempty"`
}

func approvedAsset(origin string, v assetHint) bool {
	if !v.Fetchable || !siteURL(v.URL, origin) {
		return false
	}
	uri := strings.TrimPrefix(v.URL, origin)
	return egress.DiscoveryPath(uri) && uri != "/"
}
