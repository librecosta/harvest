package provider

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/xml"
	"errors"
	"io"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/librecosta/harvest/internal/egress"
)

const sitemapNS = "http://www.sitemaps.org/schemas/sitemap/0.9"
const maxSitemapRecords = 50000

type sitemapEntry struct {
	URL          string `json:"url"`
	Relation     string `json:"relation"`
	LastMod      string `json:"lastmod,omitempty"`
	LastModState string `json:"lastmod_state"`
	Verified     bool   `json:"verified"`
}

type sitemapEvidence struct {
	URL             string         `json:"url"`
	Status          string         `json:"status"`
	Kind            string         `json:"kind"`
	Total           int            `json:"total"`
	SameOrigin      int            `json:"same_origin"`
	External        int            `json:"external"`
	Invalid         int            `json:"invalid"`
	SampleTruncated bool           `json:"sample_truncated"`
	Sample          []sitemapEntry `json:"sample,omitempty"`
}

func acquireSitemap(ctx context.Context, t Task, client egress.Client) Result {
	scope := discoveryScope(t, "sitemap")
	res, err := discoveryRequest(ctx, t, client, egress.SitemapPath, "application/xml, text/xml, application/gzip")
	if err != nil {
		return discoveryResult(scope, discoveryNetworkCode(err), nil)
	}
	defer res.Body.Close()
	if code := discoveryStatus(res.StatusCode); code != "" {
		return discoveryFailure(scope, res, code)
	}
	base := finalURL(res)
	media := mediaType(res)
	if !slices.Contains([]string{"application/xml", "text/xml", "application/gzip", "application/x-gzip", "application/octet-stream", ""}, media) {
		return discoveryResult(scope, "source_invalid", nil)
	}
	body, code := discoveryBody(res, sitemapLimit, strings.HasSuffix(base.Path, ".gz") || media == "application/gzip" || media == "application/x-gzip")
	if code != "" {
		return discoveryResult(scope, code, nil)
	}
	value, err := parseSitemap(body, base, t.ExternalKey)
	if err != nil {
		if errors.Is(err, egress.ErrBudget) {
			return discoveryResult(scope, "source_incomplete", nil)
		}
		return discoveryResult(scope, "source_invalid", nil)
	}
	return discoveryResult(scope, "", value)
}

func parseSitemap(data []byte, base *url.URL, origin string) (sitemapEvidence, error) {
	v := sitemapEvidence{URL: safeWebURL(nil, base.String()), Status: "verified"}
	d := xml.NewDecoder(bytes.NewReader(data))
	var depth int
	var rootDone, record bool
	var field, loc, lastmod string
	var haveLoc, haveLast bool
	var limit int
	invalid := errors.New("invalid sitemap XML")
	for {
		token, err := d.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return v, invalid
		}
		switch tok := token.(type) {
		case xml.Directive:
			return v, invalid
		case xml.ProcInst:
			if tok.Target != "xml" || depth != 0 || rootDone {
				return v, invalid
			}
		case xml.StartElement:
			depth++
			if depth > 12 {
				return v, egress.ErrBudget
			}
			switch depth {
			case 1:
				if rootDone || tok.Name.Space != sitemapNS {
					return v, invalid
				}
				switch tok.Name.Local {
				case "urlset":
					v.Kind, limit = "urlset", 40
				case "sitemapindex":
					v.Kind, limit = "sitemapindex", 20
				default:
					return v, invalid
				}
			case 2:
				record = tok.Name.Space == sitemapNS && (v.Kind == "urlset" && tok.Name.Local == "url" || v.Kind == "sitemapindex" && tok.Name.Local == "sitemap")
				if tok.Name.Space == sitemapNS && !record {
					return v, invalid
				}
				loc, lastmod, haveLoc, haveLast = "", "", false, false
			case 3:
				if record && tok.Name.Space == sitemapNS {
					switch tok.Name.Local {
					case "loc":
						if haveLoc {
							return v, invalid
						}
						field, haveLoc = "loc", true
					case "lastmod":
						if haveLast {
							return v, invalid
						}
						field, haveLast = "lastmod", true
					}
				}
			default:
				if field != "" {
					return v, invalid
				}
			}
		case xml.CharData:
			if depth == 0 && len(bytes.TrimSpace(tok)) != 0 {
				return v, invalid
			}
			switch field {
			case "loc":
				if len(loc)+len(tok) > 2048 {
					return v, egress.ErrBudget
				}
				loc += string(tok)
			case "lastmod":
				if len(lastmod)+len(tok) > 128 {
					return v, egress.ErrBudget
				}
				lastmod += string(tok)
			}
		case xml.EndElement:
			switch depth {
			case 1:
				rootDone = true
			case 2:
				if record {
					if v.Total >= maxSitemapRecords {
						return v, egress.ErrBudget
					}
					v.record(loc, lastmod, origin, limit)
				}
				record = false
			case 3:
				field = ""
			}
			depth--
		}
	}
	if !rootDone || depth != 0 {
		return v, invalid
	}
	return v, nil
}

func (v *sitemapEvidence) record(loc, lastmod, origin string, limit int) {
	v.Total++
	loc = strings.TrimSpace(loc)
	u, err := url.Parse(loc)
	if err != nil || !u.IsAbs() {
		v.Invalid++
		return
	}
	link := pageURL(nil, loc)
	if link == "" {
		v.Invalid++
		return
	}
	relation := "external"
	if siteURL(link, origin) || link == origin {
		relation = "same_origin"
		v.SameOrigin++
	} else {
		v.External++
	}
	value, state := sitemapDate(strings.TrimSpace(lastmod))
	entry := sitemapEntry{URL: link, Relation: relation, LastMod: value, LastModState: state}
	i, exists := slices.BinarySearchFunc(v.Sample, entry, compareSitemap)
	if exists {
		return
	}
	if len(v.Sample) < limit {
		v.Sample = slices.Insert(v.Sample, i, entry)
	} else {
		v.SampleTruncated = true
		if i < limit {
			copy(v.Sample[i+1:], v.Sample[i:limit-1])
			v.Sample[i] = entry
		}
	}
}

func compareSitemap(a, b sitemapEntry) int {
	if c := strings.Compare(a.URL, b.URL); c != 0 {
		return c
	}
	if c := strings.Compare(a.LastMod, b.LastMod); c != 0 {
		return c
	}
	return strings.Compare(a.LastModState, b.LastModState)
}

func sitemapDate(s string) (string, string) {
	if s == "" {
		return "", "missing"
	}
	if d, err := time.Parse(time.DateOnly, s); err == nil {
		return d.Format(time.DateOnly), "valid"
	}
	if d, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return d.UTC().Format(time.RFC3339Nano), "valid"
	}
	return "", "invalid"
}

func normalizeSitemap(t Task, state string, data jsontext.Value) (jsontext.Value, jsontext.Value, error) {
	v, err := ObservationObject[sitemapEvidence](data)
	limit := 40
	if v.Kind == "sitemapindex" {
		limit = 20
	}
	valid := err == nil && state == "present" && v.Status == "verified" && siteURL(v.URL, t.ExternalKey) &&
		(v.Kind == "urlset" || v.Kind == "sitemapindex") && v.Total >= 0 && v.Total <= maxSitemapRecords &&
		v.SameOrigin >= 0 && v.External >= 0 && v.Invalid >= 0 && v.SameOrigin <= v.Total && v.External <= v.Total && v.Invalid <= v.Total &&
		v.Total == v.SameOrigin+v.External+v.Invalid && len(v.Sample) <= limit && len(v.Sample) <= v.SameOrigin+v.External
	for _, entry := range v.Sample {
		rel := "external"
		if siteURL(entry.URL, t.ExternalKey) || entry.URL == t.ExternalKey {
			rel = "same_origin"
		}
		date, kind := sitemapDate(entry.LastMod)
		valid = valid && !entry.Verified && entry.Relation == rel && entry.URL != "" && entry.URL == pageURL(nil, entry.URL) &&
			date == entry.LastMod && (entry.LastModState == kind || entry.LastMod == "" && entry.LastModState == "invalid")
	}
	slices.SortFunc(v.Sample, compareSitemap)
	return webData(valid, v, nil)
}
