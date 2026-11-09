package provider

import (
	"bytes"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"io"
	"net/url"
	"slices"
	"strings"
	"unicode/utf8"

	"golang.org/x/net/html"
	"golang.org/x/net/html/charset"
)

type linkedEntity struct {
	Kind        string `json:"kind"`
	Name        string `json:"name,omitempty"`
	URL         string `json:"url,omitempty"`
	Description string `json:"description,omitempty"`
}

type pageFacts struct {
	Title           string            `json:"title,omitempty"`
	Description     string            `json:"description,omitempty"`
	Language        string            `json:"language,omitempty"`
	Canonical       string            `json:"canonical,omitempty"`
	OpenGraph       map[string]string `json:"open_graph,omitempty"`
	Twitter         map[string]string `json:"twitter,omitempty"`
	Icons           []string          `json:"icons,omitempty"`
	Manifest        string            `json:"manifest,omitempty"`
	LinkedData      []linkedEntity    `json:"json_ld,omitempty"`
	Ruleset         string            `json:"ruleset_version"`
	Technology      []technology      `json:"technology,omitempty"`
	Feeds           []feedHint        `json:"feeds,omitempty"`
	AssetCandidates []assetHint       `json:"asset_candidates,omitempty"`
}

type pageEvidence struct {
	pageFacts
	SemanticSHA256 string `json:"semantic_sha256"`
}

var ogFields = []string{"title", "description", "type", "url", "image", "image:alt", "site_name", "locale"}
var twitterFields = []string{"card", "title", "description", "image", "image:alt", "site"}

func pageURL(base *url.URL, raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	value := safeWebURL(base, raw)
	if len(value) > 512 {
		return ""
	}
	return value
}

func parsePage(body []byte, contentType string, base *url.URL, signals webSignals) (pageEvidence, error) {
	enc, label, certain := charset.DetermineEncoding(body, contentType)
	// Prefer full-document UTF-8 over the detector's ambiguous Windows-1252
	// fallback. An explicit HTTP charset or BOM still takes precedence.
	if label == "utf-8" && utf8.Valid(body) {
		body = bytes.TrimPrefix(body, []byte("\xef\xbb\xbf"))
	} else if certain || label != "windows-1252" || !utf8.Valid(body) {
		decoded, err := io.ReadAll(io.LimitReader(enc.NewDecoder().Reader(bytes.NewReader(body)), webBodyLimit+1))
		if err != nil || len(decoded) > webBodyLimit {
			return pageEvidence{}, errors.New("HTML decoding exceeded bounds")
		}
		body = decoded
	}
	out := pageFacts{Ruleset: webRuleset, OpenGraph: make(map[string]string), Twitter: make(map[string]string)}
	z := html.NewTokenizer(bytes.NewReader(body))
	z.SetMaxBuf(64 << 10)
	var title, linked, bodyStarted bool
	var doc bool
	var hints homepageLinks
	for range 25000 {
		switch z.Next() {
		case html.ErrorToken:
			if z.Err() != io.EOF || !doc {
				return pageEvidence{}, errors.New("HTML cannot be interpreted within bounds")
			}
			hints.apply(&out, base)
			out.Icons = sortedUnique(out.Icons)
			out.Technology = identify(signals)
			return pageEvidence{out, pageHash(out)}, nil
		case html.StartTagToken, html.SelfClosingTagToken:
			tag, attrs := z.TagName()
			name := string(tag)
			switch name {
			case "html", "head", "body", "title", "meta", "link", "main", "div", "p", "h1", "script", "section", "article", "astro-island":
				doc = true
			}
			switch name {
			case "body":
				bodyStarted = true
				continue
			case "title":
				title = !bodyStarted && out.Title == ""
				continue
			case "astro-island":
				signals["marker:astro-island"] = true
				continue
			case "html", "meta", "script", "link", "base":
			default:
				continue
			}
			// Consume each attribute once and retain only the fields needed.
			// The tokenizer's buffer bounds total attribute bytes per token.
			values := make(map[string]string)
			for attrs {
				key, value, more := z.TagAttr()
				switch k := string(key); k {
				case "name", "content", "property", "rel", "href", "src", "id", "type", "lang", "xml:lang", "data-template", "title", "sizes":
					if _, exists := values[k]; !exists && len(value) <= 2048 {
						values[k] = string(value)
					}
				}
				attrs = more
			}
			switch name {
			case "html":
				out.Language = cleanText(values["lang"], 35)
				if out.Language == "" {
					out.Language = cleanText(values["xml:lang"], 35)
				}
			case "meta":
				if !bodyStarted {
					pageMeta(&out, base, values, signals)
				}
				if values["id"] == "data-discourse-setup" && values["data-template"] != "" {
					signals["marker:discourse-setup"] = true
				}
			case "script":
				linked = strings.EqualFold(values["type"], "application/ld+json") && len(out.LinkedData) < 4
				if strings.EqualFold(values["type"], "application/json") {
					switch values["id"] {
					case "__NEXT_DATA__":
						signals["marker:next-data"] = true
					case "__NUXT_DATA__":
						signals["marker:nuxt-data"] = true
					}
				}
				signals.asset(base, values["src"])
			case "base":
				if !bodyStarted {
					hints.readBase(base, values["href"])
				}
			case "link":
				if !bodyStarted {
					pageLink(&out, base, values)
					hints.readLink(values)
				}
				for rel := range strings.FieldsSeq(strings.ToLower(values["rel"])) {
					if rel == "stylesheet" || rel == "modulepreload" || rel == "preload" {
						signals.asset(base, values["href"])
					}
				}
			}
		case html.EndTagToken:
			tag, _ := z.TagName()
			switch string(tag) {
			case "title":
				title = false
			case "script":
				linked = false
			}
		case html.TextToken:
			switch {
			case title:
				out.Title = cleanText(string(z.Text()), 512)
				title = false
			case linked:
				out.LinkedData = appendLinked(out.LinkedData, base, z.Raw())
				linked = false
			}
		}
	}
	return pageEvidence{}, errors.New("too many HTML tokens")
}

func pageMeta(out *pageFacts, base *url.URL, attrs map[string]string, signals webSignals) {
	name := strings.ToLower(strings.TrimSpace(attrs["name"]))
	key := strings.ToLower(strings.TrimSpace(attrs["property"]))
	if key == "" {
		key = name
	}
	content := attrs["content"]
	if name == "generator" {
		signals.generator(content)
	}
	if name == "description" && out.Description == "" {
		out.Description = cleanText(content, 1024)
	}
	for _, group := range []struct {
		prefix string
		fields []string
		dest   map[string]string
	}{
		{"og:", ogFields, out.OpenGraph}, {"twitter:", twitterFields, out.Twitter},
	} {
		field, ok := strings.CutPrefix(key, group.prefix)
		if !ok || !slices.Contains(group.fields, field) || group.dest[field] != "" {
			continue
		}
		v := cleanText(content, 512)
		if field == "url" || field == "image" {
			v = pageURL(base, content)
		}
		if v != "" {
			group.dest[field] = v
		}
	}
}

func pageLink(out *pageFacts, base *url.URL, attrs map[string]string) {
	for rel := range strings.FieldsSeq(strings.ToLower(attrs["rel"])) {
		switch rel {
		case "canonical":
			if out.Canonical == "" {
				out.Canonical = pageURL(base, attrs["href"])
			}
		case "manifest":
			if out.Manifest == "" {
				out.Manifest = pageURL(base, attrs["href"])
			}
		case "icon", "apple-touch-icon", "mask-icon":
			if icon := pageURL(base, attrs["href"]); icon != "" && len(out.Icons) < 4 && !slices.Contains(out.Icons, icon) {
				out.Icons = append(out.Icons, icon)
			}
		}
	}
}

type ldNode struct {
	Type        jsontext.Value   `json:"@type"`
	Graph       []jsontext.Value `json:"@graph"`
	Name        string           `json:"name"`
	URL         string           `json:"url"`
	Description string           `json:"description"`
}

func appendLinked(values []linkedEntity, base *url.URL, data []byte) []linkedEntity {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || len(data) > 12<<10 {
		return values
	}
	nodes := []jsontext.Value{data}
	if data[0] == '[' && json.Unmarshal(data, &nodes) != nil {
		return values
	}
	for _, node := range nodes[:min(12, len(nodes))] {
		var item ldNode
		if json.Unmarshal(node, &item) != nil {
			continue
		}
		values = appendLinkedNode(values, base, item)
		for _, child := range item.Graph[:min(12, len(item.Graph))] {
			if len(values) >= 4 {
				break
			}
			var entry ldNode
			if json.Unmarshal(child, &entry) == nil {
				values = appendLinkedNode(values, base, entry)
			}
		}
	}
	return values
}

func appendLinkedNode(values []linkedEntity, base *url.URL, item ldNode) []linkedEntity {
	if len(values) >= 4 {
		return values
	}
	var kind string
	if json.Unmarshal(item.Type, &kind) != nil {
		var types []string
		if json.Unmarshal(item.Type, &types) != nil {
			return values
		}
		for _, candidate := range types {
			if candidate == "WebSite" || candidate == "Organization" {
				kind = candidate
				break
			}
		}
	}
	if kind != "WebSite" && kind != "Organization" {
		return values
	}
	v := linkedEntity{kind, cleanText(item.Name, 256), pageURL(base, item.URL), cleanText(item.Description, 512)}
	if (v.Name != "" || v.URL != "" || v.Description != "") && !slices.Contains(values, v) {
		return append(values, v)
	}
	return values
}

func pageHash(v pageFacts) string {
	data, _ := json.Marshal(v, json.Deterministic(true))
	return hashWeb(data)
}

func normalizeHTML(task Task, state string, value jsontext.Value) (jsontext.Value, jsontext.Value, error) {
	v, err := ObservationObject[pageEvidence](value)
	valid := err == nil && state == "present" && validText(v.Title, 512) && validText(v.Description, 1024) &&
		validText(v.Language, 35) && v.Canonical == pageURL(nil, v.Canonical) && v.Manifest == pageURL(nil, v.Manifest) &&
		len(v.Icons) <= 4 && len(v.LinkedData) <= 4 && v.Ruleset == webRuleset && validTechnology(v.Technology)
	for _, icon := range v.Icons {
		valid = valid && icon != "" && icon == pageURL(nil, icon)
	}
	for _, group := range []struct {
		values map[string]string
		fields []string
	}{{v.OpenGraph, ogFields}, {v.Twitter, twitterFields}} {
		for key, val := range group.values {
			valid = valid && slices.Contains(group.fields, key) && val != "" && validText(val, 513)
			if key == "url" || key == "image" {
				valid = valid && val == pageURL(nil, val)
			}
		}
	}
	for _, item := range v.LinkedData {
		valid = valid && (item.Kind == "WebSite" || item.Kind == "Organization") &&
			validText(item.Name, 256) && validText(item.Description, 512) && item.URL == pageURL(nil, item.URL)
	}
	valid = valid && len(v.Feeds) <= 4 && len(v.AssetCandidates) <= 5
	for _, f := range v.Feeds {
		valid = valid && f.URL != "" && f.URL == pageURL(nil, f.URL) && feedType(f.MIME) &&
			f.Source == "html_alternate" && f.Status == "discovered" && !f.Verified && validText(f.Title, 128)
	}
	for _, a := range v.AssetCandidates {
		valid = valid && a.URL != "" && a.URL == pageURL(nil, a.URL) && (a.Kind == "icon" || a.Kind == "manifest") &&
			(!a.Fetchable || approvedAsset(task.ExternalKey, a)) && slices.Equal(a.Sizes, parseSizes(strings.Join(a.Sizes, " ")))
	}
	return webData(valid && v.SemanticSHA256 == pageHash(v.pageFacts), v, v.pageFacts)
}
