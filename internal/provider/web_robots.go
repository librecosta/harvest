package provider

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/librecosta/harvest/internal/egress"
)

const robotToken = "HarvestBot"

type robotRule struct {
	Path  string `json:"path"`
	Allow bool   `json:"allow"`
}

type robotsEvidence struct {
	URL      string      `json:"url"`
	Status   string      `json:"status"`
	Agent    string      `json:"agent"`
	Match    string      `json:"match,omitempty"`
	Rules    []robotRule `json:"rules,omitempty"`
	Sitemaps []string    `json:"sitemaps,omitempty"`
}

func acquireRobots(ctx context.Context, t Task, client egress.Client) Result {
	scope := discoveryScope(t, "robots")
	res, err := discoveryRequest(ctx, t, client, egress.RobotsPath, "text/plain")
	if err != nil {
		return discoveryResult(scope, discoveryNetworkCode(err), nil)
	}
	defer res.Body.Close()
	base := finalURL(res)
	if res.StatusCode == 404 {
		return webResult([]SegmentScope{scope}, "", webSegment(scope, "confirmed_empty", "robots_missing",
			robotsEvidence{URL: safeWebURL(nil, base.String()), Status: "missing", Agent: robotToken}))
	}
	if code := discoveryStatus(res.StatusCode); code != "" {
		return discoveryFailure(scope, res, code)
	}
	if media := mediaType(res); media != "text/plain" && media != "" {
		return discoveryResult(scope, "source_invalid", nil)
	}
	data, code := discoveryBody(res, robotsLimit, false)
	if code != "" {
		return discoveryResult(scope, code, nil)
	}
	value, err := parseRobots(data, base)
	if errors.Is(err, egress.ErrBudget) {
		return discoveryResult(scope, "source_incomplete", nil)
	}
	if err != nil {
		return discoveryResult(scope, "source_invalid", nil)
	}
	return discoveryResult(scope, "", value)
}

func parseRobots(raw []byte, base *url.URL) (robotsEvidence, error) {
	out := robotsEvidence{URL: safeWebURL(nil, base.String()), Status: "verified", Agent: robotToken}
	if !utf8.Valid(raw) {
		return out, errors.New("invalid robots encoding")
	}
	best := -1
	group := -1
	hasRules := false
	active := false
	var size int
	var malformed, incomplete, content, recognized bool
	for line := range strings.SplitSeq(strings.TrimPrefix(string(raw), "\ufeff"), "\n") {
		line, _, _ = strings.Cut(line, "#")
		content = content || strings.TrimSpace(line) != ""
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key, value = strings.ToLower(strings.TrimSpace(key)), strings.TrimSpace(value)
		switch key {
		case "user-agent":
			if hasRules {
				group, hasRules, active = -1, false, false
			}
			match := -1
			agent := strings.ToLower(value)
			recognized = recognized || agent == "*" || validProduct(agent)
			if agent == "*" {
				match = 0
			} else if validProduct(agent) && strings.HasPrefix(strings.ToLower(robotToken), agent) {
				match = len(agent)
			}
			if match > group {
				group = match
			}
			if match > best {
				best, size, malformed, incomplete = match, 0, false, false
				out.Rules = nil
				out.Match = agent
			}
			active = group >= 0 && group == best
		case "allow", "disallow":
			hasRules = true
			if !active || value == "" {
				continue
			}
			path, valid := robotOctets(value, true)
			if !valid {
				malformed = true
				continue
			}
			if len(path) > 1024 || len(out.Rules) == 256 || size+len(path) > 12<<10 {
				incomplete = true
				continue
			}
			out.Rules = append(out.Rules, robotRule{path, key == "allow"})
			size += len(path)
		case "sitemap":
			u, err := url.Parse(value)
			if err == nil && u.IsAbs() {
				if hint := pageURL(nil, value); hint != "" && len(out.Sitemaps) < 8 && !slices.Contains(out.Sitemaps, hint) {
					recognized = true
					out.Sitemaps = append(out.Sitemaps, hint)
				}
			}
		}
	}
	if malformed {
		return out, errors.New("malformed robots rule")
	}
	if incomplete {
		return out, egress.ErrBudget
	}
	if content && !recognized {
		return out, errors.New("unrecognized robots policy")
	}
	orderRobots(&out)
	return out, nil
}

func orderRobots(v *robotsEvidence) {
	slices.SortFunc(v.Rules, func(a, b robotRule) int {
		if a.Path != b.Path {
			return strings.Compare(a.Path, b.Path)
		}
		if a.Allow == b.Allow {
			return 0
		}
		if a.Allow {
			return -1
		}
		return 1
	})
	v.Rules = slices.Compact(v.Rules)
	v.Sitemaps = sortedUnique(v.Sitemaps)
}

func validProduct(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

func robotOctets(value string, pattern bool) (string, bool) {
	if !strings.HasPrefix(value, "/") {
		return "", false
	}
	const hex = "0123456789ABCDEF"
	var out strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c == '%' {
			if i+2 >= len(value) {
				return "", false
			}
			pair := strings.ToUpper(value[i+1 : i+3])
			a, b := strings.IndexByte(hex, pair[0]), strings.IndexByte(hex, pair[1])
			if a < 0 || b < 0 {
				return "", false
			}
			c = byte(a<<4 | b)
			i += 2
			if !unreserved(c) {
				out.WriteByte('%')
				out.WriteString(pair)
				continue
			}
		} else if !unreserved(c) && !strings.ContainsRune(":/?[]@!&'()+,;=", rune(c)) &&
			!(pattern && (c == '*' || c == '$' && i == len(value)-1)) {
			out.WriteByte('%')
			out.WriteByte(hex[c>>4])
			out.WriteByte(hex[c&15])
			continue
		}
		out.WriteByte(c)
	}
	return out.String(), true
}

func unreserved(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-._~", rune(c))
}

// AllowsRobots evaluates only a validated policy for HarvestBot. A missing
// policy means an observed 404 and is distinct from an unavailable policy.
func AllowsRobots(data jsontext.Value, uri string) (bool, error) {
	v, err := ObservationObject[robotsEvidence](data)
	if err != nil {
		return false, err
	}
	if v.Status == "missing" {
		return true, nil
	}
	path, ok := robotOctets(uri, false)
	if !ok || v.Status != "verified" {
		return false, errors.New("unknown robots policy")
	}
	allowed, best := true, -1
	for _, r := range v.Rules {
		pattern, anchor := strings.CutSuffix(r.Path, "$")
		n := len(strings.ReplaceAll(pattern, "*", ""))
		if n < best {
			continue
		}
		if !strings.Contains(pattern, "*") {
			if !strings.HasPrefix(path, pattern) || (anchor && path != pattern) {
				continue
			}
		} else {
			pieces := strings.Split(pattern, "*")
			for i := range pieces {
				pieces[i] = regexp.QuoteMeta(pieces[i])
			}
			expr := "^" + strings.Join(pieces, ".*")
			if anchor {
				expr += "$"
			}
			matched, err := regexp.MatchString(expr, path)
			if err != nil || !matched {
				continue
			}
		}
		if n > best {
			allowed, best = r.Allow, n
		} else {
			allowed = allowed || r.Allow
		}
	}
	return allowed, nil
}

func normalizeRobots(t Task, state string, data jsontext.Value) (jsontext.Value, jsontext.Value, error) {
	v, err := ObservationObject[robotsEvidence](data)
	valid := err == nil && siteURL(v.URL, t.ExternalKey) && v.Agent == robotToken && len(v.Rules) <= 256 && len(v.Sitemaps) <= 8
	if state == "confirmed_empty" {
		valid = valid && v.Status == "missing" && v.Match == "" && len(v.Rules) == 0 && len(v.Sitemaps) == 0
	} else {
		valid = valid && state == "present" && v.Status == "verified" && (v.Match == "" || v.Match == "*" || validProduct(v.Match) && strings.HasPrefix(strings.ToLower(robotToken), v.Match))
	}
	size := 0
	for _, r := range v.Rules {
		path, ok := robotOctets(r.Path, true)
		valid = valid && ok && path == r.Path && len(path) <= 1024
		size += len(path)
	}
	for _, u := range v.Sitemaps {
		valid = valid && u != "" && pageURL(nil, u) == u
	}
	orderRobots(&v)
	return webData(valid && size <= 12<<10, v, nil)
}
