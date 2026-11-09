package provider

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json/jsontext"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/librecosta/harvest/internal/egress"
)

type webRedirect struct {
	Status   int    `json:"status"`
	To       string `json:"to,omitempty"`
	Followed bool   `json:"followed"`
}

type httpFacts struct {
	Origin      string            `json:"origin"`
	FinalURL    string            `json:"final_url"`
	Status      int               `json:"status"`
	Redirects   []webRedirect     `json:"redirects,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Technology  []technology      `json:"technology,omitempty"`
	Ruleset     string            `json:"ruleset_version"`
	BodyState   string            `json:"body_state"`
}

type httpEvidence struct {
	httpFacts
	LatencyMS    int64  `json:"latency_ms"`
	Timing       string `json:"timing"`
	Protocol     string `json:"connection_protocol"`
	BodyBytes    int    `json:"body_bytes"`
	BodySHA256   string `json:"body_sha256,omitempty"`
	Truncated    bool   `json:"truncated"`
	EgressKind   string `json:"egress_kind"`
	EgressRegion string `json:"egress_region,omitempty"`
}

func acquireHTTP(ctx context.Context, task Task, client egress.Client) Result {
	scopes := httpScopes(task)
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, task.ExternalKey+"/", nil)
	if err != nil {
		return webResult(scopes, "policy_denied")
	}
	r.Header.Set("Accept", "text/html,application/xhtml+xml;q=0.9,*/*;q=0.5")
	if task.Scope.Locale != "" {
		r.Header.Set("Accept-Language", strings.ReplaceAll(task.Scope.Locale, "_", "-"))
	}
	started := time.Now()
	res, err := client.Do(r)
	elapsed := time.Since(started).Milliseconds()
	if err != nil {
		return webResult(scopes, webNetworkCode(err))
	}
	defer res.Body.Close()
	path := egress.ResponsePath(res)
	kind, _, _ := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if len(kind) > 128 {
		kind = ""
	}
	signals := headerSignals(res.Header)
	value := httpEvidence{
		httpFacts: httpFacts{Origin: task.ExternalKey, FinalURL: task.ExternalKey + "/",
			Status: res.StatusCode, ContentType: kind, Headers: selectedHeaders(res.Header),
			Technology: identify(signals), Ruleset: webRuleset, Redirects: redirectSummary(res), BodyState: "not_read"},
		LatencyMS: elapsed, Timing: "response_headers", Protocol: res.Proto,
		EgressKind: path.Kind, EgressRegion: path.Region,
	}
	if res.Request != nil {
		value.FinalURL = safeWebURL(nil, res.Request.URL.String())
	}
	code := webStatusCode(res.StatusCode)
	var body []byte
	var bodyErr error
	if code == "" && res.StatusCode != http.StatusNotModified {
		body, bodyErr = io.ReadAll(io.LimitReader(res.Body, webBodyLimit+1))
		if len(body) > webBodyLimit {
			body, bodyErr = body[:webBodyLimit], egress.ErrBudget
		}
		value.BodyBytes, value.BodySHA256 = len(body), hashWeb(body)
		value.BodyState = "complete"
		if bodyErr != nil && !errors.Is(bodyErr, io.EOF) {
			value.Truncated, value.BodyState = true, "incomplete"
		}
	}
	page := webSegment(scopes[1], "not_covered", "", nil)
	if res.StatusCode >= 200 && res.StatusCode < 300 && res.StatusCode != http.StatusNoContent &&
		(kind == "text/html" || kind == "application/xhtml+xml") {
		page = parseHTTPPage(scopes[1], res, body, value.Truncated, signals)
	}
	segments := []Segment{webSegment(scopes[0], "present", "", value), page}
	if len(scopes) > 2 {
		segments = append(segments, tlsSegment(task, scopes[2], res))
	}
	return webResult(scopes, code, segments...)
}

func parseHTTPPage(scope SegmentScope, res *http.Response, body []byte, truncated bool, signals webSignals) Segment {
	if truncated || res.StatusCode == http.StatusPartialContent || len(body) == 0 {
		return webSegment(scope, "unavailable", "body_incomplete", nil)
	}
	switch strings.ToLower(strings.TrimSpace(res.Header.Get("Content-Encoding"))) {
	case "", "identity":
	case "gzip":
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return webSegment(scope, "invalid", "html_invalid", nil)
		}
		decoded, err := io.ReadAll(io.LimitReader(reader, webBodyLimit+1))
		_ = reader.Close()
		if err != nil || len(decoded) > webBodyLimit {
			return webSegment(scope, "unavailable", "body_incomplete", nil)
		}
		body = decoded
	default:
		return webSegment(scope, "unavailable", "unsupported_encoding", nil)
	}
	value, err := parsePage(body, res.Header.Get("Content-Type"), res.Request.URL, signals)
	if err != nil {
		return webSegment(scope, "invalid", "html_invalid", nil)
	}
	return webSegment(scope, "present", "", value)
}

func redirectSummary(final *http.Response) []webRedirect {
	var hops []webRedirect
	for res := final; res != nil && res.Request != nil; res = res.Request.Response {
		switch res.StatusCode {
		case 301, 302, 303, 307, 308:
			hop := webRedirect{Status: res.StatusCode, Followed: res != final}
			if loc, err := res.Location(); err == nil {
				hop.To = safeWebURL(nil, loc.String())
			}
			hops = append(hops, hop)
		}
	}
	slices.Reverse(hops)
	return hops
}

func safeWebURL(base *url.URL, raw string) string {
	// The complete URL includes an origin in addition to the 2 KiB request URI.
	const maxURL = 2400
	if len(raw) > maxURL || strings.ContainsAny(raw, "\\") {
		return ""
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.User != nil || u.Opaque != "" || strings.ContainsAny(u.Path, "\\\x00\r\n\t") {
		return ""
	}
	if base != nil {
		u = base.ResolveReference(u)
	}
	u.Host = strings.ToLower(u.Host)
	if (u.Scheme != "http" && u.Scheme != "https") || !egress.ValidHostPort(u) {
		return ""
	}
	u.RawQuery, u.ForceQuery, u.Fragment, u.RawFragment = "", false, "", ""
	if s := u.String(); len(s) <= maxURL {
		return s
	}
	return ""
}

func selectedHeaders(h http.Header) map[string]string {
	out := make(map[string]string)
	for key, allowed := range map[string][]string{
		"strict-transport-security": nil, "content-security-policy": nil,
		"x-content-type-options": {"nosniff"}, "x-frame-options": {"deny", "sameorigin"},
		"referrer-policy": {"no-referrer", "same-origin", "strict-origin", "strict-origin-when-cross-origin", "no-referrer-when-downgrade", "origin", "origin-when-cross-origin", "unsafe-url"},
	} {
		v := strings.ToLower(strings.TrimSpace(h.Get(key)))
		if len(allowed) == 0 && v != "" {
			out[key] = "present"
		} else if slices.Contains(allowed, v) {
			out[key] = v
		}
	}
	var cache []string
	for directive := range strings.SplitSeq(strings.ToLower(h.Get("Cache-Control")), ",") {
		k, v, eq := strings.Cut(strings.TrimSpace(directive), "=")
		switch k {
		case "public", "private", "no-cache", "no-store", "must-revalidate", "immutable":
			cache = append(cache, k)
		case "max-age", "s-maxage":
			if n, err := strconv.ParseUint(v, 10, 32); eq && err == nil {
				cache = append(cache, k+"="+strconv.FormatUint(n, 10))
			}
		}
		if len(cache) == 12 {
			break
		}
	}
	if len(cache) != 0 {
		out["cache-control"] = strings.Join(sortedUnique(cache), ", ")
	}
	return out
}

func normalizeHTTP(task Task, state string, value jsontext.Value) (jsontext.Value, jsontext.Value, error) {
	v, err := ObservationObject[httpEvidence](value)
	valid := err == nil && state == "present" && v.Origin == task.ExternalKey &&
		v.Status >= 200 && v.Status <= 599 && strings.HasPrefix(v.FinalURL, task.ExternalKey+"/") &&
		v.FinalURL == safeWebURL(nil, v.FinalURL) && len(v.Redirects) <= egress.MaxWebRedirects+1 &&
		v.Timing == "response_headers" && v.LatencyMS >= 0 && v.LatencyMS <= 60000 &&
		v.BodyBytes >= 0 && v.BodyBytes <= webBodyLimit && (v.BodySHA256 == "" || webDigest(v.BodySHA256)) &&
		(v.BodyState == "not_read" && v.BodyBytes == 0 && v.BodySHA256 == "" ||
			(v.BodyState == "complete" || v.BodyState == "incomplete") && webDigest(v.BodySHA256)) &&
		v.Truncated == (v.BodyState == "incomplete") &&
		(v.Protocol == "HTTP/1.0" || v.Protocol == "HTTP/1.1") &&
		len(v.ContentType) <= 128 && validText(v.ContentType, 129) && validVantage(v.EgressKind) &&
		(v.EgressRegion == "" || v.EgressRegion == task.Scope.EgressRegion) &&
		v.Ruleset == webRuleset && validTechnology(v.Technology)
	headers := make(http.Header)
	for k, val := range v.Headers {
		headers.Set(k, val)
	}
	clean := selectedHeaders(headers)
	for k, val := range v.Headers {
		valid = valid && clean[k] == val
	}
	for i, r := range v.Redirects {
		valid = valid && slices.Contains([]int{301, 302, 303, 307, 308}, r.Status) &&
			(r.To == "" || r.To == safeWebURL(nil, r.To)) &&
			(r.Followed && i < egress.MaxWebRedirects && strings.HasPrefix(r.To, task.ExternalKey+"/") ||
				!r.Followed && i == len(v.Redirects)-1)
	}
	return webData(valid, v, v.httpFacts)
}
