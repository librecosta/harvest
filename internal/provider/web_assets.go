package provider

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"encoding/xml"
	"errors"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/librecosta/harvest/internal/egress"
)

type dimensions struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

type manifestIcon struct {
	URL   string   `json:"url"`
	MIME  string   `json:"mime,omitempty"`
	Sizes []string `json:"sizes,omitempty"`
}

type manifestEvidence struct {
	Name      string         `json:"name,omitempty"`
	ShortName string         `json:"short_name,omitempty"`
	StartURL  string         `json:"start_url,omitempty"`
	Icons     []manifestIcon `json:"icons,omitempty"`
}

type assetEvidence struct {
	URL           string            `json:"url"`
	Kind          string            `json:"kind"`
	Status        string            `json:"status"`
	MIME          string            `json:"mime"`
	Bytes         int               `json:"bytes"`
	SHA256        string            `json:"sha256"`
	DeclaredSizes []string          `json:"declared_sizes,omitempty"`
	Dimensions    []dimensions      `json:"dimensions,omitempty"`
	Manifest      *manifestEvidence `json:"manifest,omitempty"`
}

func normalizeAssetParams(data jsontext.Value) (jsontext.Value, error) {
	p, err := Object[AssetParams](data)
	if p.Kind == "" {
		p.Kind = "icon"
	}
	if err != nil || (p.Kind != "icon" && p.Kind != "manifest") || p.Index < 0 || p.Index > 4 ||
		(p.ObservationID == nil && (p.Kind != "icon" || p.Index != 0)) ||
		(p.ObservationID != nil && (p.ObservationID[6]>>4 != 7 || p.ObservationID[8]>>6 != 2)) {
		return nil, errors.New("invalid asset selection")
	}
	return json.Marshal(p)
}

// ApprovedAssetPath derives a path from the exact stored homepage payload.
// Old B1 observations carry no eligibility provenance.
func ApprovedAssetPath(origin string, p AssetParams, data jsontext.Value) (string, []string) {
	page, err := ObservationObject[pageEvidence](data)
	if err != nil || len(page.AssetCandidates) > 5 {
		return "", nil
	}
	i := 0
	for _, hint := range page.AssetCandidates {
		if hint.Kind != p.Kind {
			continue
		}
		if i == p.Index {
			if approvedAsset(origin, hint) {
				return strings.TrimPrefix(hint.URL, origin), hint.Sizes
			}
			return "", nil
		}
		i++
	}
	return "", nil
}

func acquireAssets(ctx context.Context, t Task, client egress.Client) Result {
	scope := discoveryScope(t, "assets")
	params, err := Object[AssetParams](t.Params)
	if err != nil {
		return discoveryResult(scope, "source_invalid", nil)
	}
	path := egress.FaviconPath
	if params.ObservationID != nil {
		path = t.WebPath
		if path == "" {
			return discoveryResult(scope, "candidate_unavailable", nil)
		}
	}
	accept := "image/png, image/jpeg, image/gif, image/x-icon, image/svg+xml"
	limit := int64(assetLimit)
	if params.Kind == "manifest" {
		accept = "application/manifest+json, application/json"
		limit = manifestLimit
	}
	res, err := discoveryRequest(ctx, t, client, path, accept)
	if err != nil {
		return discoveryResult(scope, discoveryNetworkCode(err), nil)
	}
	defer res.Body.Close()
	if code := discoveryStatus(res.StatusCode); code != "" {
		return discoveryFailure(scope, res, code)
	}
	body, code := discoveryBody(res, limit, false)
	if code != "" {
		return discoveryResult(scope, code, nil)
	}
	media := mediaType(res)
	base := finalURL(res)
	v := assetEvidence{URL: safeWebURL(nil, base.String()), Kind: params.Kind, Status: "verified", MIME: media, Bytes: len(body), SHA256: hashWeb(body)}
	if params.Kind == "manifest" {
		if media != "application/manifest+json" && media != "application/json" {
			return discoveryResult(scope, "source_invalid", nil)
		}
		v.Manifest, err = parseManifest(body, base, t.ExternalKey)
	} else {
		v.DeclaredSizes = t.AssetSizes
		v.MIME, v.Dimensions, err = inspectIcon(media, body)
	}
	if err != nil {
		return discoveryResult(scope, "source_invalid", nil)
	}
	return discoveryResult(scope, "", v)
}

func parseManifest(body []byte, base *url.URL, origin string) (*manifestEvidence, error) {
	var raw struct {
		Name      string `json:"name"`
		ShortName string `json:"short_name"`
		StartURL  string `json:"start_url"`
		Icons     []struct {
			Src   string `json:"src"`
			Type  string `json:"type"`
			Sizes string `json:"sizes"`
		} `json:"icons"`
	}
	body = bytes.TrimSpace(body)
	if len(body) == 0 || body[0] != '{' || json.Unmarshal(body, &raw) != nil {
		return nil, errors.New("invalid Web manifest")
	}
	v := &manifestEvidence{Name: cleanText(raw.Name, 256), ShortName: cleanText(raw.ShortName, 128)}
	if start := pageURL(base, raw.StartURL); siteURL(start, origin) || start == origin {
		v.StartURL = start
	}
	for _, icon := range raw.Icons {
		if link := pageURL(base, icon.Src); link != "" && len(v.Icons) < 8 {
			kind := icon.Type
			if !iconMIME(kind) {
				kind = ""
			}
			v.Icons = append(v.Icons, manifestIcon{link, kind, parseSizes(icon.Sizes)})
		}
	}
	slices.SortFunc(v.Icons, func(a, b manifestIcon) int {
		if c := strings.Compare(a.URL, b.URL); c != 0 {
			return c
		}
		if c := strings.Compare(a.MIME, b.MIME); c != 0 {
			return c
		}
		return slices.Compare(a.Sizes, b.Sizes)
	})
	return v, nil
}

func parseSizes(value string) []string {
	var out []string
	for part := range strings.FieldsSeq(strings.ToLower(value)) {
		if part == "any" {
			out = append(out, part)
		} else if x, y, ok := strings.Cut(part, "x"); ok {
			w, we := strconv.Atoi(x)
			h, he := strconv.Atoi(y)
			if we == nil && he == nil && validDimensions(dimensions{w, h}) {
				out = append(out, strconv.Itoa(w)+"x"+strconv.Itoa(h))
			}
		}
		if len(out) == 8 {
			break
		}
	}
	return sortedUnique(out)
}

func iconMIME(s string) bool {
	return slices.Contains([]string{"image/png", "image/jpeg", "image/gif", "image/svg+xml", "image/x-icon", "image/vnd.microsoft.icon"}, s)
}

func validDimensions(d dimensions) bool {
	return d.Width > 0 && d.Width <= 4096 && d.Height > 0 && d.Height <= 4096
}

func inspectIcon(media string, body []byte) (string, []dimensions, error) {
	invalid := errors.New("invalid icon")
	switch media {
	case "image/svg+xml":
		d := xml.NewDecoder(bytes.NewReader(body))
		for range 16 {
			token, err := d.Token()
			if err != nil {
				break
			}
			switch x := token.(type) {
			case xml.Directive:
				return "", nil, invalid
			case xml.StartElement:
				if x.Name.Local == "svg" && (x.Name.Space == "" || x.Name.Space == "http://www.w3.org/2000/svg") {
					return media, nil, nil
				}
				return "", nil, invalid
			}
		}
		return "", nil, invalid
	case "image/x-icon", "image/vnd.microsoft.icon", "application/octet-stream":
		icons, err := icoDimensions(body)
		return "image/vnd.microsoft.icon", icons, err
	case "image/png", "image/jpeg", "image/gif":
		v, format, err := image.DecodeConfig(bytes.NewReader(body))
		size := dimensions{v.Width, v.Height}
		if err == nil && "image/"+format == media && validDimensions(size) {
			return media, []dimensions{size}, nil
		}
	}
	return "", nil, invalid
}

func icoDimensions(body []byte) ([]dimensions, error) {
	invalid := errors.New("invalid ICO directory")
	if len(body) < 6 || !bytes.Equal(body[:4], []byte{0, 0, 1, 0}) {
		return nil, invalid
	}
	n := int(binary.LittleEndian.Uint16(body[4:]))
	end := 6 + n*16
	if n < 1 || n > 16 || len(body) < end {
		return nil, invalid
	}
	out := make([]dimensions, 0, n)
	for row := 6; row < end; row += 16 {
		entry := body[row : row+16]
		size, offset := uint64(binary.LittleEndian.Uint32(entry[8:])), uint64(binary.LittleEndian.Uint32(entry[12:]))
		if size == 0 || offset < uint64(end) || offset+size > uint64(len(body)) {
			return nil, invalid
		}
		w, h := int(entry[0]), int(entry[1])
		if w == 0 {
			w = 256
		}
		if h == 0 {
			h = 256
		}
		out = append(out, dimensions{w, h})
	}
	slices.SortFunc(out, func(a, b dimensions) int {
		if a.Width != b.Width {
			return a.Width - b.Width
		}
		return a.Height - b.Height
	})
	return slices.Compact(out), nil
}

func normalizeAssets(t Task, state string, data jsontext.Value) (jsontext.Value, jsontext.Value, error) {
	v, err := ObservationObject[assetEvidence](data)
	p, _ := Object[AssetParams](t.Params)
	valid := err == nil && state == "present" && v.Status == "verified" && v.Kind == p.Kind && siteURL(v.URL, t.ExternalKey) &&
		egress.DiscoveryPath(strings.TrimPrefix(v.URL, t.ExternalKey)) && webDigest(v.SHA256) && v.Bytes > 0 && v.Bytes <= assetLimit
	if v.Kind == "manifest" && v.Manifest != nil {
		m := v.Manifest
		valid = valid && (v.MIME == "application/manifest+json" || v.MIME == "application/json") && v.Bytes <= manifestLimit &&
			validText(m.Name, 256) && validText(m.ShortName, 128) && len(m.Icons) <= 8 && len(v.Dimensions) == 0 && len(v.DeclaredSizes) == 0 &&
			(m.StartURL == "" || m.StartURL == t.ExternalKey || siteURL(m.StartURL, t.ExternalKey))
		for _, icon := range m.Icons {
			valid = valid && icon.URL != "" && icon.URL == pageURL(nil, icon.URL) && (icon.MIME == "" || iconMIME(icon.MIME)) &&
				slices.Equal(icon.Sizes, parseSizes(strings.Join(icon.Sizes, " ")))
		}
		// JSON property order and ignored fields are diagnostics only.
		return webData(valid, v, struct {
			URL      string
			Manifest *manifestEvidence
		}{v.URL, m})
	}
	valid = valid && v.Kind == "icon" && v.Manifest == nil && iconMIME(v.MIME) && len(v.Dimensions) <= 16 &&
		(v.MIME == "image/svg+xml") == (len(v.Dimensions) == 0) &&
		slices.Equal(v.DeclaredSizes, parseSizes(strings.Join(v.DeclaredSizes, " ")))
	for _, d := range v.Dimensions {
		valid = valid && validDimensions(d)
	}
	return webData(valid, v, nil)
}
