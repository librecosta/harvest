package provider

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	steam "github.com/gofurry/steam-go"
	"golang.org/x/net/html"

	"github.com/librecosta/harvest/internal/egress"
)

const (
	steamStore        = "https://store.steampowered.com"
	steamAPI          = "https://api.steampowered.com"
	steamDetailsPath  = "/api/appdetails"
	steamNewsPath     = "/ISteamNews/GetNewsForApp/v2/"
	steamPlayersPath  = "/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
	steamDetailsLimit = 512 << 10
	steamNewsLimit    = 128 << 10
	steamPlayersLimit = 4 << 10
	steamNewsExcerpt  = 512
)

type steamDetailsParams struct {
	Language string `json:"language"`
	Country  string `json:"store_country"`
}

type steamNewsParams struct {
	Count uint32 `json:"count"`
}

type steamSource struct {
	AppID uint32 `json:"app_id"`
	Tier  string `json:"tier"`
	URL   string `json:"source_url"`
}

func SteamManifest(fixture *egress.FixtureNetwork) Manifest {
	specs := []struct {
		name, origin, path string
		sections           []string
		budget             int64
		minInterval        time.Duration
		params             func(jsontext.Value) (jsontext.Value, error)
		run                func(context.Context, Task, egress.Client) Result
		normalize          func(Task, SegmentScope, string, jsontext.Value) (jsontext.Value, jsontext.Value, error)
	}{
		{"steam.details", steamStore, steamDetailsPath, []string{"details", "price", "media"}, steamDetailsLimit, time.Hour, normalizeSteamDetailsParams, acquireSteamDetails, normalizeSteamDetails},
		{"steam.news", steamAPI, steamNewsPath, []string{"news"}, steamNewsLimit, time.Hour, normalizeSteamNewsParams, acquireSteamNews, normalizeSteamNews},
		{"steam.players", steamAPI, steamPlayersPath, []string{"players"}, steamPlayersLimit, 30 * time.Minute, Normalize[struct{}], acquireSteamPlayers, normalizeSteamPlayers},
	}
	m := Manifest{Name: "steam", Resources: []Resource{{"app", steamApp}}}
	for _, spec := range specs {
		obs := &ObservationPolicy{
			Scopes: func(t Task) []SegmentScope {
				out := make([]SegmentScope, 0, len(spec.sections))
				for _, section := range spec.sections {
					out = append(out, steamScope(t, section))
				}
				return out
			},
			Source: "steam", MaxDataBytes: 32 << 10, Sample: spec.name == "steam.players",
			NormalizeScopedData: spec.normalize,
			DiagnosticEvidence: append(slices.Clone(webDiagnostics), "source_invalid", "source_incomplete", "source_redirect", "upstream_unavailable",
				"upstream_blocked", "steam_unavailable", "price_unavailable", "price_unreleased", "media_unavailable"),
		}
		if spec.name == "steam.news" {
			obs.EmptyEvidence = "empty_page"
		}
		m.Capabilities = append(m.Capabilities, Capability{
			Offer: Offer{spec.name, 1, 1, "steam_v1"}, Resource: "app", MinInterval: spec.minInterval, Timeout: 20 * time.Second,
			Partial: spec.name == "steam.details", PointInTimeOnly: spec.name == "steam.players", NormalizeParams: spec.params, Execute: spec.run, Observation: obs,
			Network: &egress.Source{Steam: true, AllowDirect: true, RequireRegion: true, Origins: []string{spec.origin},
				FixedPath: spec.path, MaxRequests: 1, MaxBodyBytes: spec.budget, ContentTypes: []string{"application/json", "text/json", "text/javascript"}, FixtureNetwork: fixture},
		})
	}
	return m
}

func steamLanguage(s string) string {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(s), "_", "-")) {
	case "english", "en", "en-us":
		return "english"
	case "schinese", "zh-cn", "zh-hans":
		return "schinese"
	}
	return ""
}

func normalizeSteamDetailsParams(data jsontext.Value) (jsontext.Value, error) {
	v, err := Object[steamDetailsParams](data)
	v.Language, v.Country = steamLanguage(v.Language), strings.ToUpper(strings.TrimSpace(v.Country))
	if err != nil || v.Language == "" || !slices.Contains([]string{"CN", "US", "HK"}, v.Country) {
		return nil, errors.New("unsupported Steam language or store country")
	}
	return json.Marshal(v)
}

func normalizeSteamNewsParams(data jsontext.Value) (jsontext.Value, error) {
	v, err := Object[struct {
		Count *uint32 `json:"count"`
	}](data)
	if err != nil || v.Count != nil && (*v.Count < 1 || *v.Count > 10) {
		return nil, errors.New("invalid Steam news count")
	}
	count := uint32(10)
	if v.Count != nil {
		count = *v.Count
	}
	return json.Marshal(steamNewsParams{count})
}

func NormalizeSteamScope(capability string, data jsontext.Value, scope Scope) (Scope, error) {
	scope.EgressRegion = strings.ToLower(scope.EgressRegion)
	if capability == "steam.details" {
		v, err := Object[steamDetailsParams](data)
		if err != nil || scope.Locale != "" && steamLanguage(scope.Locale) != v.Language ||
			scope.Region != "" && !strings.EqualFold(scope.Region, v.Country) {
			return Scope{}, errors.New("Steam scope conflicts with the requested language or store country")
		}
		scope.Locale, scope.Region = "en", strings.ToLower(v.Country)
		if v.Language == "schinese" {
			scope.Locale = "zh-cn"
		}
	} else if scope.Locale != "" || scope.Region != "" {
		return Scope{}, errors.New("Steam official API scopes only accept egress_region")
	}
	return scope, nil
}

// SteamRequestURI attests the SDK's compiled request shape without carrying
// an arbitrary URL or credentials in a Permit. Both ends derive it from the Job.
func SteamRequestURI(capability, app string, params jsontext.Value) string {
	if normalized, err := steamApp(app); err != nil || normalized != app {
		return ""
	}
	query := url.Values{"appid": {app}}
	switch capability {
	case "steam.details":
		p, err := Object[steamDetailsParams](params)
		if err != nil || steamLanguage(p.Language) != p.Language || p.Language == "" || !slices.Contains([]string{"CN", "US", "HK"}, p.Country) {
			return ""
		}
		query = url.Values{"appids": {app}, "cc": {p.Country}, "l": {p.Language}}
		return steamDetailsPath + "?" + query.Encode()
	case "steam.news":
		p, err := Object[steamNewsParams](params)
		if err != nil || p.Count < 1 || p.Count > 10 {
			return ""
		}
		query.Set("count", strconv.FormatUint(uint64(p.Count), 10))
		query.Set("maxlength", strconv.Itoa(steamNewsExcerpt))
		return steamNewsPath + "?" + query.Encode()
	case "steam.players":
		return steamPlayersPath + "?" + query.Encode()
	}
	return ""
}

func steamAppID(t Task) uint32 {
	id, _ := strconv.ParseUint(t.ExternalKey, 10, 32)
	return uint32(id)
}

func steamProvenance(t Task) steamSource {
	switch t.Offer.Capability {
	case "steam.details":
		return steamSource{steamAppID(t), "observed_storefront", steamStore + steamDetailsPath}
	case "steam.news":
		return steamSource{steamAppID(t), "official_api", steamAPI + steamNewsPath}
	default:
		return steamSource{steamAppID(t), "official_api", steamAPI + steamPlayersPath}
	}
}

func steamScope(t Task, section string) SegmentScope {
	return SegmentScope{Namespace: t.Namespace, Scope: t.Scope, Dimensions: map[string]string{"section": section}}
}

func steamSegment(t Task, section, code string, data any) Segment {
	state, evidence := "present", ""
	if data == nil {
		state, evidence = "unavailable", code
		if code == "source_invalid" || code == "source_incomplete" {
			state = "invalid"
		}
	} else if code == "empty_page" {
		state, evidence = "confirmed_empty", code
	}
	s := webSegment(steamScope(t, section), state, evidence, data)
	s.Source = "steam"
	return s
}

func steamResult(code string, segments ...Segment) Result {
	scopes := make([]SegmentScope, len(segments))
	for i := range segments {
		scopes[i] = segments[i].Scope
	}
	return webResult(scopes, code, segments...)
}

func steamFailed(t Task, code string, sections ...string) Result {
	segments := make([]Segment, 0, len(sections))
	for _, section := range sections {
		segments = append(segments, steamSegment(t, section, code, nil))
	}
	return steamResult(code, segments...)
}

var errSteamJSON = errors.New("invalid Steam response JSON")

func steamError(err error) string {
	switch {
	case errors.Is(err, egress.ErrUpstreamBlocked):
		return "upstream_blocked"
	case errors.Is(err, egress.ErrBudget):
		return "source_incomplete"
	case errors.Is(err, errSteamJSON), errors.Is(err, egress.ErrResponse):
		return "source_invalid"
	}
	var api *steam.APIError
	if errors.As(err, &api) {
		if code := webStatusCode(api.StatusCode); code != "" {
			return code
		}
		switch {
		case api.StatusCode >= 500:
			return "upstream_unavailable"
		case api.StatusCode >= 300 && api.StatusCode < 400:
			return "source_redirect"
		case api.Kind == steam.ErrorKindDecode:
			return "source_invalid"
		case api.Kind == steam.ErrorKindAPIResponse:
			return "steam_unavailable"
		case api.Kind == steam.ErrorKindRequestBuild:
			return "policy_denied"
		}
	}
	return webNetworkCode(err)
}

// Steam uses HTML in text fields. Retain visible text and discard markup,
// script/style contents and attributes, including URLs in those attributes.
func steamText(raw string, limit int) string {
	if !strings.Contains(raw, "<") {
		return cleanText(raw, limit)
	}
	z := html.NewTokenizer(strings.NewReader(raw))
	z.SetMaxBuf(64 << 10)
	var text strings.Builder
	hidden := false
	for text.Len() < 2*limit {
		switch z.Next() {
		case html.ErrorToken:
			return cleanText(text.String(), limit)
		case html.StartTagToken:
			tag, _ := z.TagName()
			hidden = hidden || string(tag) == "script" || string(tag) == "style"
		case html.EndTagToken:
			tag, _ := z.TagName()
			if string(tag) == "script" || string(tag) == "style" {
				hidden = false
			}
		case html.TextToken:
			if !hidden {
				text.Write(z.Text())
				text.WriteByte(' ')
			}
		}
	}
	return cleanText(text.String(), limit)
}
