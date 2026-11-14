package provider

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"

	steam "github.com/gofurry/steam-go"
	"github.com/gofurry/steam-go/api/storeservice"
	"github.com/librecosta/harvest/internal/egress"
)

const (
	SteamCatalog     = "steam.catalog"
	catalogOrigin    = "https://partner.steam-api.com"
	catalogPath      = "/IStoreService/GetAppList/v1/"
	catalogBodyLimit = 128 << 10
)

type catalogParams struct {
	Since uint32 `json:"if_modified_since"`
	After uint32 `json:"last_appid"`
	Limit uint32 `json:"max_results"`
}

type catalogPage struct {
	Request catalogParams           `json:"request"`
	Apps    []storeservice.StoreApp `json:"apps"`
	More    bool                    `json:"have_more_results"`
	Last    uint32                  `json:"last_appid"`
}

func catalogCapability(fixture *egress.FixtureNetwork) Capability {
	return Capability{
		Offer: Offer{SteamCatalog, 1, 1, "steam_v1"}, Resource: "catalog", MinInterval: time.Hour, Timeout: 20 * time.Second,
		NormalizeParams: normalizeCatalogParams, Execute: acquireCatalog,
		Network: &egress.Source{Steam: true, SteamCatalog: true, AllowDirect: true, RequireRegion: true,
			Origins: []string{catalogOrigin}, FixedPath: catalogPath, MaxRequests: 1, MaxBodyBytes: catalogBodyLimit,
			ContentTypes: []string{"application/json", "text/json"}, FixtureNetwork: fixture},
		Observation: &ObservationPolicy{Source: "steam", MaxDataBytes: 32 << 10,
			Scopes:              func(t Task) []SegmentScope { return []SegmentScope{steamScope(t, "catalog")} },
			NormalizeScopedData: normalizeCatalog,
			DiagnosticEvidence:  append(append([]string{}, webDiagnostics...), "source_invalid", "source_incomplete", "source_redirect", "steam_unavailable", "upstream_unavailable", "upstream_blocked"),
		},
	}
}

func catalogKey(key string) (string, error) {
	if key != "public" {
		return "", errors.New("invalid Steam catalog key")
	}
	return key, nil
}

func normalizeCatalogParams(raw jsontext.Value) (jsontext.Value, error) {
	p, err := Object[catalogParams](raw)
	if err != nil || p.Limit < 1 || p.Limit > 64 {
		return nil, errors.New("invalid Steam catalog pagination")
	}
	return json.Marshal(p)
}

func catalogURI(key string, raw jsontext.Value) string {
	p, err := Object[catalogParams](raw)
	if key != "public" || err != nil || p.Limit < 1 || p.Limit > 64 {
		return ""
	}
	q := url.Values{"include_games": {"true"}, "include_dlc": {"false"}, "include_software": {"false"},
		"include_videos": {"false"}, "include_hardware": {"false"}, "last_appid": {strconv.FormatUint(uint64(p.After), 10)},
		"max_results": {strconv.FormatUint(uint64(p.Limit), 10)}}
	if p.Since > 0 {
		q.Set("if_modified_since", strconv.FormatUint(uint64(p.Since), 10))
	}
	return catalogPath + "?" + q.Encode()
}

type catalogPresence struct {
	Response *struct {
		Apps *[]jsontext.Value `json:"apps"`
		More *bool             `json:"have_more_results"`
		Last *uint32           `json:"last_appid"`
	} `json:"response"`
}

func acquireCatalog(ctx context.Context, t Task, network egress.Client) Result {
	var presence catalogPresence
	sdk, err := newSteamClientWithOptions(network, catalogBodyLimit, nil, func(raw []byte) bool {
		return json.Unmarshal(raw, &presence) == nil && presence.Response != nil &&
			presence.Response.Apps != nil && presence.Response.More != nil && presence.Response.Last != nil
	}, steam.WithBaseURL(catalogOrigin))
	if err != nil {
		return steamFailed(t, "policy_denied", "catalog")
	}
	defer sdk.Close()
	p, _ := Object[catalogParams](t.Params)
	yes, no := true, false
	opts := &storeservice.GetAppListOptions{IncludeGames: &yes, IncludeDLC: &no, IncludeSoftware: &no, IncludeVideos: &no, IncludeHardware: &no,
		LastAppID: &p.After, MaxResults: &p.Limit}
	if p.Since > 0 {
		opts.IfModifiedSince = &p.Since
	}
	response, err := sdk.API.StoreService.GetAppList(ctx, opts)
	if err != nil {
		return steamFailed(t, steamError(err), "catalog")
	}
	v := response.Response
	page := catalogPage{p, v.Apps, v.HaveMoreResults, v.LastAppID}
	for i := range page.Apps {
		if len(page.Apps[i].Name) > 1024 || strings.ContainsAny(page.Apps[i].Name, "<>") {
			return steamFailed(t, "source_invalid", "catalog")
		}
		page.Apps[i].Name = cleanText(page.Apps[i].Name, 192)
	}
	if !validCatalogPage(p, page, time.Now()) {
		return steamFailed(t, "source_invalid", "catalog")
	}
	return steamResult("", steamSegment(t, "catalog", "", page))
}

func validCatalogPage(p catalogParams, page catalogPage, now time.Time) bool {
	if page.Request != p || page.Apps == nil || len(page.Apps) > int(p.Limit) {
		return false
	}
	last := p.After
	for _, row := range page.Apps {
		if row.AppID <= last || row.LastModified <= 0 || row.LastModified < int64(p.Since) ||
			row.LastModified > now.Add(2*time.Minute).Unix() || row.Name == "" || strings.ContainsAny(row.Name, "<>") || !validText(row.Name, 192) {
			return false
		}
		last = row.AppID
	}
	if len(page.Apps) == 0 {
		return !page.More && (page.Last == 0 || page.Last == last)
	}
	return page.Last == last
}

func normalizeCatalog(t Task, _ SegmentScope, state string, data jsontext.Value) (jsontext.Value, jsontext.Value, error) {
	p, err := Object[catalogParams](t.Params)
	page, parseErr := ObservationObject[catalogPage](data)
	return webData(err == nil && parseErr == nil && state == "present" && validCatalogPage(p, page, time.Now()), page, nil)
}
