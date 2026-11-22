package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/gofurry/steam-go/api/storebrowseservice"
	"github.com/librecosta/harvest/internal/egress"
)

const (
	SteamAssets         = "steam.assets"
	SteamAssetsKey      = SteamAssets + ":1:1:steam_assets_v1"
	steamBrowsePath     = "/IStoreBrowseService/GetItems/v1/"
	steamBrowseMaxBytes = 512 << 10
	steamBrowseSurface  = "store_browse"
	steamAssetsLimit    = 32
	steamAssetsEmpty    = "store_assets_empty"
)

// StoreBrowseProof is a Worker-only diagnostic without response content or credentials.
type StoreBrowseProof struct {
	LibraryLogoSlots                                                        []string
	HTTPStatus, ResponseBytes, Success, ItemCount, AssetCount, SegmentBytes int
	StoreItems, MatchingApp, Application, Visible, AssetsPresent            bool
	Elapsed                                                                 time.Duration
}

func steamAssetsCapability(fixture *egress.FixtureNetwork) Capability {
	return Capability{
		Offer: Offer{SteamAssets, 1, 1, "steam_assets_v1"}, Resource: "app", MinInterval: 6 * time.Hour, Timeout: 20 * time.Second,
		NormalizeParams: normalizeSteamDetailsParams, Execute: acquireSteamAssets,
		Network: &egress.Source{Steam: true, AllowDirect: true, RequireRegion: true, Origins: []string{steamAPI},
			FixedPath: steamBrowsePath, MaxRequests: 1, MaxBodyBytes: steamBrowseMaxBytes, ContentTypes: []string{"application/json", "text/json"}, FixtureNetwork: fixture},
		Observation: &ObservationPolicy{Source: "steam", MaxDataBytes: steamSegmentBytes, MaxSegmentBytes: steamSegmentBytes,
			Scopes: func(t Task) []SegmentScope { return []SegmentScope{steamScope(t, "assets")} }, NormalizeScopedData: normalizeSteamAssets,
			EmptyEvidence: steamAssetsEmpty,
			DiagnosticEvidence: append(slices.Clone(webDiagnostics), "source_invalid", "source_incomplete", "source_redirect",
				"steam_unavailable", "upstream_unavailable", "upstream_blocked"),
		},
	}
}

func steamAssetsRequest(appID uint32, raw jsontext.Value) (storebrowseservice.GetItemsRequest, bool) {
	p, err := Object[steamDetailsParams](raw)
	if err != nil || appID == 0 || steamLanguage(p.Language) != p.Language || p.Language == "" ||
		!slices.Contains([]string{"CN", "US", "HK"}, p.Country) {
		return storebrowseservice.GetItemsRequest{}, false
	}
	return storebrowseservice.GetItemsRequest{
		IDs:         []storebrowseservice.StoreItemID{{AppID: appID}},
		Context:     &storebrowseservice.StoreBrowseContext{CountryCode: p.Country, Language: p.Language},
		DataRequest: &storebrowseservice.StoreBrowseDataRequest{IncludeAssets: true},
	}, true
}

// Build only the attested URI. The SDK constructs and sends the HTTP request.
func steamAssetsURI(app string, raw jsontext.Value) string {
	appID, _ := strconv.ParseUint(app, 10, 32)
	request, ok := steamAssetsRequest(uint32(appID), raw)
	if !ok {
		return ""
	}
	// The SDK still uses v1 omitempty for zero numeric ID alternatives.
	body, err := json.Marshal(request, json.OmitZeroStructFields(true))
	if err != nil {
		return ""
	}
	return steamBrowsePath + "?" + (url.Values{"input_json": {string(body)}}).Encode()
}

type steamBrowseItem struct {
	ItemType *int                      `json:"item_type"`
	ID       *uint32                   `json:"id"`
	AppID    *uint32                   `json:"appid"`
	Success  *int                      `json:"success"`
	Visible  *bool                     `json:"visible"`
	Assets   map[string]jsontext.Value `json:"assets"`
}

type steamAsset struct {
	ID         string    `json:"asset_id"`
	Type       string    `json:"asset_type"`
	Family     string    `json:"asset_family"`
	Slot       string    `json:"source_slot"`
	URL        string    `json:"url"`
	Locale     string    `json:"locale"`
	Source     string    `json:"source_surface"`
	ObservedAt time.Time `json:"observed_at,omitzero"`
	Digest     string    `json:"source_digest,omitempty"`
}

type steamAssetSet struct {
	AppID     uint32       `json:"app_id"`
	Assets    []steamAsset `json:"assets"`
	Truncated bool         `json:"truncated"`
}

func acquireSteamAssets(ctx context.Context, t Task, network egress.Client) Result {
	proof := &StoreBrowseProof{}
	start := time.Now()
	finish := func(r Result) Result {
		proof.Elapsed = time.Since(start)
		r.StoreBrowse = proof
		return r
	}
	fail := func(code string) Result { return finish(steamFailed(t, code, "assets")) }
	appID := steamAppID(t)
	request, ok := steamAssetsRequest(appID, t.Params)
	if !ok {
		return finish(Result{State: "failed", Code: "invalid_payload"})
	}
	sdk, err := newSteamClientWithOptions(steamTransport{client: network, maxBytes: steamBrowseMaxBytes, status: &proof.HTTPStatus})
	if err != nil {
		return fail("policy_denied")
	}
	defer sdk.Close()
	body, err := sdk.API.StoreBrowseService.GetItemsRaw(ctx, request)
	observedAt := time.Now().UTC()
	if err != nil {
		return fail(steamError(err))
	}
	proof.ResponseBytes = len(body)
	var envelope struct {
		Response *struct {
			Items *[]steamBrowseItem `json:"store_items"`
		} `json:"response"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		return fail("source_invalid")
	}
	if envelope.Response == nil || envelope.Response.Items == nil {
		return fail("source_incomplete")
	}
	items := *envelope.Response.Items
	proof.StoreItems, proof.ItemCount = true, len(items)
	if len(items) == 0 {
		return fail("steam_unavailable")
	}
	if len(items) != 1 {
		return fail("source_invalid")
	}
	item := items[0]
	if item.Success != nil {
		proof.Success = *item.Success
	}
	proof.Application = item.ItemType != nil && *item.ItemType == 0
	proof.MatchingApp = (item.AppID != nil || item.ID != nil) &&
		(item.AppID == nil || *item.AppID == appID) && (item.ID == nil || *item.ID == appID)
	proof.Visible = item.Visible != nil && *item.Visible
	proof.AssetsPresent = item.Assets != nil
	if !proof.Application || !proof.MatchingApp {
		return fail("source_invalid")
	}
	if proof.Success != 1 || item.Visible != nil && !*item.Visible {
		return fail("steam_unavailable")
	}
	if item.Assets == nil {
		return fail("source_incomplete")
	}
	for _, slot := range []string{"library_logo", "logo", "library_logo_2x", "logo_2x"} {
		if raw, present := item.Assets[slot]; present && string(raw) != "null" && string(raw) != `""` {
			proof.LibraryLogoSlots = append(proof.LibraryLogoSlots, slot)
		}
	}

	rows, rejected := projectSteamAssets(appID, t.Scope.Locale, item.Assets, observedAt)
	var seg Segment
	switch {
	case len(rows) != 0:
		seg = steamSegment(t, "assets", "", nil)
		seg.State = "present"
	case len(item.Assets) == 0 && proof.Visible:
		seg = steamSegment(t, "assets", steamAssetsEmpty, nil)
		seg.State, seg.Evidence = "confirmed_empty", steamAssetsEmpty
	case rejected:
		return fail("source_invalid")
	default:
		return fail("source_incomplete")
	}
	seg.ObservedAt = observedAt
	seg, proof.AssetCount, proof.SegmentBytes = fitSteamAssets(seg, appID, rows)
	if proof.SegmentBytes == 0 {
		return fail("source_invalid")
	}
	return finish(steamResult("", seg))
}

func steamAssetID(appID uint32, slot, locale, link string) string {
	value := strconv.FormatUint(uint64(appID), 10) + "\x00" + slot + "\x00" + locale + "\x00" + link
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func fitSteamAssets(seg Segment, appID uint32, rows []steamAsset) (Segment, int, int) {
	set := steamAssetSet{AppID: appID, Assets: make([]steamAsset, 0, min(len(rows), steamAssetsLimit)), Truncated: len(rows) > steamAssetsLimit}
	for _, row := range rows {
		if len(set.Assets) == steamAssetsLimit {
			break
		}
		set.Assets = append(set.Assets, row)
		var wire []byte
		data, err := json.Marshal(set)
		if err == nil {
			seg.Data = data
			wire, err = json.Marshal(seg)
		}
		if err != nil || len(wire) > steamSegmentBytes {
			set.Assets = set.Assets[:len(set.Assets)-1]
			set.Truncated = true
		}
	}
	if seg.State == "present" && len(set.Assets) == 0 {
		return seg, 0, 0
	}
	var err error
	seg.Data, err = json.Marshal(set)
	if err != nil {
		return seg, 0, 0
	}
	wire, err := json.Marshal(seg)
	if err != nil || len(wire) > steamSegmentBytes {
		return seg, 0, 0
	}
	return seg, len(set.Assets), len(wire)
}

func normalizeSteamAssets(t Task, scope SegmentScope, state string, data jsontext.Value) (jsontext.Value, jsontext.Value, error) {
	v, err := ObservationObject[steamAssetSet](data)
	if err != nil || v.Assets == nil || v.AppID != steamAppID(t) || len(v.Assets) > steamAssetsLimit ||
		(state == "present" && len(v.Assets) == 0) || (state == "confirmed_empty" && (len(v.Assets) != 0 || v.Truncated)) ||
		(state != "present" && state != "confirmed_empty") {
		return webData(false, v, nil)
	}
	semantic := v
	semantic.Assets = slices.Clone(v.Assets)
	last := -1
	for i, row := range v.Assets {
		index := slices.IndexFunc(steamAssetSlots, func(slot steamAssetSlot) bool { return slot.kind == row.Type })
		if index <= last {
			return webData(false, v, nil)
		}
		slot := steamAssetSlots[index]
		link, digest := validateSteamAssetURL(row.URL, v.AppID, slot.family == "community")
		if !slot.hasSlot(row.Slot) || link == "" || row.URL != link || row.Digest != digest ||
			row.Family != slot.family || row.Source != steamBrowseSurface || row.Locale != scope.Locale ||
			row.ID != steamAssetID(v.AppID, row.Slot, row.Locale, row.URL) ||
			row.ObservedAt.Year() < 1970 || row.ObservedAt.Year() > 9999 || row.ObservedAt.UTC() != row.ObservedAt ||
			row.ObservedAt != v.Assets[0].ObservedAt {
			return webData(false, v, nil)
		}
		last = index
		semantic.Assets[i].ObservedAt = time.Time{}
	}
	return webData(true, v, semantic)
}
