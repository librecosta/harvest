package provider

import (
	"bytes"
	json "encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

type browseTransport func(*http.Request) (*http.Response, error)

func (f browseTransport) Do(r *http.Request) (*http.Response, error) { return f(r) }

func assetsTask() Task {
	t := steamMediaTask("440", 1)
	t.Offer = Offer{SteamAssets, 1, 1, "steam_assets_v1"}
	return t
}

func TestSteamAssetsRequestAttestation(t *testing.T) {
	task := assetsTask()
	want := SteamRequestURI(SteamAssets, task.ExternalKey, task.Params)
	calls := 0
	result := acquireSteamAssets(t.Context(), task, browseTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.RequestURI() != want || r.URL.Scheme != "https" || r.URL.Host != "api.steampowered.com" ||
			r.Method != http.MethodGet || r.Header.Get("X-WebAPI-Key") != "" {
			t.Errorf("GetItems request does not match its permit attestation\nactual %s\nwanted %s%s", r.URL.String(), steamAPI, want)
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}},
			Body: io.NopCloser(bytes.NewReader([]byte(`{"response":{"store_items":[]}}`))), Request: r}, nil
	}))
	if calls != 1 || result.StoreBrowse.HTTPStatus != 200 || result.Code != "steam_unavailable" {
		t.Fatalf("unexpected GetItems Request result %d %s %v", calls, result.Code, result.StoreBrowse)
	}
}

const testAssetDigest = "0508d712ca859e3ef0921a32c49088ccfb05b0a0"

func storeBrowseFixture(identity, assets string) string {
	return `{"response":{"store_items":[{` + identity + `,"visible":true,` + assets + `}]}}`
}

func assetsResponse(t *testing.T, task Task, status int, body string) Result {
	t.Helper()
	calls := 0
	source := browseTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.RequestURI() != SteamRequestURI(SteamAssets, task.ExternalKey, task.Params) {
			t.Error("unexpected GetItems URI")
		}
		return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: http.Header{"Content-Type": {"application/json"}},
			ContentLength: int64(len(body)), Request: r, Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	res := acquireSteamAssets(t.Context(), task, source)
	if calls != 1 || res.StoreBrowse == nil || res.StoreBrowse.HTTPStatus != status || res.SteamEvidence != nil {
		t.Fatalf("status/permit request count discrepancy: %d, %v", calls, res.StoreBrowse)
	}
	wire, err := json.Marshal(res)
	if err != nil || bytes.Contains(wire, []byte("StoreBrowse")) || bytes.Contains(wire, []byte("HTTPStatus")) {
		t.Fatal("Worker proof entered the Completion wire")
	}
	return res
}

func TestSteamAssetsSourceStates(t *testing.T) {
	const id = `"item_type":0,"id":440,"appid":440,"success":1`
	empty := storeBrowseFixture(id, `"assets":{}`)
	large := empty + strings.Repeat(" ", steamBrowseMaxBytes-len(empty))
	for _, tc := range []struct {
		label, body, code, state string
		status                   int
	}{
		{"confirmed empty", empty, "", "confirmed_empty", 200},
		{"exact upstream boundary", large, "", "confirmed_empty", 200},
		{"over upstream boundary", large + " ", "source_incomplete", "invalid", 200},
		{"missing assets", storeBrowseFixture(id, `"name":"Fixture"`), "source_incomplete", "invalid", 200},
		{"null assets", storeBrowseFixture(id, `"assets":null`), "source_incomplete", "invalid", 200},
		{"unknown asset", storeBrowseFixture(id, `"assets":{"new_slot":"image.png"}`), "source_incomplete", "invalid", 200},
		{"unsafe asset", storeBrowseFixture(id, `"assets":{"header":"https://example.test/steam/apps/440/image.png"}`), "source_invalid", "invalid", 200},
		{"fallback id", storeBrowseFixture(`"item_type":0,"id":440,"success":1`, `"assets":{}`), "", "confirmed_empty", 200},
		{"appid only", storeBrowseFixture(`"item_type":0,"appid":440,"success":1`, `"assets":{}`), "", "confirmed_empty", 200},
		{"id conflict", storeBrowseFixture(`"item_type":0,"appid":440,"id":441,"success":1`, `"assets":{}`), "source_invalid", "invalid", 200},
		{"package", storeBrowseFixture(`"item_type":1,"id":440,"success":1`, `"assets":{}`), "source_invalid", "invalid", 200},
		{"missing type", storeBrowseFixture(`"id":440,"success":1`, `"assets":{}`), "source_invalid", "invalid", 200},
		{"failed item", storeBrowseFixture(`"item_type":0,"id":440,"success":0`, `"assets":{}`), "steam_unavailable", "unavailable", 200},
		{"missing business success", storeBrowseFixture(`"item_type":0,"id":440`, `"assets":{}`), "steam_unavailable", "unavailable", 200},
		{"boolean unsuccessful", storeBrowseFixture(`"item_type":0,"id":440,"success":false`, `"assets":{}`), "source_invalid", "invalid", 200},
		{"hidden", strings.Replace(empty, `"visible":true`, `"visible":false`, 1), "steam_unavailable", "unavailable", 200},
		{"empty without visibility proof", strings.Replace(empty, `"visible":true,`, "", 1), "source_incomplete", "invalid", 200},
		{"no item", `{"response":{"store_items":[]}}`, "steam_unavailable", "unavailable", 200},
		{"mixed items", `{"response":{"store_items":[{"item_type":0,"appid":440,"success":1,"assets":{}},{"item_type":0,"appid":441}]}}`, "source_invalid", "invalid", 200},
		{"malformed", `{"response":`, "source_invalid", "invalid", 200},
		{"forbidden", `{"error":"API key required"}`, "upstream_auth", "unavailable", 403},
		{"throttled", "", "upstream_throttled", "unavailable", 429},
		{"server error", "", "upstream_unavailable", "unavailable", 500},
	} {
		t.Run(tc.label, func(t *testing.T) {
			task := assetsTask()
			res := assetsResponse(t, task, tc.status, tc.body)
			if len(res.Segments) != 1 || res.Code != tc.code || res.Segments[0].State != tc.state || res.Segments[0].Withdraw {
				t.Fatalf("unexpected diagnostic: %s/%s %+v", res.State, res.Code, res.Segments)
			}
			if tc.state == "confirmed_empty" && (res.State != "succeeded" || res.Segments[0].Evidence != steamAssetsEmpty) {
				t.Fatal("explicit successful empty payload was not accepted")
			}
		})
	}
}

func TestSteamAssetsProjectionAndSemanticHash(t *testing.T) {
	task := assetsTask()
	assetFields := map[string]string{"asset_url_format": "steam/apps/440/${FILENAME}?t=123",
		"community_icon":  testAssetDigest,
		"logo":            testAssetDigest + "/library%20logo.png",
		"main_capsule":    testAssetDigest + "/same.jpg",
		"header":          testAssetDigest + "/same.jpg",
		"library_hero_2x": "invalid//filename",
	}
	for _, key := range []string{"library_capsule", "library_capsule_2x", "library_hero", "logo_2x",
		"small_capsule", "main_capsule_2x", "small_capsule_2x", "header_2x", "hero_capsule", "hero_capsule_2x", "page_background", "raw_page_background"} {
		assetFields[key] = testAssetDigest + "/" + key + ".jpg"
	}
	run := func(task Task, fields map[string]string) (Segment, steamAssetSet, []byte) {
		t.Helper()
		raw, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		body := storeBrowseFixture(`"item_type":0,"id":440,"success":1`, `"assets":`+string(raw))
		res := assetsResponse(t, task, 200, body)
		if res.State != "succeeded" || len(res.StoreBrowse.LibraryLogoSlots) == 0 {
			t.Fatalf("projection failed: %+v", res.StoreBrowse)
		}
		seg := res.Segments[0]
		_, semantic, err := normalizeSteamAssets(task, seg.Scope, seg.State, seg.Data)
		data, decodeErr := ObservationObject[steamAssetSet](seg.Data)
		if err != nil || decodeErr != nil {
			t.Fatalf("compiled validator rejected projection: %v %v", err, decodeErr)
		}
		return seg, data, semantic
	}
	seg, first, hash := run(task, assetFields)
	_, again, repeatHash := run(task, assetFields)
	if len(first.Assets) != 16 || !bytes.Equal(hash, repeatHash) || first.Truncated || !reflect.DeepEqual(first.Assets[0].ID, again.Assets[0].ID) {
		t.Fatal("publication lost valid assets or collection time contaminated its semantic projection")
	}
	capsule, hero, logo := first.Assets[0], first.Assets[1], first.Assets[2]
	if capsule.Type != "library_capsule" || hero.Type != "library_hero" || logo.Type != "library_logo" || logo.Slot != "logo" ||
		logo.Family != "library" || logo.Digest != testAssetDigest || !strings.HasSuffix(logo.URL, "/library%20logo.png") ||
		!logo.ObservedAt.Equal(seg.ObservedAt) || strings.Contains(logo.URL, "?") ||
		first.Assets[5].URL != first.Assets[7].URL || first.Assets[5].ID == first.Assets[7].ID {
		t.Fatal("library identity, source order or repeated URL roles were lost")
	}
	assetFields["asset_url_format"] = "steam/apps/440/${FILENAME}?t=456"
	_, _, newQuery := run(task, assetFields)
	if !bytes.Equal(hash, newQuery) {
		t.Fatal("ephemeral template query changed semantic identity")
	}
	assetFields["library_logo"] = assetFields["logo"]
	_, alias, aliasHash := run(task, assetFields)
	if alias.Assets[2].Slot != "library_logo" || alias.Assets[2].ID == logo.ID || bytes.Equal(hash, aliasHash) {
		t.Fatal("vendor Slot change did not affect source identity")
	}
	assetFields["library_capsule"] = testAssetDigest + "/new_capsule.jpg"
	_, updated, urlHash := run(task, assetFields)
	if updated.Assets[0].ID == capsule.ID || bytes.Equal(aliasHash, urlHash) {
		t.Fatal("URL replacement did not affect source identity")
	}
	params, err := normalizeSteamDetailsParams([]byte(`{"language":"zh_Hans","store_country":"hk"}`))
	scope, scopeErr := NormalizeSteamScope(SteamAssets, params, Scope{EgressRegion: "CN"})
	if err != nil || scopeErr != nil || scope.Locale != "zh-cn" || scope.Region != "hk" || scope.EgressRegion != "cn" {
		t.Fatal("StoreBrowse market, language and egress were not normalized")
	}
	task.Params, task.Scope = params, scope
	uri, err := url.Parse(SteamRequestURI(SteamAssets, "440", params))
	if err != nil || !strings.Contains(uri.Query().Get("input_json"), `"country_code":"HK","language":"schinese"`) {
		t.Fatal("attested request lost market/locale")
	}
	_, localized, _ := run(task, assetFields)
	if localized.Assets[0].Locale != "zh-cn" || localized.Assets[0].ID == updated.Assets[0].ID {
		t.Fatal("localized asset ID collided")
	}
}

func TestSteamAssetURLValidation(t *testing.T) {
	const template = "steam/apps/440/${FILENAME}?t=123"
	name := testAssetDigest + "/header.jpg"
	for _, tc := range []struct {
		format, file string
	}{
		{"https://shared.steamstatic.com.evil.test/steam/apps/440/${FILENAME}", name},
		{"https://shared.steamstatic.com@evil.test/steam/apps/440/${FILENAME}", name},
		{"https://user@shared.steamstatic.com/steam/apps/440/${FILENAME}", name},
		{"http://shared.steamstatic.com/steam/apps/440/${FILENAME}", name},
		{"https://shared.steamstatic.com:444/steam/apps/440/${FILENAME}", name},
		{"https://shared.steamstatic.com/steam/apps/441/${FILENAME}", name},
		{"https://shared.steamstatic.com/steam/apps/440/${FILENAME}#fragment", name},
		{"https://shared.steamstatic.com/steam/apps/440/other/${FILENAME}", name},
		{"//shared.steamstatic.com/steam/apps/440/${FILENAME}", name},
		{"steam/apps/440/${FILENAME}/${FILENAME}", name},
		{"steam/apps/440/no-placeholder", name},
		{"", name},
		{template, "../header.jpg"},
		{template, testAssetDigest + "/%2e%2e/header.jpg"},
		{template, testAssetDigest + "/%252e%252e.jpg"},
		{template, testAssetDigest + "/header%2fother.jpg"},
		{template, testAssetDigest + "/header%5cother.jpg"},
		{template, testAssetDigest + "/header%C0%AE.jpg"},
		{template, testAssetDigest + "/header%0A.jpg"},
		{template, testAssetDigest + "/header\r.jpg"},
		{template, testAssetDigest + "/header.js"},
		{template, strings.Repeat("a", 2048) + ".jpg"},
		{template, "https://shared.steamstatic.com/steam/apps/441/header.jpg"},
		{template, "https://outside.test/steam/apps/440/header.jpg"},
	} {
		if link, _ := resolveSteamAssetURL(440, tc.format, tc.file, false); link != "" {
			t.Fatalf("unsafe fixture URL accepted: %q", link)
		}
	}
	if link, _ := resolveSteamAssetURL(440, "https://shared.akamai.steamstatic.com:443/store_item_assets/steam/apps/440/%s?t=6", name, false); link == "" || strings.Contains(link, "?") {
		t.Fatal("reviewed absolute template or default HTTPS port failed")
	}
	if link, _ := resolveSteamAssetURL(440, "", testAssetDigest+"/logo.png", true); link != "" {
		t.Fatal("community icon accepted an unverified hash")
	}
}

func TestSteamAssetsOutputBudget(t *testing.T) {
	task := assetsTask()
	seg := steamSegment(task, "assets", "", nil)
	seg.State = "present"
	rows := make([]steamAsset, 33)
	for i := range rows {
		rows[i] = steamAsset{ID: fmt.Sprintf("%064x", i), Type: "header", Family: "store", Slot: "header",
			URL: "https://shared.steamstatic.com/steam/apps/440/header.jpg", Locale: "en", Source: steamBrowseSurface, ObservedAt: time.Now().UTC()}
	}
	for _, tc := range []struct {
		padding int
		max     int
	}{{0, 32}, {1940, 14}} {
		payload := append([]steamAsset{}, rows...)
		for i := range payload {
			payload[i].URL += strings.Repeat("a", tc.padding)
		}
		got, n, wire := fitSteamAssets(seg, 440, payload)
		set, err := ObservationObject[steamAssetSet](got.Data)
		if err != nil || n != tc.max || wire > 32<<10 || !set.Truncated || !reflect.DeepEqual(set.Assets, payload[:n]) {
			t.Fatalf("budget lost a complete ordered candidate, count=%d size=%d err=%v", n, wire, err)
		}
	}
}

func TestSteamAssetsExactEncodedSegmentLimit(t *testing.T) {
	task := assetsTask()
	seg := steamSegment(task, "assets", "", nil)
	seg.State = "present"
	seg.ObservedAt = time.Date(2026, 11, 23, 0, 0, 0, 0, time.UTC)
	var rows []steamAsset
	for _, slot := range steamAssetSlots {
		if slot.family == "community" {
			continue
		}
		link := "https://shared.steamstatic.com/steam/apps/440/" + strings.Repeat("a", 1000) + ".jpg"
		rows = append(rows, steamAsset{ID: steamAssetID(440, slot.primary, "en", link), Type: slot.kind, Slot: slot.primary,
			Family: slot.family, URL: link, Locale: "en", Source: steamBrowseSurface, ObservedAt: seg.ObservedAt})
	}
	_, count, wire := fitSteamAssets(seg, 440, rows)
	if count != len(rows) || wire >= steamSegmentBytes {
		t.Fatal("baseline does not fit")
	}
	remaining := steamSegmentBytes - wire
	for i := range rows {
		extra := min(steamMediaURLBytes-len(rows[i].URL)-1, remaining)
		rows[i].URL = strings.TrimSuffix(rows[i].URL, ".jpg") + strings.Repeat("a", extra) + ".jpg"
		rows[i].ID = steamAssetID(440, rows[i].Slot, "en", rows[i].URL)
		remaining -= extra
	}
	exact, count, wire := fitSteamAssets(seg, 440, rows)
	if count != len(rows) || wire != steamSegmentBytes {
		t.Fatalf("exact fixture: count=%d, wire=%d", count, wire)
	}
	if _, _, err := normalizeSteamAssets(task, exact.Scope, exact.State, exact.Data); err != nil {
		t.Fatal(err)
	}
	last := &rows[len(rows)-1]
	last.URL = strings.TrimSuffix(last.URL, ".jpg") + "a.jpg"
	last.ID = steamAssetID(440, last.Slot, "en", last.URL)
	bounded, count, wire := fitSteamAssets(seg, 440, rows)
	v, err := ObservationObject[steamAssetSet](bounded.Data)
	if err != nil || count != len(rows)-1 || wire >= steamSegmentBytes || !v.Truncated {
		t.Fatalf("one extra byte must drop a complete candidate, got %d %d %v", count, wire, err)
	}
	if _, _, err := normalizeSteamAssets(task, bounded.Scope, bounded.State, bounded.Data); err != nil {
		t.Fatal(err)
	}
}
