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

	"github.com/gofurry/steam-go/web/storefront"
	"github.com/librecosta/harvest/internal/egress"
)

const (
	SteamEvents          = "steam.events"
	SteamEventsKey       = SteamEvents + ":1:1:steam_events_v1"
	steamEventsPath      = "/events/ajaxgetadjacentpartnerevents"
	steamEventsDataBytes = 480 << 10
	steamEventsBodyBytes = 128 << 10
	steamEventsCount     = 4
	steamEventsEmpty     = "empty_events"
)

type steamEventsParams struct {
	Language string `json:"language"`
}

func normalizeSteamEventsParams(raw jsontext.Value) (jsontext.Value, error) {
	p, err := Object[steamEventsParams](raw)
	p.Language = steamLanguage(p.Language)
	if err != nil || p.Language == "" {
		return nil, errors.New("invalid Store Events language")
	}
	return json.Marshal(p)
}

func steamEventsOptions(raw jsontext.Value) (storefront.GetAdjacentPartnerEventsOptions, bool) {
	p, err := Object[steamEventsParams](raw)
	options := storefront.GetAdjacentPartnerEventsOptions{CountBefore: 1, CountAfter: steamEventsCount, LanguageList: "0"}
	switch p.Language {
	case "english":
	case "schinese":
		options.LanguageList = "6_0"
	default:
		return options, false
	}
	return options, err == nil
}

func steamEventsURI(app string, raw jsontext.Value) string {
	options, ok := steamEventsOptions(raw)
	if !ok {
		return ""
	}
	return steamEventsPath + "?" + (url.Values{
		"appid": {app}, "count_before": {strconv.Itoa(options.CountBefore)},
		"count_after": {strconv.Itoa(options.CountAfter)}, "lang_list": {options.LanguageList},
	}).Encode()
}

func steamEventsCapability(fixture *egress.FixtureNetwork) Capability {
	return Capability{
		Offer: Offer{SteamEvents, 1, 1, "steam_events_v1"}, Resource: "app", MinInterval: time.Hour, Timeout: 20 * time.Second,
		NormalizeParams: normalizeSteamEventsParams, Execute: acquireSteamEvents,
		Network: &egress.Source{Steam: true, AllowDirect: true, RequireRegion: true, Origins: []string{steamStore},
			FixedPath: steamEventsPath, MaxRequests: 1, MaxBodyBytes: MaxSteamEvidenceBytes,
			ContentTypes: []string{"application/json", "text/json", "text/javascript"}, FixtureNetwork: fixture},
		Observation: &ObservationPolicy{Source: "steam", MaxDataBytes: steamEventsDataBytes, MaxSegmentBytes: steamEventsDataBytes + 4096,
			Scopes:              func(t Task) []SegmentScope { return []SegmentScope{steamScope(t, "events")} },
			NormalizeScopedData: normalizeSteamEvents, EmptyEvidence: steamEventsEmpty,
			DiagnosticEvidence: append(slices.Clone(webDiagnostics), "source_invalid", "source_incomplete", "source_redirect",
				"steam_unavailable", "upstream_unavailable", "upstream_blocked"),
		},
	}
}

type steamEventWire struct {
	AppID          uint32 `json:"appid"`
	GID            string `json:"gid"`
	Clan           string `json:"clan_steamid"`
	Type           *int   `json:"event_type"`
	Name           string `json:"event_name"`
	Start          *int64 `json:"rtime32_start_time"`
	End            *int64 `json:"rtime32_end_time"`
	Created        *int64 `json:"rtime_created"`
	Modified       *int64 `json:"rtime32_last_modified"`
	Published      *int   `json:"published"`
	Hidden         *int   `json:"hidden"`
	Unlisted       *int   `json:"unlisted"`
	Forum          string `json:"forum_topic_id"`
	NewsPost       string `json:"news_post_gid"`
	ServerPassword string `json:"server_password"`
	Comments       *int   `json:"comment_count"`
	VotesUp        *int   `json:"votes_up"`
	VotesDown      *int   `json:"votes_down"`
	Announcement   *struct {
		GID       string   `json:"gid"`
		Event     string   `json:"event_gid"`
		Headline  string   `json:"headline"`
		Body      *string  `json:"body"`
		URL       string   `json:"url"`
		Forum     string   `json:"forum_topic_id"`
		Post      *int64   `json:"posttime"`
		Update    *int64   `json:"updatetime"`
		Language  *int     `json:"language"`
		Tags      []string `json:"tags"`
		Comments  *int     `json:"commentcount"`
		VotesUp   *int     `json:"voteupcount"`
		VotesDown *int     `json:"votedowncount"`
		Hidden    *int     `json:"hidden"`
		Banned    *int     `json:"banned"`
	} `json:"announcement_body"`
}

type steamEvent struct {
	EventGID        string   `json:"event_gid"`
	AnnouncementGID string   `json:"announcement_gid"`
	ForumTopicID    string   `json:"forum_topic_id,omitempty"`
	ClanID          string   `json:"clan_steam_id,omitempty"`
	Type            *int     `json:"event_type,omitzero"`
	Name            string   `json:"event_name"`
	Headline        string   `json:"headline"`
	Body            string   `json:"raw_body"`
	OriginalURL     string   `json:"original_url,omitempty"`
	URL             string   `json:"url"`
	Language        *int     `json:"language,omitzero"`
	LanguageState   string   `json:"language_state"`
	Published       *bool    `json:"published,omitzero"`
	Hidden          bool     `json:"hidden"`
	Tags            []string `json:"tags"`
	Comments        *int     `json:"comment_count,omitzero"`
	VotesUp         *int     `json:"vote_up_count,omitzero"`
	VotesDown       *int     `json:"vote_down_count,omitzero"`
	Post            *int64   `json:"post_time,omitzero"`
	Update          *int64   `json:"update_time,omitzero"`
	Start           *int64   `json:"start_time,omitzero"`
	End             *int64   `json:"end_time,omitzero"`
	Created         *int64   `json:"created_time,omitzero"`
	Modified        *int64   `json:"modified_time,omitzero"`
}

type steamEventsDiagnostic struct {
	EventGID string `json:"event_gid,omitempty"`
	Code     string `json:"code"`
}

type steamEventPage struct {
	AppID           uint32                  `json:"app_id"`
	RequestedLocale string                  `json:"requested_locale"`
	Events          []steamEvent            `json:"events"`
	Diagnostics     []steamEventsDiagnostic `json:"diagnostics,omitempty"`
	Truncated       bool                    `json:"truncated"`
}

func steamEventID(s string) bool {
	id, err := strconv.ParseUint(s, 10, 64)
	return err == nil && id > 0 && strconv.FormatUint(id, 10) == s
}

func flagged(v *int) bool { return v != nil && *v != 0 }

func firstKnown[T any](a, b *T) *T {
	if a != nil {
		return a
	}
	return b
}

func readSteamEventPage(raw []byte, appID uint32, locale string) (steamEventPage, bool, string) {
	page := steamEventPage{AppID: appID, RequestedLocale: locale, Events: []steamEvent{}}
	var e struct {
		Success *int              `json:"success"`
		Events  *[]jsontext.Value `json:"events"`
	}
	if json.Unmarshal(raw, &e) != nil {
		return page, false, "source_invalid"
	}
	if e.Success == nil || *e.Success != 1 || e.Events == nil {
		return page, false, "steam_unavailable"
	}
	verified := len(*e.Events) > 0 && len(*e.Events) <= steamEventsCount+1
	seen := make(map[string]bool)
	for i, data := range *e.Events {
		if i == steamEventsCount+1 {
			page.Diagnostics = append(page.Diagnostics, steamEventsDiagnostic{Code: "event_limit"})
			page.Truncated = true
			break
		}
		var raw steamEventWire
		var gid string
		if err := json.Unmarshal(data, &raw); err == nil && steamEventID(raw.GID) {
			gid = raw.GID
		}
		item, code := projectSteamEvent(raw, appID, locale)
		if gid == "" || raw.AppID != appID || raw.ServerPassword != "" ||
			raw.Announcement != nil && raw.Announcement.Event != raw.GID {
			verified = false
			code = "identity_invalid"
		}
		if code == "" && seen[gid] {
			code = "duplicate_event"
		}
		if code == "" {
			wire, err := json.Marshal(item)
			if err != nil || len(wire) > 160<<10 {
				code = "event_budget"
			} else {
				page.Events = append(page.Events, item)
				seen[gid] = true
				wire, err = json.Marshal(page)
				if err != nil || len(wire) > steamEventsDataBytes-4096 {
					page.Events = page.Events[:len(page.Events)-1]
					code = "page_budget"
				}
			}
		}
		if code != "" {
			page.Diagnostics = append(page.Diagnostics, steamEventsDiagnostic{gid, code})
			page.Truncated = page.Truncated || strings.HasSuffix(code, "_budget")
		}
	}
	return page, verified, ""
}

func projectSteamEvent(e steamEventWire, appID uint32, locale string) (steamEvent, string) {
	a := e.Announcement
	if !steamEventID(e.GID) || e.AppID != appID || a == nil || a.Event != e.GID ||
		!steamEventID(a.GID) || e.NewsPost != "" && e.NewsPost != "0" && e.NewsPost != a.GID {
		return steamEvent{}, "identity_invalid"
	}
	if a.Body == nil || strings.TrimSpace(*a.Body) == "" || strings.TrimSpace(a.Headline) == "" {
		return steamEvent{}, "body_missing"
	}
	if strings.ContainsRune(*a.Body, 0) || strings.ContainsRune(a.Headline, 0) || strings.ContainsRune(e.Name, 0) || strings.ContainsRune(a.URL, 0) {
		return steamEvent{}, "invalid_text"
	}
	if len(*a.Body) > steamEventsBodyBytes || len(a.Headline) > 2048 || len(e.Name) > 2048 || len(a.Tags) > 64 || len(a.URL) > 2048 ||
		slices.ContainsFunc(a.Tags, func(tag string) bool { return len(tag) > 128 || strings.ContainsRune(tag, 0) }) {
		return steamEvent{}, "field_budget"
	}
	for _, value := range []*int64{a.Post, a.Update, e.Start, e.End, e.Created, e.Modified} {
		if value != nil && (*value < 0 || *value > 4102444800) {
			return steamEvent{}, "invalid_time"
		}
	}
	for _, value := range []*int{e.Type, a.Language, a.Comments, a.VotesUp, a.VotesDown, e.Comments, e.VotesUp, e.VotesDown} {
		if value != nil && (*value < 0 || *value > 2147483647) {
			return steamEvent{}, "invalid_number"
		}
	}
	var published *bool
	if e.Published != nil && (*e.Published == 0 || *e.Published == 1) {
		value := *e.Published == 1
		published = &value
	}
	state := "unknown"
	if a.Language != nil {
		state = "different"
		if *a.Language == 0 && locale == "zh-cn" {
			state = "fallback"
		}
		if *a.Language == 0 && locale == "en" || *a.Language == 6 && locale == "zh-cn" {
			state = "declared_match"
		}
	}
	forum := a.Forum
	if !steamEventID(forum) {
		forum = e.Forum
	}
	if !steamEventID(forum) {
		forum = ""
	}
	clan := e.Clan
	if !steamEventID(clan) {
		clan = ""
	}
	return steamEvent{
		EventGID: e.GID, AnnouncementGID: a.GID, ForumTopicID: forum, ClanID: clan, Type: e.Type, Name: e.Name,
		Headline: a.Headline, Body: *a.Body, OriginalURL: a.URL,
		URL:      steamStore + "/news/app/" + strconv.FormatUint(uint64(appID), 10) + "/view/" + a.GID,
		Language: a.Language, LanguageState: state, Published: published, Hidden: flagged(e.Hidden) || flagged(e.Unlisted) || flagged(a.Hidden) || flagged(a.Banned),
		Tags: a.Tags, Comments: firstKnown(a.Comments, e.Comments), VotesUp: firstKnown(a.VotesUp, e.VotesUp), VotesDown: firstKnown(a.VotesDown, e.VotesDown),
		Post: a.Post, Update: a.Update, Start: e.Start, End: e.End, Created: e.Created, Modified: e.Modified,
	}, ""
}

func acquireSteamEvents(ctx context.Context, t Task, network egress.Client) Result {
	options, ok := steamEventsOptions(t.Params)
	app := steamAppID(t)
	if !ok || app == 0 {
		return Result{State: "failed", Code: "invalid_payload"}
	}
	var evidence SteamEvidence
	sdk, err := newSteamClientWithOptions(steamTransport{client: network, maxBytes: MaxSteamEvidenceBytes, evidence: &evidence})
	if err != nil {
		return steamFailed(t, "policy_denied", "events")
	}
	defer sdk.Close()
	raw, err := sdk.Web.Storefront.GetAdjacentPartnerEventsRaw(ctx, app, &options)
	if err != nil {
		return steamFailed(t, steamError(err), "events")
	}
	page, verified, code := readSteamEventPage(raw, app, t.Scope.Locale)
	if code != "" {
		return steamFailed(t, code, "events")
	}
	state, reason := "present", ""
	if len(page.Events) == 0 && len(page.Diagnostics) == 0 {
		state, reason = "confirmed_empty", steamEventsEmpty
	}
	seg := observationSegment(steamScope(t, "events"), state, reason, page, steamEventsDataBytes)
	seg.Source, seg.OutputSchema, seg.ObservedAt = "steam", 1, evidence.ObservedAt
	result := steamResult("", seg)
	if t.CaptureSteamEvidence && verified && len(page.Events) > 0 {
		result.SteamEvidence = &evidence
	}
	return result
}

func normalizeSteamEvents(t Task, scope SegmentScope, state string, raw jsontext.Value) (jsontext.Value, jsontext.Value, error) {
	page, err := Object[steamEventPage](raw)
	if err != nil || len(raw) > steamEventsDataBytes || page.AppID != steamAppID(t) || page.RequestedLocale != scope.Locale ||
		len(page.Events) > steamEventsCount+1 || len(page.Diagnostics) > steamEventsCount+2 ||
		(state == "confirmed_empty") != (len(page.Events) == 0 && len(page.Diagnostics) == 0) {
		return nil, nil, errors.New("invalid Store Events page")
	}
	seen := make(map[string]bool)
	for _, item := range page.Events {
		if !steamEventID(item.EventGID) || !steamEventID(item.AnnouncementGID) || seen[item.EventGID] ||
			len(item.Body) == 0 || len(item.Body) > steamEventsBodyBytes ||
			item.URL != steamStore+"/news/app/"+t.ExternalKey+"/view/"+item.AnnouncementGID {
			return nil, nil, errors.New("invalid Store Events article")
		}
		seen[item.EventGID] = true
	}
	data, err := json.Marshal(page)
	return data, data, err
}
