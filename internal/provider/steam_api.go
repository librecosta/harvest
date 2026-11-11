package provider

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"slices"
	"strings"
	"time"

	"github.com/gofurry/steam-go/api/steamnews"

	"github.com/librecosta/harvest/internal/egress"
)

type steamNewsItem struct {
	GID         string    `json:"gid"`
	Title       string    `json:"title"`
	URL         string    `json:"url,omitempty"`
	Publisher   string    `json:"publisher,omitempty"`
	Feed        string    `json:"feed,omitempty"`
	FeedLabel   string    `json:"feed_label,omitempty"`
	Excerpt     string    `json:"excerpt,omitempty"`
	PublishedAt time.Time `json:"published_at"`
	Tags        []string  `json:"tags,omitempty"`
}

type steamNewsPage struct {
	steamSource
	Items []steamNewsItem `json:"items"`
}

type steamPlayersSample struct {
	steamSource
	Count  *int64 `json:"player_count"`
	Result int    `json:"result"`
}

func acquireSteamNews(ctx context.Context, t Task, network egress.Client) Result {
	sdk, err := newSteamClient(network, steamNewsLimit, nil)
	if err != nil {
		return steamFailed(t, "policy_denied", "news")
	}
	defer sdk.Close()
	p, _ := Object[steamNewsParams](t.Params)
	data, err := sdk.API.SteamNews.GetNewsForApp(ctx, steamAppID(t), &steamnews.GetNewsForAppOptions{Count: p.Count, MaxLength: steamNewsExcerpt})
	if err != nil {
		return steamFailed(t, steamError(err), "news")
	}
	v, code := projectSteamNews(t, p, data.AppNews)
	if code == "source_invalid" || code == "source_incomplete" {
		return steamFailed(t, code, "news")
	}
	return steamResult("", steamSegment(t, "news", code, v))
}

func projectSteamNews(t Task, p steamNewsParams, page steamnews.AppNews) (steamNewsPage, string) {
	v := steamNewsPage{steamSource: steamProvenance(t), Items: []steamNewsItem{}}
	if page.AppID != steamAppID(t) || page.NewsItems == nil || page.Count < 0 {
		return v, "source_invalid"
	}
	if len(page.NewsItems) > 40 {
		return v, "source_incomplete"
	}
	seen := make(map[string]steamNewsItem)
	for _, row := range page.NewsItems {
		item := steamNewsItem{GID: row.GID, Title: steamText(row.Title, 256), URL: pageURL(nil, row.URL),
			Publisher: steamText(row.Author, 128), Feed: steamText(row.FeedName, 96), FeedLabel: steamText(row.FeedLabel, 128),
			Excerpt: steamText(row.Contents, steamNewsExcerpt), PublishedAt: time.Unix(row.Date, 0).UTC()}
		for _, tag := range row.Tags[:min(len(row.Tags), 8)] {
			if clean := steamText(tag, 64); clean != "" {
				item.Tags = append(item.Tags, clean)
			}
		}
		item.Tags = sortedUnique(item.Tags)
		if row.AppID != 0 && row.AppID != steamAppID(t) || !validSteamNewsItem(item) {
			return v, "source_invalid"
		}
		if prior, ok := seen[item.GID]; ok {
			if compareSteamNewsItem(prior, item) != 0 {
				return v, "source_invalid"
			}
			continue
		}
		seen[item.GID] = item
		v.Items = append(v.Items, item)
	}
	slices.SortFunc(v.Items, compareSteamNewsOrder)
	v.Items = v.Items[:min(len(v.Items), int(p.Count))]
	if len(v.Items) == 0 {
		return v, "empty_page"
	}
	return v, ""
}

func compareSteamNewsItem(a, b steamNewsItem) int {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return slices.Compare(left, right)
}

func compareSteamNewsOrder(a, b steamNewsItem) int {
	if c := b.PublishedAt.Compare(a.PublishedAt); c != 0 {
		return c
	}
	return strings.Compare(a.GID, b.GID)
}

func validSteamNewsItem(v steamNewsItem) bool {
	if v.GID == "" || len(v.GID) > 64 || strings.Trim(v.GID, "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_") != "" ||
		v.Title == "" || !validText(v.Title, 256) || v.URL != pageURL(nil, v.URL) || !validText(v.Publisher, 128) ||
		!validText(v.Feed, 96) || !validText(v.FeedLabel, 128) || !validText(v.Excerpt, steamNewsExcerpt) ||
		v.PublishedAt.Year() < 1970 || v.PublishedAt.Year() > 2100 || v.PublishedAt.Unix() <= 0 ||
		v.PublishedAt.Nanosecond() != 0 || len(v.Tags) > 8 || !slices.Equal(v.Tags, sortedUnique(slices.Clone(v.Tags))) {
		return false
	}
	return !slices.ContainsFunc(v.Tags, func(tag string) bool { return tag == "" || !validText(tag, 64) })
}

func normalizeSteamNews(t Task, _ SegmentScope, state string, data jsontext.Value) (jsontext.Value, jsontext.Value, error) {
	v, err := ObservationObject[steamNewsPage](data)
	params, _ := Object[steamNewsParams](t.Params)
	valid := err == nil && v.steamSource == steamProvenance(t) && v.Items != nil && len(v.Items) <= int(params.Count) &&
		(state == "confirmed_empty" && len(v.Items) == 0 || state == "present" && len(v.Items) > 0) &&
		slices.IsSortedFunc(v.Items, compareSteamNewsOrder)
	seen := make(map[string]bool, len(v.Items))
	for i := range v.Items {
		item := &v.Items[i]
		valid = valid && !seen[item.GID] && validSteamNewsItem(*item)
		seen[item.GID] = true
		item.PublishedAt = item.PublishedAt.UTC()
	}
	return webData(valid, v, nil)
}

func acquireSteamPlayers(ctx context.Context, t Task, network egress.Client) Result {
	sdk, err := newSteamClient(network, steamPlayersLimit, nil)
	if err != nil {
		return steamFailed(t, "policy_denied", "players")
	}
	defer sdk.Close()
	body, err := sdk.API.SteamUserStats.GetNumberOfCurrentPlayersRaw(ctx, steamAppID(t))
	if err != nil {
		return steamFailed(t, steamError(err), "players")
	}
	// The SDK owns the request. Its typed integer collapses null, missing
	// and explicit zero, so check presence in the bounded response here.
	var raw struct {
		Response struct {
			Result int     `json:"result"`
			Count  *int64  `json:"player_count"`
			AppID  *uint32 `json:"appid"`
		} `json:"response"`
	}
	if json.Unmarshal(body, &raw) != nil || raw.Response.Count == nil || *raw.Response.Count < 0 ||
		raw.Response.AppID != nil && *raw.Response.AppID != steamAppID(t) {
		return steamFailed(t, "source_invalid", "players")
	}
	if raw.Response.Result != 1 {
		return steamFailed(t, "steam_unavailable", "players")
	}
	return steamResult("", steamSegment(t, "players", "", steamPlayersSample{steamProvenance(t), raw.Response.Count, raw.Response.Result}))
}

func normalizeSteamPlayers(t Task, _ SegmentScope, state string, data jsontext.Value) (jsontext.Value, jsontext.Value, error) {
	v, err := ObservationObject[steamPlayersSample](data)
	valid := err == nil && state == "present" && v.steamSource == steamProvenance(t) && v.Result == 1 && v.Count != nil && *v.Count >= 0
	return webData(valid, v, nil)
}
