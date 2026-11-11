package provider

import (
	"strconv"
	"strings"
	"testing"

	"github.com/gofurry/steam-go/api/steamnews"
)

func BenchmarkSteamNewsProjection(b *testing.B) {
	app := Task{ExternalKey: "440", Offer: Offer{Capability: "steam.news"}}
	fixture := steamnews.AppNews{AppID: 440, Count: 10}
	for i := range 10 {
		fixture.NewsItems = append(fixture.NewsItems, steamnews.NewsItem{
			GID: strconv.Itoa(i + 100), Title: "Fixture update", FeedName: "steam_community_announcements",
			URL:  "https://store.steampowered.com/news/app/440/view/1?tracking=value",
			Date: 1_700_000_000 + int64(i), Contents: "<p>" + strings.Repeat("Fixture update ", 35) + "</p>",
		})
	}
	b.ReportAllocs()
	for b.Loop() {
		out, code := projectSteamNews(app, steamNewsParams{Count: 10}, fixture)
		if code != "" || len(out.Items) != 10 {
			b.Fatal("invalid projected page")
		}
	}
}
