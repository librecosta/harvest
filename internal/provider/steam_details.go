package provider

import (
	"context"
	"encoding/json/jsontext"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/gofurry/steam-go/web/storefront"

	"github.com/librecosta/harvest/internal/egress"
)

// The SDK's scalar fields do not preserve JSON presence. This projection
// covers only the facts for which null/absence differs from false or zero.
type steamStorePresence struct {
	Success bool `json:"success"`
	Data    struct {
		AppID     uint32          `json:"steam_appid"`
		IsFree    *bool           `json:"is_free"`
		Platforms *steamPlatforms `json:"platforms"`
		Release   *steamRelease   `json:"release_date"`
		Price     *steamAmounts   `json:"price_overview"`
	} `json:"data"`
}

type steamPlatforms struct {
	Windows *bool `json:"windows,omitempty"`
	Mac     *bool `json:"mac,omitempty"`
	Linux   *bool `json:"linux,omitempty"`
}

type steamRelease struct {
	ComingSoon *bool  `json:"coming_soon,omitempty"`
	Date       string `json:"date_text,omitempty"`
}

type steamAmounts struct {
	Currency string `json:"currency"`
	Initial  *int64 `json:"initial"`
	Final    *int64 `json:"final"`
	Discount *int   `json:"discount_percent"`
}

type steamLabel struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type steamDescription struct {
	steamSource
	Name               string          `json:"name"`
	Type               string          `json:"type,omitempty"`
	Description        string          `json:"description,omitempty"`
	SupportedLanguages string          `json:"supported_languages,omitempty"`
	Developers         []string        `json:"developers,omitempty"`
	Publishers         []string        `json:"publishers,omitempty"`
	Platforms          *steamPlatforms `json:"platforms,omitempty"`
	Categories         []steamLabel    `json:"categories,omitempty"`
	Genres             []steamLabel    `json:"genres,omitempty"`
	Release            *steamRelease   `json:"release,omitempty"`
}

type steamPrice struct {
	steamSource
	State   string        `json:"state"`
	Amounts *steamAmounts `json:"amounts,omitempty"`
}

type steamMedia struct {
	steamSource
	Header      string   `json:"header,omitempty"`
	Capsule     string   `json:"capsule,omitempty"`
	Screenshots []string `json:"screenshots,omitempty"`
	Movies      []string `json:"movies,omitempty"`
}

func acquireSteamDetails(ctx context.Context, t Task, network egress.Client) Result {
	var presence map[string]steamStorePresence
	sdk, err := newSteamClient(network, steamDetailsLimit, &presence)
	if err != nil {
		return steamFailed(t, "policy_denied", "details", "price", "media")
	}
	defer sdk.Close()
	p, _ := Object[steamDetailsParams](t.Params)
	match, err := sdk.Web.Storefront.GetResolvedAppDetails(ctx, steamAppID(t), &storefront.GetAppDetailsOptions{CountryCode: p.Country, Language: p.Language})
	if err != nil {
		return steamFailed(t, steamError(err), "details", "price", "media")
	}
	strict := presence[match.ResponseKey]
	if !strict.Success || strict.Data.AppID != steamAppID(t) || match.RequestedAppID != steamAppID(t) || match.Result.Data.SteamAppID != steamAppID(t) || !match.Result.Success {
		return steamFailed(t, "source_invalid", "details", "price", "media")
	}
	d, known := match.Result.Data, strict.Data
	source := steamProvenance(t)
	metadata := steamDescription{steamSource: source, Name: steamText(d.Name, 256), Type: steamText(d.Type, 48),
		Description: steamText(d.ShortDescription, 1536), SupportedLanguages: steamText(d.SupportedLanguages, 768),
		Developers: steamNames(d.Developers), Publishers: steamNames(d.Publishers), Platforms: known.Platforms, Release: known.Release}
	if metadata.Release != nil {
		metadata.Release.Date = steamText(d.ReleaseDate.Date, 128)
	}
	for _, row := range d.Categories[:min(len(d.Categories), 24)] {
		if row.ID > 0 {
			metadata.Categories = append(metadata.Categories, steamLabel{ID: strconv.Itoa(row.ID), Name: steamText(row.Description, 128)})
		}
	}
	for _, row := range d.Genres[:min(len(d.Genres), 24)] {
		if positiveSteamNumber(row.ID) {
			metadata.Genres = append(metadata.Genres, steamLabel{ID: row.ID, Name: steamText(row.Description, 128)})
		}
	}
	metadata.Categories = sortSteamLabels(metadata.Categories)
	metadata.Genres = sortSteamLabels(metadata.Genres)
	detail := steamSegment(t, "details", "source_invalid", nil)
	if metadata.Name != "" {
		detail = steamSegment(t, "details", "", metadata)
	}
	price := steamSegment(t, "price", "price_unavailable", nil)
	switch {
	case known.IsFree != nil && *known.IsFree && (known.Price == nil || validSteamAmounts(known.Price) && *known.Price.Final == 0):
		price = steamSegment(t, "price", "", steamPrice{steamSource: source, State: "free"})
	case (known.IsFree == nil || !*known.IsFree) && validSteamAmounts(known.Price):
		state := "priced"
		if *known.Price.Final == 0 {
			state = "zero_price"
		}
		price = steamSegment(t, "price", "", steamPrice{steamSource: source, State: state, Amounts: known.Price})
	case known.Release != nil && known.Release.ComingSoon != nil && *known.Release.ComingSoon:
		price = steamSegment(t, "price", "price_unreleased", nil)
	}
	media := steamMedia{steamSource: source, Header: steamMediaURL(d.HeaderImage), Capsule: steamMediaURL(d.CapsuleImage)}
	if media.Capsule == "" {
		media.Capsule = steamMediaURL(d.CapsuleImageV5)
	}
	for _, screenshot := range d.Screenshots[:min(len(d.Screenshots), 8)] {
		link := steamMediaURL(screenshot.PathFull)
		if link == "" {
			link = steamMediaURL(screenshot.PathThumbnail)
		}
		if link != "" {
			media.Screenshots = append(media.Screenshots, link)
		}
	}
	for _, movie := range d.Movies[:min(len(d.Movies), 4)] {
		for _, candidate := range []string{movie.MP4.Max, movie.WebM.Max, movie.HLSH264, movie.DASHH264, movie.DASHAV1, movie.Thumbnail} {
			if link := steamMediaURL(candidate); link != "" {
				media.Movies = append(media.Movies, link)
				break
			}
		}
	}
	media.Screenshots, media.Movies = sortedUnique(media.Screenshots), sortedUnique(media.Movies)
	assets := steamSegment(t, "media", "media_unavailable", nil)
	if media.hasLinks() {
		assets = steamSegment(t, "media", "", media)
	}
	return steamResult("", detail, price, assets)
}

func positiveSteamNumber(raw string) bool {
	v, err := strconv.ParseUint(raw, 10, 64)
	return err == nil && v > 0 && strconv.FormatUint(v, 10) == raw
}

func steamNames(raw []string) []string {
	var values []string
	for _, s := range raw[:min(len(raw), 12)] {
		if name := steamText(s, 128); name != "" {
			values = append(values, name)
		}
	}
	return sortedUnique(values)
}

func sortSteamLabels(rows []steamLabel) []steamLabel {
	slices.SortFunc(rows, func(a, b steamLabel) int {
		if a.ID != b.ID {
			return strings.Compare(a.ID, b.ID)
		}
		return strings.Compare(a.Name, b.Name)
	})
	return slices.CompactFunc(rows, func(a, b steamLabel) bool { return a.ID == b.ID })
}

func steamMediaURL(raw string) string {
	link := pageURL(nil, raw)
	u, err := url.Parse(link)
	if err != nil || u.Scheme != "https" || u.Port() != "" || u.Path == "" || u.Path == "/" {
		return ""
	}
	host := u.Hostname()
	if strings.HasSuffix(host, ".steamstatic.com") || strings.HasSuffix(host, ".steamcontent.com") ||
		host == "steamcdn-a.akamaihd.net" || host == "steamuserimages-a.akamaihd.net" {
		return link
	}
	return ""
}

func (v steamMedia) hasLinks() bool {
	return v.Header != "" || v.Capsule != "" || len(v.Screenshots) > 0 || len(v.Movies) > 0
}

func validSteamAmounts(p *steamAmounts) bool {
	return p != nil && len(p.Currency) == 3 && strings.Trim(p.Currency, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") == "" &&
		p.Initial != nil && p.Final != nil && p.Discount != nil && *p.Final >= 0 && *p.Initial >= *p.Final && *p.Discount >= 0 && *p.Discount <= 100
}

func normalizeSteamDetails(t Task, scope SegmentScope, state string, data jsontext.Value) (jsontext.Value, jsontext.Value, error) {
	source := steamProvenance(t)
	switch scope.Dimensions["section"] {
	case "details":
		v, err := ObservationObject[steamDescription](data)
		valid := err == nil && state == "present" && v.steamSource == source && v.Name != "" && validText(v.Name, 256) &&
			validText(v.Type, 48) && validText(v.Description, 1536) && validText(v.SupportedLanguages, 768) &&
			(v.Release == nil || validText(v.Release.Date, 128)) && slices.Equal(v.Developers, steamNames(v.Developers)) &&
			slices.Equal(v.Publishers, steamNames(v.Publishers))
		for _, rows := range [][]steamLabel{v.Categories, v.Genres} {
			valid = valid && len(rows) <= 24 && slices.Equal(rows, sortSteamLabels(slices.Clone(rows)))
			for _, row := range rows {
				valid = valid && positiveSteamNumber(row.ID) && validText(row.Name, 128)
			}
		}
		return webData(valid, v, nil)
	case "price":
		v, err := ObservationObject[steamPrice](data)
		valid := err == nil && state == "present" && v.steamSource == source
		if v.State == "free" {
			valid = valid && v.Amounts == nil
		} else {
			valid = valid && validSteamAmounts(v.Amounts) &&
				(v.State == "zero_price" && *v.Amounts.Final == 0 || v.State == "priced" && *v.Amounts.Final > 0)
		}
		return webData(valid, v, nil)
	default:
		v, err := ObservationObject[steamMedia](data)
		valid := err == nil && state == "present" && v.steamSource == source && v.hasLinks() && v.Header == steamMediaURL(v.Header) &&
			v.Capsule == steamMediaURL(v.Capsule) && len(v.Screenshots) <= 8 && len(v.Movies) <= 4
		for _, rows := range [][]string{v.Screenshots, v.Movies} {
			valid = valid && slices.Equal(rows, sortedUnique(slices.Clone(rows)))
			for _, link := range rows {
				valid = valid && link != "" && steamMediaURL(link) == link
			}
		}
		return webData(valid, v, nil)
	}
}
