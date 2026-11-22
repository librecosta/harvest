package provider

import (
	"bytes"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/gofurry/steam-go/web/storefront"
)

// These types measure the proposed media wire format in offline tests.
type contractMovie struct {
	ID        string            `json:"movie_id"`
	Name      string            `json:"name"`
	Thumbnail string            `json:"thumbnail_url,omitempty"`
	Highlight *bool             `json:"highlight,omitempty"`
	Order     int               `json:"order"`
	Sources   map[string]string `json:"sources"`
}

type contractScreenshot struct {
	ID        string `json:"screenshot_id"`
	Thumbnail string `json:"thumbnail_url,omitempty"`
	Full      string `json:"full_url,omitempty"`
	Order     int    `json:"order"`
}

type contractMedia struct {
	AppID       uint32               `json:"app_id"`
	Images      map[string]string    `json:"images"`
	Movies      []contractMovie      `json:"movies"`
	Screenshots []contractScreenshot `json:"screenshots"`
	Truncated   bool                 `json:"truncated"`
}

func TestSteamV2ContractBoundaries(t *testing.T) {
	const largeID = "18446744073709551615"
	raw := jsontext.Value(largeID)
	id, err := strconv.ParseUint(string(raw), 10, 64)
	encoded, marshalErr := json.Marshal(contractMovie{ID: strconv.FormatUint(id, 10)})
	if err != nil || marshalErr != nil || !bytes.Contains(encoded, []byte(`"movie_id":"`+largeID+`"`)) {
		t.Fatal("movie identity did not survive a value outside the JavaScript safe integer range")
	}
	const cdn = "https://cdn.steamstatic.com/"
	all := storefront.StoreMovie{ID: 1, Thumbnail: cdn + "thumb.jpg",
		MP4:     storefront.StoreMovieSources{P480: cdn + "480.mp4", Max: cdn + "max.mp4"},
		WebM:    storefront.StoreMovieSources{P480: cdn + "480.webm", Max: cdn + "max.webm"},
		DASHAV1: cdn + "av1.mpd", DASHH264: cdn + "h264.mpd", HLSH264: cdn + "h264.m3u8"}
	projected, formats := projectContractMedia(storefront.AppDetailsData{Movies: []storefront.StoreMovie{all}}, 434000)
	if len(formats) != 7 || projected.Movies[0].Sources["mp4_max"] != all.MP4.Max ||
		projected.Movies[0].Sources["hls_h264"] != all.HLSH264 || projected.Movies[0].Thumbnail != all.Thumbnail {
		t.Fatal("movie projection confused direct file, manifest or thumbnail roles")
	}

	media := contractMedia{AppID: 434000}
	for i := range 30 {
		media.Screenshots = append(media.Screenshots, contractScreenshot{strconv.Itoa(i), "", "https://cdn.steamstatic.com/" + strings.Repeat("a", 1800), i})
	}
	for i := range 12 {
		media.Movies = append(media.Movies, contractMovie{ID: strconv.Itoa(i + 1), Order: i, Sources: map[string]string{
			"mp4_max": "https://cdn.steamstatic.com/" + strings.Repeat("b", 1800),
		}})
	}
	encoded, err = json.Marshal(media)
	if err != nil || len(encoded) <= 32<<10 {
		t.Fatalf("expected an over-budget 30+12 fixture, got %d bytes (%v)", len(encoded), err)
	}

	var fields map[string]jsontext.Value
	if err := json.Unmarshal([]byte(`{"explicit_null":null,"empty":"","negative":false}`), &fields); err != nil ||
		len(fields["absent"]) != 0 || string(fields["explicit_null"]) != "null" ||
		string(fields["empty"]) != `""` || string(fields["negative"]) != "false" {
		t.Fatal("missed a source presence state")
	}
}

func TestSteamV2ArchivedSamples(t *testing.T) {
	dir := os.Getenv("HARVEST_STEAM_V2_SAMPLES_DIR")
	if dir == "" {
		t.Skip("HARVEST_STEAM_V2_SAMPLES_DIR is unset")
	}
	for _, app := range []uint32{434000, 1332010} {
		for _, locale := range []string{"en", "zh-cn"} {
			name := strconv.FormatUint(uint64(app), 10) + "-" + locale
			t.Run(name, func(t *testing.T) {
				body, err := os.ReadFile(filepath.Join(dir, name+".json"))
				if err != nil {
					t.Fatal(err)
				}
				if !ValidSteamEvidence(Task{ExternalKey: strconv.FormatUint(uint64(app), 10)}, body) {
					t.Fatal("archived digest sample failed source identity validation")
				}
				var envelope storefront.AppDetailsEnvelope
				if err := json.Unmarshal(body, &envelope); err != nil {
					t.Fatal("archived AppDetails decode failed")
				}
				match, err := storefront.ResolveAppDetails(envelope, app)
				if err != nil {
					t.Fatal("archived AppDetails identity resolution failed")
				}
				d := match.Result.Data
				media, flavors := projectContractMedia(d, app)
				if media.Images["header_url"] == "" || len(media.Movies) == 0 || len(media.Screenshots) == 0 ||
					len(media.Movies) != len(d.Movies) || len(media.Screenshots) != len(d.Screenshots) ||
					d.DetailedDescription == "" || d.AboutTheGame == "" || d.SupportedLanguages == "" || d.ReleaseDate == nil {
					t.Fatal("archived sample lost a required coverage field")
				}
				wire, err := json.Marshal(media)
				if err != nil || len(wire) > 32<<10 {
					t.Fatalf("actual media projection exceeds segment budget: %d bytes", len(wire))
				}
				expanded := media
				expanded.Screenshots = nil
				expanded.Movies = nil
				for i := range 30 {
					item := media.Screenshots[i%len(media.Screenshots)]
					item.ID, item.Order = strconv.Itoa(i), i
					expanded.Screenshots = append(expanded.Screenshots, item)
				}
				for i := range 12 {
					item := media.Movies[i%len(media.Movies)]
					item.ID, item.Order = strconv.Itoa(i+1), i
					expanded.Movies = append(expanded.Movies, item)
				}
				budgetSample, err := json.Marshal(expanded)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("screenshots=%d movies=%d formats=%v media_bytes=%d expanded_30_12_bytes=%d full_description_bytes=%d pc=%t mac=%t linux=%t support=%t ratings=%t",
					len(media.Screenshots), len(media.Movies), flavors, len(wire), len(budgetSample),
					len(d.DetailedDescription), d.PCRequirements != nil, d.MacRequirements != nil, d.LinuxRequirements != nil,
					d.SupportInfo != nil, len(d.Ratings) > 0)
			})
		}
	}
}

func projectContractMedia(d storefront.AppDetailsData, app uint32) (contractMedia, map[string]int) {
	media := contractMedia{AppID: app, Images: make(map[string]string)}
	for field, raw := range map[string]string{
		"header_url": d.HeaderImage, "capsule_url": d.CapsuleImage, "capsule_v5_url": d.CapsuleImageV5,
		"background_url": d.Background, "background_raw_url": d.BackgroundRaw,
	} {
		if link := steamMediaURL(raw); link != "" {
			media.Images[field] = link
		}
	}
	formats := make(map[string]int)
	for i, m := range d.Movies {
		if m.ID <= 0 {
			continue
		}
		v := contractMovie{ID: strconv.Itoa(m.ID), Name: m.Name, Thumbnail: steamMediaURL(m.Thumbnail),
			Highlight: &m.Highlight, Order: i, Sources: make(map[string]string)}
		for field, raw := range map[string]string{
			"mp4_480": m.MP4.P480, "mp4_max": m.MP4.Max, "webm_480": m.WebM.P480, "webm_max": m.WebM.Max,
			"dash_av1": m.DASHAV1, "dash_h264": m.DASHH264, "hls_h264": m.HLSH264,
		} {
			if link := steamMediaURL(raw); link != "" {
				v.Sources[field] = link
				formats[field]++
			}
		}
		media.Movies = append(media.Movies, v)
	}
	for i, s := range d.Screenshots {
		if s.ID < 0 {
			continue
		}
		media.Screenshots = append(media.Screenshots, contractScreenshot{ID: strconv.Itoa(s.ID), Thumbnail: steamMediaURL(s.PathThumbnail),
			Full: steamMediaURL(s.PathFull), Order: i})
	}
	return media, formats
}
