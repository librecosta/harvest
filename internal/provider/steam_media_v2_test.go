package provider

import (
	"bytes"
	json "encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/gofurry/steam-go/web/storefront"
)

const mediaFixtureV2 = `{"alias":{"success":true,"data":{
  "steam_appid":440,"name":"Fixture","is_free":true,
  "header_image":"https://cdn.steamstatic.com/header.jpg",
  "capsule_image":"https://cdn.steamstatic.com/capsule.jpg",
  "capsule_imagev5":"https://cdn.steamstatic.com/capsule_v5.jpg",
  "background":"https://cdn.steamstatic.com/background.jpg",
  "background_raw":"https://cdn.steamstatic.com/background_raw.jpg",
  "movies":[
    {"id":17,"name":"Thumbnail only","thumbnail":"https://cdn.steamstatic.com/only.jpg"},
    {"id":18446744073709551615,"name":"Z first","thumbnail":"https://cdn.steamstatic.com/thumb.jpg","highlight":false,
     "mp4":{"480":"https://cdn.steamstatic.com/480.mp4","max":"https://cdn.steamstatic.com:443/a%2Fb.mp4?token=fixture"},
     "webm":{"480":"https://cdn.steamstatic.com/480.webm","max":"https://cdn.steamstatic.com/max.webm"},
     "dash_av1":"https://cdn.steamcontent.com/av1.mpd","dash_h264":"https://cdn.steamcontent.com/h264.mpd",
     "hls_h264":"https://cdn.steamcontent.com/video.m3u8"},
    {"id":"17","name":"A second","hls_h264":"https://cdn.steamcontent.com/video.m3u8"},
    {"id":17,"name":"Duplicate","hls_h264":"https://cdn.steamcontent.com/duplicate.m3u8"},
    {"id":12,"name":"Third","dash_h264":"https://cdn.steamcontent.com/h264.mpd",
     "mp4":{"max":"https://cdn.steamcontent.com/video.m3u8"}},
    {"id":35,"hls_h264":"https://cdn.steamcontent.com.evil.test/evil.m3u8"}
  ],
  "screenshots":[
    {"id":99,"path_thumbnail":"https://cdn.steamstatic.com/z.jpg","path_full":"https://cdn.steamstatic.com/z-full.jpg"},
    {"id":0,"path_full":"https://cdn.steamstatic.com/a-full.jpg"},
    {"id":0,"path_full":"https://cdn.steamstatic.com/duplicate.jpg"}]
}}}`

func steamMediaTask(app string, version int32) Task {
	t := evidenceTask(true)
	t.ExternalKey = app
	t.Offer = Offer{Capability: "steam.details", RequestSchema: 1, OutputSchema: version, Implementation: fmt.Sprintf("steam_v%d", version)}
	return t
}

func mediaSegment(t testing.TB, result Result) Segment {
	t.Helper()
	for _, seg := range result.Segments {
		if seg.Scope.Dimensions["section"] == "media" {
			return seg
		}
	}
	t.Fatal("missing media segment")
	return Segment{}
}

func readMediaV2(t testing.TB, seg Segment) steamMediaV2 {
	t.Helper()
	out, err := ObservationObject[steamMediaV2](seg.Data)
	if err != nil || seg.State != "present" || seg.OutputSchema != 2 {
		t.Fatalf("expected v2 media, got state %s, schema %d, error %v", seg.State, seg.OutputSchema, err)
	}
	return out
}

func TestSteamMediaV2FormatsAndIdentity(t *testing.T) {
	source := &evidenceSource{body: []byte(mediaFixtureV2)}
	task := steamMediaTask("440", 2)
	result := acquireSteamDetails(t.Context(), task, source)
	if source.calls != 1 || result.State != "succeeded" || result.SteamEvidence == nil ||
		!bytes.Equal(result.SteamEvidence.Body, source.body) || !ValidSteamEvidence(task, source.body) {
		t.Fatalf("SDK fetch, source validation and original evidence must share one response: calls=%d %s/%s media=%s raw=%t valid=%t", source.calls, result.State, result.Code, mediaSegment(t, result).Evidence, result.SteamEvidence != nil, ValidSteamEvidence(task, source.body))
	}
	seg := mediaSegment(t, result)
	v := readMediaV2(t, seg)
	if v.AppID != 440 || *v.Truncated || len(v.Movies) != 3 || len(v.Screenshots) != 2 {
		t.Fatalf("unexpected projection, app=%d, movie=%d, screenshot=%d", v.AppID, len(v.Movies), len(v.Screenshots))
	}
	first, second, third := v.Movies[0], v.Movies[1], v.Movies[2]
	s := first.Sources
	if first.ID != "18446744073709551615" || first.Order != 1 || first.Name != "Z first" ||
		first.Thumbnail == "" || first.Highlight == nil || *first.Highlight || s.MP4Max != "https://cdn.steamstatic.com/a%2Fb.mp4" ||
		s.MP4480 == "" || s.WebM480 == "" || s.WebMMax == "" || s.DASHAV1 == "" || s.DASHH264 == "" || s.HLSH264 == "" {
		t.Fatal("first movie lost identity, a role or original URL path encoding")
	}
	if second.ID != "17" || second.Name != "A second" || second.Order != 2 || second.Highlight != nil ||
		second.Sources != (steamMovieSourcesV2{HLSH264: s.HLSH264}) || third.ID != "12" || third.Order != 4 ||
		third.Sources != (steamMovieSourcesV2{DASHH264: s.DASHH264}) {
		t.Fatal("original order, duplicate identity or manifest-only playback was lost")
	}
	if v.Screenshots[0].ID != "99" || v.Screenshots[0].Thumbnail == "" || v.Screenshots[0].Full == "" ||
		v.Screenshots[1].ID != "0" || v.Screenshots[1].Order != 1 || v.Screenshots[1].Thumbnail != "" ||
		v.Screenshots[1].Full == "" || v.Images == nil || v.Images.Header == "" || v.Images.Capsule == "" ||
		v.Images.CapsuleV5 == "" || v.Images.Background == "" || v.Images.BackgroundRaw == "" {
		t.Fatal("ordered screenshot and named image roles must be preserved")
	}
	normalized, semantic, err := normalizeSteamMediaV2(440, "present", seg.Data)
	if err != nil || !bytes.Equal(normalized, semantic) || !bytes.Equal(normalized, seg.Data) {
		t.Fatal("v2 media failed canonical server validation")
	}
	for _, changed := range []string{
		strings.Replace(string(seg.Data), `"movie_id":"17"`, `"movie_id":"18446744073709551615"`, 1),
		strings.Replace(string(seg.Data), `"order":2`, `"order":0`, 1),
		strings.Replace(string(seg.Data), `"app_id":440`, `"app_id":441`, 1),
		strings.Replace(string(seg.Data), "cdn.steamcontent.com", "cdn.steamcontent.com.evil.test", 1),
		`{"app_id":440,"movies":["https://cdn.steamstatic.com/file.mp4"]}`,
	} {
		if _, _, err := normalizeSteamMediaV2(440, "present", []byte(changed)); err == nil {
			t.Fatal("accepted a mismatched media payload")
		}
	}

	for _, bad := range []string{
		"http://cdn.steamstatic.com/test.mp4", "https://user@cdn.steamstatic.com/test.mp4",
		"https://cdn.steamstatic.com:444/test.mp4", "https://cdn.steamstatic.com/test.mp4#",
		"https://cdn.steamstatic.com.evil.test/test.mp4", "https://notsteamstatic.com/test.mp4",
		"https://cdn.steamstatic.com/" + strings.Repeat("a", steamMediaURLBytes),
	} {
		if steamMediaURLV2(bad, ".mp4") != "" {
			t.Fatal("URL policy accepted invalid input")
		}
	}
}

func TestSteamMediaV2PartialAndConflictingSource(t *testing.T) {
	task := steamMediaTask("440", 2)
	for _, body := range []string{
		`{"alias":{"success":true,"data":{"steam_appid":440,"name":"Fixture","is_free":true,"movies":[{"id":1,"thumbnail":"https://cdn.steamstatic.com/only.jpg"}]}}}`,
		`{"alias":{"success":true,"data":{"steam_appid":440,"name":"Fixture","is_free":true,"movies":{}}}}`,
	} {
		res := acquireSteamDetails(t.Context(), task, &evidenceSource{body: []byte(body)})
		if seg := mediaSegment(t, res); res.State != "partial" || seg.State != "unavailable" || len(seg.Data) != 0 || seg.OutputSchema != 2 {
			t.Fatal("invalid or unavailable media affected other scopes")
		}
	}
	for _, body := range []string{
		`{"440":{"success":true,"data":{"steam_appid":441,"name":"wrong"}},"alias":{"success":true,"data":{"steam_appid":440,"name":"match"}}}`,
		`{"a":{"success":true,"data":{"steam_appid":440,"name":"first"}},"b":{"success":true,"data":{"steam_appid":440,"name":"second"}}}`,
	} {
		res := acquireSteamDetails(t.Context(), task, &evidenceSource{body: []byte(body)})
		if res.State != "failed" || res.SteamEvidence != nil || ValidSteamEvidence(task, []byte(body)) {
			t.Fatal("ambiguous AppDetails became valid v2 data or evidence")
		}
	}
}

func mediaBudgetFixture(items, pathLen int) []byte {
	shots, movies := make([]string, 0, items), make([]string, 0, items)
	prefix := "https://cdn.steamstatic.com/" + strings.Repeat("a", pathLen)
	for i := range items {
		shots = append(shots, fmt.Sprintf(`{"id":%d,"path_thumbnail":%q,"path_full":%q}`, items-i, prefix+"-t.jpg", prefix+"-f.jpg"))
		movies = append(movies, fmt.Sprintf(`{"id":%d,"name":"Fixture","mp4":{"480":%q,"max":%q},"webm":{"max":%q},"dash_h264":%q,"hls_h264":%q}`,
			items-i, prefix+"-480.mp4", prefix+"-max.mp4", prefix+".webm", prefix+".mpd", prefix+".m3u8"))
	}
	return []byte(fmt.Sprintf(`{"440":{"success":true,"data":{"steam_appid":440,"name":"Fixture","is_free":true,"header_image":"https://cdn.steamstatic.com/header.jpg","movies":[%s],"screenshots":[%s]}}}`,
		strings.Join(movies, ","), strings.Join(shots, ",")))
}

func TestSteamMediaV2EncodedBudget(t *testing.T) {
	task := steamMediaTask("440", 2)
	for _, tc := range []struct {
		name         string
		rows, length int
	}{
		{"count_limit", 34, 80},
		{"near_byte_limit", 34, 1750},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &evidenceSource{body: mediaBudgetFixture(tc.rows, tc.length)}
			res := acquireSteamDetails(t.Context(), task, src)
			seg := mediaSegment(t, res)
			v := readMediaV2(t, seg)
			wire, err := json.Marshal(seg)
			if err != nil || len(wire) > steamSegmentBytes || res.State != "succeeded" || !*v.Truncated ||
				len(v.Movies) == 0 || len(v.Screenshots) == 0 || v.Images.Header == "" ||
				v.Movies[0].Order != 0 || v.Screenshots[0].Order != 0 {
				t.Fatalf("invalid budget projection, wire=%d movies=%d screenshots=%d", len(wire), len(v.Movies), len(v.Screenshots))
			}
			if tc.length == 80 && (len(v.Movies) != 12 || len(v.Screenshots) != 30) ||
				tc.length > 1000 && len(wire) < 29<<10 {
				t.Fatalf("fixture did not exercise the intended boundary, size=%d media=%d/%d", len(wire), len(v.Movies), len(v.Screenshots))
			}
			for _, m := range v.Movies {
				if m.Sources.MP4480 == "" || m.Sources.MP4Max == "" || m.Sources.WebMMax == "" ||
					m.Sources.DASHH264 == "" || m.Sources.HLSH264 == "" {
					t.Fatal("encoded limit split a movie's formats")
				}
			}
			t.Logf("segment=%d data=%d movies=%d screenshots=%d", len(wire), len(seg.Data), len(v.Movies), len(v.Screenshots))
		})
	}
}

func TestSteamMediaV2RealArchives(t *testing.T) {
	dir := os.Getenv("HARVEST_STEAM_V2_SAMPLES_DIR")
	if dir == "" {
		t.Skip("HARVEST_STEAM_V2_SAMPLES_DIR is unset")
	}
	for _, app := range []string{"434000", "1332010"} {
		for _, locale := range []string{"en", "zh-cn"} {
			t.Run(app+"-"+locale, func(t *testing.T) {
				body, err := os.ReadFile(filepath.Join(dir, app+"-"+locale+".json"))
				if err != nil {
					t.Fatal(err)
				}
				var envelope storefront.AppDetailsEnvelope
				if err := json.Unmarshal(body, &envelope); err != nil {
					t.Fatal(err)
				}
				id, _ := strconv.ParseUint(app, 10, 32)
				match, err := storefront.ResolveAppDetails(envelope, uint32(id))
				if err != nil {
					t.Fatal(err)
				}
				task := steamMediaTask(app, 2)
				task.Scope.Locale = locale
				country := "HK"
				if app == "1332010" && locale == "en" {
					country = "CN"
				} else if app == "1332010" {
					country = "US"
				}
				language := "english"
				if locale == "zh-cn" {
					language = "schinese"
				}
				task.Params = []byte(fmt.Sprintf(`{"language":%q,"store_country":%q}`, language, country))
				task.Scope.Region = strings.ToLower(country)
				source := &evidenceSource{body: body}
				v2 := acquireSteamDetails(t.Context(), task, source)
				task.Offer.OutputSchema, task.Offer.Implementation = 1, "steam_v1"
				v1 := acquireSteamDetails(t.Context(), task, source)
				seg := mediaSegment(t, v2)
				media := readMediaV2(t, seg)
				legacy := mediaSegment(t, v1)
				old, err := ObservationObject[steamMedia](legacy.Data)
				if err != nil || legacy.OutputSchema != 1 || len(old.Screenshots) != 8 || len(old.Movies) == 0 || source.calls != 2 ||
					media.Images.Header == "" || media.Images.Capsule == "" || *media.Truncated ||
					len(media.Movies) != len(match.Result.Data.Movies) || len(media.Screenshots) != len(match.Result.Data.Screenshots) ||
					!bytes.Equal(v2.SteamEvidence.Body, v1.SteamEvidence.Body) {
					t.Fatal("v1/v2 compatibility or archive provenance failed")
				}
				for index, m := range media.Movies {
					raw := match.Result.Data.Movies[index]
					if m.ID != strconv.Itoa(raw.ID) || m.Order != index || m.Thumbnail == "" || m.Sources.DASHAV1 == "" ||
						m.Sources.DASHH264 == "" || m.Sources.HLSH264 == "" || m.Sources.MP4Max != "" || m.Sources.WebMMax != "" {
						t.Fatal("archived movie roles, presence or ordering were lost")
					}
				}
				for index, s := range media.Screenshots {
					if s.ID != strconv.Itoa(match.Result.Data.Screenshots[index].ID) || s.Order != index || s.Full == "" || s.Thumbnail == "" {
						t.Fatal("archived screenshot fields were lost")
					}
				}
				wire, err := json.Marshal(seg)
				if err != nil || len(wire) > steamSegmentBytes {
					t.Fatal("archived media does not fit the encoded segment")
				}
				t.Logf("movies=%d screenshots=%d v1_data=%d v2_data=%d v2_segment=%d", len(media.Movies), len(media.Screenshots), len(legacy.Data), len(seg.Data), len(wire))
			})
		}
	}
}

func BenchmarkSteamMediaVersions(b *testing.B) {
	for _, body := range []struct {
		name string
		data []byte
	}{
		{"typical", mediaBudgetFixture(12, 75)},
		{"near_limit", mediaBudgetFixture(34, 1750)},
	} {
		for _, version := range []int32{1, 2} {
			prefix := fmt.Sprintf("%s/schema_%d", body.name, version)
			task := steamMediaTask("440", version)
			task.CaptureSteamEvidence = false
			source := &evidenceSource{body: body.data}
			b.Run(prefix+"/acquire", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_ = acquireSteamDetails(b.Context(), task, source)
				}
			})
			result := acquireSteamDetails(b.Context(), task, source)
			b.Run(prefix+"/serialize", func(b *testing.B) {
				b.ReportAllocs()
				wire, _ := json.Marshal(result)
				b.SetBytes(int64(len(wire)))
				for b.Loop() {
					_, _ = json.Marshal(result)
				}
			})
		}
	}
}
