package provider

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"net/url"
	"path"
	"strings"
	"unicode"

	"github.com/gofurry/steam-go/web/storefront"
)

const (
	steamMovieLimit      = 12
	steamScreenshotLimit = 30
	steamMediaURLBytes   = 2048
)

// Shadow the SDK's native-int media IDs while sharing its other typed facts.
// Raw subtrees also let one malformed media item leave other facts intact.
type steamMediaSourceV2 struct {
	Movies      jsontext.Value `json:"movies"`
	Screenshots jsontext.Value `json:"screenshots"`
}

func resolveSteamDetailsV2(raw []byte, appID uint32) (storefront.AppDetailsMatch, steamMediaSourceV2, error) {
	var rows map[string]struct {
		Success bool `json:"success"`
		Data    struct {
			storefront.AppDetailsData
			Movies      jsontext.Value `json:"movies"`
			Screenshots jsontext.Value `json:"screenshots"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &rows); err != nil {
		return storefront.AppDetailsMatch{}, steamMediaSourceV2{}, errSteamJSON
	}
	envelope := make(storefront.AppDetailsEnvelope, len(rows))
	for key, row := range rows {
		envelope[key] = storefront.AppDetailsResult{Success: row.Success, Data: row.Data.AppDetailsData}
	}
	match, err := storefront.ResolveAppDetails(envelope, appID)
	if err != nil {
		return match, steamMediaSourceV2{}, err
	}
	data := rows[match.ResponseKey].Data
	return match, steamMediaSourceV2{Movies: data.Movies, Screenshots: data.Screenshots}, nil
}

type steamImagesV2 struct {
	Header        string `json:"header_url,omitempty"`
	Capsule       string `json:"capsule_url,omitempty"`
	CapsuleV5     string `json:"capsule_v5_url,omitempty"`
	Background    string `json:"background_url,omitempty"`
	BackgroundRaw string `json:"background_raw_url,omitempty"`
}

type steamMovieSourcesV2 struct {
	MP4480   string `json:"mp4_480,omitempty"`
	MP4Max   string `json:"mp4_max,omitempty"`
	WebM480  string `json:"webm_480,omitempty"`
	WebMMax  string `json:"webm_max,omitempty"`
	DASHAV1  string `json:"dash_av1,omitempty"`
	DASHH264 string `json:"dash_h264,omitempty"`
	HLSH264  string `json:"hls_h264,omitempty"`
}

type steamMovieV2 struct {
	ID        string              `json:"movie_id"`
	Name      string              `json:"name"`
	Thumbnail string              `json:"thumbnail_url,omitempty"`
	Highlight *bool               `json:"highlight,omitempty"`
	Order     int                 `json:"order"`
	Sources   steamMovieSourcesV2 `json:"sources"`
}

type steamScreenshotV2 struct {
	ID        string `json:"screenshot_id"`
	Thumbnail string `json:"thumbnail_url,omitempty"`
	Full      string `json:"full_url,omitempty"`
	Order     int    `json:"order"`
}

type steamMediaV2 struct {
	AppID       uint32              `json:"app_id"`
	Images      *steamImagesV2      `json:"images"`
	Movies      []steamMovieV2      `json:"movies"`
	Screenshots []steamScreenshotV2 `json:"screenshots"`
	Truncated   *bool               `json:"truncated"`
}

func steamMediaURLV2(raw, format string) string {
	if raw == "" || len(raw) > steamMediaURLBytes || strings.ContainsAny(raw, "#\\") ||
		strings.ContainsFunc(raw, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Path == "" || u.Path == "/" ||
		strings.HasSuffix(u.Host, ":") || u.Port() != "" && u.Port() != "443" {
		return ""
	}
	host := strings.ToLower(u.Hostname())
	if !strings.HasSuffix(host, ".steamstatic.com") && !strings.HasSuffix(host, ".steamcontent.com") &&
		host != "steamcdn-a.akamaihd.net" && host != "steamuserimages-a.akamaihd.net" {
		return ""
	}
	switch ext := strings.ToLower(path.Ext(u.Path)); ext {
	case ".mp4", ".webm", ".mpd", ".m3u8":
		if ext != format {
			return ""
		}
	}
	u.Host, u.RawQuery, u.ForceQuery = host, "", false
	link := u.String()
	if len(link) > steamMediaURLBytes {
		return ""
	}
	return link
}

func steamMediaID(raw jsontext.Value, zero bool) string {
	id := string(raw)
	if len(id) >= 2 && id[0] == '"' {
		var decoded string
		if json.Unmarshal(raw, &decoded) != nil {
			return ""
		}
		id = decoded
	}
	if steamMediaIDValid(id, zero) {
		return id
	}
	return ""
}

func steamMediaIDValid(id string, zero bool) bool {
	return zero && id == "0" || positiveSteamNumber(id)
}

func decodeSteamMovieV2(raw jsontext.Value, order int) (steamMovieV2, bool) {
	var row struct {
		ID        jsontext.Value               `json:"id"`
		Name      string                       `json:"name"`
		Thumbnail string                       `json:"thumbnail"`
		Highlight *bool                        `json:"highlight"`
		MP4       storefront.StoreMovieSources `json:"mp4"`
		WebM      storefront.StoreMovieSources `json:"webm"`
		DASHAV1   string                       `json:"dash_av1"`
		DASHH264  string                       `json:"dash_h264"`
		HLSH264   string                       `json:"hls_h264"`
	}
	if json.Unmarshal(raw, &row) != nil {
		return steamMovieV2{}, false
	}
	m := steamMovieV2{ID: steamMediaID(row.ID, false), Name: cleanText(row.Name, 256),
		Thumbnail: steamMediaURLV2(row.Thumbnail, ""), Highlight: row.Highlight, Order: order,
		Sources: steamMovieSourcesV2{
			MP4480: steamMediaURLV2(row.MP4.P480, ".mp4"), MP4Max: steamMediaURLV2(row.MP4.Max, ".mp4"),
			WebM480: steamMediaURLV2(row.WebM.P480, ".webm"), WebMMax: steamMediaURLV2(row.WebM.Max, ".webm"),
			DASHAV1: steamMediaURLV2(row.DASHAV1, ".mpd"), DASHH264: steamMediaURLV2(row.DASHH264, ".mpd"),
			HLSH264: steamMediaURLV2(row.HLSH264, ".m3u8"),
		}}
	return m, m.ID != "" && m.Sources != (steamMovieSourcesV2{})
}

func decodeSteamScreenshotV2(raw jsontext.Value, order int) (steamScreenshotV2, bool) {
	var row struct {
		ID        jsontext.Value `json:"id"`
		Thumbnail string         `json:"path_thumbnail"`
		Full      string         `json:"path_full"`
	}
	if json.Unmarshal(raw, &row) != nil {
		return steamScreenshotV2{}, false
	}
	s := steamScreenshotV2{ID: steamMediaID(row.ID, true), Thumbnail: steamMediaURLV2(row.Thumbnail, ""),
		Full: steamMediaURLV2(row.Full, ""), Order: order}
	return s, s.ID != "" && (s.Full != "" || s.Thumbnail != "")
}

func steamMediaCandidates[T any](raw jsontext.Value, limit int, decode func(jsontext.Value, int) (T, bool), identity func(T) string) ([]T, bool) {
	out := make([]T, 0)
	if len(raw) == 0 {
		return out, false
	}
	var rows []jsontext.Value
	if json.Unmarshal(raw, &rows) != nil {
		return out, false
	}
	seen := make(map[string]bool)
	for i, row := range rows {
		item, valid := decode(row, i)
		id := identity(item)
		if !valid || seen[id] {
			continue
		}
		if len(out) == limit {
			return out, true
		}
		seen[id] = true
		out = append(out, item)
	}
	return out, false
}

func steamMediaSegmentV2(t Task, d storefront.AppDetailsData, raw steamMediaSourceV2) Segment {
	movies, moreMovies := steamMediaCandidates(raw.Movies, steamMovieLimit, decodeSteamMovieV2, func(v steamMovieV2) string { return v.ID })
	shots, moreShots := steamMediaCandidates(raw.Screenshots, steamScreenshotLimit, decodeSteamScreenshotV2, func(v steamScreenshotV2) string { return v.ID })
	truncated := moreMovies || moreShots
	media := steamMediaV2{AppID: d.SteamAppID,
		Images: &steamImagesV2{
			Header: steamMediaURLV2(d.HeaderImage, ""), Capsule: steamMediaURLV2(d.CapsuleImage, ""),
			CapsuleV5: steamMediaURLV2(d.CapsuleImageV5, ""), Background: steamMediaURLV2(d.Background, ""),
			BackgroundRaw: steamMediaURLV2(d.BackgroundRaw, ""),
		},
		Movies: []steamMovieV2{}, Screenshots: []steamScreenshotV2{}, Truncated: &truncated}
	if *media.Images == (steamImagesV2{}) && len(movies) == 0 && len(shots) == 0 {
		return steamSegment(t, "media", "media_unavailable", nil)
	}
	seg := steamSegment(t, "media", "", media)
	base, err := json.Marshal(seg)
	if err != nil {
		return steamSegment(t, "media", "source_invalid", nil)
	}
	remaining := steamSegmentBytes - len(base)
	// Encoded arrays start as []. A complete encoded item and its separator
	// are the exact delta. A false-to-true truncation change saves one byte.
	fits := func(item any, count int) bool {
		encoded, err := json.Marshal(item)
		cost := len(encoded) + min(count, 1)
		if err != nil || cost > remaining {
			if !truncated {
				truncated = true
				remaining++
			}
			return false
		}
		remaining -= cost
		return true
	}
	addMovie := func(movie steamMovieV2) {
		if fits(movie, len(media.Movies)) {
			media.Movies = append(media.Movies, movie)
		}
	}
	addShot := func(shot steamScreenshotV2) {
		if fits(shot, len(media.Screenshots)) {
			media.Screenshots = append(media.Screenshots, shot)
		}
	}
	if len(movies) > 0 {
		addMovie(movies[0])
		movies = movies[1:]
	}
	if len(shots) > 0 {
		addShot(shots[0])
		shots = shots[1:]
	}
	for _, movie := range movies {
		addMovie(movie)
	}
	for _, shot := range shots {
		addShot(shot)
	}
	seg.Data, err = json.Marshal(media)
	if err != nil {
		return steamSegment(t, "media", "source_invalid", nil)
	}
	return seg
}

func validSteamMediaLinks(format string, links ...string) bool {
	for _, link := range links {
		if steamMediaURLV2(link, format) != link {
			return false
		}
	}
	return true
}

func validSteamMediaItems[T any](values []T, limit int, fields func(T) (string, int, bool)) bool {
	if values == nil || len(values) > limit {
		return false
	}
	seen := make(map[string]bool, len(values))
	prior := -1
	for _, v := range values {
		id, order, valid := fields(v)
		if !valid || seen[id] || order <= prior || order >= MaxSteamEvidenceBytes {
			return false
		}
		seen[id], prior = true, order
	}
	return true
}

func normalizeSteamMediaV2(appID uint32, state string, data jsontext.Value) (jsontext.Value, jsontext.Value, error) {
	v, err := ObservationObject[steamMediaV2](data)
	if err != nil || state != "present" || v.AppID != appID || v.Images == nil || v.Truncated == nil {
		return webData(false, v, nil)
	}
	image := *v.Images
	valid := (image != (steamImagesV2{}) || len(v.Movies) != 0 || len(v.Screenshots) != 0) &&
		validSteamMediaLinks("", image.Header, image.Capsule, image.CapsuleV5, image.Background, image.BackgroundRaw) &&
		validSteamMediaItems(v.Movies, steamMovieLimit, func(m steamMovieV2) (string, int, bool) {
			s := m.Sources
			return m.ID, m.Order, steamMediaIDValid(m.ID, false) && validText(m.Name, 256) && s != (steamMovieSourcesV2{}) &&
				validSteamMediaLinks("", m.Thumbnail) && validSteamMediaLinks(".mp4", s.MP4480, s.MP4Max) &&
				validSteamMediaLinks(".webm", s.WebM480, s.WebMMax) && validSteamMediaLinks(".mpd", s.DASHAV1, s.DASHH264) &&
				validSteamMediaLinks(".m3u8", s.HLSH264)
		}) && validSteamMediaItems(v.Screenshots, steamScreenshotLimit, func(s steamScreenshotV2) (string, int, bool) {
		return s.ID, s.Order, steamMediaIDValid(s.ID, true) && (s.Thumbnail != "" || s.Full != "") &&
			validSteamMediaLinks("", s.Thumbnail, s.Full)
	})
	return webData(valid, v, nil)
}
