package provider

import (
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	sdkassets "github.com/gofurry/steam-go/addons/assets"
)

type steamAssetSlot struct {
	kind, family, primary, alternate string
}

// Order is also admission priority when the encoded budget is tight.
var steamAssetSlots = []steamAssetSlot{
	{"library_capsule", "library", "library_capsule", ""},
	{"library_hero", "library", "library_hero", ""},
	{"library_logo", "library", "library_logo", "logo"},
	{"library_capsule_2x", "library", "library_capsule_2x", ""},
	{"library_hero_2x", "library", "library_hero_2x", ""},
	{"library_logo_2x", "library", "library_logo_2x", "logo_2x"},
	{"main_capsule", "store", "main_capsule", ""},
	{"small_capsule", "store", "small_capsule", ""},
	{"header", "store", "header", ""},
	{"main_capsule_2x", "store", "main_capsule_2x", ""},
	{"small_capsule_2x", "store", "small_capsule_2x", ""},
	{"header_2x", "store", "header_2x", ""},
	{"hero_capsule", "store", "hero_capsule", ""},
	{"hero_capsule_2x", "store", "hero_capsule_2x", ""},
	{"page_background", "store", "page_background", ""},
	{"page_background_raw", "store", "raw_page_background", ""},
	{"community_icon", "community", "community_icon", ""},
}

func (s steamAssetSlot) hasSlot(slot string) bool {
	return slot != "" && (slot == s.primary || slot == s.alternate)
}

func projectSteamAssets(appID uint32, locale string, fields map[string]jsontext.Value, at time.Time) ([]steamAsset, bool) {
	var format string
	_ = json.Unmarshal(fields["asset_url_format"], &format)
	var rows []steamAsset
	rejected := false
	for _, slot := range steamAssetSlots {
		for _, key := range []string{slot.primary, slot.alternate} {
			raw, exists := fields[key]
			if !exists {
				continue
			}
			var name string
			if json.Unmarshal(raw, &name) != nil {
				rejected = true
				continue
			}
			if name == "" {
				continue
			}
			link, digest := resolveSteamAssetURL(appID, format, name, slot.family == "community")
			if link == "" {
				rejected = true
				continue
			}
			rows = append(rows, steamAsset{ID: steamAssetID(appID, key, locale, link), Type: slot.kind, Family: slot.family,
				Slot: key, URL: link, Locale: locale, Source: steamBrowseSurface, ObservedAt: at, Digest: digest})
			break
		}
	}
	return rows, rejected
}

// Steam publishes hash-backed store filenames and an exact community icon hash.
func resolveSteamAssetURL(appID uint32, format, name string, community bool) (string, string) {
	if appID == 0 {
		return "", ""
	}
	if community {
		if !steamAssetDigest(name) {
			return "", ""
		}
		return validateSteamAssetURL(sdkassets.CommunityIconURL(appID, strings.ToLower(name)), appID, true)
	}
	if raw, err := url.Parse(name); err == nil && raw.IsAbs() {
		return validateSteamAssetURL(name, appID, false)
	}
	if !plainAssetURL(name) || strings.HasPrefix(name, "/") || strings.ContainsAny(name, "?:") {
		return "", ""
	}
	components, ok := steamAssetPath(name)
	if !ok || len(components) > 2 || len(components) == 2 && !steamAssetDigest(components[0]) {
		return "", ""
	}
	if !plainAssetURL(format) {
		return "", ""
	}
	format, _, _ = strings.Cut(format, "?")
	placeholder := "${FILENAME}"
	if !strings.Contains(format, placeholder) {
		placeholder = "%s"
	}
	if !strings.HasSuffix(format, "/"+placeholder) || strings.Count(format, placeholder) != 1 {
		return "", ""
	}
	resolved := strings.Replace(format, placeholder, name, 1)
	if strings.HasPrefix(resolved, "steam/apps/") {
		resolved = sdkassets.DefaultStoreItemAssetBaseURL + resolved
	} else if strings.HasPrefix(resolved, "/store_item_assets/steam/apps/") || strings.HasPrefix(resolved, "/steam/apps/") {
		resolved = strings.TrimSuffix(sdkassets.DefaultStaticCDNBaseURL, "/") + resolved
	}
	return validateSteamAssetURL(resolved, appID, false)
}

func plainAssetURL(s string) bool {
	return s != "" && len(s) <= steamMediaURLBytes && !strings.ContainsAny(s, "#\\") &&
		!strings.ContainsFunc(s, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) || unicode.Is(unicode.Cf, r) })
}

func steamAssetPath(raw string) ([]string, bool) {
	segments := strings.Split(raw, "/")
	for i, encoded := range segments {
		value, err := url.PathUnescape(encoded)
		if err != nil || !utf8.ValidString(value) || value == "" || value == "." || value == ".." || strings.ContainsAny(value, "/\\%") ||
			strings.ContainsFunc(value, func(r rune) bool { return unicode.IsControl(r) || unicode.Is(unicode.Cf, r) }) {
			return nil, false
		}
		segments[i] = value
	}
	return segments, true
}

func validateSteamAssetURL(raw string, appID uint32, community bool) (string, string) {
	if !plainAssetURL(raw) || appID == 0 {
		return "", ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Opaque != "" ||
		strings.HasSuffix(u.Host, ":") || u.Port() != "" && u.Port() != "443" {
		return "", ""
	}
	host := strings.ToLower(u.Hostname())
	p := u.EscapedPath()
	var tail string
	if community {
		if host != "cdn.cloudflare.steamstatic.com" {
			return "", ""
		}
		tail = strings.TrimPrefix(p, "/steamcommunity/public/images/apps/")
	} else {
		switch host {
		case "shared.steamstatic.com", "shared.akamai.steamstatic.com", "shared.cloudflare.steamstatic.com", "shared.fastly.steamstatic.com",
			"shared.st.dl.eccdnx.com", "shared.cdn.steamchina.queniuam.com":
		default:
			return "", ""
		}
		tail = strings.TrimPrefix(p, "/store_item_assets/steam/apps/")
		if tail == p {
			tail = strings.TrimPrefix(p, "/steam/apps/")
		}
	}
	if tail == p {
		return "", ""
	}
	wantID := strconv.FormatUint(uint64(appID), 10)
	if prefix, _, _ := strings.Cut(tail, "/"); prefix != wantID {
		return "", ""
	}
	segments, ok := steamAssetPath(tail)
	if !ok || len(segments) < 2 || len(segments) > 3 {
		return "", ""
	}
	file := segments[len(segments)-1]
	ext := strings.ToLower(path.Ext(file))
	switch ext {
	case ".jpg", ".jpeg", ".png", ".webp", ".avif", ".gif":
	default:
		return "", ""
	}
	var digest string
	if community {
		if len(segments) != 2 || ext != ".jpg" || !steamAssetDigest(strings.TrimSuffix(file, path.Ext(file))) {
			return "", ""
		}
		digest = strings.TrimSuffix(file, path.Ext(file))
	} else if len(segments) == 3 {
		if !steamAssetDigest(segments[1]) {
			return "", ""
		}
		digest = segments[1]
	}
	u.Host, u.RawQuery, u.ForceQuery = host, "", false
	link := u.String()
	if len(link) > steamMediaURLBytes {
		return "", ""
	}
	return link, strings.ToLower(digest)
}

func steamAssetDigest(raw string) bool {
	if len(raw) != 40 {
		return false
	}
	var value [20]byte
	_, err := hex.Decode(value[:], []byte(raw))
	return err == nil
}
