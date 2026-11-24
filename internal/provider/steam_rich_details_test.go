package provider

import (
	"bytes"
	json "encoding/json/v2"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestSteamDetailsV3BudgetAndFieldStates(t *testing.T) {
	v3 := steamMediaTask("440", 3)
	body := func(size int) []byte {
		return fmt.Appendf(nil, `{"alias":{"success":true,"data":{
		"steam_appid":440,"name":"Game","is_free":true,"header_image":"https://cdn.steamstatic.com/header.jpg",
		"required_age":"0","pc_requirements":{"minimum":"One"},"mac_requirements":[],
		"content_descriptors":{"ids":[2,5],"notes":"A description"},
		"ratings":{"esrb":{"rating":"t","required_age":"13"}},
		"platforms":{"windows":true,"mac":false},"controller_support":"full",
		"release_date":{"coming_soon":true,"date":"2027"},
		"linux_requirements":{"minimum":13},"support_info":{"url":"","email":"support@example.org"},
		"supported_languages":"English<strong>*</strong><br>Dialect ABC<br><strong>*</strong>languages with full audio support",
		"detailed_description":%q,"about_the_game":""}}}`, strings.Repeat("x", size))
	}
	for _, size := range []int{64 << 10, 193 << 10} {
		source := &evidenceSource{body: body(size)}
		result := acquireSteamDetails(t.Context(), v3, source)
		if source.calls != 1 || result.SteamEvidence == nil || !bytes.Equal(result.SteamEvidence.Body, source.body) ||
			len(result.Segments) != 4 || mediaSegment(t, result).OutputSchema != 3 {
			t.Fatalf("request, evidence or fourth segment missing: %s/%s %d", result.State, result.Code, len(result.Segments))
		}
		seg := result.Segments[3]
		v, err := ObservationObject[steamRichDetails](seg.Data)
		if err != nil || v.AboutTheGame == nil || *v.AboutTheGame != "" || v.Mac == nil || v.Linux != nil ||
			!slices.Contains(v.Invalid, "linux_requirements") || v.RequiredAge == nil || *v.RequiredAge != 0 {
			t.Fatalf("field presence unavailable: %v", err)
		}
		if v.Descriptors == nil || !slices.Equal(v.Descriptors.IDs, []int{2, 5}) || v.Descriptors.Notes == nil ||
			*v.Descriptors.Notes != "A description" || v.Ratings["esrb"].Rating == nil || *v.Ratings["esrb"].Rating != "t" ||
			v.Platforms == nil || v.Platforms.Mac == nil || *v.Platforms.Mac || v.Platforms.Linux != nil ||
			v.ControllerSupport == nil || *v.ControllerSupport != "full" || v.Release == nil ||
			v.Release.Date != "2027" || v.Release.ComingSoon == nil || !*v.Release.ComingSoon {
			t.Fatal("structured Steam facts lost")
		}
		if size == 64<<10 {
			if v.DetailedDescription == nil || len(*v.DetailedDescription) != size || len(v.Truncated) != 0 {
				t.Fatal("ordinary long document changed")
			}
		} else if v.DetailedDescription != nil || !slices.Contains(v.Truncated, "detailed_description") {
			t.Fatal("oversized document was not explicitly omitted")
		}
		if len(v.Languages) != 2 || v.Languages[0].FullAudio == nil || !*v.Languages[0].FullAudio ||
			v.Languages[0].Interface != nil || v.Languages[1].Known {
			t.Fatal("language evidence lost or inferred")
		}
		normalized, _, err := normalizeSteamRichDetails(v3, seg.State, seg.Data)
		if err != nil || len(normalized) < 1 {
			t.Fatalf("completion rejected rich segment: %v", err)
		}
		var normalizedWire steamRichDetails
		if err = json.Unmarshal(normalized, &normalizedWire); err != nil {
			t.Fatal(err)
		}
	}
}
