package execution

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/librecosta/harvest/internal/provider"
)

func TestSteamMediaV2FullSegmentLimit(t *testing.T) {
	r, err := provider.New(provider.SteamManifest(nil))
	if err != nil {
		t.Fatal(err)
	}
	cap, ok := r.CapabilityByKey(provider.SteamMediaV2Key)
	if !ok {
		t.Fatal("schema 2 is missing")
	}
	task := provider.Task{Offer: cap.Offer, Namespace: "test", ExternalKey: "440",
		Scope: provider.Scope{Locale: "en", Region: "us", EgressRegion: "cn"}}
	var media provider.Segment
	diagnostics := make([]provider.Segment, 0, 2)
	for _, scope := range cap.Observation.Scopes(task) {
		s := provider.Segment{Scope: scope, Source: "steam", OutputSchema: 2, ObservedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			State: "unavailable"}
		if scope.Dimensions["section"] == "media" {
			s.State = "present"
			media = s
		} else {
			diagnostics = append(diagnostics, s)
		}
	}
	type screenshot struct {
		ID    string `json:"screenshot_id"`
		Full  string `json:"full_url"`
		Order int    `json:"order"`
	}
	data := struct {
		AppID       uint32       `json:"app_id"`
		Images      struct{}     `json:"images"`
		Movies      []struct{}   `json:"movies"`
		Screenshots []screenshot `json:"screenshots"`
		Truncated   bool         `json:"truncated"`
	}{AppID: 440, Movies: []struct{}{}, Truncated: true}
	for i := range 17 {
		data.Screenshots = append(data.Screenshots, screenshot{strconv.Itoa(i), "https://cdn.steamstatic.com/" + strings.Repeat("a", 1810) + ".jpg", i})
	}
	wireLength := func() int {
		t.Helper()
		value, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		media.Data = jsontext.Value(value)
		wire, err := json.Marshal(media)
		if err != nil {
			t.Fatal(err)
		}
		return len(wire)
	}
	remaining := cap.Observation.MaxSegmentBytes - wireLength()
	if remaining < 0 {
		t.Fatal("fixture starts above the budget")
	}
	// Distribute the final bytes so every individual URL still meets its limit.
	for i := range data.Screenshots {
		n := min(100, remaining)
		data.Screenshots[i].Full += strings.Repeat("x", n)
		remaining -= n
	}
	for _, overflow := range []bool{false, true} {
		extra := 0
		if overflow {
			data.Screenshots[0].Full += "y"
			extra = 1
		}
		if got := wireLength(); got != cap.Observation.MaxSegmentBytes+extra {
			t.Fatalf("boundary fixture is %d bytes", got)
		}
		res := provider.Result{State: "partial", Segments: append([]provider.Segment{media}, diagnostics...)}
		_, err := normalizeSegments(cap, task, &res)
		if !overflow && err != nil || overflow && !errors.Is(err, ErrTooLarge) {
			t.Fatalf("full segment boundary extra=%d returned %v", extra, err)
		}
	}
}
