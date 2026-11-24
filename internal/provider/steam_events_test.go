package provider

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

const eventsExample = `{"success":1,"events":[{"appid":730,"gid":"9007199254740993","published":1,"hidden":0,"announcement_body":{"gid":"9007199254740994","event_gid":"9007199254740993","language":0,"headline":"Update","body":"[b]Full[/b]","posttime":1795474927,"commentcount":0}},{"appid":730,"gid":"9007199254740995","published":1,"announcement_body":{"gid":"9007199254740996","event_gid":"9007199254740995","language":6,"headline":"更新","body":"[b]中文版[/b]","posttime":1795474927}}]}`

func TestSteamEventsSDKAndEvidence(t *testing.T) {
	task := steamMediaTask("730", 1)
	task.Offer = Offer{SteamEvents, 1, 1, "steam_events_v1"}
	task.Params = []byte(`{"language":"schinese"}`)
	task.Scope.Locale, task.Scope.Region = "zh-cn", ""
	task.CaptureSteamEvidence = true
	calls := 0
	res := acquireSteamEvents(t.Context(), task, browseTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Method != "GET" || r.URL.Host != "store.steampowered.com" ||
			r.URL.RequestURI() != SteamRequestURI(SteamEvents, "730", task.Params) ||
			r.URL.Query().Get("lang_list") != "6_0" || r.URL.Query().Get("count_after") != "4" {
			t.Fatalf("wrong request %s", r.URL)
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}},
			Body: io.NopCloser(strings.NewReader(eventsExample)), Request: r}, nil
	}))
	if calls != 1 || res.State != "succeeded" || res.SteamEvidence == nil ||
		string(res.SteamEvidence.Body) != eventsExample || !ValidSteamEvidence(task, res.SteamEvidence.Body) {
		t.Fatalf("one attested request with exact evidence required, calls=%d, result=%s", calls, res.State)
	}
	page, verified, code := readSteamEventPage(res.SteamEvidence.Body, 730, "zh-cn")
	if !verified || code != "" || len(page.Events) != 2 || page.Events[0].LanguageState != "fallback" ||
		page.Events[1].LanguageState != "declared_match" || page.Events[0].Comments == nil || *page.Events[0].Comments != 0 ||
		page.Events[1].Comments != nil || page.Events[0].EventGID == page.Events[0].AnnouncementGID {
		t.Fatalf("source facts lost: %+v", page)
	}
}

func TestSteamEventsPageQuarantine(t *testing.T) {
	mixed := strings.Replace(eventsExample, `"appid":730`, `"appid":440`, 1)
	page, verified, _ := readSteamEventPage([]byte(mixed), 730, "en")
	if verified || len(page.Events) != 1 || len(page.Diagnostics) != 1 || page.Diagnostics[0].Code != "identity_invalid" {
		t.Fatalf("foreign app was not isolated: %+v", page)
	}
	long := strings.Replace(eventsExample, "[b]Full[/b]", strings.Repeat("x", steamEventsBodyBytes+1), 1)
	page, verified, _ = readSteamEventPage([]byte(long), 730, "en")
	if !verified || !page.Truncated || len(page.Events) != 1 || page.Diagnostics[0].Code != "field_budget" {
		t.Fatalf("long article did not preserve good sibling: %+v", page.Diagnostics)
	}
	page, verified, code := readSteamEventPage([]byte(`{"success":1,"events":[]}`), 730, "en")
	if verified || code != "" || len(page.Events) != 0 || len(page.Diagnostics) != 0 {
		t.Fatal("explicit empty page was not distinguished")
	}
	_, _, code = readSteamEventPage([]byte(`{"success":0,"events":[]}`), 730, "en")
	if code != "steam_unavailable" {
		t.Fatalf("failed source was confused with empty: %s", code)
	}
}
