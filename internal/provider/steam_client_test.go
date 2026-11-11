package provider

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

type steamProbe struct {
	t      *testing.T
	calls  int
	closed bool
}

func (p *steamProbe) CloseIdleConnections() { p.closed = true }

func (p *steamProbe) Do(req *http.Request) (*http.Response, error) {
	p.calls++
	status, body := 503, "{}"
	if p.calls == 2 {
		status = 302
	} else if p.calls == 3 {
		status, body = 200, "{\"response\":{\"result\":1,\"player_count\":0}}"
	}
	if p.calls == 1 && req.URL.Host != "store.steampowered.com" ||
		p.calls > 1 && req.URL.Host != "api.steampowered.com" || req.URL.Query().Has("key") {
		p.t.Fatal("SDK did not use the expected keyless Steam surface")
	}
	return &http.Response{StatusCode: status, Header: http.Header{"Location": {"https://example.com/"},
		"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
}

func TestSteamSDKInjectedClient(t *testing.T) {
	probe := &steamProbe{t: t}
	first, err := newSteamClient(probe, 4096, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Web.Storefront.GetResolvedAppDetails(t.Context(), 440, nil); err == nil || probe.calls != 1 {
		t.Fatal("Storefront did not stop after one 503")
	}
	if _, err := first.API.SteamUserStats.GetNumberOfCurrentPlayers(t.Context(), 440); err == nil || probe.calls != 2 {
		t.Fatal("official API followed a redirect")
	}
	first.Close()
	if probe.closed {
		t.Fatal("SDK closed the borrowed transport")
	}
	second, err := newSteamClient(probe, 4096, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	v, err := second.API.SteamUserStats.GetNumberOfCurrentPlayers(t.Context(), 440)
	if err != nil || v.Response.Result != 1 || v.Response.PlayerCount != 0 || probe.calls != 3 {
		t.Fatal("the guarded client was not reusable after SDK Close")
	}
}
