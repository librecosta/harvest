//go:build integration

package integration

import (
	"crypto/rand"
	"crypto/tls"
	json "encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/librecosta/harvest/internal/provider"
)

type steamLab struct {
	t                    *testing.T
	network              *networkLab
	store, news, players atomic.Int32
	held, release        chan struct{}
	requests             []steamHit // guarded by network.mu
}

type steamHit struct {
	host, proxy, peer, app, country, language string
	at                                        time.Time
}

func newSteamLab(t *testing.T, p *testPlane) (*steamLab, *fakeProxy, *fakeProxy) {
	t.Helper()
	l := &steamLab{t: t, network: &networkLab{t: t, peers: make(map[string]string)}, held: make(chan struct{}, 1), release: make(chan struct{}, 1)}
	ca, sign := testWebCA(t)
	server := httptest.NewUnstartedServer(http.HandlerFunc(l.serve))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{sign(21, []string{"store.steampowered.com", "api.steampowered.com"}, false, false)}}
	server.StartTLS()
	t.Cleanup(server.Close)
	l.network.pinnedTargets = map[string]string{webPin + ":443": server.Listener.Addr().String()}
	raw, err := json.Marshal(webFixtureConfig{Targets: l.network.pinnedTargets, CA: string(ca)})
	if err != nil {
		t.Fatal(err)
	}
	p.webFixture = filepath.Join(p.scratch, "steam-fixture.json")
	if err := os.WriteFile(p.webFixture, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return l, l.network.proxy("web_connect", "http", rand.Text()), l.network.proxy("web_socks", "socks5", "")
}

func (l *steamLab) serve(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if r.Method != http.MethodGet || r.TLS == nil || r.TLS.ServerName != r.Host ||
		r.Header.Get("Proxy-Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Authorization") != "" || q.Has("key") {
		l.t.Error("Steam fixture received an unauthorized request")
	}
	app := q.Get("appid")
	if r.Host == "store.steampowered.com" {
		app = q.Get("appids")
	}
	l.network.mu.Lock()
	l.requests = append(l.requests, steamHit{r.Host, l.network.peers[r.RemoteAddr], r.RemoteAddr, app, q.Get("cc"), q.Get("l"), time.Now()})
	l.network.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Host == "store.steampowered.com" && r.URL.Path == "/api/appdetails":
		if app == "" || len(q) != 3 || q.Get("cc") == "" || q.Get("l") == "" {
			l.t.Error("Storefront request shape")
		}
		l.serveStore(w, r, app)
	case r.Host == "api.steampowered.com" && r.URL.Path == "/ISteamNews/GetNewsForApp/v2/":
		if app == "" || len(q) != 3 || q.Get("maxlength") != "512" || q.Get("count") == "" {
			l.t.Error("News request shape")
		}
		l.serveNews(w, app)
	case r.Host == "api.steampowered.com" && r.URL.Path == "/ISteamUserStats/GetNumberOfCurrentPlayers/v1/":
		if app == "" || len(q) != 1 {
			l.t.Error("Players request shape")
		}
		l.servePlayers(w, r)
	default:
		l.t.Error("unexpected Steam endpoint")
		w.WriteHeader(404)
	}
}

func (l *steamLab) serveStore(w http.ResponseWriter, r *http.Request, app string) {
	q := r.URL.Query()
	mode := l.store.Load()
	switch mode {
	case 3:
		_, _ = fmt.Fprintf(w, `{%q:{"success":true,"data":{"steam_appid":999,"name":"wrong"}}}`, app)
		return
	case 4:
		_, _ = fmt.Fprintf(w, `{%q:{"success":true,"data":{"steam_appid":%s,"name":"x"}},"alternate":{"success":true,"data":{"steam_appid":%s,"name":"y"}}}`, app, app, app)
		return
	case 5:
		_, _ = fmt.Fprintf(w, `{%q:{"success":false}}`, app)
		return
	case 6:
		_, _ = fmt.Fprintf(w, `{%q:{"success":false},%q:{"success":true,"data":{"steam_appid":%s,"name":"duplicate"}}}`, app, app, app)
		return
	case 7:
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(429)
		return
	case 8:
		w.WriteHeader(403)
		return
	case 9:
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, "<html>Access denied</html>")
		return
	case 10:
		http.Redirect(w, r, "https://outside.example.com/secret?key=never-store", 302)
		return
	case 11:
		w.WriteHeader(503)
		return
	case 12:
		_, _ = io.WriteString(w, strings.Repeat(" ", 520<<10))
		return
	}
	pricing := `"is_free":false,"price_overview":{"currency":"USD","initial":1200,"final":900,"discount_percent":25},`
	switch {
	case mode == 1:
		pricing = `"is_free":false,`
	case mode == 2:
		pricing = `"is_free":false,"price_overview":{"currency":"USD","initial":1200,"final":0,"discount_percent":100},`
	case mode == 13:
		pricing = `"is_free":false,"price_overview":{"currency":"USD","initial":1200,"discount_percent":100},`
	case q.Get("cc") == "HK":
		pricing = `"is_free":true,`
	}
	_, _ = fmt.Fprintf(w, `{%q:{"success":true,"data":{"steam_appid":%s,%s"name":%q,"type":"game",
"short_description":"<b>Fixture</b><script>never-store</script>","header_image":"https://cdn.steamstatic.com/asset.jpg?token=never-store",
"capsule_image":"https://external.example.com/capsule.png","supported_languages":"English<br>简体中文","platforms":{"windows":true},
"release_date":{"coming_soon":false,"date":"Coming 2027"},"developers":["Fixture Dev"],"publishers":["Fixture Pub"],
"genres":[{"id":"1","description":"Action"}],"categories":[{"id":2,"description":"Single-player"}]}}}`,
		app, app, pricing, q.Get("l")+" "+q.Get("cc"))
}

func (l *steamLab) serveNews(w http.ResponseWriter, app string) {
	const item = `{"gid":"123456","title":"Fixture Update","url":"https://store.steampowered.com/news/app/440/view/1?key=never-store#fragment","author":"Fixture Publisher","feedname":"steam_community_announcements","date":1700000000,"contents":%q,"tags":["patch","patch"]}`
	article := fmt.Sprintf(item, "<p>"+strings.Repeat("Fixture ", 140)+"</p><script>never-store</script>")
	items := article + "," + article
	switch l.news.Load() {
	case 1:
		items = ""
	case 2:
		app = "999"
	case 3:
		items = strings.Replace(article, "1700000000", "-1", 1)
	case 4:
		_, _ = fmt.Fprintf(w, `{"appnews":{"appid":%s}}`, app)
		return
	}
	_, _ = fmt.Fprintf(w, `{"appnews":{"appid":%s,"newsitems":[%s],"count":2}}`, app, items)
}

func (l *steamLab) servePlayers(w http.ResponseWriter, r *http.Request) {
	body := `{"response":{"result":1,"player_count":0}}`
	switch l.players.Load() {
	case 1:
		body = `{"response":{"result":8,"player_count":0}}`
	case 2:
		body = `{"response":{"result":1,"player_count":null}}`
	case 3:
		body = `{"response":{"result":1}}`
	case 4:
		body = `{"response":{"result":1,"player_count":-1}}`
	case 5:
		<-r.Context().Done()
		return
	case 6:
		select {
		case l.held <- struct{}{}:
		default:
		}
		select {
		case <-l.release:
		case <-r.Context().Done():
			return
		}
	}
	_, _ = io.WriteString(w, body)
}

func (l *steamLab) hits() []steamHit {
	l.network.mu.Lock()
	defer l.network.mu.Unlock()
	return append([]steamHit{}, l.requests...)
}

func steamOffers() []provider.Offer {
	return []provider.Offer{
		{Capability: "steam.details", RequestSchema: 1, OutputSchema: 1, Implementation: "steam_v1"},
		{Capability: "steam.news", RequestSchema: 1, OutputSchema: 1, Implementation: "steam_v1"},
		{Capability: "steam.players", RequestSchema: 1, OutputSchema: 1, Implementation: "steam_v1"},
	}
}
