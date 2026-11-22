package routers

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"log/slog"
	"os"
	"strings"
	"testing"
	"uuid"

	"github.com/gofiber/fiber/v3"
	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/database"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/handlers"
	"github.com/librecosta/harvest/internal/provider"
)

func TestEgressPolicyNative(t *testing.T) {
	path := os.Getenv("HARVEST_NATIVE_POLICY_TEST_DB_CONFIG")
	if path == "" {
		t.Skip("HARVEST_NATIVE_POLICY_TEST_DB_CONFIG is unset")
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Host != "127.0.0.1" || !strings.HasSuffix(cfg.Database.Name, "_test") {
		t.Fatal("egress registry test requires an isolated loopback database")
	}
	pool, err := database.Open(t.Context(), cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := database.CheckSchema(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	registry, err := provider.New(provider.Builtins()...)
	if err != nil {
		t.Fatal(err)
	}
	const capability, namespace = "web.http", "public"
	compiled, ok := registry.Capability(capability)
	if !ok {
		t.Fatal("missing compiled HTTP capability")
	}
	operator := config.Principal{Name: "operator", Token: strings.Repeat("a", 32), Role: "operator", Namespaces: []string{namespace}}
	reader := config.Principal{Name: "reader", Token: strings.Repeat("b", 32), Role: "reader", Namespaces: []string{namespace}}
	foreign := config.Principal{Name: "foreign", Token: strings.Repeat("c", 32), Role: "operator", Namespaces: []string{"foreign"}}
	worker := config.Principal{Name: "native_worker", Token: strings.Repeat("d", 32), Role: "worker", Namespaces: []string{namespace, "foreign"},
		Capabilities: []string{capability}, EgressProfiles: []string{"china", "overseas"}, MaxSlots: 2}
	ps := []config.Principal{operator, reader, foreign, worker}
	eng := execution.New(pool, registry, ps, config.Retention{})
	network := egress.NetworkConfig{GlobalBucket: "global",
		Routes: []egress.RouteConfig{
			{Profile: "china", Mode: egress.Direct, Paths: []egress.PathConfig{{ID: "direct", Kind: "direct", Region: "cn", Bucket: "home"}}},
			{Profile: "overseas", Mode: egress.RequireProxy, Paths: []egress.PathConfig{{ID: "edge", Kind: "http", Region: "intl", Bucket: "proxy"}}},
		},
		Sources: []egress.SourceConfig{
			{Capability: capability, Namespace: namespace, Profile: "china", EgressRegion: "cn", ProviderBucket: "web", AccountBucket: "account"},
			{Capability: capability, Namespace: "foreign", Profile: "china", EgressRegion: "cn", ProviderBucket: "web", AccountBucket: "account"},
		},
	}
	for _, name := range []string{"global", "home", "proxy", "web", "account"} {
		network.Quotas = append(network.Quotas, egress.QuotaConfig{ID: name, Burst: 2, PerMinute: 8, Concurrency: 2})
	}
	if err := eng.InitNetworkPolicy(t.Context(), network); err != nil {
		t.Fatal(err)
	}
	if err := eng.LoadNetworkPolicy(t.Context()); err != nil {
		t.Fatal(err)
	}
	app := fiber.New(fiber.Config{ErrorHandler: handlers.FiberError})
	Register(app, nil, handlers.NewAPI(eng, ps, slog.New(slog.DiscardHandler)))
	call := func(method, path, token string, input, into any, want int) {
		t.Helper()
		got, body, err := registryRequest(app, method, path, token, input)
		if err != nil || got != want {
			t.Fatalf("%s %s returned %d, want %d: %s (%v)", method, path, got, want, body, err)
		}
		if into != nil && json.Unmarshal(body, into) != nil {
			t.Fatalf("invalid API result %s", body)
		}
	}
	const base = "/api/v1/operations/egress-policy?namespace="
	id := uuid.NewV7()
	direct := egress.Advertisement{Profile: "china", Mode: egress.Direct, ID: "direct", Kind: "direct", Region: "cn", State: "healthy", Capacity: 2}
	proxy := egress.Advertisement{Profile: "overseas", Mode: egress.RequireProxy, ID: "edge", Kind: "http", Region: "intl", State: "healthy", Capacity: 2}
	call("POST", "/internal/v1/workers/register", worker.Token, execution.Registration{ID: id, NodeID: "shared-worker",
		Offers: []provider.Offer{compiled.Offer}, Paths: []egress.Advertisement{direct, proxy}, Capacity: 2}, nil, 204)
	legacyID := uuid.NewV7()
	call("POST", "/internal/v1/workers/register", worker.Token, execution.Registration{ID: legacyID,
		Offers: []provider.Offer{compiled.Offer}, Paths: []egress.Advertisement{direct, proxy}, Capacity: 2}, nil, 204)
	var view execution.EgressPolicyView
	call("GET", base+namespace, reader.Token, nil, &view, 200)
	if len(view.Rules) != 1 || len(view.Rules[0].Workers) != 2 {
		t.Fatalf("online legacy worker missing from policy: %+v", view)
	}
	if _, err := pool.Exec(t.Context(), "UPDATE harvest.workers SET last_seen=clock_timestamp()-interval '1 minute' WHERE id=$1", legacyID); err != nil {
		t.Fatal(err)
	}
	call("GET", base+namespace, reader.Token, nil, &view, 200)
	if view.CanEdit || len(view.Rules) != 1 || len(view.Rules[0].Options) != 2 || len(view.Rules[0].Workers) != 1 ||
		!view.Rules[0].Workers[0].Shared || view.Rules[0].Workers[0].RuntimeID != nil || len(view.Rules[0].Workers[0].Paths) != 0 {
		t.Fatalf("unexpected scoped worker view: %+v", view)
	}
	call("GET", base+"foreign", operator.Token, nil, nil, 403)
	var target struct {
		ID uuid.UUID `json:"id"`
	}
	call("POST", "/api/v1/targets", operator.Token, execution.TargetInput{Provider: "web", ResourceType: "origin",
		ExternalKey: "https://example.com", Namespace: namespace}, &target, 201)
	submit := func(region string) uuid.UUID {
		var job struct {
			ID uuid.UUID `json:"id"`
		}
		call("POST", "/api/v1/jobs", operator.Token, execution.Submit{Request: &execution.Request{
			TargetID: target.ID, Capability: capability, RequestSchema: 1, Params: jsontext.Value("{}"), Scope: provider.Scope{EgressRegion: region},
		}}, &job, 202)
		return job.ID
	}
	cnJob, intlJob := submit("cn"), submit("intl")
	runtime := "/internal/v1/workers/" + id.String()
	var claim execution.Claim
	call("POST", runtime+"/claim", worker.Token, nil, &claim, 200)
	if claim.JobID != cnJob || claim.Network == nil || len(claim.Network.Paths) != 1 || claim.Network.Paths[0].Kind != egress.Direct {
		t.Fatalf("incorrect CN direct claim: %+v", claim)
	}
	call("POST", runtime+"/claim", worker.Token, nil, nil, 204)
	edit := execution.EgressPolicyEdit{ExpectedRevision: view.Revision, Capability: capability, Profile: "overseas",
		Mode: egress.RequireProxy, Region: "intl", RequestID: uuid.NewV7(), ActorID: uuid.NewV7(), ActorName: "Operator", ActorRole: "operator"}
	call("PATCH", base+namespace, reader.Token, edit, nil, 403)
	invalid := edit
	invalid.Region = "cn"
	call("PATCH", base+namespace, operator.Token, invalid, nil, 400)
	invalid = edit
	invalid.Mode = "direct"
	call("PATCH", base+namespace, operator.Token, invalid, nil, 400)
	call("PATCH", base+namespace, operator.Token, edit, nil, 200)
	call("PATCH", base+namespace, operator.Token, edit, nil, 409)
	call("GET", base+"foreign", foreign.Token, nil, &view, 200)
	if len(view.Rules) != 1 || view.Rules[0].Profile != "china" {
		t.Fatal("scoped edit altered another namespace")
	}
	var actor uuid.UUID
	if err := pool.QueryRow(t.Context(), "SELECT actor_id FROM harvest.egress_policy_actions WHERE request_id=$1", edit.RequestID).Scan(&actor); err != nil || actor != edit.ActorID {
		t.Fatalf("missing correlated actor audit: %v", err)
	}
	proxy.State = "cooldown"
	call("POST", runtime+"/heartbeat", worker.Token, execution.Heartbeat{Paths: []egress.Advertisement{direct, proxy}}, nil, 200)
	call("POST", runtime+"/claim", worker.Token, nil, nil, 204)
	call("GET", base+namespace, operator.Token, nil, &view, 200)
	if view.Rules[0].Reason != "no_healthy_egress" || view.Rules[0].Workers[0].Reason != "path_cooldown" {
		t.Fatalf("missing cooldown diagnostic: %+v", view)
	}
	proxy.State = "degraded"
	call("POST", runtime+"/heartbeat", worker.Token, execution.Heartbeat{Paths: []egress.Advertisement{direct, proxy}}, nil, 200)
	call("POST", runtime+"/claim", worker.Token, nil, &claim, 200)
	if claim.JobID != intlJob || claim.Network == nil || len(claim.Network.Paths) != 1 || claim.Network.Paths[0].Kind != "http" ||
		claim.Network.Paths[0].Region != "intl" || claim.Network.AllowDirect {
		t.Fatalf("incorrect overseas proxy claim: %+v", claim)
	}
}
