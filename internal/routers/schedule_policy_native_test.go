package routers

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/gofiber/fiber/v3"
	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/database"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/handlers"
	"github.com/librecosta/harvest/internal/provider"
)

func TestScheduleExecutionPolicyNative(t *testing.T) {
	path := os.Getenv("HARVEST_NATIVE_SCHEDULE_TEST_DB_CONFIG")
	if path == "" {
		t.Skip("HARVEST_NATIVE_SCHEDULE_TEST_DB_CONFIG is unset")
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Host != "127.0.0.1" || !strings.HasSuffix(cfg.Database.Name, "_test") {
		t.Fatal("schedule policy test requires isolated PostgreSQL")
	}
	db, err := database.Open(t.Context(), cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := database.CheckSchema(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	registry, err := provider.New(provider.Builtins()...)
	if err != nil {
		t.Fatal(err)
	}
	const namespace, capability, profile = "schedule_test", "web.http", "mixed"
	capabilityDef, _ := registry.Capability(capability)
	writer := config.Principal{Name: "schedule_writer", Token: strings.Repeat("a", 32), Role: "writer", Namespaces: []string{namespace}}
	foreign := config.Principal{Name: "foreign", Token: strings.Repeat("b", 32), Role: "writer", Namespaces: []string{"foreign"}}
	worker := config.Principal{Name: "schedule_worker", Token: strings.Repeat("c", 32), Role: "worker", Namespaces: []string{namespace},
		Capabilities: []string{capability}, EgressProfiles: []string{profile}, MaxSlots: 4}
	principals := []config.Principal{writer, foreign, worker}
	engine := execution.New(db, registry, principals, config.Retention{})
	direct := egress.Advertisement{Profile: profile, Mode: egress.PreferProxy, ID: "direct", Kind: "direct", Region: "cn", State: "healthy", Capacity: 4}
	proxy := egress.Advertisement{Profile: profile, Mode: egress.PreferProxy, ID: "edge", Kind: "socks5", Region: "intl", State: "healthy", Capacity: 4}
	net := egress.NetworkConfig{GlobalBucket: "global",
		Routes: []egress.RouteConfig{{Profile: profile, Mode: egress.PreferProxy, AllowDirect: true,
			Paths: []egress.PathConfig{{ID: direct.ID, Kind: direct.Kind, Region: direct.Region, Bucket: "line"}, {ID: proxy.ID, Kind: proxy.Kind, Region: proxy.Region, Bucket: "proxy"}}}},
		Sources: []egress.SourceConfig{{Capability: capability, Namespace: namespace, Profile: profile, ProviderBucket: "provider", AccountBucket: "account"}},
	}
	for _, key := range []string{"global", "line", "proxy", "provider", "account"} {
		net.Quotas = append(net.Quotas, egress.QuotaConfig{ID: key, Burst: 4, PerMinute: 10, Concurrency: 4})
	}
	if err := engine.InitNetworkPolicy(t.Context(), net); err != nil {
		t.Fatal(err)
	}
	if err := engine.LoadNetworkPolicy(t.Context()); err != nil {
		t.Fatal(err)
	}
	app := fiber.New(fiber.Config{ErrorHandler: handlers.FiberError})
	Register(app, nil, handlers.NewAPI(engine, principals, slog.New(slog.DiscardHandler)))
	call := func(method, endpoint, token string, input, result any, status int) {
		t.Helper()
		got, body, err := registryRequest(app, method, endpoint, token, input)
		if err != nil || got != status {
			t.Fatalf("%s %s returned %d, expected %d: %s (%v)", method, endpoint, got, status, body, err)
		}
		if result != nil {
			if err := json.Unmarshal(body, result); err != nil {
				t.Fatal(err)
			}
		}
	}

	var target struct {
		ID uuid.UUID `json:"id"`
	}
	call("POST", "/api/v1/targets", writer.Token, execution.TargetInput{Provider: "web", ResourceType: "origin",
		ExternalKey: "https://example.com", Namespace: namespace}, &target, 201)
	request := execution.Request{TargetID: target.ID, Capability: capability, RequestSchema: 1,
		Params: jsontext.Value("{}"), Scope: provider.Scope{EgressRegion: "cn"}}
	scheduleInput := execution.ScheduleInput{Request: request, Enabled: new(false), Recurrence: execution.Recurrence{
		IntervalSeconds: 86400, AnchorAt: time.Now().UTC().Truncate(time.Second), Policy: "state_refresh",
	}}
	var schedule struct {
		ID       uuid.UUID `json:"id"`
		Revision int64     `json:"revision"`
	}
	call("POST", "/api/v1/schedules", writer.Token, scheduleInput, &schedule, 201)
	base := "/api/v1/schedules/" + schedule.ID.String()
	var effective execution.SchedulePolicyView
	call("GET", base+"/execution-policy", foreign.Token, nil, nil, 404)
	call("GET", base+"/execution-policy", writer.Token, nil, &effective, 200)
	if len(effective.Regions) != 1 || effective.Regions[0] != "cn" || len(effective.AllowedRegions) != 2 || effective.MaxAttempts != 3 {
		t.Fatalf("unexpected inherited policy %+v", effective)
	}
	var unsupported struct {
		Code string `json:"code"`
	}
	call("POST", base+"/execution-policy/preview", writer.Token, execution.SchedulePolicyPreview{
		Revision: 1, IntervalSeconds: 900, ExecutionPolicy: &execution.ExecutionPolicy{Regions: []string{"jp"}},
	}, &unsupported, 400)
	if unsupported.Code != "schedule_egress_region_not_allowed" {
		t.Fatal(unsupported.Code)
	}

	runtime := uuid.NewV7()
	runtimePath := "/internal/v1/workers/" + runtime.String()
	call("POST", "/internal/v1/workers/register", worker.Token, execution.Registration{ID: runtime, NodeID: "schedule-node",
		Offers: []provider.Offer{capabilityDef.Offer}, Paths: []egress.Advertisement{direct, proxy}, Capacity: 4}, nil, 204)
	runNow := func(key string, revision int64) uuid.UUID {
		t.Helper()
		job, err := engine.SubmitJob(t.Context(), writer, key, execution.Submit{ScheduleID: &schedule.ID, ExpectedScheduleRevision: revision})
		if err != nil {
			t.Fatal(err)
		}
		return job.ID
	}
	old := runNow("old-version", 1)
	var inherited execution.Claim
	call("POST", runtimePath+"/claim", worker.Token, nil, &inherited, 200)
	if inherited.JobID != old || len(inherited.Network.Paths) != 1 || inherited.Network.Paths[0].ID != direct.ID {
		t.Fatalf("inherited claim routed incorrectly: %+v", inherited)
	}

	override := execution.ExecutionPolicy{Regions: []string{"cn", "intl"}, MaxAttempts: 2, Backoff: "fixed"}
	changed := scheduleInput.Recurrence
	changed.IntervalSeconds = 43200
	patch := execution.SchedulePatch{Revision: 1, ExecutionPolicy: &override, Recurrence: &changed, Enabled: new(true)}
	call("PATCH", base, writer.Token, patch, &schedule, 200)
	call("PATCH", base, writer.Token, patch, nil, 409)
	if schedule.Revision != 2 {
		t.Fatal(schedule.Revision)
	}
	call("GET", base+"/execution-policy", writer.Token, nil, &effective, 200)
	if len(effective.Regions) != 2 || effective.MaxAttempts != 2 || effective.Backoff != "fixed" || len(effective.Workers) != 1 {
		t.Fatalf("unexpected override %+v", effective)
	}
	// A later configuration may affect future jobs while the old lease can still
	// grant a permit on its original path and retains the original retry budget.
	permitPath := "/internal/v1/attempts/" + inherited.AttemptID.String() + "/permit"
	permit := execution.PermitRequest{Fence: inherited.Fence, Intent: egress.Intent{
		OperationID: uuid.NewV7(), Profile: profile, PathID: direct.ID, TargetID: target.ID,
		Destination: "https://example.com", Method: "GET", RequestURI: "/", MaxBytes: capabilityDef.Network.MaxBodyBytes,
	}}
	call("POST", permitPath, worker.Token, permit, nil, 200)
	newID := runNow("new-version", 2)
	if newID == old {
		t.Fatal("later schedule was coalesced into a running older revision")
	}
	direct.State = "cooldown"
	call("POST", runtimePath+"/heartbeat", worker.Token, execution.Heartbeat{Paths: []egress.Advertisement{direct, proxy}}, nil, 200)
	var routed execution.Claim
	call("POST", runtimePath+"/claim", worker.Token, nil, &routed, 200)
	if routed.JobID != newID || routed.Task.Scope.EgressRegion != "cn" || len(routed.Network.Paths) != 1 || routed.Network.Paths[0].Region != "intl" {
		t.Fatalf("explicit fallback or immutable scope is incorrect: %+v", routed)
	}
	permitPath = "/internal/v1/attempts/" + routed.AttemptID.String() + "/permit"
	permit.Fence, permit.OperationID = routed.Fence, uuid.NewV7()
	call("POST", permitPath, worker.Token, permit, nil, 403)
	permit.PathID = proxy.ID
	call("POST", permitPath, worker.Token, permit, nil, 200)

	var region string
	var attempts int32
	if err := db.QueryRow(t.Context(), "SELECT a.egress_region,j.max_attempts FROM harvest.attempts a JOIN harvest.jobs j ON j.id=a.job_id WHERE a.id=$1", routed.AttemptID).Scan(&region, &attempts); err != nil || region != "intl" || attempts != 2 {
		t.Fatalf("policy was not persisted on the job/attempt: region=%s attempts=%d err=%v", region, attempts, err)
	}
	call("POST", "/internal/v1/attempts/"+routed.AttemptID.String()+"/complete", worker.Token, execution.Completion{
		Fence: routed.Fence, OutputSchema: 1, Result: provider.Result{State: "failed", Code: "upstream_throttled",
			Segments: []provider.Segment{{Scope: provider.SegmentScope{Namespace: namespace, Scope: request.Scope,
				Dimensions: map[string]string{"section": "http"}}, State: "unavailable", ObservedAt: time.Now().UTC(),
				OutputSchema: 1, Source: "web", Evidence: "upstream_throttled"}}},
	}, nil, 200)
	var state string
	if err := db.QueryRow(t.Context(), "SELECT state FROM harvest.jobs WHERE id=$1", newID).Scan(&state); err != nil || state != "failed" {
		t.Fatalf("upstream throttle must be terminal: %s %v", state, err)
	}
	var configuredScope string
	if err := db.QueryRow(t.Context(),
		"SELECT execution_egress_region,scope->>'egress_region' FROM harvest.observations WHERE job_id=$1", newID).
		Scan(&region, &configuredScope); err != nil || region != "intl" || configuredScope != "cn" {
		t.Fatalf("observation endpoint and canonical scope were not kept independent: %s %s %v", region, configuredScope, err)
	}
	retryID := runNow("retry-backoff-version", 2)
	call("POST", runtimePath+"/claim", worker.Token, nil, &routed, 200)
	if routed.JobID != retryID {
		t.Fatal("wrong job claimed after terminal result")
	}
	call("POST", "/internal/v1/attempts/"+routed.AttemptID.String()+"/complete", worker.Token, execution.Completion{
		Fence: routed.Fence, OutputSchema: 1, Result: provider.Result{State: "failed", Code: "transport_error"},
	}, nil, 200)
	var delay float64
	if err := db.QueryRow(t.Context(),
		"SELECT state, extract(epoch FROM available_at-updated_at)::float8 FROM harvest.jobs WHERE id=$1", retryID).
		Scan(&state, &delay); err != nil || state != "retry_wait" || delay != 2 {
		t.Fatalf("unexpected retry state or fixed delay: %s %v %v", state, delay, err)
	}
	point := changed
	point.Policy = "point_in_time"
	call("PATCH", base, writer.Token, execution.SchedulePatch{Revision: 2, Recurrence: &point}, &unsupported, 400)
	if unsupported.Code != "schedule_retry_not_allowed" {
		t.Fatal(unsupported.Code)
	}
	call("PATCH", base, writer.Token, execution.SchedulePatch{Revision: 2, Recurrence: &point, ExecutionPolicy: &execution.ExecutionPolicy{}}, nil, 200)
	pointID := runNow("point-version", 3)
	if err := db.QueryRow(t.Context(), "SELECT max_attempts FROM harvest.jobs WHERE id=$1", pointID).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("point-in-time run must use one attempt: %d %v", attempts, err)
	}
}
