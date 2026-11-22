package routers

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
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
	"github.com/librecosta/harvest/internal/worker"
)

// Run with an explicit YAML for an isolated loopback PostgreSQL 18 database.
func TestWorkerNodesNative(t *testing.T) {
	path := os.Getenv("HARVEST_NATIVE_TEST_DB_CONFIG")
	if path == "" {
		t.Skip("HARVEST_NATIVE_TEST_DB_CONFIG is unset")
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Host != "127.0.0.1" || !strings.HasSuffix(cfg.Database.Name, "_test") {
		t.Fatal("worker registry test requires a loopback database whose name ends in _test")
	}
	pool, err := database.Open(t.Context(), cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := database.CheckSchema(t.Context(), pool); err != nil {
		t.Fatal(err)
	}

	offer := provider.Offer{Capability: "registry.run", RequestSchema: 1, OutputSchema: 1, Implementation: "registry_v1"}
	type value struct {
		Name string `json:"name"`
	}
	registry, err := provider.New(provider.Manifest{
		Name:      "registry",
		Resources: []provider.Resource{{Name: "key", Normalize: func(input string) (string, error) { return input, nil }}},
		Capabilities: []provider.Capability{{
			Offer: offer, Resource: "key", Timeout: time.Minute, MinInterval: time.Second,
			NormalizeParams: provider.Normalize[value], NormalizeOutput: provider.Normalize[value],
			Execute: func(_ context.Context, _ provider.Task, _ egress.Client) provider.Result {
				return provider.Result{State: "succeeded", Data: jsontext.Value(`{"name":"result"}`)}
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	scope := "registry_" + strings.ReplaceAll(uuid.NewV7().String(), "-", "")
	private := scope + "_private"
	workerPrincipal := config.Principal{Name: "worker", Token: strings.Repeat("a", 32), Role: "worker",
		Namespaces: []string{scope}, Capabilities: []string{offer.Capability}, MaxSlots: 1, EgressProfiles: []string{"local"}}
	shared := workerPrincipal
	shared.Name, shared.Token, shared.Namespaces = "shared", strings.Repeat("b", 32), []string{scope, private}
	operator := config.Principal{Name: "operator", Token: strings.Repeat("c", 32), Role: "operator", Namespaces: []string{scope}}
	global := config.Principal{Name: "global", Token: strings.Repeat("d", 32), Role: "operator", Namespaces: []string{scope, private}}
	reader := config.Principal{Name: "reader", Token: strings.Repeat("e", 32), Role: "reader", Namespaces: []string{scope}}
	principals := []config.Principal{workerPrincipal, shared, operator, global, reader}
	newAPI := func() *fiber.App {
		app := fiber.New(fiber.Config{ErrorHandler: handlers.FiberError})
		engine := execution.New(pool, registry, principals, config.Retention{})
		Register(app, nil, handlers.NewAPI(engine, principals, slog.New(slog.DiscardHandler)))
		return app
	}
	app := newAPI()
	call := func(method, path, token string, input, output any, status int) {
		t.Helper()
		got, body, err := registryRequest(app, method, path, token, input)
		if err != nil || got != status {
			t.Fatalf("%s %s got %d, want %d: %s (%v)", method, path, got, status, body, err)
		}
		if output != nil {
			if err := json.Unmarshal(body, output); err != nil {
				t.Fatal(err)
			}
		}
	}
	const nodePath = "/api/v1/operations/worker-nodes"
	const workerPath = "/internal/v1/workers/"
	suffix := "?namespace=" + scope
	id := scope + "_one"
	reg := execution.Registration{ID: uuid.NewV7(), NodeID: id, Offers: []provider.Offer{offer}, Capacity: 1,
		EgressProfiles: []string{"local"}, Paths: []egress.Advertisement{{Profile: "local", Mode: "require_proxy",
			ID: "local-clash", Kind: "http", Region: "intl", State: "healthy", Capacity: 1}}}
	call("GET", nodePath+suffix, "", nil, nil, 401)
	call("POST", workerPath+"register", workerPrincipal.Token, reg, nil, 204)
	call("POST", workerPath+"register", workerPrincipal.Token, reg, nil, 204)
	second := reg
	second.ID = uuid.NewV7()
	call("POST", workerPath+"register", workerPrincipal.Token, second, nil, 409)
	call("POST", workerPath+"register", shared.Token, second, nil, 403)
	sharedReg := reg
	sharedReg.ID, sharedReg.NodeID = uuid.NewV7(), scope+"_shared"
	call("POST", workerPath+"register", shared.Token, sharedReg, nil, 204)
	legacy := reg
	legacy.ID, legacy.NodeID = uuid.NewV7(), ""
	call("POST", workerPath+"register", workerPrincipal.Token, legacy, nil, 204)
	var oldHeartbeat map[string]any
	call("POST", workerPath+legacy.ID.String()+"/heartbeat", workerPrincipal.Token, execution.Heartbeat{}, &oldHeartbeat, 200)
	if _, exists := oldHeartbeat["desired_state"]; exists {
		t.Fatal("legacy heartbeat response changed")
	}
	healthPath := nodePath + "/" + id + "/egress-health" + suffix
	check := egress.HealthReport{Profile: "local", PathID: "local-clash", Kind: "http", Region: "intl", CheckType: "google_doh",
		LastOutcome: "transport_failure", Failures: 1, TransportFailures: 1, TrailingFailures: 1, LastLatencyMS: 42, LatencyMS: 42}
	batch := execution.Heartbeat{Health: &execution.WorkerHealthBatch{Sequence: 1, Reports: []egress.HealthReport{check}}}
	call("POST", workerPath+reg.ID.String()+"/heartbeat", workerPrincipal.Token, batch, nil, 200)
	call("POST", workerPath+reg.ID.String()+"/heartbeat", workerPrincipal.Token, batch, nil, 200)
	call("GET", healthPath, global.Token, nil, nil, 200)
	call("GET", nodePath+"/"+sharedReg.NodeID+"/egress-health"+suffix, operator.Token, nil, nil, 404)
	var list execution.WorkerNodeList
	call("GET", nodePath+suffix, reader.Token, nil, &list, 200)
	if len(list.Items) != 1 || list.Items[0].NodeID != id || list.Items[0].CanManage {
		t.Fatalf("unexpected restricted list: %+v", list)
	}
	call("GET", nodePath+"/"+sharedReg.NodeID+suffix, operator.Token, nil, nil, 404)
	call("GET", nodePath+"/"+sharedReg.NodeID+suffix, global.Token, nil, nil, 404)
	call("GET", nodePath+"/"+sharedReg.NodeID, global.Token, nil, nil, 200)
	call("GET", nodePath+"?namespace="+private, operator.Token, nil, nil, 403)

	startTarget := "/api/v1/targets"
	var target struct {
		ID uuid.UUID `json:"id"`
	}
	call("POST", startTarget, operator.Token, execution.TargetInput{
		Namespace: scope, Provider: "registry", ResourceType: "key", ExternalKey: id,
	}, &target, 201)
	var job struct {
		ID uuid.UUID `json:"id"`
	}
	call("POST", "/api/v1/jobs", operator.Token, execution.Submit{Request: &execution.Request{
		TargetID: target.ID, Capability: offer.Capability, RequestSchema: 1, Params: jsontext.Value(`{"name":"lease-test"}`),
	}}, &job, 202)
	runtimePath := workerPath + reg.ID.String()
	var claim execution.Claim
	call("POST", runtimePath+"/claim", workerPrincipal.Token, nil, &claim, 200)
	if claim.JobID != job.ID {
		t.Fatalf("unexpected job %v", claim.JobID)
	}
	detailPath := nodePath + "/" + id
	var detail execution.WorkerNode
	call("GET", detailPath+suffix, operator.Token, nil, &detail, 200)
	if !detail.Online || !detail.CanManage || detail.ActiveTasks != 1 || len(detail.Tasks) != 1 ||
		detail.Tasks[0].JobID != job.ID || len(detail.Paths) != 1 || detail.Paths[0].ID != "local-clash" {
		t.Fatalf("missing safe status or active job: %+v", detail)
	}

	actor, request := uuid.NewV7(), uuid.NewV7()
	paused := execution.WorkerNodeEdit{Revision: 1, State: "paused", RequestID: request,
		ActorID: &actor, ActorName: "真实运营员", ActorRole: "owner"}
	statePath := detailPath + "/state" + suffix
	call("POST", statePath, reader.Token, paused, nil, 403)
	call("POST", nodePath+"/"+sharedReg.NodeID+"/state"+suffix, operator.Token, paused, nil, 404)
	call("POST", statePath, operator.Token, paused, nil, 200)
	call("POST", statePath, operator.Token, paused, nil, 200)
	var heartbeat execution.Heartbeat
	call("POST", runtimePath+"/heartbeat", workerPrincipal.Token, execution.Heartbeat{}, &heartbeat, 200)
	if heartbeat.DesiredState != "paused" {
		t.Fatalf("heartbeat did not see pause: %+v", heartbeat)
	}
	call("POST", runtimePath+"/claim", workerPrincipal.Token, nil, nil, 503)
	attempt := "/internal/v1/attempts/" + claim.AttemptID.String()
	call("POST", attempt+"/renew", workerPrincipal.Token, claim.Fence, nil, 200)
	call("POST", statePath, operator.Token, execution.WorkerNodeEdit{Revision: 1, State: "active", RequestID: uuid.NewV7()}, nil, 409)

	app = newAPI()
	drain := execution.WorkerNodeEdit{Revision: 2, State: "draining", RequestID: uuid.NewV7()}
	resume := execution.WorkerNodeEdit{Revision: 2, State: "active", RequestID: uuid.NewV7()}
	var wg sync.WaitGroup
	statuses := make(chan int, 2)
	for _, edit := range []execution.WorkerNodeEdit{drain, resume} {
		wg.Go(func() {
			code, _, _ := registryRequest(app, "POST", statePath, operator.Token, edit)
			statuses <- code
		})
	}
	wg.Wait()
	if a, b := <-statuses, <-statuses; min(a, b) != 200 || max(a, b) != 409 {
		t.Fatalf("competing state operations returned %d and %d", a, b)
	}
	call("GET", detailPath+suffix, operator.Token, nil, &detail, 200)
	if detail.DesiredState != "draining" {
		call("POST", statePath, operator.Token, execution.WorkerNodeEdit{Revision: detail.Revision, State: "draining", RequestID: uuid.NewV7()}, nil, 200)
	}
	call("POST", runtimePath+"/claim", workerPrincipal.Token, nil, nil, 503)
	call("POST", runtimePath+"/heartbeat", workerPrincipal.Token, execution.Heartbeat{Draining: true}, nil, 200)
	call("POST", workerPath+"register", workerPrincipal.Token, second, nil, 409)
	call("POST", attempt+"/complete", workerPrincipal.Token, execution.Completion{
		Fence: claim.Fence, OutputSchema: 1, Result: provider.Result{State: "succeeded", Data: jsontext.Value(`{"name":"completed while draining"}`)},
	}, nil, 200)
	if _, err := pool.Exec(t.Context(), "UPDATE harvest.workers SET last_seen=clock_timestamp()-interval '31 seconds' WHERE id=$1", reg.ID); err != nil {
		t.Fatal(err)
	}
	call("GET", detailPath+suffix, operator.Token, nil, &detail, 200)
	if detail.Online || detail.ActiveTasks != 0 || detail.DesiredState != "draining" {
		t.Fatalf("expected persistent offline drained node: %+v", detail)
	}
	call("POST", workerPath+"register", workerPrincipal.Token, second, nil, 204)
	call("POST", runtimePath+"/heartbeat", workerPrincipal.Token, execution.Heartbeat{}, nil, 409)
	call("POST", runtimePath+"/claim", workerPrincipal.Token, nil, nil, 409)
	call("POST", workerPath+"register", workerPrincipal.Token, reg, nil, 409)
	newRuntime := workerPath + second.ID.String()
	var health execution.WorkerHealthView
	call("GET", healthPath, operator.Token, nil, &health, 200)
	if health.WindowHours != 24 || len(health.Checks) != 1 || health.Checks[0].Failures != 1 || health.Checks[0].ConsecutiveFailures != 1 {
		t.Fatalf("worker health was duplicated or lost on restart: %+v", health)
	}
	call("POST", newRuntime+"/heartbeat", workerPrincipal.Token, batch, nil, 200)
	check.LastOutcome, check.Failures, check.TransportFailures, check.TrailingFailures, check.Successes = "ok", 0, 0, 0, 1
	call("POST", newRuntime+"/heartbeat", workerPrincipal.Token, execution.Heartbeat{
		Health: &execution.WorkerHealthBatch{Sequence: 2, Reports: []egress.HealthReport{check}},
	}, nil, 200)
	call("GET", healthPath, operator.Token, nil, &health, 200)
	if len(health.Checks) != 1 || health.Checks[0].Failures != 2 || health.Checks[0].Successes != 1 ||
		health.Checks[0].ConsecutiveFailures != 0 || health.Checks[0].LastOutcome != "ok" {
		t.Fatalf("recovery did not preserve prior statistics: %+v", health)
	}
	call("POST", newRuntime+"/heartbeat", workerPrincipal.Token, execution.Heartbeat{}, &heartbeat, 200)
	if heartbeat.DesiredState != "draining" {
		t.Fatal("node lost its desired state across runtime replacement")
	}
	call("POST", newRuntime+"/claim", workerPrincipal.Token, nil, nil, 503)
	call("POST", statePath, operator.Token, execution.WorkerNodeEdit{Revision: detail.Revision, State: "active", RequestID: uuid.NewV7()}, nil, 200)
	call("POST", newRuntime+"/claim", workerPrincipal.Token, nil, nil, 204)
	var auditedActor uuid.UUID
	var auditedName, auditedRole, previousState, desiredState string
	if err := pool.QueryRow(t.Context(), `SELECT actor_id,actor_name,actor_role,previous_state,desired_state FROM harvest.worker_node_actions WHERE request_id=$1`, request).
		Scan(&auditedActor, &auditedName, &auditedRole, &previousState, &desiredState); err != nil ||
		auditedActor != actor || auditedName != paused.ActorName || auditedRole != "owner" || previousState != "active" || desiredState != "paused" {
		t.Fatalf("missing audit: %v", err)
	}

	// Exercise the native Worker loop over loopback HTTP, including its own
	// startup heartbeat and graceful shutdown signaling.
	call("GET", detailPath+suffix, operator.Token, nil, &detail, 200)
	call("POST", statePath, operator.Token, execution.WorkerNodeEdit{Revision: detail.Revision, State: "paused", RequestID: uuid.NewV7()}, nil, 200)
	call("POST", newRuntime+"/heartbeat", workerPrincipal.Token, execution.Heartbeat{Draining: true}, nil, 200)
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- app.Listener(listener, fiber.ListenConfig{DisableStartupMessage: true}) }()
	t.Cleanup(func() {
		defer listener.Close()
		if err := app.ShutdownWithTimeout(2 * time.Second); err != nil {
			t.Error(err)
		}
		if err := <-serverDone; err != nil {
			t.Error(err)
		}
	})
	cfgWorker := config.Worker{NodeID: id, ControlURL: "http://" + listener.Addr().String(), Token: workerPrincipal.Token,
		Offers: []provider.Offer{offer}, Concurrency: 1}
	var restartedID uuid.UUID
	for range 2 {
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- worker.Run(ctx, cfgWorker, registry, slog.New(slog.DiscardHandler)) }()
		deadline := time.Now().Add(2 * execution.HeartbeatInterval)
		var startupSeen time.Time
		for {
			var view execution.WorkerNode
			call("GET", detailPath+suffix, operator.Token, nil, &view, 200)
			if view.RuntimeID != second.ID && view.RuntimeID != restartedID {
				if !view.Online || view.DesiredState != "paused" || view.NodeID != id || view.RuntimeDraining {
					cancel()
					t.Fatalf("native runtime lost the node policy: %+v", view)
				}
				if startupSeen.IsZero() {
					startupSeen = view.LastSeen
				}
				if view.LastSeen.After(startupSeen) {
					restartedID = view.RuntimeID
					break
				}
			}
			if time.Now().After(deadline) {
				cancel()
				t.Fatal("native worker did not register")
			}
			time.Sleep(50 * time.Millisecond)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}

	for range 2 {
		candidate := reg
		candidate.ID, candidate.NodeID = uuid.NewV7(), scope+"_race"
		wg.Go(func() {
			code, _, _ := registryRequest(app, "POST", workerPath+"register", workerPrincipal.Token, candidate)
			statuses <- code
		})
	}
	wg.Wait()
	if a, b := <-statuses, <-statuses; min(a, b) != 204 || max(a, b) != 409 {
		t.Fatalf("concurrent node registrations returned %d and %d", a, b)
	}
}

func registryRequest(app *fiber.App, method, path, token string, input any) (int, []byte, error) {
	var body []byte
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return 0, nil, err
		}
		body = data
	}
	request := httptest.NewRequest(method, path, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if method == http.MethodPost && path == "/api/v1/jobs" {
		request.Header.Set("Idempotency-Key", uuid.NewV7().String())
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := app.Test(request, fiber.TestConfig{Timeout: 5 * time.Second})
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	out, err := io.ReadAll(response.Body)
	if response.StatusCode == http.StatusNoContent {
		return response.StatusCode, nil, err
	}
	return response.StatusCode, out, err
}
