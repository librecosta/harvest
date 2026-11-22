package worker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/database"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/handlers"
	"github.com/librecosta/harvest/internal/provider"
	"github.com/librecosta/harvest/internal/routers"
)

type steamEvidencePlane struct {
	t             *testing.T
	db            *pgxpool.Pool
	engine        *execution.Engine
	api           client
	registry      *provider.Registry
	pool          *egress.Pool
	cfg           config.Worker
	writer, agent config.Principal
	operator      config.Principal
	workerID      uuid.UUID
	body          atomic.Pointer[[]byte]
	requests      atomic.Int32
	log           *slog.Logger
	lost          atomic.Bool
	completion    execution.Completion
}

func newSteamEvidencePlane(t *testing.T) *steamEvidencePlane {
	t.Helper()
	path := os.Getenv("HARVEST_NATIVE_EVIDENCE_DB_CONFIG")
	if path == "" {
		t.Skip("HARVEST_NATIVE_EVIDENCE_DB_CONFIG is unset")
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Host != "127.0.0.1" || !strings.HasSuffix(cfg.Database.Name, "_test") {
		t.Fatal("requires an isolated loopback PostgreSQL database")
	}
	db, err := database.Open(t.Context(), cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := database.CheckSchema(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	s := &steamEvidencePlane{t: t, db: db, workerID: uuid.NewV7(), log: slog.New(slog.DiscardHandler)}
	s.registry, err = provider.New(provider.SteamManifest(s.steamFixture()))
	if err != nil {
		t.Fatal(err)
	}
	const capability, profile = "steam.details", "local"
	offer, _ := s.registry.Capability(capability)
	namespaces := []string{"evidence_one", "evidence_two"}
	s.writer = config.Principal{Name: "writer", Role: "writer", Token: strings.Repeat("a", 40), Namespaces: namespaces}
	s.agent = config.Principal{Name: "worker", Role: "worker", Token: strings.Repeat("b", 40), Namespaces: namespaces,
		Capabilities: []string{capability}, EgressProfiles: []string{profile}, MaxSlots: 1}
	foreign := s.agent
	foreign.Name, foreign.Token, foreign.Namespaces = "foreign", strings.Repeat("c", 40), []string{"foreign"}
	s.operator = config.Principal{Name: "operator", Role: "operator", Token: strings.Repeat("d", 40), Namespaces: namespaces}
	limited := config.Principal{Name: "limited", Role: "operator", Token: strings.Repeat("e", 40), Namespaces: namespaces[:1]}
	wrongCapability := config.Principal{Name: "wrong_capability", Role: "operator", Token: strings.Repeat("f", 40),
		Namespaces: namespaces, Capabilities: []string{"steam.news"}}
	reader := config.Principal{Name: "reader", Role: "reader", Token: strings.Repeat("g", 40), Namespaces: namespaces}
	people := []config.Principal{s.writer, s.agent, foreign, s.operator, limited, wrongCapability, reader}
	s.engine = execution.New(db, s.registry, people, config.Retention{})
	netPolicy := egress.NetworkConfig{GlobalBucket: "global",
		Routes: []egress.RouteConfig{{Profile: profile, Mode: egress.PreferProxy, AllowDirect: true,
			Paths: []egress.PathConfig{
				{ID: egress.Direct, Kind: egress.Direct, Region: "cn", Bucket: "line"},
				{ID: "edge", Kind: "socks5", Region: "intl", Bucket: "proxy"},
			}}},
	}
	for _, namespace := range namespaces {
		netPolicy.Sources = append(netPolicy.Sources, egress.SourceConfig{Capability: capability, Namespace: namespace,
			Profile: profile, ProviderBucket: "provider", AccountBucket: "account"})
	}
	for _, bucket := range []string{"global", "line", "provider", "account", "proxy"} {
		netPolicy.Quotas = append(netPolicy.Quotas, egress.QuotaConfig{ID: bucket, Burst: 64, PerMinute: 60, Concurrency: 1})
	}
	if err := s.engine.InitNetworkPolicy(t.Context(), netPolicy); err != nil {
		t.Fatal(err)
	}
	if err := s.engine.LoadNetworkPolicy(t.Context()); err != nil {
		t.Fatal(err)
	}
	app := fiber.New(fiber.Config{BodyLimit: execution.MaxBodySize, ErrorHandler: handlers.FiberError})
	routers.Register(app, nil, handlers.NewAPI(s.engine, people, s.log))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- app.Listener(listener, fiber.ListenConfig{DisableStartupMessage: true}) }()
	t.Cleanup(func() {
		if err := app.ShutdownWithTimeout(time.Second); err != nil {
			t.Error(err)
		}
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	s.cfg = config.Worker{ControlURL: "http://" + listener.Addr().String(), Token: s.agent.Token, Offers: []provider.Offer{offer.Offer},
		Concurrency: 1, EgressProfiles: []string{profile}, SteamEvidenceAppIDs: []int64{440},
		Egress: egress.Config{Profiles: []egress.ProfileConfig{{Name: profile, Mode: egress.PreferProxy, AllowDirect: true, DirectRegion: "cn"}}}}
	s.pool = egress.NewPool(s.cfg.Egress)
	t.Cleanup(s.pool.Close)
	s.api = newClient(s.cfg)
	original := s.api.http.Transport.(*http.Transport)
	t.Cleanup(original.CloseIdleConnections)
	s.api.http.Transport = evidenceRoundTrip(func(req *http.Request) (*http.Response, error) {
		started := time.Now()
		response, err := original.RoundTrip(req)
		switch {
		case strings.HasSuffix(req.URL.Path, "/evidence"):
			if response != nil && response.StatusCode == 204 && s.lost.CompareAndSwap(true, false) {
				response.Body.Close()
				return nil, io.ErrUnexpectedEOF
			}
			t.Logf("upload: %d bytes, %s", req.ContentLength, time.Since(started))
		case strings.HasSuffix(req.URL.Path, "/complete"):
			body, readErr := req.GetBody()
			if readErr != nil {
				t.Fatal(readErr)
			}
			payload, readErr := io.ReadAll(body)
			body.Close()
			if readErr != nil || json.Unmarshal(payload, &s.completion) != nil {
				t.Fatal("could not decode the Completion as sent on the wire")
			}
			t.Logf("completion: %s", time.Since(started))
		}
		return response, err
	})
	if err := s.engine.RegisterWorker(t.Context(), s.agent, execution.Registration{ID: s.workerID, Offers: s.cfg.Offers,
		EgressProfiles: s.cfg.EgressProfiles, Capacity: 1, Paths: s.pool.Advertise(1)}); err != nil {
		t.Fatal(err)
	}
	return s
}

type evidenceRoundTrip func(*http.Request) (*http.Response, error)

func (f evidenceRoundTrip) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func (s *steamEvidencePlane) steamFixture() *egress.FixtureNetwork {
	t := s.t
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{"store.steampowered.com"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests.Add(1)
		if r.Method != "GET" || r.Host != "store.steampowered.com" || r.URL.Path != "/api/appdetails" || r.URL.Query().Get("appids") != "440" {
			t.Error("unexpected Storefront request")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(*s.body.Load())
	}))
	upstream.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	upstream.StartTLS()
	t.Cleanup(upstream.Close)
	return &egress.FixtureNetwork{RootCAs: roots,
		Lookup: func(context.Context, string, string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
		},
		DialDirect: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, upstream.Listener.Addr().String())
		},
	}
}

func (s *steamEvidencePlane) claim(namespace, locale, country string) (execution.Claim, time.Time) {
	t := s.t
	t.Helper()
	ctx := t.Context()
	target, err := s.engine.RegisterTarget(ctx, s.writer, execution.TargetInput{Provider: "steam", ResourceType: "app", ExternalKey: "440", Namespace: namespace})
	if err != nil {
		t.Fatal(err)
	}
	req := execution.Request{TargetID: target.ID, Capability: "steam.details", RequestSchema: 1,
		Params: jsontext.Value(fmt.Sprintf(`{"language":%q,"store_country":%q}`, locale, country)), Scope: provider.Scope{EgressRegion: "cn"}}
	job, err := s.engine.SubmitJob(ctx, s.writer, uuid.NewV7().String(), execution.Submit{Request: &req})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	claim, err := s.engine.Claim(ctx, s.agent, s.workerID)
	if err != nil || claim == nil || claim.JobID != job.ID {
		t.Fatalf("claim: %+v %v", claim, err)
	}
	return *claim, started
}

func (s *steamEvidencePlane) sql(query string, args ...any) {
	s.t.Helper()
	if _, err := s.db.Exec(s.t.Context(), query, args...); err != nil {
		s.t.Fatal(err)
	}
}

func (s *steamEvidencePlane) count(query string, args ...any) int {
	s.t.Helper()
	var count int
	if err := s.db.QueryRow(s.t.Context(), query, args...).Scan(&count); err != nil {
		s.t.Fatal(err)
	}
	return count
}

func (s *steamEvidencePlane) recover() {
	s.sql("UPDATE harvest.steam_evidence_staging SET next_check_at = clock_timestamp()")
	s.engine.RecoverSteamEvidence(s.t.Context(), s.log)
}

func (s *steamEvidencePlane) run(claim execution.Claim, started time.Time, body []byte, enabled bool) {
	s.t.Helper()
	s.body.Store(&body)
	prior := s.requests.Load()
	cfg := s.cfg
	if !enabled {
		cfg.SteamEvidenceAppIDs = nil
	}
	execute(s.t.Context(), s.api, s.registry, s.pool, claim, started, cfg, s.log)
	if s.requests.Load() != prior+1 {
		s.t.Fatal("evidence processing must use exactly one Storefront request")
	}
	job, attempts, err := s.engine.Job(s.t.Context(), s.writer, claim.JobID)
	if err != nil || job.State != "partial" || len(attempts) != 1 || attempts[0].ReceiptState == nil {
		s.t.Fatalf("completion returned state %s, attempts %d, error %v", job.State, len(attempts), err)
	}
}

func evidenceFixture(size int, name, markup string) []byte {
	text := fmt.Sprintf("{\n  \"alias\": {\"success\":true,\"data\":{\"name\":%q, \"steam_appid\":440,\"detailed_description\":%q}}\n}\n", name, markup)
	return []byte(text + strings.Repeat(" ", size-len(text)))
}

func TestSteamEvidenceNative(t *testing.T) {
	s := newSteamEvidencePlane(t)
	const attributed = "SELECT count(*) FROM harvest.steam_evidence_attribution WHERE attempt_id=$1"
	const staged = "SELECT count(*) FROM harvest.steam_evidence_staging WHERE attempt_id=$1"
	const events = "SELECT count(*) FROM harvest.change_events"
	a := evidenceFixture(4<<10, "Fixture", "<b>Original A</b>")
	b := evidenceFixture(64<<10, "Fixture", "<b>Original B</b>")
	first, start := s.claim("evidence_one", "english", "US")
	s.lost.Store(true)
	s.run(first, start, a, true)
	if s.lost.Load() || s.count(staged, first.AttemptID) != 1 || s.count(attributed, first.AttemptID) != 0 {
		t.Fatal("lost upload response did not leave exactly one staged record")
	}
	var pending evidenceList
	s.evidenceGET(evidenceURL+"?namespace=evidence_one", s.operator.Token, 200, &pending)
	if len(pending.Items) != 0 {
		t.Fatal("staging appeared in the archive list")
	}
	s.evidenceGET(evidenceURL+"/"+first.AttemptID.String()+"/raw", s.operator.Token, 404, nil)
	s.sql(`CREATE FUNCTION reject_evidence_fixture() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'fixture archive failure'; END; $$;
CREATE TRIGGER reject_evidence_fixture BEFORE INSERT ON harvest.steam_evidence_attribution
FOR EACH ROW EXECUTE FUNCTION reject_evidence_fixture();`)
	s.recover()
	baseEvents := s.count(events)
	if baseEvents != 1 || s.count("SELECT count(*) FROM harvest.current_results") != 1 ||
		s.count(staged, first.AttemptID) != 1 || s.count("SELECT count(*) FROM harvest.steam_evidence") != 0 {
		t.Fatal("archive failure must roll back only evidence and preserve the Observation and its Change Event")
	}
	s.sql("DROP TRIGGER reject_evidence_fixture ON harvest.steam_evidence_attribution")
	s.recover()
	if s.count(attributed, first.AttemptID) != 1 || s.count(staged, first.AttemptID) != 0 {
		t.Fatal("archive recovery did not consume staging")
	}
	var replay execution.Receipt
	if _, err := s.api.request(t.Context(), http.MethodPost, "/attempts/"+first.AttemptID.String()+"/complete", s.completion, &replay); err != nil ||
		replay.AttemptID != first.AttemptID || replay.State != "partial" || s.count(attributed, first.AttemptID) != 1 || s.count(events) != baseEvents {
		t.Fatalf("Completion replay changed attribution or observations: %+v %v", replay, err)
	}
	var payload, digest []byte
	var firstID uuid.UUID
	var payloadBytes, storedBytes int
	var observed time.Time
	err := s.db.QueryRow(t.Context(), `SELECT id,payload,payload_sha256,payload_bytes,pg_column_size(payload),first_observed_at
FROM harvest.steam_evidence WHERE first_attempt_id=$1`, first.AttemptID).Scan(&firstID, &payload, &digest, &payloadBytes, &storedBytes, &observed)
	if err != nil || !bytes.Equal(payload, a) || sha256.Sum256(payload) != sha256.Sum256(a) || !bytes.Equal(digest, shaSlice(a)) || payloadBytes != len(a) || !execution.UUIDv7(firstID) {
		t.Fatalf("archived bytes, digest or identity differ: %v", err)
	}
	t.Logf("first payload: logical %d bytes, PostgreSQL column %d bytes", payloadBytes, storedBytes)

	for _, body := range [][]byte{b, a, a} {
		claim, start := s.claim("evidence_one", "english", "US")
		s.run(claim, start, body, true)
		s.recover()
		if s.count(attributed, claim.AttemptID) != 1 {
			t.Fatal("the repeated Job's Attempt is absent from attribution")
		}
	}
	if s.count(events) != baseEvents || s.count("SELECT count(*) FROM harvest.steam_evidence") != 2 ||
		s.count("SELECT count(*) FROM harvest.steam_evidence_attribution WHERE evidence_id=$1 AND observed_at >= $2", firstID, observed) != 3 ||
		s.count("SELECT count(*) FROM harvest.steam_evidence WHERE id=$1 AND last_confirmed_at > first_observed_at", firstID) != 1 {
		t.Fatal("A→B→A must re-confirm A without introducing raw or semantic duplicates")
	}
	legacy, start := s.claim("evidence_one", "english", "US")
	s.run(legacy, start, a, false)
	if s.count(staged, legacy.AttemptID)+s.count(attributed, legacy.AttemptID) != 0 {
		t.Fatal("non-enabled Worker staged evidence")
	}

	for _, tc := range []struct {
		namespace, lang, country, region string
		body                             []byte
	}{
		{"evidence_one", "schinese", "US", "cn", a},
		{"evidence_one", "english", "CN", "cn", a},
		{"evidence_one", "english", "HK", "cn", evidenceFixture(256<<10, "Fixture", "<p>large</p>")},
		{"evidence_two", "english", "US", "cn", evidenceFixture(provider.MaxSteamEvidenceBytes, "Fixture", "<p>boundary</p>")},
		{"evidence_one", "english", "US", "intl", a},
	} {
		claim, start := s.claim(tc.namespace, tc.lang, tc.country)
		s.run(claim, start, tc.body, true)
		if tc.region != "cn" {
			// Simulate a different pinned route for the archive's source-key check.
			s.sql("UPDATE harvest.attempts SET egress_region=$1 WHERE id=$2", tc.region, claim.AttemptID)
		}
		s.recover()
		locale := "en"
		if tc.lang == "schinese" {
			locale = "zh-cn"
		}
		if s.count(`SELECT count(*) FROM harvest.steam_evidence WHERE first_attempt_id=$1 AND namespace=$2
AND locale=$3 AND store_country=$4 AND egress_region=$5 AND payload_sha256=$6 AND payload_bytes=$7`,
			claim.AttemptID, tc.namespace, locale, tc.country, tc.region, shaSlice(tc.body), len(tc.body)) != 1 {
			t.Fatal("distinct canonical source scope was conflated")
		}
		t.Logf("scope %s/%s/%s/%s: %d logical bytes, %d PostgreSQL column bytes",
			tc.namespace, locale, tc.country, tc.region, len(tc.body),
			s.count("SELECT pg_column_size(payload) FROM harvest.steam_evidence WHERE first_attempt_id=$1", claim.AttemptID))
	}

	s.testEvidenceAccess(firstID, a)
	var oldID uuid.UUID
	if err := s.db.QueryRow(t.Context(), "SELECT id FROM harvest.steam_evidence WHERE namespace='evidence_one' AND payload_sha256=$1", shaSlice(b)).Scan(&oldID); err != nil {
		t.Fatal(err)
	}
	var versions []execution.Claim
	for i := range 4 {
		claim, start := s.claim("evidence_one", "english", "US")
		s.run(claim, start, evidenceFixture(4<<10, "Fixture", fmt.Sprintf("<p>version %d</p>", i)), true)
		versions = append(versions, claim)
	}
	s.recoverConcurrently(s.engine)
	for _, claim := range versions {
		if s.count(attributed, claim.AttemptID) != 1 || s.count(staged, claim.AttemptID) != 0 {
			t.Fatal("the sixth distinct body did not rotate in")
		}
	}
	if s.count("SELECT count(*) FROM harvest.steam_evidence WHERE namespace='evidence_one' AND locale='en' AND store_country='US' AND egress_region='cn'") != 5 ||
		s.count("SELECT count(*) FROM harvest.steam_evidence_attribution WHERE evidence_id=$1", oldID) != 0 {
		t.Fatal("rotation did not retain five versions and remove the oldest lineage")
	}
	s.evidenceGET(evidenceURL+"/"+oldID.String()+"/raw", s.operator.Token, 404, nil)
	s.testOutOfOrderArchive(a)
	s.testCompletionFailure(a)
	s.testTrackedRetention(firstID, a)
	s.testArchiveBudgets()
	s.testEvidenceRejections(a, b)
}

func shaSlice(body []byte) []byte {
	hash := sha256.Sum256(body)
	return hash[:]
}

func (s *steamEvidencePlane) testOutOfOrderArchive(body []byte) {
	t := s.t
	t.Helper()
	early, start := s.claim("evidence_two", "schinese", "CN")
	s.run(early, start, body, true)
	s.sql("UPDATE harvest.steam_evidence_staging SET next_check_at=clock_timestamp()+interval '1 hour' WHERE attempt_id=$1", early.AttemptID)
	late, start := s.claim("evidence_two", "schinese", "CN")
	s.run(late, start, body, true)
	s.sql("UPDATE harvest.steam_evidence_staging SET next_check_at=clock_timestamp() WHERE attempt_id=$1", late.AttemptID)
	s.engine.RecoverSteamEvidence(t.Context(), s.log)
	if s.count("SELECT count(*) FROM harvest.steam_evidence WHERE first_attempt_id=$1", late.AttemptID) != 1 {
		t.Fatal("the later upload was not archived first")
	}
	s.recover()
	if s.count(`SELECT count(*) FROM harvest.steam_evidence e
JOIN harvest.steam_evidence_attribution a ON a.evidence_id=e.id
WHERE e.first_attempt_id=$1 AND e.first_job_id=$2 AND a.attempt_id=$3
  AND e.last_confirmed_at=a.observed_at AND e.first_observed_at<e.last_confirmed_at`,
		early.AttemptID, early.JobID, late.AttemptID) != 1 {
		t.Fatal("out-of-order recovery did not preserve first and last acquisition times")
	}
}

func (s *steamEvidencePlane) testCompletionFailure(a []byte) {
	t := s.t
	t.Helper()
	claim, _ := s.claim("evidence_two", "schinese", "HK")
	base := "/attempts/" + claim.AttemptID.String()
	ctx := t.Context()
	if err := s.api.postEvidence(ctx, base+"/evidence", claim.Fence, provider.SteamEvidence{Body: a, ObservedAt: time.Now()}, sha256.Sum256(a)); err != nil {
		t.Fatal(err)
	}
	s.sql(`CREATE TRIGGER reject_observation_fixture BEFORE INSERT ON harvest.observations
FOR EACH ROW EXECUTE FUNCTION reject_evidence_fixture();`)
	defer s.sql("DROP TRIGGER reject_observation_fixture ON harvest.observations")
	complete := s.completion
	complete.Fence = claim.Fence
	for i := range complete.Result.Segments {
		complete.Result.Segments[i].Scope.Namespace = claim.Task.Namespace
		complete.Result.Segments[i].Scope.Scope = claim.Task.Scope
	}
	var receipt execution.Receipt
	_, err := s.api.request(ctx, http.MethodPost, base+"/complete", complete, &receipt)
	var response responseError
	if !errors.As(err, &response) || response.status != 503 {
		t.Fatalf("expected injected Completion transaction failure, got %+v (%v)", response, err)
	}
	s.recover()
	if s.count("SELECT count(*) FROM harvest.steam_evidence_attribution WHERE attempt_id=$1", claim.AttemptID) != 0 {
		t.Fatal("failed Completion was archived")
	}
	complete.Result = provider.Result{State: "failed", Code: "invalid_payload"}
	if _, err := s.api.request(ctx, http.MethodPost, base+"/complete", complete, &receipt); err != nil {
		t.Fatal(err)
	}
	s.recover()
	if s.count("SELECT count(*) FROM harvest.steam_evidence_staging WHERE attempt_id=$1", claim.AttemptID) != 0 {
		t.Fatal("terminal failed Attempt retained staged evidence")
	}
}

func (s *steamEvidencePlane) testEvidenceRejections(a, b []byte) {
	t := s.t
	t.Helper()
	ctx := t.Context()
	claim, _ := s.claim("evidence_one", "english", "US")
	path := "/attempts/" + claim.AttemptID.String() + "/evidence"
	check := func(api client, fence execution.Fence, body []byte, status int) {
		t.Helper()
		candidate := provider.SteamEvidence{Body: body, ObservedAt: time.Now().UTC()}
		err := api.postEvidence(ctx, path, fence, candidate, sha256.Sum256(body))
		var response responseError
		if status == 204 && err != nil || status != 204 && (!errors.As(err, &response) || response.status != status) {
			t.Fatalf("upload expected %d; got %+v (%v)", status, response, err)
		}
	}
	untrusted := s.api
	untrusted.token = ""
	check(untrusted, claim.Fence, a, 401)
	untrusted.token = s.writer.Token
	check(untrusted, claim.Fence, a, 403)
	untrusted.token = strings.Repeat("c", 40)
	check(untrusted, claim.Fence, a, 404)
	for _, fence := range []execution.Fence{
		{JobID: uuid.NewV7(), WorkerID: claim.WorkerID, Generation: claim.Generation},
		{JobID: claim.JobID, WorkerID: uuid.NewV7(), Generation: claim.Generation},
		{JobID: claim.JobID, WorkerID: claim.WorkerID, Generation: claim.Generation + 1},
	} {
		check(s.api, fence, a, 404)
	}
	s.testEvidenceIngressLimit(path)
	check(s.api, claim.Fence, bytes.Replace(a, []byte("440"), []byte("441"), 1), 400)
	check(s.api, claim.Fence, a, 204)
	check(s.api, claim.Fence, a, 204)
	check(s.api, claim.Fence, b, 409)
	s.recover() // Interrupted Worker, valid Attempt still running.
	if s.count("SELECT count(*) FROM harvest.steam_evidence_attribution WHERE attempt_id=$1", claim.AttemptID) != 0 {
		t.Fatal("running Attempt was archived")
	}
	s.sql("UPDATE harvest.attempts SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", claim.AttemptID)
	check(s.api, claim.Fence, a, 409)
	s.sql("UPDATE harvest.steam_evidence_staging SET uploaded_at=clock_timestamp()-interval '25 hours' WHERE attempt_id=$1", claim.AttemptID)
	s.recover()
	if s.count("SELECT count(*) FROM harvest.steam_evidence_staging WHERE attempt_id=$1", claim.AttemptID) != 0 {
		t.Fatal("expired staging was retained")
	}
}

func (s *steamEvidencePlane) testEvidenceIngressLimit(path string) {
	t := s.t
	t.Helper()
	address := strings.TrimPrefix(s.cfg.ControlURL, "http://")
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	// Send only the oversized Content-Length. The server must reject it
	// before reading the body, avoiding a client write/close race.
	if _, err := fmt.Fprintf(conn, "POST /internal/v1%s HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\n\r\n",
		path, address, provider.MaxSteamEvidenceBytes+1); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected HTTP 413 before upload, got %d", response.StatusCode)
	}
}
