package worker

import (
	"bytes"
	"crypto/sha256"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"net/http"
	"testing"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

const versionedMediaFixture = `{"alias":{"success":true,"data":{
 "name":"Fixture","steam_appid":440,
 "header_image":"https://cdn.steamstatic.com/header.jpg",
 "movies":[{"id":9007199254740993,"name":"Fixture Trailer",
  "hls_h264":"https://cdn.steamcontent.com/a.m3u8","dash_h264":"https://cdn.steamcontent.com/a.mpd"}],
 "screenshots":[{"id":9,"path_full":"https://cdn.steamstatic.com/z.jpg","path_thumbnail":"https://cdn.steamstatic.com/t.jpg"}]
}}}`

func TestSteamVersionedOffersNative(t *testing.T) {
	const legacy, pilot = "evidence_one", "evidence_two"
	s := newSteamEvidencePlane(t, pilot)
	ctx := t.Context()
	v1 := s.cfg.Offers[0]
	v2, ok := s.registry.CapabilityByKey(provider.SteamMediaV2Key)
	if !ok || v2.Observation.MaxSegmentBytes != 32<<10 {
		t.Fatal("Steam v2 Offer has no compiled media budget")
	}
	var advertised []provider.Description
	s.evidenceGET("/api/v1/providers", s.writer.Token, 200, &advertised)
	seen := map[provider.Offer]bool{}
	for _, description := range advertised {
		for _, cap := range description.Capabilities {
			seen[cap.Offer] = true
		}
	}
	if !seen[v1] || !seen[v2.Offer] {
		t.Fatal("provider discovery omitted a Steam details Offer")
	}
	bad := v2.Offer
	bad.Implementation = "incorrect"
	reg := execution.Registration{ID: uuid.NewV7(), Offers: []provider.Offer{v1, bad}, Capacity: 1,
		EgressProfiles: s.cfg.EgressProfiles, Paths: s.pool.Advertise(1)}
	if err := s.engine.RegisterWorker(ctx, s.agent, reg); !errors.Is(err, execution.ErrUnsupported) {
		t.Fatalf("uncompiled Offer registered: %v", err)
	}
	reg.Offers = []provider.Offer{v1, v2.Offer}
	s.cfg.Offers = reg.Offers
	if err := Validate(s.cfg, s.registry); err != nil {
		t.Fatal(err)
	}
	if _, err := s.api.request(ctx, http.MethodPost, "/workers/register", reg, nil); err != nil {
		t.Fatal(err)
	}
	initiated := time.Now()
	legacyReader := config.Principal{Name: "legacy_reader", Role: "reader", Namespaces: []string{legacy}}
	enrollment, err := s.engine.EnrollConsumer(ctx, s.operator, execution.ConsumerEnrollment{
		Stream: legacy, Name: "legacy", Principal: legacyReader.Name,
	})
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := s.engine.StartReset(ctx, legacyReader, enrollment.ID, legacy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.engine.Acknowledge(ctx, legacyReader, enrollment.ID, execution.ConsumerAck{
		Stream: legacy, Cursor: baseline.Baseline, Reset: &baseline.Reset,
	}); err != nil {
		t.Fatal(err)
	}
	legacyClaim, began := s.claim(legacy, "english", "US")
	s.run(legacyClaim, began, []byte(versionedMediaFixture), true)
	s.recover()
	legacyPage := execution.ChangesPage{ResultPage: execution.ResultPage{Stream: legacy, Limit: 16, After: baseline.Baseline}, Consumer: enrollment.ID}
	legacyBefore, err := s.engine.Changes(ctx, legacyReader, legacyPage)
	if err != nil || len(legacyBefore.Items) != 2 {
		t.Fatalf("legacy baseline Change Stream: %v", err)
	}
	for _, event := range legacyBefore.Items {
		if event.OutputSchema != 1 {
			t.Fatal("legacy fixture emitted an unexpected schema")
		}
	}

	target, err := s.engine.RegisterTarget(ctx, s.writer, execution.TargetInput{Provider: "steam", ResourceType: "app", ExternalKey: "440", Namespace: pilot})
	if err != nil {
		t.Fatal(err)
	}
	req := func(id uuid.UUID, version *int32) execution.Request {
		return execution.Request{TargetID: id, Capability: "steam.details", RequestSchema: 1, OutputSchema: version,
			Params: []byte(`{"language":"english","store_country":"US"}`), Scope: provider.Scope{EgressRegion: "cn"}}
	}
	one, two, wrong := int32(1), int32(2), int32(4)
	submit := func(key string, request execution.Request) (q.HarvestJob, error) {
		t.Helper()
		return s.engine.SubmitJob(ctx, s.writer, key, execution.Submit{Request: &request})
	}
	submitted := func(key string, request execution.Request) q.HarvestJob {
		t.Helper()
		job, err := submit(key, request)
		if err != nil {
			t.Fatal(err)
		}
		return job
	}
	if _, err := submit("not-pilot", req(legacyClaim.Task.TargetID, &two)); !errors.Is(err, execution.ErrUnsupported) {
		t.Fatalf("expected protected legacy namespace, got %v", err)
	}
	if _, err := submit("unknown-schema", req(target.ID, &wrong)); !errors.Is(err, execution.ErrInvalid) {
		t.Fatalf("uncompiled Output Schema was accepted: %v", err)
	}
	old := req(target.ID, nil)
	next := req(target.ID, &two)
	cadence := execution.Recurrence{IntervalSeconds: 3600, AnchorAt: time.Now().UTC().Add(time.Hour), Policy: "state_refresh"}
	schedule1, err := s.engine.CreateSchedule(ctx, s.writer, execution.ScheduleInput{Request: old, Recurrence: cadence})
	if err != nil || schedule1.ExecutionKey != v1.Key() {
		t.Fatalf("default schedule changed version: %v", err)
	}
	schedule2, err := s.engine.CreateSchedule(ctx, s.writer, execution.ScheduleInput{Request: next, Recurrence: cadence})
	if err != nil || schedule2.ExecutionKey != v2.Key() {
		t.Fatalf("explicit v2 schedule failed: %v", err)
	}
	edited, err := s.engine.EditSchedule(ctx, s.writer, schedule2.ID, execution.SchedulePatch{Revision: schedule2.Revision, Request: &old})
	if err != nil || edited.ExecutionKey != v2.Key() || !bytes.Equal(edited.Fingerprint, schedule2.Fingerprint) {
		t.Fatalf("versionless schedule edit replaced its pinned Offer: %v", err)
	}
	var scheduleWire struct {
		Request execution.Request `json:"request"`
	}
	if raw := s.evidenceGET("/api/v1/schedules/"+edited.ID.String(), s.writer.Token, 200, nil); json.Unmarshal(raw, &scheduleWire) != nil ||
		scheduleWire.Request.OutputSchema == nil || *scheduleWire.Request.OutputSchema != 2 {
		t.Fatal("Schedule readback does not expose its selected output schema")
	}

	pilotJob := submitted("pilot-v2", next)
	if duplicate := submitted("pilot-v2", next); duplicate.ID != pilotJob.ID {
		t.Fatal("v2 submission replay did not find the original Job")
	}
	if claim, err := s.engine.Claim(ctx, s.agent, s.workerID); claim != nil || err != nil {
		t.Fatalf("v1-only Worker claimed a v2 Job: %v", err)
	}
	legacyJob := submitted("default-v1", old)
	if explicit := submitted("default-v1", req(target.ID, &one)); explicit.ID != legacyJob.ID || !bytes.Equal(legacyJob.Fingerprint, schedule1.Fingerprint) ||
		bytes.Equal(legacyJob.Fingerprint, pilotJob.Fingerprint) {
		t.Fatal("explicit or implicit v1 fingerprint diverged or collided with v2")
	}
	historicalWire, err := json.Marshal(struct {
		TargetID           uuid.UUID      `json:"target_id"`
		Capability         string         `json:"capability"`
		RequestSchema      int32          `json:"request_schema"`
		Params             jsontext.Value `json:"params"`
		Scope              provider.Scope `json:"scope"`
		Namespace, Version string
	}{target.ID, old.Capability, old.RequestSchema, old.Params, provider.Scope{Locale: "en", Region: "us", EgressRegion: "cn"}, pilot, v1.Key()}, json.Deterministic(true))
	historical := sha256.Sum256(historicalWire)
	if err != nil || !bytes.Equal(legacyJob.Fingerprint, historical[:]) {
		t.Fatal("v1 request fingerprint changed from the R0-B encoding")
	}

	claimVia := func(engine *execution.Engine, workerID, jobID uuid.UUID, offer provider.Offer) (execution.Claim, time.Time) {
		t.Helper()
		start := time.Now()
		claim, err := engine.Claim(ctx, s.agent, workerID)
		if err != nil || claim == nil || claim.JobID != jobID || claim.Task.Offer != offer {
			t.Fatalf("Job claim does not have the expected Offer: %v", err)
		}
		return *claim, start
	}
	first, firstStart := claimVia(s.engine, s.workerID, legacyJob.ID, v1)
	second, secondStart := claimVia(s.engine, reg.ID, pilotJob.ID, v2.Offer)
	if s.count("SELECT count(*) FROM harvest.attempts WHERE state='running'") != 2 {
		t.Fatal("v1 and v2 Attempts did not coexist")
	}
	if _, err := s.engine.Complete(ctx, s.agent, second.AttemptID, execution.Completion{Fence: second.Fence, OutputSchema: 1}); !errors.Is(err, execution.ErrUnsupported) {
		t.Fatalf("different schema completed the v2 Attempt: %v", err)
	}
	stale := second.Fence
	stale.Generation++
	if _, err := s.engine.Renew(ctx, s.agent, second.AttemptID, stale); err == nil {
		t.Fatal("v2 Attempt renewed with the wrong generation")
	}
	restarted := execution.NewConfigured(s.db, s.registry, config.Config{Principals: s.principals, SteamMediaV2Namespaces: []string{pilot}})
	if err := restarted.LoadNetworkPolicy(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Renew(ctx, s.agent, second.AttemptID, second.Fence); err != nil {
		t.Fatalf("restarted Server lost valid v2 Attempt: %v", err)
	}

	body := []byte(versionedMediaFixture)
	s.run(first, firstStart, body, true)
	s.run(second, secondStart, body, true)
	if s.completion.OutputSchema != 2 {
		t.Fatal("Worker used default Offer when executing v2 Claim")
	}
	replayStart := time.Now()
	if receipt, err := restarted.Complete(ctx, s.agent, second.AttemptID, s.completion); err != nil || receipt.State != "partial" {
		t.Fatalf("new Server could not replay v2 Completion: %v", err)
	}
	t.Logf("v2 Completion receipt replay=%s", time.Since(replayStart))
	s.sql(`CREATE FUNCTION reject_v2_archive_fixture() RETURNS trigger LANGUAGE plpgsql AS $$
	BEGIN RAISE EXCEPTION 'fixture archive failure'; END; $$;
	CREATE TRIGGER reject_v2_archive_fixture BEFORE INSERT ON harvest.steam_evidence_attribution
	FOR EACH ROW EXECUTE FUNCTION reject_v2_archive_fixture();`)
	s.recover()
	if s.count("SELECT count(*) FROM harvest.steam_evidence_staging WHERE attempt_id=$1", second.AttemptID) != 1 ||
		s.count("SELECT count(*) FROM harvest.observations WHERE attempt_id=$1 AND output_schema=2 AND state='present'", second.AttemptID) != 2 ||
		s.count("SELECT count(*) FROM harvest.change_events WHERE stream=$1 AND output_schema=2", pilot) != 2 {
		t.Fatal("archive failure altered accepted v2 results or discarded staging")
	}
	s.sql("DROP TRIGGER reject_v2_archive_fixture ON harvest.steam_evidence_attribution")
	s.recover()

	assertMedia := func(engine *execution.Engine, ns string, count int) {
		t.Helper()
		page, err := engine.CurrentResults(ctx, s.writer, execution.ResultPage{Stream: ns, Limit: 16})
		if err != nil || len(page.Items) != count {
			t.Fatalf("current-result dimensions in %s: %d %v", ns, len(page.Items), err)
		}
		for _, item := range page.Items {
			var scope provider.SegmentScope
			if err := json.Unmarshal(item.Scope, &scope); err != nil {
				t.Fatal(err)
			}
			if scope.Dimensions["section"] != "media" {
				continue
			}
			switch item.OutputSchema {
			case 1:
				var media struct {
					Movies []string `json:"movies"`
				}
				if err := json.Unmarshal(item.Data, &media); err != nil || len(media.Movies) != 1 {
					t.Fatal("existing v1 media type changed")
				}
			case 2:
				var media struct {
					Movies []struct {
						ID string `json:"movie_id"`
					} `json:"movies"`
				}
				if err := json.Unmarshal(item.Data, &media); err != nil || len(media.Movies) != 1 || media.Movies[0].ID != "9007199254740993" {
					t.Fatal("v2 media identity was not preserved through PostgreSQL")
				}
			default:
				t.Fatal("unexpected Output Schema")
			}
		}
	}
	assertMedia(restarted, pilot, 4)
	assertMedia(restarted, legacy, 2)
	digest := sha256.Sum256(body)
	if s.count("SELECT count(*) FROM harvest.steam_evidence WHERE namespace=$1 AND payload_sha256=$2 AND payload=$3", pilot, digest[:], body) != 1 ||
		s.count("SELECT count(*) FROM harvest.steam_evidence_attribution WHERE attempt_id IN ($1, $2)", first.AttemptID, second.AttemptID) != 2 {
		t.Fatal("v1 and v2 did not share exact archived evidence and distinct attribution")
	}

	for _, version := range []int32{one, two} {
		offer := v1
		if version == two {
			offer = v2.Offer
		}
		for i, input := range [][]byte{append(bytes.Clone(body), '\n'), bytes.ReplaceAll(body, []byte("/a.m3u8"), []byte("/b.m3u8"))} {
			job := submitted(uuid.NewV7().String(), req(target.ID, &version))
			claimed, began := claimVia(restarted, reg.ID, job.ID, offer)
			before := s.count("SELECT count(*) FROM harvest.change_events WHERE stream=$1", pilot)
			s.run(claimed, began, input, true)
			t.Logf("schema=%d changed=%t worker HTTP elapsed=%s", version, i == 1, time.Since(began))
			if got := s.count("SELECT count(*) FROM harvest.change_events WHERE stream=$1", pilot); got != before+i {
				t.Fatalf("schema %d emitted %d events, expected %d after media confirmation/change", version, got-before, i)
			}
			s.recover()
		}
	}

	if _, err := restarted.Changes(ctx, legacyReader, execution.ChangesPage{
		ResultPage: execution.ResultPage{Stream: pilot, Limit: 16}, Consumer: enrollment.ID,
	}); !errors.Is(err, execution.ErrForbidden) {
		t.Fatalf("legacy Consumer could read the pilot stream: %v", err)
	}
	legacyAfter, err := restarted.Changes(ctx, legacyReader, legacyPage)
	if err != nil || len(legacyAfter.Items) != len(legacyBefore.Items) {
		t.Fatal("v2 pilot changed the legacy Consumer stream")
	}
	for i := range legacyBefore.Items {
		if legacyAfter.Items[i].ID != legacyBefore.Items[i].ID || legacyAfter.Items[i].Position != legacyBefore.Items[i].Position {
			t.Fatal("legacy Consumer cursor changed during the v2 pilot")
		}
	}

	s.sql(`UPDATE harvest.schedules SET anchor_at=statement_timestamp()-interval '1 second',
		next_due_at=statement_timestamp()-interval '1 second' WHERE id IN ($1,$2)`, schedule1.ID, schedule2.ID)
	disabled := execution.New(s.db, s.registry, s.principals, config.Retention{})
	if err := disabled.LoadNetworkPolicy(ctx); err != nil {
		t.Fatal(err)
	}
	if err := disabled.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if s.count("SELECT count(*) FROM harvest.jobs WHERE schedule_id=$1 AND cause='scheduled'", schedule1.ID) != 1 ||
		s.count("SELECT count(*) FROM harvest.jobs WHERE schedule_id=$1 AND cause='scheduled'", schedule2.ID) != 0 {
		t.Fatal("disabling pilot interrupted legacy scheduling or dispatched a v2 recurrence")
	}
	if err := restarted.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if s.count("SELECT count(*) FROM harvest.jobs WHERE schedule_id=$1 AND execution_key=$2 AND cause='scheduled'", schedule2.ID, provider.SteamMediaV2Key) != 1 {
		t.Fatal("restarted Server lost the stored v2 Schedule binding")
	}
	var timedV2 uuid.UUID
	if err := s.db.QueryRow(ctx, "SELECT id FROM harvest.jobs WHERE schedule_id=$1", schedule2.ID).Scan(&timedV2); err != nil {
		t.Fatal(err)
	}
	// v1-only Worker claims the v1 due Job and cannot compete for v2.
	var timedV1 uuid.UUID
	if err := s.db.QueryRow(ctx, "SELECT id FROM harvest.jobs WHERE schedule_id=$1", schedule1.ID).Scan(&timedV1); err != nil {
		t.Fatal(err)
	}
	claimVia(restarted, s.workerID, timedV1, v1)
	leased, _ := claimVia(restarted, reg.ID, timedV2, v2.Offer)
	s.sql("UPDATE harvest.attempts SET claimed_at=claimed_at-interval '30 seconds', heartbeat_at=heartbeat_at-interval '30 seconds', lease_until=lease_until-interval '30 seconds' WHERE id=$1", leased.AttemptID)
	if _, err := restarted.Renew(ctx, s.agent, leased.AttemptID, leased.Fence); !errors.Is(err, execution.ErrFence) {
		t.Fatalf("expired v2 lease was renewed: %v", err)
	}
	if err := restarted.Reconcile(ctx); err != nil ||
		s.count("SELECT count(*) FROM harvest.attempts WHERE id=$1 AND state='expired'", leased.AttemptID) != 1 ||
		s.count("SELECT count(*) FROM harvest.jobs WHERE id=$1 AND state='retry_wait'", timedV2) != 1 {
		t.Fatalf("restart did not recover the expired v2 lease: %v", err)
	}
	s.sql("UPDATE harvest.schedules SET enabled=false WHERE id=$1", schedule1.ID)
	s.sql("UPDATE harvest.steam_evidence SET last_server_confirmed_at=last_server_confirmed_at-interval '91 days' WHERE namespace=$1", pilot)
	if _, err := restarted.PruneSteamEvidence(ctx); err != nil || s.count("SELECT count(*) FROM harvest.steam_evidence WHERE namespace=$1", pilot) != 1 {
		t.Fatalf("active v2 Schedule did not protect its newest shared evidence: %v", err)
	}
	t.Logf("versioned data plane checks=%s", time.Since(initiated))
}
