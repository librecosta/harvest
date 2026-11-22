package worker

import (
	"bytes"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

const evidenceURL = "/api/v1/operations/steam-evidence"

type evidenceList struct {
	Items []q.ListSteamEvidenceRow `json:"items"`
	After *uuid.UUID               `json:"after"`
}

func (s *steamEvidencePlane) evidenceGET(path, token string, status int, out any) []byte {
	t := s.t
	t.Helper()
	base := strings.TrimSuffix(s.api.base, "/internal/v1")
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, base+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := s.api.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != status || response.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("%s expected %d and no-store; got %d", path, status, response.StatusCode)
	}
	if status != 200 {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 513<<10))
	if err != nil || out != nil && json.Unmarshal(body, out, json.RejectUnknownMembers(true)) != nil {
		t.Fatalf("invalid successful response for %s: %v", path, err)
	}
	if strings.HasSuffix(path, "/raw") && (response.Header.Get("Content-Type") != "application/octet-stream" ||
		response.Header.Get("X-Content-Type-Options") != "nosniff" || !strings.HasPrefix(response.Header.Get("Content-Disposition"), "attachment;")) {
		t.Fatal("raw response missing download headers")
	}
	return body
}

func (s *steamEvidencePlane) testEvidenceAccess(id uuid.UUID, payload []byte) {
	t := s.t
	t.Helper()
	raw := evidenceURL + "/" + id.String() + "/raw"
	operator := s.operator.Token
	var scoped evidenceList
	scope := evidenceURL + "?namespace=evidence_one&app_id=440&locale=en&store_country=US&egress_region=cn"
	s.evidenceGET(scope+"&evidence_id="+id.String(), operator, 200, &scoped)
	if len(scoped.Items) != 1 || scoped.Items[0].ID != id || scoped.Items[0].Sha256 != fmt.Sprintf("%x", shaSlice(payload)) {
		t.Fatal("scoped metadata did not describe the intended evidence")
	}
	if returned := s.evidenceGET(raw, operator, 200, nil); !bytes.Equal(returned, payload) {
		t.Fatal("operator raw download changed the original bytes")
	}
	var pageA, pageB evidenceList
	scope += "&target_id=" + scoped.Items[0].TargetID.String() + "&limit=1"
	s.evidenceGET(scope, operator, 200, &pageA)
	if len(pageA.Items) != 1 || pageA.After == nil {
		t.Fatal("first page has no cursor")
	}
	s.evidenceGET(scope+"&after="+pageA.After.String(), operator, 200, &pageB)
	if len(pageB.Items) != 1 || bytes.Compare(pageA.Items[0].ID[:], pageB.Items[0].ID[:]) >= 0 {
		t.Fatal("cursor did not advance in UUID order")
	}
	for _, token := range []string{s.agent.Token, s.writer.Token, strings.Repeat("g", 40)} {
		s.evidenceGET(raw, token, 403, nil)
		s.evidenceGET(scope, token, 403, nil)
	}
	s.evidenceGET(raw, strings.Repeat("f", 40), 404, nil)
	s.evidenceGET(evidenceURL+"?namespace=evidence_two", strings.Repeat("e", 40), 404, nil)
	s.evidenceGET(evidenceURL+"/storage", strings.Repeat("e", 40), 404, nil)
	s.evidenceGET(raw, operator, 200, nil)
	var other evidenceList
	s.evidenceGET(evidenceURL+"?namespace=evidence_two", operator, 200, &other)
	if len(other.Items) != 1 {
		t.Fatal("the second namespace's evidence is absent")
	}
	s.evidenceGET(evidenceURL+"/"+other.Items[0].ID.String()+"/raw", strings.Repeat("e", 40), 404, nil)
	s.evidenceGET(evidenceURL+"?namespace=evidence_one&limit=101", operator, 400, nil)
	var usage execution.SteamEvidenceStorageView
	s.evidenceGET(evidenceURL+"/storage?namespace=evidence_two", operator, 200, &usage)
	if usage.Versions != 1 || usage.LogicalBytes != provider.MaxSteamEvidenceBytes ||
		usage.PayloadColumnBytes >= usage.LogicalBytes || usage.Attributions != 1 || usage.StagingRows != 0 {
		t.Fatalf("incorrect usage for boundary fixture: %+v", usage)
	}
	s.evidenceGET(evidenceURL+"/storage", operator, 200, &usage)
	if usage.Versions < 2 || usage.BudgetBytes != config.DefaultSteamEvidence().GlobalBytes {
		t.Fatalf("incorrect global usage: %+v", usage)
	}
}

func (s *steamEvidencePlane) recoverConcurrently(engine *execution.Engine) {
	t := s.t
	t.Helper()
	s.sql("UPDATE harvest.steam_evidence_staging SET next_check_at = clock_timestamp()")
	ready := make(chan struct{})
	var wg sync.WaitGroup
	start := time.Now()
	for range 2 {
		wg.Go(func() {
			<-ready
			engine.RecoverSteamEvidence(t.Context(), s.log)
		})
	}
	pruned := make(chan error, 1)
	wg.Go(func() {
		<-ready
		_, err := engine.PruneSteamEvidence(t.Context())
		pruned <- err
	})
	close(ready)
	wg.Wait()
	if err := <-pruned; err != nil {
		t.Fatal(err)
	}
	t.Logf("two recovery passes with concurrent prune: %s", time.Since(start))
}

func (s *steamEvidencePlane) testTrackedRetention(firstID uuid.UUID, payload []byte) {
	t := s.t
	t.Helper()
	ctx := t.Context()
	newest, start := s.claim("evidence_one", "english", "US")
	s.run(newest, start, payload, true)
	s.recover()
	req := execution.Request{TargetID: newest.Task.TargetID, Capability: "steam.details", RequestSchema: 1,
		Params: jsontext.Value(`{"language":"english","store_country":"US"}`), Scope: provider.Scope{EgressRegion: "cn"}}
	schedule, err := s.engine.CreateSchedule(ctx, s.writer, execution.ScheduleInput{
		Request: req, Recurrence: execution.Recurrence{IntervalSeconds: 86400, AnchorAt: time.Now().UTC().Add(time.Hour), Policy: "state_refresh"},
		ExecutionPolicy: execution.ExecutionPolicy{Regions: []string{"cn", "intl"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	setEnabled := func(enabled bool) {
		t.Helper()
		updated, err := s.engine.EditSchedule(ctx, s.writer, schedule.ID, execution.SchedulePatch{Revision: schedule.Revision, Enabled: &enabled})
		if err != nil {
			t.Fatal(err)
		}
		schedule = updated
	}

	observations := s.count("SELECT count(*) FROM harvest.observations")
	changes := s.count("SELECT count(*) FROM harvest.change_events")
	enrollment := execution.ConsumerEnrollment{Stream: "evidence_one", Name: "evidence_fixture", Principal: "reader"}
	consumer, err := s.engine.EnrollConsumer(ctx, s.operator, enrollment)
	if err != nil {
		t.Fatal(err)
	}
	s.sql(`UPDATE harvest.steam_evidence SET last_server_confirmed_at=last_server_confirmed_at-interval '91 days';
UPDATE harvest.steam_evidence_attribution SET confirmed_at=confirmed_at-interval '91 days'`)
	setEnabled(false)
	setEnabled(true)
	start = time.Now()
	pruned, err := s.engine.PruneSteamEvidence(ctx)
	if err != nil || pruned.VersionsRemoved == 0 || pruned.AttributionsRemoved == 0 {
		t.Fatalf("age pruning did not remove eligible versions and intermediate attributions: %+v %v", pruned, err)
	}
	t.Logf("age pruning with schedules: %s, %+v", time.Since(start), pruned)
	if s.count("SELECT count(*) FROM harvest.steam_evidence") != 2 ||
		s.count("SELECT count(*) FROM harvest.steam_evidence WHERE id=$1 AND locale='en' AND store_country='US'", firstID) != 1 ||
		s.count("SELECT count(*) FROM harvest.steam_evidence WHERE egress_region='intl' AND locale='en' AND store_country='US'") != 1 ||
		s.count("SELECT count(*) FROM harvest.steam_evidence_attribution WHERE evidence_id=$1", firstID) != 2 {
		t.Fatal("wrong protection for market/locale/egress or first/last attribution")
	}
	if s.count("SELECT count(*) FROM harvest.observations") != observations ||
		s.count("SELECT count(*) FROM harvest.change_events") != changes {
		t.Fatal("evidence pruning altered existing Observation or Change Stream state")
	}
	if after, err := s.engine.EnrollConsumer(ctx, s.operator, enrollment); err != nil || after != consumer {
		t.Fatalf("evidence prune changed the Consumer cursor: %+v %v", after, err)
	}

	setEnabled(false)
	restarted := execution.New(s.db, s.registry, []config.Principal{s.agent}, config.Retention{})
	if err := restarted.LoadNetworkPolicy(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.PruneSteamEvidence(ctx); err != nil {
		t.Fatal(err)
	}
	if s.count("SELECT count(*) FROM harvest.steam_evidence") != 0 ||
		s.count("SELECT count(*) FROM harvest.steam_evidence_attribution") != 0 {
		t.Fatal("a disabled Schedule protected unsupported evidence")
	}

	setEnabled(true)
	claim, start := s.claim("evidence_one", "english", "US")
	s.run(claim, start, payload, true)
	s.recover()
	observations = s.count("SELECT count(*) FROM harvest.observations")
	changes = s.count("SELECT count(*) FROM harvest.change_events")
	s.sql("UPDATE harvest.steam_evidence SET last_server_confirmed_at=last_server_confirmed_at-interval '91 days'")
	target, err := s.engine.Target(ctx, s.writer, claim.Task.TargetID)
	if err != nil {
		t.Fatal(err)
	}
	target, err = s.engine.EditTarget(ctx, s.writer, target.ID, target.Revision, "archived")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.PruneSteamEvidence(ctx); err != nil {
		t.Fatal(err)
	}
	versionsAfter := s.count("SELECT count(*) FROM harvest.steam_evidence")
	observationsAfter := s.count("SELECT count(*) FROM harvest.observations")
	changesAfter := s.count("SELECT count(*) FROM harvest.change_events")
	if versionsAfter != 0 || observationsAfter != observations || changesAfter != changes {
		t.Fatalf("archived Target prune: versions=%d, observations=%d→%d, changes=%d→%d",
			versionsAfter, observations, observationsAfter, changes, changesAfter)
	}
	if _, err := s.engine.EditTarget(ctx, s.writer, target.ID, target.Revision, "active"); err != nil {
		t.Fatal(err)
	}
	setEnabled(false)
}

func (s *steamEvidencePlane) testArchiveBudgets() {
	t := s.t
	t.Helper()
	ctx := t.Context()
	limits := config.DefaultSteamEvidence()
	limits.NamespaceBytes, limits.GlobalBytes = 512<<10, 768<<10
	limited := func(c config.SteamEvidence) *execution.Engine {
		t.Helper()
		e := execution.NewWithSteamEvidence(s.db, s.registry, []config.Principal{s.agent}, config.Retention{}, c)
		if err := e.LoadNetworkPolicy(ctx); err != nil {
			t.Fatal(err)
		}
		return e
	}
	engine := limited(limits)
	const medium = 256 << 10
	for _, ns := range []string{"evidence_one", "evidence_two"} {
		claim, start := s.claim(ns, "english", "US")
		s.run(claim, start, evidenceFixture(medium, "Fixture", "<p>base</p>"), true)
		s.recoverConcurrently(engine)
	}
	// Two distinct namespaces fit independently; only one fits globally.
	for _, ns := range []string{"evidence_one", "evidence_two"} {
		claim, start := s.claim(ns, "english", "CN")
		s.run(claim, start, evidenceFixture(medium, "Fixture", "<p>at capacity</p>"), true)
	}
	s.recoverConcurrently(engine)
	var winner string
	if err := s.db.QueryRow(ctx, "SELECT namespace FROM harvest.steam_evidence_usage WHERE logical_bytes=$1", limits.NamespaceBytes).Scan(&winner); err != nil {
		t.Fatal(err)
	}
	if s.count("SELECT sum(logical_bytes) FROM harvest.steam_evidence_usage") != int(limits.GlobalBytes) ||
		s.count("SELECT count(*) FROM harvest.steam_evidence_staging WHERE capacity_limited") != 1 ||
		s.count("SELECT sum(capacity_limited_attempts) FROM harvest.steam_evidence_usage") != 1 {
		t.Fatal("concurrent global budget did not account for exactly one rejected archive")
	}
	confirm, started := s.claim(winner, "english", "CN")
	s.run(confirm, started, evidenceFixture(medium, "Fixture", "<p>at capacity</p>"), true)
	s.recoverConcurrently(engine)
	if s.count("SELECT count(*) FROM harvest.steam_evidence_attribution WHERE attempt_id=$1", confirm.AttemptID) != 1 ||
		s.count("SELECT sum(logical_bytes) FROM harvest.steam_evidence_usage") != int(limits.GlobalBytes) ||
		s.count("SELECT sum(capacity_limited_attempts) FROM harvest.steam_evidence_usage") != 1 {
		t.Fatal("confirmation at capacity must reuse the existing payload")
	}
	s.measureEvidenceLockWait(engine)
	wide := limits
	wide.GlobalBytes = 1 << 20
	claim, start := s.claim(winner, "schinese", "HK")
	s.run(claim, start, evidenceFixture(4096, "Fixture", "<p>namespace exceeds</p>"), true)
	s.recoverConcurrently(limited(wide))
	if s.count("SELECT count(*) FROM harvest.steam_evidence_staging WHERE attempt_id=$1 AND capacity_limited", claim.AttemptID) != 1 {
		t.Fatal("namespace limit was bypassed with global capacity available")
	}
	down := limits
	down.GlobalBytes = down.NamespaceBytes
	report, err := limited(down).PruneSteamEvidence(ctx)
	if err != nil || !report.OverBudget || report.VersionsRemoved != 0 {
		t.Fatalf("reduced budget must report pressure without deleting recent evidence: %+v %v", report, err)
	}
	s.sql("UPDATE harvest.steam_evidence SET last_server_confirmed_at=last_server_confirmed_at-interval '91 days'")
	s.recoverConcurrently(engine)
	if s.count("SELECT count(*) FROM harvest.steam_evidence_staging WHERE capacity_limited") != 0 ||
		s.count("SELECT count(*) FROM harvest.steam_evidence_attribution WHERE attempt_id=$1", claim.AttemptID) != 1 ||
		s.count("SELECT sum(capacity_limited_attempts) FROM harvest.steam_evidence_usage") != 2 {
		t.Fatal("capacity did not recover or rejection metric was double counted")
	}
}

func (s *steamEvidencePlane) measureEvidenceLockWait(engine *execution.Engine) {
	t := s.t
	t.Helper()
	ctx := t.Context()
	lock, err := s.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	if _, err := lock.Exec(ctx, "SELECT singleton FROM harvest.steam_evidence_maintenance FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		_, err := engine.PruneSteamEvidence(ctx)
		done <- err
	}()
	for s.count(`SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()
AND wait_event_type='Lock' AND query LIKE '%LockSteamEvidenceMaintenance%'`) == 0 {
		if time.Since(start) > time.Second {
			t.Fatal("prune did not block on the PostgreSQL maintenance lock")
		}
		time.Sleep(time.Millisecond)
	}
	readStart := time.Now()
	s.evidenceGET("/api/v1/observations/current?stream=evidence_one&limit=10", strings.Repeat("g", 40), 200, nil)
	t.Logf("Reader current results during evidence lock: %s", time.Since(readStart))
	// Measure a known lock holder window instead of claiming a production estimate.
	waitStart := time.Now()
	time.Sleep(10 * time.Millisecond)
	if err := lock.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	t.Logf("prune with PostgreSQL lock contention: %s total, %s intentional hold", time.Since(start), time.Since(waitStart))
}
