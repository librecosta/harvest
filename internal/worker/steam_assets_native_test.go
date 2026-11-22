package worker

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

func TestSteamAssetsNative(t *testing.T) {
	const pilot, legacy = "evidence_two", "evidence_one"
	s := newSteamTestPlane(t, nil, []string{pilot})
	ctx := t.Context()
	cap, ok := s.registry.CapabilityByKey(provider.SteamAssetsKey)
	if !ok || cap.Network.MaxRequests != 1 || cap.MinInterval != 6*time.Hour {
		t.Fatal("independent StoreBrowse protocol is missing")
	}
	target := func(namespace string) uuid.UUID {
		t.Helper()
		row, err := s.engine.RegisterTarget(ctx, s.writer, execution.TargetInput{
			Namespace: namespace, Provider: "steam", ResourceType: "app", ExternalKey: "440",
		})
		if err != nil {
			t.Fatal(err)
		}
		return row.ID
	}
	app, oldApp := target(pilot), target(legacy)
	request := func(id uuid.UUID, lang, market, egressRegion string) execution.Request {
		return execution.Request{TargetID: id, Capability: provider.SteamAssets, RequestSchema: 1,
			Params: []byte(fmt.Sprintf(`{"language":%q,"store_country":%q}`, lang, market)), Scope: provider.Scope{EgressRegion: egressRegion}}
	}
	main := request(app, "english", "US", "cn")
	if _, err := s.engine.SubmitJob(ctx, s.writer, "legacy-denied", execution.Submit{Request: new(request(oldApp, "en", "US", "cn"))}); !errors.Is(err, execution.ErrUnsupported) {
		t.Fatalf("legacy namespace accepted StoreBrowse: %v", err)
	}
	pilotDisabled := execution.New(s.db, s.registry, s.principals, config.Retention{})
	if err := pilotDisabled.LoadNetworkPolicy(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pilotDisabled.SubmitJob(ctx, s.writer, "preview-denied", execution.Submit{Request: &main}); !errors.Is(err, execution.ErrUnsupported) {
		t.Fatalf("default StoreBrowse switch accepted a Job: %v", err)
	}
	scheduleInput := execution.ScheduleInput{Request: main, Recurrence: execution.Recurrence{IntervalSeconds: 21599,
		AnchorAt: time.Now().UTC().Add(time.Hour), Policy: "state_refresh"}, Enabled: new(false)}
	if _, err := s.engine.CreateSchedule(ctx, s.writer, scheduleInput); !errors.Is(err, execution.ErrInvalid) {
		t.Fatalf("short StoreBrowse cadence accepted: %v", err)
	}
	scheduleInput.Recurrence.IntervalSeconds = 21600
	schedule, err := s.engine.CreateSchedule(ctx, s.writer, scheduleInput)
	if err != nil || schedule.ExecutionKey != provider.SteamAssetsKey {
		t.Fatalf("explicit schedule: %v", err)
	}

	offers := append([]provider.Offer{}, s.cfg.Offers...)
	offers = append(offers, cap.Offer)
	register := func(cfg config.Worker, pool *egress.Pool) uuid.UUID {
		t.Helper()
		id := uuid.NewV7()
		if err := Validate(cfg, s.registry); err != nil {
			t.Fatal(err)
		}
		_, err := s.api.request(ctx, http.MethodPost, "/workers/register", execution.Registration{ID: id, Offers: cfg.Offers,
			Capacity: 1, Paths: pool.Advertise(1), EgressProfiles: cfg.EgressProfiles}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	workerCfg := s.cfg
	workerCfg.Offers = offers
	wid := register(workerCfg, s.pool)
	submit := func(req execution.Request) q.HarvestJob {
		t.Helper()
		job, err := s.engine.SubmitJob(ctx, s.writer, uuid.NewV7().String(), execution.Submit{Request: &req})
		if err != nil {
			t.Fatal(err)
		}
		return job
	}
	pull := func(id uuid.UUID, job q.HarvestJob) execution.Claim {
		t.Helper()
		claimed, err := s.engine.Claim(ctx, s.agent, id)
		if err != nil || claimed == nil || claimed.JobID != job.ID || claimed.Task.Offer != cap.Offer {
			t.Fatalf("StoreBrowse claim: %v", err)
		}
		return *claimed
	}
	baseBody := []byte(`{"response":{"store_items":[{"item_type":0,"id":440,"appid":440,"success":1,"visible":true,"assets":{
	 "asset_url_format":"steam/apps/440/${FILENAME}?t=1",
	 "library_capsule":"capsule.jpg","library_hero":"hero.jpg","logo":"library_logo.png",
	 "header":"header.jpg","community_icon":"0508d712ca859e3ef0921a32c49088ccfb05b0a0"
	}}]}}`)
	run := func(claim execution.Claim, cfg config.Worker, pool *egress.Pool, body []byte, status int32, jobState string) {
		t.Helper()
		s.body.Store(&body)
		s.browseStatus.Store(status)
		before := s.requests.Load()
		execute(ctx, s.api, s.registry, pool, claim, time.Now(), cfg, s.log)
		if s.requests.Load() != before+1 ||
			s.count("SELECT count(*) FROM harvest.quota_permits WHERE attempt_id=$1 AND account_key='official_account' AND bucket_keys @> ARRAY['global','official']::text[] AND finished_at IS NOT NULL", claim.AttemptID) != 1 {
			t.Fatal("StoreBrowse did not debit exactly one shared fenced permit")
		}
		row, _, err := s.engine.Job(ctx, s.writer, claim.JobID)
		if err != nil || row.State != jobState {
			t.Fatalf("unexpected Job result %s/%s: %v", row.State, row.LastCode, err)
		}
	}
	job := submit(main)
	if claim, err := s.engine.Claim(ctx, s.agent, s.workerID); claim != nil || err != nil {
		t.Fatalf("old Worker could claim the new Capability: %v", err)
	}
	first := pull(wid, job)
	run(first, workerCfg, s.pool, baseBody, 200, "succeeded")
	if _, err := s.engine.Complete(ctx, s.agent, first.AttemptID, s.completion); err != nil {
		t.Fatalf("Completion replay: %v", err)
	}
	if s.count("SELECT count(*) FROM harvest.steam_evidence_staging") != 0 || s.count("SELECT count(*) FROM harvest.steam_evidence") != 0 {
		t.Fatal("StoreBrowse entered the AppDetails evidence store")
	}
	if s.count("SELECT count(*) FROM harvest.observations WHERE attempt_id=$1 AND state='present' AND data->'assets' @> '[{\"asset_type\":\"library_logo\",\"source_slot\":\"logo\"}]'::jsonb", first.AttemptID) != 1 {
		t.Fatal("official vendor Logo alias was not archived in its own asset type")
	}
	eventCount := func() int {
		return s.count("SELECT count(*) FROM harvest.change_events WHERE stream=$1 AND capability='steam.assets'", pilot)
	}
	if eventCount() != 1 {
		t.Fatal("first result should write one event")
	}
	run(pull(wid, submit(main)), workerCfg, s.pool, baseBody, 200, "succeeded")
	if eventCount() != 1 {
		t.Fatal("same contents at a later observed_at emitted an event")
	}
	changed := []byte(strings.ReplaceAll(string(baseBody), "hero.jpg", "hero_next.jpg"))
	run(pull(wid, submit(main)), workerCfg, s.pool, changed, 200, "succeeded")
	if eventCount() != 2 {
		t.Fatal("changed vendor URL did not emit one new event")
	}
	run(pull(wid, submit(main)), workerCfg, s.pool, []byte(`{"error":"API key required"}`), 403, "failed")
	if eventCount() != 2 || s.count(`SELECT count(*) FROM harvest.current_results c JOIN harvest.observations o ON o.id=c.observation_id
		WHERE c.stream=$1 AND o.data->'assets' @? '$[*] ? (@.asset_type == "library_hero" && @.url like_regex "hero_next[.]jpg$")'`, pilot) != 1 {
		t.Fatal("HTTP authentication failure changed the last-known-good assets")
	}

	retry := submit(main)
	if claim, err := s.engine.Claim(ctx, s.agent, wid); claim != nil || err != nil {
		t.Fatalf("StoreBrowse bypassed the shared account cooldown: %v", err)
	}
	s.sql("UPDATE harvest.quota_buckets SET cooldown_until=statement_timestamp() WHERE key='official_account'")
	failed := pull(wid, retry)
	run(failed, workerCfg, s.pool, []byte(`{}`), 500, "retry_wait")
	s.sql("UPDATE harvest.jobs SET available_at=statement_timestamp() WHERE id=$1", retry.ID)
	next := pull(wid, retry)
	if next.AttemptID == failed.AttemptID || next.Generation <= failed.Generation {
		t.Fatal("retry reused the old Attempt identity")
	}
	run(next, workerCfg, s.pool, changed, 200, "succeeded")
	if eventCount() != 2 {
		t.Fatal("retry confirmation changed observations")
	}

	country := request(app, "zh-hans", "hk", "cn")
	run(pull(wid, submit(country)), workerCfg, s.pool, baseBody, 200, "succeeded")
	policy, err := s.engine.NetworkPolicy(s.operator)
	if err != nil || policy.Policy == nil {
		t.Fatalf("read fixture policy: %v", err)
	}
	for _, route := range policy.Policy.Routes {
		for i := range route.Paths {
			if route.Paths[i].Kind == egress.Direct {
				route.Paths[i].Region = "intl"
			}
		}
	}
	if _, err := s.engine.ReplaceNetworkPolicy(ctx, s.operator, execution.PolicyUpdate{ExpectedRevision: policy.Revision, Policy: policy.Policy}); err != nil {
		t.Fatal(err)
	}
	otherCfg := workerCfg
	otherCfg.Egress.Profiles = []egress.ProfileConfig{{Name: "local", Mode: egress.PreferProxy, AllowDirect: true, DirectRegion: "intl"}}
	otherPool := egress.NewPool(otherCfg.Egress)
	t.Cleanup(otherPool.Close)
	other := register(otherCfg, otherPool)
	run(pull(other, submit(request(app, "en", "US", "intl"))), otherCfg, otherPool, baseBody, 200, "succeeded")
	if eventCount() != 4 || s.count("SELECT count(*) FROM harvest.current_results WHERE stream=$1 AND capability='steam.assets'", pilot) != 3 ||
		s.count("SELECT count(*) FROM harvest.change_events WHERE stream=$1", legacy) != 0 {
		t.Fatal("Namespace, locale, market or pinned egress did not stay isolated")
	}
	empty := []byte(`{"response":{"store_items":[{"item_type":0,"id":440,"success":1,"visible":true,"assets":{}}]}}`)
	run(pull(other, submit(request(app, "en", "US", "intl"))), otherCfg, otherPool, empty, 200, "succeeded")
	if eventCount() != 5 || s.count("SELECT count(*) FROM harvest.observations WHERE capability='steam.assets' AND state='confirmed_empty'") != 1 {
		t.Fatal("explicit empty did not update only its own scope")
	}
}
