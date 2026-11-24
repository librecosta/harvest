package worker

import (
	"bytes"
	json "encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

func TestSteamRichDetailsNative(t *testing.T) {
	const pilot = "evidence_two"
	root := os.Getenv("HARVEST_RICH_DETAILS_ARCHIVES")
	if root == "" {
		t.Skip("requires private AppDetails archive directory")
	}
	s := newSteamTestPlaneForApp(t, nil, nil, []string{pilot})
	ctx := t.Context()
	v3, ok := s.registry.CapabilityByKey(provider.SteamDetailsV3Key)
	if !ok {
		t.Fatal("missing v3 Offer")
	}
	v := int32(3)
	for _, sample := range []struct {
		app, market string
		appID       int64
	}{{"434000", "HK", 434000}, {"1332010", "CN", 1332010}, {"1145360", "US", 1145360}} {
		body, err := os.ReadFile(filepath.Join(root, sample.app+".json"))
		if err != nil {
			t.Fatal(err)
		}
		target, err := s.engine.RegisterTarget(ctx, s.writer, execution.TargetInput{Provider: "steam", ResourceType: "app", ExternalKey: sample.app, Namespace: pilot})
		if err != nil {
			t.Fatal(err)
		}
		params, _ := json.Marshal(map[string]string{"language": "english", "store_country": sample.market})
		request := execution.Request{TargetID: target.ID, Capability: "steam.details", RequestSchema: 1, OutputSchema: &v, Params: params, Scope: provider.Scope{EgressRegion: "cn"}}
		job, err := s.engine.SubmitJob(ctx, s.writer, uuid.NewV7().String(), execution.Submit{Request: &request})
		if err != nil {
			t.Fatal(err)
		}
		if claim, err := s.engine.Claim(ctx, s.agent, s.workerID); err != nil || claim != nil {
			t.Fatalf("v1-only Worker claimed v3: %v", err)
		}
		cfg := s.cfg
		cfg.Offers = []provider.Offer{v3.Offer}
		cfg.SteamEvidenceAppIDs = []int64{sample.appID}
		worker := uuid.NewV7()
		if err = s.engine.RegisterWorker(ctx, s.agent, execution.Registration{ID: worker, Offers: cfg.Offers, EgressProfiles: cfg.EgressProfiles,
			Capacity: 1, Paths: s.pool.Advertise(1)}); err != nil {
			t.Fatal(err)
		}
		claim, err := s.engine.Claim(ctx, s.agent, worker)
		if err != nil || claim == nil || claim.JobID != job.ID {
			t.Fatalf("v3 claim: %v", err)
		}
		start := time.Now()
		s.fixtureApp.Store(&sample.app)
		s.body.Store(&body)
		prior := s.requests.Load()
		execute(ctx, s.api, s.registry, s.pool, *claim, start, cfg, s.log)
		s.recover()
		if s.requests.Load() != prior+1 || s.completion.OutputSchema != 3 || len(s.completion.Result.Segments) != 4 {
			t.Fatalf("%s expected one request and four v3 segments", sample.app)
		}
		if count := s.count("SELECT count(*) FROM harvest.steam_evidence_attribution a JOIN harvest.steam_evidence e ON e.id=a.evidence_id WHERE a.attempt_id=$1 AND e.payload=$2", claim.AttemptID, body); count != 1 {
			t.Fatalf("%s missing byte-exact evidence attribution", sample.app)
		}
		job, _, err = s.engine.Job(ctx, s.writer, job.ID)
		if err != nil || job.State != "succeeded" && job.State != "partial" {
			t.Fatalf("v3 completion %s %v", job.State, err)
		}
		for _, seg := range s.completion.Result.Segments {
			section := seg.Scope.Dimensions["section"]
			t.Logf("AppID %s raw=%d section=%s payload=%d state=%s", sample.app, len(body), section, len(seg.Data), seg.State)
			if section == "rich_details" {
				if seg.State != "present" || !bytes.Contains(seg.Data, []byte("detailed_description")) || !bytes.Contains(seg.Data, []byte("languages")) {
					t.Fatal("missing rich details")
				}
				if count := s.count("SELECT count(*) FROM harvest.observations WHERE attempt_id=$1 AND output_schema=3 AND scope#>>'{dimensions,section}'='rich_details'", claim.AttemptID); count != 1 {
					t.Fatal("missing persisted rich details")
				}
			}
		}
	}
	unapproved, err := s.engine.RegisterTarget(ctx, s.writer, execution.TargetInput{Provider: "steam", ResourceType: "app", ExternalKey: "434000", Namespace: "evidence_one"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.engine.SubmitJob(ctx, s.writer, uuid.NewV7().String(), execution.Submit{Request: &execution.Request{TargetID: unapproved.ID, Capability: "steam.details",
		RequestSchema: 1, OutputSchema: &v, Params: []byte(`{"language":"english","store_country":"US"}`), Scope: provider.Scope{EgressRegion: "cn"}}})
	if !errors.Is(err, execution.ErrUnsupported) {
		t.Fatalf("non-preview v3: %v", err)
	}
}
