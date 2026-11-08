//go:build integration

package integration

import (
	"context"
	"crypto/rand"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

var detailOffer = provider.Offer{Capability: "fixture.detail", RequestSchema: 1, OutputSchema: 1, Implementation: "observations_v1"}
var sampleOffer = provider.Offer{Capability: "fixture.sample", RequestSchema: 1, OutputSchema: 1, Implementation: "observations_v1"}

type observationParams struct {
	Mode    string `json:"mode"`
	Value   int    `json:"value"`
	Padding int    `json:"padding"`
	Delay   int    `json:"delay_ms"`
}

type observationOutput struct {
	Value   *int   `json:"value,omitempty"`
	Padding string `json:"padding,omitempty"`
}

func a3Scopes(task provider.Task) []provider.SegmentScope {
	locales := []string{"en"}
	if task.Offer == detailOffer {
		locales = append(locales, "fr")
	}
	scopes := make([]provider.SegmentScope, 0, len(locales))
	for _, locale := range locales {
		scope := task.Scope
		scope.Locale = locale
		scopes = append(scopes, provider.SegmentScope{Namespace: task.Namespace, Scope: scope, Dimensions: map[string]string{"view": "detail"}})
	}
	return scopes
}

func a3Capability(sample bool) provider.Capability {
	offer := detailOffer
	policy := &provider.ObservationPolicy{Scopes: a3Scopes, Source: "fixture", EmptyEvidence: "complete", WithdrawEvidence: "removed", MaxDataBytes: provider.MaxSegmentData}
	if sample {
		offer = sampleOffer
		policy.Sample, policy.EmptyEvidence, policy.WithdrawEvidence = true, "", ""
	}
	policy.NormalizeData = func(state string, data jsontext.Value) (jsontext.Value, error) {
		out, err := provider.ObservationObject[observationOutput](data)
		if err != nil || strings.Trim(out.Padding, "x") != "" || (state == "present" && out.Value == nil) || (state == "confirmed_empty" && (out.Value != nil || out.Padding != "")) {
			return nil, errors.New("invalid fixture observation")
		}
		return json.Marshal(out)
	}
	return provider.Capability{
		Offer: offer, Resource: "key", MinInterval: time.Second, Timeout: 25 * time.Second, Partial: !sample, Observation: policy,
		NormalizeParams: func(data jsontext.Value) (jsontext.Value, error) {
			p, err := provider.Object[observationParams](data)
			if err != nil || p.Padding < 0 || p.Padding > 500<<10 || p.Delay < 0 || p.Delay > 5000 {
				return nil, errors.New("invalid observation fixture params")
			}
			switch p.Mode {
			case "present", "partial", "empty", "withdraw", "missing", "not_modified":
			default:
				return nil, errors.New("invalid observation fixture mode")
			}
			if sample && p.Mode != "present" && p.Mode != "missing" {
				return nil, errors.New("invalid sample fixture mode")
			}
			return json.Marshal(p)
		},
		Execute: func(ctx context.Context, task provider.Task, _ egress.Client) provider.Result {
			p, err := provider.Object[observationParams](task.Params)
			if err != nil {
				return provider.Result{State: "failed", Code: "invalid_payload"}
			}
			timer := time.NewTimer(time.Duration(p.Delay) * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return provider.Result{State: "failed", Code: "timeout"}
			case <-timer.C:
				return a3Result(task, p, time.Now().UTC())
			}
		},
	}
}

func a3Result(task provider.Task, p observationParams, observed time.Time) provider.Result {
	result := provider.Result{State: "succeeded"}
	data, _ := json.Marshal(observationOutput{Value: &p.Value, Padding: strings.Repeat("x", p.Padding)})
	for _, scope := range a3Scopes(task) {
		s := provider.Segment{Scope: scope, OutputSchema: 1, ObservedAt: observed, State: "present", Data: data, Source: "fixture"}
		switch p.Mode {
		case "partial":
			if scope.Locale == "fr" {
				s.State, s.Data = "unavailable", nil
				result.State, result.Code = "partial", "incomplete"
			}
		case "empty", "withdraw":
			s.State, s.Data, s.Evidence = "confirmed_empty", jsontext.Value("{}"), "complete"
			if p.Mode == "withdraw" {
				s.Withdraw, s.Evidence = true, "removed"
			}
		case "not_modified":
			s.State, s.Data = "not_modified", nil
		case "missing":
			result.State, result.Code = "failed", "upstream_unavailable"
			return result
		}
		result.Segments = append(result.Segments, s)
	}
	return result
}

func a3Request(t *testing.T, target uuid.UUID, offer provider.Offer, params observationParams) execution.Request {
	t.Helper()
	data, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	return execution.Request{TargetID: target, Capability: offer.Capability, RequestSchema: 1, Params: data}
}

func (p *testPlane) a3Config() {
	p.t.Helper()
	data, err := os.ReadFile(p.config)
	if err != nil {
		p.t.Fatal(err)
	}
	p.observations = true
	p.tokens["restricted"] = rand.Text() + rand.Text()
	cfg := strings.ReplaceAll(string(data), "capabilities: [fixture.run]", "capabilities: [fixture.run, fixture.detail, fixture.sample]")
	cfg = strings.ReplaceAll(cfg, "namespaces: [public]", "namespaces: [public, snapshots, retention]")
	cfg = strings.Replace(cfg, "public, private, scheduler, embedded", "public, private, scheduler, embedded, snapshots, retention", 1)
	cfg = strings.Replace(cfg, "  - name: \"worker_a\"",
		fmt.Sprintf("  - name: restricted\n    role: reader\n    token: %q\n    namespaces: [public]\n    capabilities: [fixture.detail]\n  - name: \"worker_a\"", p.tokens["restricted"]), 1)
	if err := os.WriteFile(p.config, []byte(cfg), 0o600); err != nil {
		p.t.Fatal(err)
	}
}

func (p *testPlane) a3Worker(who string, offer provider.Offer) *child {
	p.t.Helper()
	data := strings.Replace(p.workerYAML(who), fixtureOffer.Capability, offer.Capability, 1)
	data = strings.Replace(data, fixtureOffer.Implementation, offer.Implementation, 1)
	path := filepath.Join(p.scratch, who+"-a3.yaml")
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		p.t.Fatal(err)
	}
	return p.start("worker", "--config", path)
}

func (p *testPlane) a3Register(who string) uuid.UUID {
	p.t.Helper()
	in := execution.Registration{ID: uuid.NewV7(), Offers: []provider.Offer{detailOffer, sampleOffer}, Capacity: 1}
	p.call("POST", "/internal/v1/workers/register", who, "", in, nil, 204)
	return in.ID
}

func (p *testPlane) a3Claim(who string, worker, target uuid.UUID, offer provider.Offer, params observationParams) (execution.Claim, execution.Completion) {
	p.t.Helper()
	job := p.submit(uuid.NewV7().String(), a3Request(p.t, target, offer, params))
	var claim execution.Claim
	p.call("POST", "/internal/v1/workers/"+worker.String()+"/claim", who, "", nil, &claim, 200)
	if claim.JobID != job.ID {
		p.t.Fatalf("unexpected claim %s (wanted %s)", claim.JobID, job.ID)
	}
	return claim, execution.Completion{Fence: claim.Fence, OutputSchema: offer.OutputSchema, Result: a3Result(claim.Task, params, time.Now().UTC())}
}

func completePath(claim execution.Claim) string {
	return "/internal/v1/attempts/" + claim.AttemptID.String() + "/complete"
}
