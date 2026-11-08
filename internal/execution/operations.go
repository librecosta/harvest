package execution

import (
	"context"
	"slices"
	"uuid"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
)

type Operations struct {
	Activity q.ActivitySummaryRow `json:"activity"`
	Jobs     []q.JobSummaryRow    `json:"jobs"`
	Workers  []q.WorkerSummaryRow `json:"workers"`
	Draining bool                 `json:"draining"`
}

func (e *Engine) workerPrincipals(namespace string, caps []string) []string {
	var names []string
	for _, p := range e.workerPolicies {
		if !canNamespace(p, namespace) {
			continue
		}
		if len(caps) != 0 && !slices.ContainsFunc(caps, func(c string) bool { return slices.Contains(p.Capabilities, c) }) {
			continue
		}
		names = append(names, p.Name)
	}
	return names
}

func (e *Engine) Operations(ctx context.Context, p config.Principal, namespace string) (Operations, error) {
	if !canNamespace(p, namespace) {
		return Operations{}, ErrForbidden
	}
	jobs, err := e.queries.JobSummary(ctx, q.JobSummaryParams{Namespace: namespace, Capabilities: p.Capabilities})
	if err != nil {
		return Operations{}, err
	}
	workers, err := e.queries.WorkerSummary(ctx, e.workerPrincipals(namespace, p.Capabilities))
	if err != nil {
		return Operations{}, err
	}
	activity, err := e.queries.ActivitySummary(ctx, q.ActivitySummaryParams{ScopeNamespace: namespace, Capabilities: p.Capabilities})
	return Operations{Activity: activity, Jobs: jobs, Workers: workers, Draining: e.draining.Load()}, err
}

func (e *Engine) JobWaitingReason(ctx context.Context, job q.HarvestJob) (string, error) {
	if job.State != "queued" && job.State != "retry_wait" {
		return "", nil
	}
	if e.draining.Load() {
		return "server_draining", nil
	}
	now, err := e.queries.DBNow(ctx)
	if err != nil {
		return "", err
	}
	if !job.DeadlineAt.After(now) {
		return "deadline_exceeded", nil
	}
	if job.AvailableAt.After(now) {
		return "retry_backoff", nil
	}
	target, err := e.queries.Target(ctx, q.TargetParams{ID: job.TargetID, Namespaces: []string{job.Namespace}})
	if err != nil {
		return "", err
	}
	if target.State != "active" {
		return "target_archived", nil
	}
	names := e.workerPrincipals(job.Namespace, []string{job.Capability})
	eligible, err := e.queries.EligibleWorker(ctx, q.EligibleWorkerParams{Principals: names, ExecutionKey: job.ExecutionKey})
	if err != nil {
		return "", err
	}
	if !eligible {
		return "no_eligible_worker", nil
	}
	return "awaiting_claim", nil
}

func (e *Engine) WorkerReadiness(p config.Principal) error {
	if e.draining.Load() {
		return ErrDraining
	}
	if !slices.ContainsFunc(p.Capabilities, func(name string) bool {
		cap, ok := e.Registry.Capability(name)
		return ok && cap.Execute != nil
	}) {
		return ErrUnsupported
	}
	return nil
}

func (e *Engine) ValidateLocalWorker(p config.Principal, w config.Worker) error {
	_, _, err := e.registrationKeys(p, Registration{uuid.NewV7(), w.Offers, w.EgressProfiles, w.Concurrency})
	return err
}
