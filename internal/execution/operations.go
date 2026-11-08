package execution

import (
	"context"
	"slices"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/egress"
)

type Operations struct {
	Activity        q.ActivitySummaryRow `json:"activity"`
	Jobs            []q.JobSummaryRow    `json:"jobs"`
	Workers         []WorkerStatus       `json:"workers"`
	Quota           []q.QuotaSummaryRow  `json:"quota,omitempty"`
	RetainedPermits int64                `json:"retained_permits"`
	DBPressure      DBPressure           `json:"db_pressure"`
	Draining        bool                 `json:"draining"`
	Results         ResultStorage        `json:"results"`
}

type WorkerStatus struct {
	ID       uuid.UUID              `json:"id"`
	Capacity int32                  `json:"capacity"`
	LastSeen time.Time              `json:"last_seen"`
	Draining bool                   `json:"draining"`
	Paths    []egress.Advertisement `json:"paths,omitempty"`
}

type DBPressure struct {
	Open     int32 `json:"open"`
	Acquired int32 `json:"acquired"`
	Max      int32 `json:"max"`
	Waits    int64 `json:"waits"`
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
	if err != nil {
		return Operations{}, err
	}
	stat := e.db.Stat()
	out := Operations{Activity: activity, Jobs: jobs, Draining: e.draining.Load(), Workers: make([]WorkerStatus, 0, len(workers)),
		DBPressure: DBPressure{stat.TotalConns(), stat.AcquiredConns(), stat.MaxConns(), stat.EmptyAcquireCount()}}
	keys, profiles := e.visibleNetwork(p, namespace)
	for _, w := range workers {
		paths, err := decodePaths(w.NetworkPaths)
		if err != nil {
			return Operations{}, err
		}
		paths = slices.DeleteFunc(paths, func(a egress.Advertisement) bool { return !slices.Contains(profiles, a.Profile) })
		out.Workers = append(out.Workers, WorkerStatus{w.ID, w.Capacity, w.LastSeen, w.Draining, paths})
	}
	if len(keys) > 0 {
		out.Quota, err = e.queries.QuotaSummary(ctx, keys)
		if err != nil {
			return Operations{}, err
		}
		out.RetainedPermits, err = e.queries.QuotaPermitCount(ctx, keys)
	}
	if err == nil {
		out.Results, err = e.resultStorage(ctx, p, namespace)
	}
	return out, err
}

func (e *Engine) visibleNetwork(p config.Principal, namespace string) ([]string, []string) {
	allowed := make(map[string]bool)
	hidden := make(map[string]bool)
	var profiles []string
	for _, s := range e.network.Sources {
		keys := []string{e.network.GlobalBucket, s.ProviderBucket, s.AccountBucket}
		for _, path := range e.route(s.Profile).Paths {
			keys = append(keys, path.Bucket)
		}
		set := hidden
		if s.Namespace == namespace && canCapability(p, s.Capability) {
			set = allowed
			profiles = append(profiles, s.Profile)
		}
		for _, k := range keys {
			set[k] = true
		}
	}
	var keys []string
	for k := range allowed {
		if !hidden[k] {
			keys = append(keys, k)
		}
	}
	return keys, profiles
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
	if cap, ok := e.Registry.Capability(job.Capability); ok && cap.Network != nil {
		return e.networkWaiting(ctx, job, cap)
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
	pool := egress.NewPool(w.Egress)
	paths := pool.Advertise(w.Concurrency)
	_, _, err := e.registrationKeys(p, Registration{ID: uuid.NewV7(), Offers: w.Offers, EgressProfiles: w.EgressProfiles, Capacity: w.Concurrency})
	if err != nil {
		return err
	}
	_, err = e.validatePaths(p, w.Concurrency, paths)
	return err
}
