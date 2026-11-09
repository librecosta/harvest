package execution

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/provider"
)

func (e *Engine) registrationKeys(p config.Principal, req Registration) ([]string, []string, error) {
	if !UUIDv7(req.ID) || req.Capacity < 1 || req.Capacity > p.MaxSlots ||
		len(req.Offers) < 1 || len(req.Offers) > 32 || len(req.EgressProfiles) > 32 {
		return nil, nil, ErrInvalid
	}
	keys := make([]string, 0, len(req.Offers))
	for _, offer := range req.Offers {
		c, ok := e.Registry.Capability(offer.Capability)
		if !ok || c.Execute == nil || c.Offer != offer || !canCapability(p, offer.Capability) ||
			slices.Contains(keys, offer.Key()) {
			return nil, nil, ErrUnsupported
		}
		keys = append(keys, offer.Key())
	}
	profiles := make([]string, 0, len(req.EgressProfiles))
	for _, name := range req.EgressProfiles {
		if !slices.Contains(p.EgressProfiles, name) || slices.Contains(profiles, name) {
			return nil, nil, ErrForbidden
		}
		profiles = append(profiles, name)
	}
	slices.Sort(keys)
	slices.Sort(profiles)
	return keys, profiles, nil
}

func (e *Engine) RegisterWorker(ctx context.Context, p config.Principal, in Registration) error {
	e.admission.RLock()
	defer e.admission.RUnlock()
	if e.draining.Load() {
		return ErrDraining
	}
	keys, profiles, err := e.registrationKeys(p, in)
	if err != nil {
		return err
	}
	paths, err := e.validatePaths(p, in.Capacity, in.Paths)
	if err != nil {
		return err
	}
	_, err = tx(ctx, e, func(db *q.Queries) (struct{}, error) {
		err := db.InsertWorker(ctx, q.InsertWorkerParams{
			ID: in.ID, Principal: p.Name, Offers: keys, EgressProfiles: profiles, Capacity: in.Capacity, NetworkPaths: paths,
		})
		if err != nil {
			return struct{}{}, err
		}
		w, err := found(db.Worker(ctx, q.WorkerParams{ID: in.ID, Principal: p.Name}))
		if err == nil && (w.Capacity != in.Capacity || !slices.Equal(w.Offers, keys) || !slices.Equal(w.EgressProfiles, profiles) || !samePaths(w.NetworkPaths, paths)) {
			err = ErrConflict
		}
		return struct{}{}, err
	})
	return err
}

func (e *Engine) Heartbeat(ctx context.Context, p config.Principal, id uuid.UUID, in Heartbeat) (Heartbeat, error) {
	args := q.HeartbeatWorkerParams{ID: id, Principal: p.Name, Draining: in.Draining}
	if in.Paths != nil {
		// Registration fixes identity and capacity for the lifetime of the
		// process. Heartbeats may change only coarse health.
		old, err := found(e.queries.Worker(ctx, q.WorkerParams{ID: id, Principal: p.Name}))
		if err != nil {
			return Heartbeat{}, err
		}
		paths, err := e.validatePaths(p, old.Capacity, in.Paths)
		if err != nil {
			return Heartbeat{}, err
		}
		if !samePaths(old.NetworkPaths, paths) {
			return Heartbeat{}, ErrConflict
		}
		args.Paths = paths
	}
	w, err := found(e.queries.HeartbeatWorker(ctx, args))
	return Heartbeat{Draining: w.Draining || e.draining.Load()}, err
}

func (e *Engine) Claim(ctx context.Context, p config.Principal, id uuid.UUID) (*Claim, error) {
	e.admission.RLock()
	defer e.admission.RUnlock()
	if e.draining.Load() {
		return nil, ErrDraining
	}
	return tx(ctx, e, func(db *q.Queries) (*Claim, error) {
		// Worker -> Job -> Attempt. Only claims hold a Worker row lock.
		w, err := found(db.LockWorker(ctx, q.LockWorkerParams{ID: id, Principal: p.Name}))
		if err != nil {
			return nil, err
		}
		now, err := db.DBNow(ctx)
		if err != nil {
			return nil, err
		}
		if w.Draining || !w.LastSeen.Add(WorkerTTL).After(now) {
			return nil, ErrDraining
		}
		if slices.ContainsFunc(w.EgressProfiles, func(name string) bool { return !slices.Contains(p.EgressProfiles, name) }) {
			return nil, ErrForbidden
		}
		offers, networkOffers, err := e.dispatchOffers(ctx, db, p, w)
		if err != nil || (len(offers) == 0 && len(networkOffers) == 0) {
			return nil, err
		}
		active, err := db.WorkerLeaseCount(ctx, w.ID)
		if err != nil || active >= int64(min(w.Capacity, p.MaxSlots)) {
			return nil, err
		}
		job, err := db.LockClaimableJob(ctx, q.LockClaimableJobParams{Offers: offers, Namespaces: p.Namespaces, NetworkOffers: networkOffers})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		work, err := e.resolveStored(ctx, db, p.Namespaces, job.TargetID, job.Capability, job.ExecutionKey,
			job.Params, job.Scope, job.Fingerprint)
		if err != nil || work.target.State != "active" {
			return nil, err
		}
		now, err = db.DBNow(ctx)
		if err != nil || !job.DeadlineAt.After(now) || job.AvailableAt.After(now) {
			return nil, err
		}
		if e.draining.Load() {
			return nil, ErrDraining
		}
		task, err := work.task()
		if err != nil {
			return nil, err
		}
		job, err = db.MarkJobClaimed(ctx, q.MarkJobClaimedParams{ID: job.ID, UpdatedAt: now})
		if err != nil {
			return nil, err
		}
		until := leaseEnd(job, now)
		var networkRevision int64
		if work.capability.Network != nil {
			networkRevision = e.policy.RoutingRevision
		}
		a, err := db.InsertAttempt(ctx, q.InsertAttemptParams{
			ID: uuid.NewV7(), JobID: job.ID, WorkerID: w.ID, Generation: job.Generation, ClaimedAt: now, LeaseUntil: until, NetworkRevision: networkRevision,
		})
		if err != nil {
			return nil, err
		}
		claim := &Claim{
			Fence: Fence{job.ID, w.ID, job.Generation}, AttemptID: a.ID,
			LeaseUntil: until.UTC(), LeaseMS: until.Sub(now).Milliseconds(), Task: task,
		}
		if cap, _ := e.Registry.Capability(task.Offer.Capability); cap.Network != nil {
			paths, err := decodePaths(w.NetworkPaths)
			if err != nil {
				return nil, err
			}
			claim.Network, _ = e.plan(p, e.source(task.Offer.Capability, task.Namespace), task.Scope.EgressRegion, paths)
		}
		return claim, nil
	})
}

func leaseEnd(job q.HarvestJob, now time.Time) time.Time {
	until := now.Add(LeaseTTL)
	if job.DeadlineAt.Before(until) {
		return job.DeadlineAt
	}
	return until
}

// The Job row lock is the sole transition lock. All writers of Attempt rows
// take it first, including cancellation, renewal, expiry and completion.
func lockAttempt(ctx context.Context, db *q.Queries, p config.Principal, id uuid.UUID, fence Fence) (q.HarvestJob, q.HarvestAttempt, q.HarvestWorker, error) {
	if !UUIDv7(fence.JobID) || !UUIDv7(fence.WorkerID) || fence.Generation < 1 {
		return q.HarvestJob{}, q.HarvestAttempt{}, q.HarvestWorker{}, ErrInvalid
	}
	job, err := lockJob(ctx, db, p, fence.JobID)
	if err != nil {
		return job, q.HarvestAttempt{}, q.HarvestWorker{}, err
	}
	w, err := found(db.Worker(ctx, q.WorkerParams{ID: fence.WorkerID, Principal: p.Name}))
	if err != nil {
		return job, q.HarvestAttempt{}, w, err
	}
	a, err := found(db.Attempt(ctx, q.AttemptParams{ID: id, JobID: job.ID, WorkerID: fence.WorkerID, Generation: fence.Generation}))
	return job, a, w, err
}

func validLease(job q.HarvestJob, a q.HarvestAttempt, now time.Time) bool {
	return a.State == "running" && job.Generation == a.Generation &&
		a.LeaseUntil.After(now) && job.DeadlineAt.After(now)
}

func (e *Engine) Renew(ctx context.Context, p config.Principal, id uuid.UUID, fence Fence) (Renewal, error) {
	return tx(ctx, e, func(db *q.Queries) (Renewal, error) {
		job, a, _, err := lockAttempt(ctx, db, p, id, fence)
		if err != nil {
			return Renewal{}, err
		}
		now, err := db.DBNow(ctx)
		if err != nil {
			return Renewal{}, err
		}
		if !validLease(job, a, now) {
			return Renewal{}, ErrFence
		}
		cap, ok := e.Registry.Capability(job.Capability)
		if !ok || cap.Execute == nil || cap.Key() != job.ExecutionKey {
			return Renewal{}, ErrUnsupported
		}
		if job.State == "cancel_requested" {
			return Renewal{}, ErrCancelled
		}
		if job.State != "running" {
			return Renewal{}, ErrFence
		}
		until := leaseEnd(job, now)
		_, err = db.RenewAttempt(ctx, q.RenewAttemptParams{ID: a.ID, HeartbeatAt: now, LeaseUntil: until})
		return Renewal{until.UTC(), until.Sub(now).Milliseconds()}, err
	})
}

func (e *Engine) Complete(ctx context.Context, p config.Principal, id uuid.UUID, in Completion) (Receipt, error) {
	return tx(ctx, e, func(db *q.Queries) (Receipt, error) {
		job, a, _, err := lockAttempt(ctx, db, p, id, in.Fence)
		if err != nil {
			return Receipt{}, err
		}
		cap, ok := e.Registry.Capability(job.Capability)
		if !ok || cap.Execute == nil || cap.Key() != job.ExecutionKey || cap.OutputSchema != in.OutputSchema {
			return Receipt{}, ErrUnsupported
		}
		result, retryable, err := normalizeResult(cap, in.Result)
		if err != nil {
			return Receipt{}, err
		}
		var segments []acceptedSegment
		if cap.Observation != nil && result.State != "cancelled" && (result.State != "failed" || len(result.Segments) != 0) {
			work, err := e.resolveStored(ctx, db, []string{job.Namespace}, job.TargetID, job.Capability, job.ExecutionKey, job.Params, job.Scope, job.Fingerprint)
			if err != nil {
				return Receipt{}, err
			}
			task, err := work.task()
			if err != nil {
				return Receipt{}, err
			}
			segments, err = normalizeSegments(cap, task, &result)
			if err != nil {
				return Receipt{}, err
			}
		}
		hash, err := hashValue(result)
		if err != nil {
			return Receipt{}, err
		}
		if a.ReceiptState != nil {
			if !bytes.Equal(a.ResultHash, hash) {
				return Receipt{}, ErrConflict
			}
			return Receipt{a.ID, job.ID, *a.ReceiptState, a.EndedAt.UTC()}, nil
		}
		now, err := db.DBNow(ctx)
		if err != nil {
			return Receipt{}, err
		}
		if !validLease(job, a, now) {
			return Receipt{}, ErrFence
		}
		switch job.State {
		case "cancel_requested":
			if result.State != "cancelled" {
				return Receipt{}, ErrCancelled
			}
		case "running":
			if result.State == "cancelled" {
				return Receipt{}, ErrConflict
			}
		default:
			return Receipt{}, ErrFence
		}
		if len(segments) != 0 {
			now, err = e.writeObservations(ctx, db, job, a, segments, cap.Observation.Sample, now)
			if err != nil {
				return Receipt{}, err
			}
		}
		state, available := retryState(job, now, result.State, retryable)
		if state == "retry_wait" && result.Code == "network_quota_deferred" {
			available = now.Add(10 * time.Second)
			if !available.Before(job.DeadlineAt) {
				state, available = result.State, now
			}
		}
		if err := db.FinishAttempt(ctx, q.FinishAttemptParams{
			ID: a.ID, EndedAt: &now, State: result.State, ErrorCode: result.Code, ResultHash: hash, ReceiptState: &state,
		}); err != nil {
			return Receipt{}, err
		}
		_, err = setState(ctx, db, job, state, result.Code, now, available, &a.ID, hash)
		if err == nil && len(segments) != 0 {
			var end time.Time
			end, err = db.DBNow(ctx)
			if err == nil && !validLease(job, a, end) {
				return Receipt{}, ErrFence
			}
		}
		return Receipt{a.ID, job.ID, state, now.UTC()}, err
	})
}

func normalizeResult(cap provider.Capability, result provider.Result) (provider.Result, bool, error) {
	retryable := false
	switch result.State {
	case "succeeded":
		if result.Code != "" {
			return result, false, ErrInvalid
		}
	case "partial", "failed":
		if result.State == "partial" && !cap.Partial {
			return result, false, ErrInvalid
		}
		switch result.Code {
		case "upstream_unavailable", "transport_error", "timeout", "internal_unavailable", "network_quota_deferred", "network_policy_changed":
			retryable = true
		case "invalid_payload", "policy_denied", "upstream_auth", "upstream_throttled", "incomplete":
		default:
			return result, false, ErrInvalid
		}
	case "cancelled":
		if result.Code != "cancelled" {
			return result, false, ErrInvalid
		}
	default:
		return result, false, ErrInvalid
	}
	if cap.Observation != nil {
		if len(result.Data) != 0 || (result.State == "cancelled" && len(result.Segments) != 0) {
			return result, false, ErrInvalid
		}
		return result, retryable, nil
	}
	if len(result.Segments) != 0 {
		return result, false, ErrInvalid
	}
	if result.State == "failed" || result.State == "cancelled" {
		if len(result.Data) != 0 {
			return result, false, ErrInvalid
		}
	} else {
		value, err := cap.NormalizeOutput(result.Data)
		if err != nil {
			return result, false, ErrInvalid
		}
		result.Data = value
	}
	return result, retryable, nil
}
