package execution

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
)

const jobLifetime = 5 * time.Minute
const maxAttempts = 3

func (w work) jobArgs(now time.Time, cause string) q.InsertJobParams {
	return q.InsertJobParams{
		ID: uuid.NewV7(), TargetID: w.target.ID, Namespace: w.target.Namespace, Capability: w.capability.Capability,
		RequestSchema: w.capability.RequestSchema, ExecutionKey: w.capability.Key(), Params: w.params, Scope: w.scope,
		Fingerprint: w.fingerprint, Cause: cause, AvailableAt: now, DeadlineAt: now.Add(jobLifetime), MaxAttempts: maxAttempts,
	}
}

func (e *Engine) SubmitJob(ctx context.Context, p config.Principal, key string, in Submit) (q.HarvestJob, error) {
	e.admission.RLock()
	defer e.admission.RUnlock()
	if !validSubmissionKey(key) || (in.Request == nil) == (in.ScheduleID == nil) {
		return q.HarvestJob{}, ErrInvalid
	}
	if e.draining.Load() {
		return q.HarvestJob{}, ErrDraining
	}
	return tx(ctx, e, func(db *q.Queries) (q.HarvestJob, error) {
		var (
			w        work
			err      error
			revision *int64
			digest   []byte
		)
		if in.ScheduleID != nil {
			// A retry of Run Now always refers to the first accepted invocation,
			// even if the schedule has since changed.
			digest, err = hashValue(struct{ RunSchedule uuid.UUID }{*in.ScheduleID})
		} else {
			w, err = e.resolve(ctx, db, p, *in.Request)
			if err == nil {
				digest = w.fingerprint
			}
		}
		if err != nil {
			return q.HarvestJob{}, err
		}
		prior, err := reserve(ctx, db, p, key, digest)
		if err != nil || prior != nil {
			if prior != nil {
				return *prior, err
			}
			return q.HarvestJob{}, err
		}
		if in.ScheduleID != nil {
			s, err := found(db.LockSchedule(ctx, q.LockScheduleParams{ID: *in.ScheduleID, Namespaces: p.Namespaces}))
			if err != nil {
				return q.HarvestJob{}, err
			}
			if !canCapability(p, s.Capability) {
				return q.HarvestJob{}, ErrForbidden
			}
			w, err = e.resolveStored(ctx, db, p.Namespaces, s.TargetID, s.Capability, s.ExecutionKey,
				s.Params, s.Scope, s.Fingerprint)
			if err != nil {
				return q.HarvestJob{}, err
			}
			revision = &s.Revision
		}
		if w.target.State != "active" {
			return q.HarvestJob{}, ErrConflict
		}
		now, err := db.DBNow(ctx)
		if err != nil {
			return q.HarvestJob{}, err
		}
		args := q.InsertManualJobParams{
			ID: uuid.NewV7(), TargetID: w.target.ID, Namespace: w.target.Namespace, Capability: w.capability.Capability,
			RequestSchema: w.capability.RequestSchema, ExecutionKey: w.capability.Key(), Params: w.params, Scope: w.scope,
			Fingerprint: w.fingerprint, ScheduleID: in.ScheduleID, ScheduleRevision: revision,
			AvailableAt: now, DeadlineAt: now.Add(jobLifetime), MaxAttempts: maxAttempts,
		}
		job, err := db.InsertManualJob(ctx, args)
		if err != nil {
			return job, err
		}
		if (job.State == "queued" || job.State == "retry_wait") && !job.DeadlineAt.After(now) {
			if _, err := setState(ctx, db, job, "failed", "deadline_exceeded", now, job.AvailableAt, nil, nil); err != nil {
				return job, err
			}
			job, err = db.InsertManualJob(ctx, args)
			if err != nil {
				return job, err
			}
		}
		return job, attach(ctx, db, p, key, job.ID)
	})
}

func (e *Engine) RetryJob(ctx context.Context, p config.Principal, key string, id uuid.UUID) (q.HarvestJob, error) {
	e.admission.RLock()
	defer e.admission.RUnlock()
	if !validSubmissionKey(key) {
		return q.HarvestJob{}, ErrInvalid
	}
	if e.draining.Load() {
		return q.HarvestJob{}, ErrDraining
	}
	digest, err := hashValue(struct{ RetryJob uuid.UUID }{id})
	if err != nil {
		return q.HarvestJob{}, err
	}
	return tx(ctx, e, func(db *q.Queries) (q.HarvestJob, error) {
		prior, err := reserve(ctx, db, p, key, digest)
		if err != nil || prior != nil {
			if prior != nil {
				return *prior, err
			}
			return q.HarvestJob{}, err
		}
		old, err := found(db.Job(ctx, q.JobParams{ID: id, Namespaces: p.Namespaces}))
		if err != nil {
			return old, err
		}
		if !canCapability(p, old.Capability) {
			return old, ErrNotFound
		}
		if !terminal(old.State) {
			return old, ErrConflict
		}
		w, err := e.resolveStored(ctx, db, p.Namespaces, old.TargetID, old.Capability, old.ExecutionKey,
			old.Params, old.Scope, old.Fingerprint)
		if err != nil {
			return old, err
		}
		if w.target.State != "active" {
			return old, ErrConflict
		}
		now, err := db.DBNow(ctx)
		if err != nil {
			return old, err
		}
		args := w.jobArgs(now, "retry_requested")
		args.OriginJobID, args.Priority = &id, 1
		job, err := db.InsertJob(ctx, args)
		if err != nil {
			return job, err
		}
		return job, attach(ctx, db, p, key, job.ID)
	})
}

func validSubmissionKey(key string) bool {
	return len(key) > 0 && len(key) <= 128 &&
		strings.IndexFunc(key, func(r rune) bool { return r < '!' || r > '~' }) < 0
}

func reserve(ctx context.Context, db *q.Queries, p config.Principal, key string, digest []byte) (*q.HarvestJob, error) {
	count, err := db.ReserveSubmission(ctx, q.ReserveSubmissionParams{Principal: p.Name, Key: key, RequestHash: digest})
	if err != nil || count != 0 {
		return nil, err
	}
	stored, err := db.Submission(ctx, q.SubmissionParams{Principal: p.Name, Key: key})
	if err != nil {
		return nil, err
	}
	if stored.JobID == nil || !bytes.Equal(digest, stored.RequestHash) {
		return nil, ErrConflict
	}
	job, err := found(db.Job(ctx, q.JobParams{ID: *stored.JobID, Namespaces: p.Namespaces}))
	if err == nil && !canCapability(p, job.Capability) {
		return nil, ErrNotFound
	}
	return &job, err
}

func attach(ctx context.Context, db *q.Queries, p config.Principal, key string, id uuid.UUID) error {
	return db.AttachSubmission(ctx, q.AttachSubmissionParams{Principal: p.Name, Key: key, JobID: &id})
}

func (e *Engine) Jobs(ctx context.Context, p config.Principal, page Page) ([]q.HarvestJob, error) {
	if !canNamespace(p, page.Namespace) {
		return nil, ErrForbidden
	}
	return e.queries.ListJobs(ctx, q.ListJobsParams{
		Namespace: page.Namespace, AfterID: page.After, PageSize: page.Size, Capabilities: p.Capabilities,
	})
}

func (e *Engine) Job(ctx context.Context, p config.Principal, id uuid.UUID) (q.HarvestJob, []q.HarvestAttempt, error) {
	type snapshot struct {
		job      q.HarvestJob
		attempts []q.HarvestAttempt
	}
	view, err := txOptions(ctx, e, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(db *q.Queries) (snapshot, error) {
		job, err := found(db.Job(ctx, q.JobParams{ID: id, Namespaces: p.Namespaces}))
		if err != nil {
			return snapshot{}, err
		}
		if !canCapability(p, job.Capability) {
			return snapshot{}, ErrNotFound
		}
		attempts, err := db.JobAttempts(ctx, id)
		return snapshot{job, attempts}, err
	})
	return view.job, view.attempts, err
}

func (e *Engine) CancelJob(ctx context.Context, p config.Principal, id uuid.UUID) (q.HarvestJob, error) {
	return tx(ctx, e, func(db *q.Queries) (q.HarvestJob, error) {
		job, err := lockJob(ctx, db, p, id)
		if err != nil {
			return job, err
		}
		if terminal(job.State) {
			return job, ErrTerminal
		}
		if job.State == "cancel_requested" {
			return job, nil
		}
		now, err := db.DBNow(ctx)
		if err != nil {
			return job, err
		}
		state := "cancelled"
		if job.State == "running" {
			state = "cancel_requested"
		}
		return setState(ctx, db, job, state, state, now, job.AvailableAt, nil, nil)
	})
}

func lockJob(ctx context.Context, db *q.Queries, p config.Principal, id uuid.UUID) (q.HarvestJob, error) {
	job, err := found(db.LockJob(ctx, q.LockJobParams{ID: id, Namespaces: p.Namespaces}))
	if err == nil && !canCapability(p, job.Capability) {
		err = ErrNotFound
	}
	return job, err
}

func setState(ctx context.Context, db *q.Queries, job q.HarvestJob, state, code string, now, available time.Time,
	attemptID *uuid.UUID, hash []byte) (q.HarvestJob, error) {
	return db.SetJobState(ctx, q.SetJobStateParams{
		ID: job.ID, State: state, LastCode: code, UpdatedAt: now, AvailableAt: available, AcceptedAttemptID: attemptID, AcceptedHash: hash,
	})
}

func retryState(job q.HarvestJob, now time.Time, terminalState string, retryable bool) (string, time.Time) {
	if retryable && job.AttemptCount > 0 && job.AttemptCount < job.MaxAttempts {
		delay := time.Second * time.Duration(1<<(job.AttemptCount-1))
		if next := now.Add(delay); next.Before(job.DeadlineAt) {
			return "retry_wait", next
		}
	}
	return terminalState, job.AvailableAt
}

func (e *Engine) recoverOne(ctx context.Context) (bool, error) {
	return tx(ctx, e, func(db *q.Queries) (bool, error) {
		job, err := db.LockExpiredJob(ctx)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		a, err := db.AttemptForGeneration(ctx, q.AttemptForGenerationParams{JobID: job.ID, Generation: job.Generation})
		if err != nil {
			return false, err
		}
		now, err := db.DBNow(ctx)
		if err != nil || a.State != "running" || a.LeaseUntil.After(now) {
			return false, err
		}
		attemptState, code := "expired", "worker_lost"
		state, available := retryState(job, now, "failed", true)
		if job.State == "cancel_requested" {
			attemptState, code, state, available = "cancelled", "cancelled", "cancelled", job.AvailableAt
		}
		if err := db.FinishAttempt(ctx, q.FinishAttemptParams{ID: a.ID, EndedAt: &now, State: attemptState, ErrorCode: code}); err != nil {
			return false, err
		}
		_, err = setState(ctx, db, job, state, code, now, available, nil, nil)
		return true, err
	})
}
