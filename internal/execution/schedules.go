package execution

import (
	"context"
	"errors"
	"strings"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/robfig/cron/v3"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
)

const pointWindow = 5 * time.Second
const maxMissed = 1000000
const cronCountLimit = 256

var cronUTC = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

type cadence struct {
	interval time.Duration
	cron     cron.Schedule
}

func parseCadence(rule Recurrence, minimum time.Duration) (cadence, error) {
	if rule.Policy != "state_refresh" && rule.Policy != "point_in_time" ||
		rule.AnchorAt.Year() < 2000 || rule.AnchorAt.Year() > 2100 {
		return cadence{}, ErrInvalid
	}
	if rule.IntervalSeconds != 0 {
		interval := time.Duration(rule.IntervalSeconds) * time.Second
		if rule.Cron != "" || interval < minimum || interval > 30*24*time.Hour {
			return cadence{}, ErrInvalid
		}
		return cadence{interval: interval}, nil
	}
	if minimum > time.Minute || len(rule.Cron) > 120 || strings.Contains(rule.Cron, "=") ||
		len(strings.Fields(rule.Cron)) != 5 {
		return cadence{}, ErrInvalid
	}
	schedule, err := cronUTC.Parse(rule.Cron)
	if err != nil || schedule.Next(rule.AnchorAt.UTC().Add(-time.Nanosecond)).IsZero() {
		return cadence{}, ErrInvalid
	}
	return cadence{cron: schedule}, nil
}

func (c cadence) first(anchor time.Time) time.Time {
	if c.cron != nil {
		return c.cron.Next(anchor.Add(-time.Nanosecond))
	}
	return anchor
}

// due is the persisted first unreconciled slot. For cron, the predicate
// Next(t) <= now is monotonic in [due-1s, now]; binary search locates the
// newest slot even after long downtime. Counting missed slots is capped.
func (c cadence) slots(due, now time.Time) (latest, next time.Time, missed int32) {
	if c.interval != 0 {
		skipped := now.Sub(due) / c.interval
		latest = due.Add(skipped * c.interval)
		return latest, latest.Add(c.interval), int32(min(skipped, maxMissed))
	}
	lo, hi := due.Unix()-1, now.Unix()
	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		candidate := c.cron.Next(time.Unix(mid, 0).UTC())
		if !candidate.IsZero() && !candidate.After(now) {
			lo = mid
		} else {
			hi = mid
		}
	}
	latest = c.cron.Next(time.Unix(lo, 0).UTC())
	next = c.cron.Next(now)
	for cursor := due; cursor.Before(latest); {
		missed++
		if missed >= cronCountLimit {
			break
		}
		cursor = c.cron.Next(cursor)
		if cursor.IsZero() {
			break
		}
	}
	return latest, next, missed
}

func recurrence(s q.HarvestSchedule) Recurrence {
	return Recurrence{s.IntervalSeconds, s.Cron, s.AnchorAt.UTC(), s.Policy}
}

func (e *Engine) CreateSchedule(ctx context.Context, p config.Principal, in ScheduleInput) (q.HarvestSchedule, error) {
	e.admission.RLock()
	defer e.admission.RUnlock()
	if e.draining.Load() {
		return q.HarvestSchedule{}, ErrDraining
	}
	return tx(ctx, e, func(db *q.Queries) (q.HarvestSchedule, error) {
		w, err := e.resolve(ctx, db, p, in.Request)
		if err != nil {
			return q.HarvestSchedule{}, err
		}
		if w.target.State != "active" {
			return q.HarvestSchedule{}, ErrConflict
		}
		r := in.Recurrence
		r.AnchorAt = pgTime(r.AnchorAt)
		r.Cron = strings.Join(strings.Fields(r.Cron), " ")
		if w.capability.PointInTimeOnly && r.Policy != "point_in_time" {
			return q.HarvestSchedule{}, ErrInvalid
		}
		c, err := parseCadence(r, w.capability.MinInterval)
		if err != nil {
			return q.HarvestSchedule{}, err
		}
		return db.InsertSchedule(ctx, q.InsertScheduleParams{
			ID: uuid.NewV7(), TargetID: w.target.ID, Namespace: w.target.Namespace, Capability: w.capability.Capability,
			RequestSchema: w.capability.RequestSchema, ExecutionKey: w.capability.Key(), Params: w.params, Scope: w.scope,
			Fingerprint: w.fingerprint, IntervalSeconds: r.IntervalSeconds, Cron: r.Cron, AnchorAt: r.AnchorAt,
			NextDueAt: c.first(r.AnchorAt), Policy: r.Policy, Enabled: in.Enabled == nil || *in.Enabled,
		})
	})
}

func (e *Engine) Schedule(ctx context.Context, p config.Principal, id uuid.UUID) (q.HarvestSchedule, error) {
	s, err := found(e.queries.Schedule(ctx, q.ScheduleParams{ID: id, Namespaces: p.Namespaces}))
	if err == nil && !canCapability(p, s.Capability) {
		err = ErrNotFound
	}
	return s, err
}

func (e *Engine) Schedules(ctx context.Context, p config.Principal, page Page) ([]q.HarvestSchedule, error) {
	if !canNamespace(p, page.Namespace) {
		return nil, ErrForbidden
	}
	return e.queries.ListSchedules(ctx, q.ListSchedulesParams{
		Namespace: page.Namespace, AfterID: page.After, PageSize: page.Size, Capabilities: p.Capabilities,
	})
}

func (e *Engine) EditSchedule(ctx context.Context, p config.Principal, id uuid.UUID, patch SchedulePatch) (q.HarvestSchedule, error) {
	e.admission.RLock()
	defer e.admission.RUnlock()
	if e.draining.Load() {
		return q.HarvestSchedule{}, ErrDraining
	}
	if patch.Revision < 1 || (patch.Request == nil && patch.Recurrence == nil && patch.Enabled == nil) {
		return q.HarvestSchedule{}, ErrInvalid
	}
	return tx(ctx, e, func(db *q.Queries) (q.HarvestSchedule, error) {
		s, err := found(db.LockSchedule(ctx, q.LockScheduleParams{ID: id, Namespaces: p.Namespaces}))
		if err != nil {
			return s, err
		}
		if !canCapability(p, s.Capability) {
			return s, ErrNotFound
		}
		if s.Revision != patch.Revision {
			return s, ErrConflict
		}
		r := recurrence(s)
		cap, _ := e.Registry.Capability(s.Capability)
		if patch.Request != nil {
			if patch.Request.TargetID != s.TargetID {
				return s, ErrInvalid
			}
			w, err := e.resolve(ctx, db, p, *patch.Request)
			if err != nil {
				return s, err
			}
			cap = w.capability
			s.Capability, s.RequestSchema, s.ExecutionKey = cap.Capability, cap.RequestSchema, cap.Key()
			s.Params, s.Scope, s.Fingerprint = w.params, w.scope, w.fingerprint
		}
		if patch.Recurrence != nil {
			r = *patch.Recurrence
			r.AnchorAt = pgTime(r.AnchorAt)
			r.Cron = strings.Join(strings.Fields(r.Cron), " ")
		}
		if patch.Request != nil || patch.Recurrence != nil {
			if cap.Execute == nil || cap.Key() != s.ExecutionKey {
				return s, ErrUnsupported
			}
			if cap.PointInTimeOnly && r.Policy != "point_in_time" {
				return s, ErrInvalid
			}
			c, err := parseCadence(r, cap.MinInterval)
			if err != nil {
				return s, err
			}
			if patch.Recurrence != nil && r != recurrence(s) {
				s.NextDueAt = c.first(r.AnchorAt)
			}
		}
		if patch.Enabled != nil {
			s.Enabled = *patch.Enabled
		}
		return db.UpdateSchedule(ctx, q.UpdateScheduleParams{
			ID: s.ID, Capability: s.Capability, RequestSchema: s.RequestSchema, ExecutionKey: s.ExecutionKey,
			Params: s.Params, Scope: s.Scope, Fingerprint: s.Fingerprint, IntervalSeconds: r.IntervalSeconds,
			Cron: r.Cron, AnchorAt: r.AnchorAt, Policy: r.Policy, NextDueAt: s.NextDueAt, Enabled: s.Enabled,
		})
	})
}

func (e *Engine) reconcileOne(ctx context.Context, offers []string) (bool, error) {
	return tx(ctx, e, func(db *q.Queries) (bool, error) {
		s, err := db.LockDueSchedule(ctx, offers)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		w, err := e.resolveStored(ctx, db, []string{s.Namespace}, s.TargetID, s.Capability, s.ExecutionKey,
			s.Params, s.Scope, s.Fingerprint)
		if err != nil || w.target.State != "active" {
			return false, err
		}
		now, err := db.DBNow(ctx)
		if err != nil {
			return false, err
		}
		c, err := parseCadence(recurrence(s), w.capability.MinInterval)
		if err != nil {
			return false, err
		}
		if s.NextDueAt.After(now) {
			return false, nil
		}
		latest, next, missed := c.slots(s.NextDueAt.UTC(), now.UTC())
		if next.IsZero() {
			return false, ErrInvalid
		}
		args := w.jobArgs(now, "scheduled")
		args.ScheduleID, args.ScheduleRevision, args.ScheduledFor = &s.ID, &s.Revision, &latest
		if s.Policy == "point_in_time" {
			args.DeadlineAt, args.MaxAttempts = latest.Add(pointWindow), 1
		}
		if args.DeadlineAt.After(now) {
			if _, err := db.InsertJob(ctx, args); err != nil {
				return false, err
			}
		} else {
			missed = min(missed+1, maxMissed)
		}
		return true, db.AdvanceSchedule(ctx, q.AdvanceScheduleParams{ID: s.ID, NextDueAt: next, Missed: missed})
	})
}

// Reconcile is bounded per tick and may run concurrently on two sessions.
func (e *Engine) Reconcile(ctx context.Context) error {
	e.admission.RLock()
	defer e.admission.RUnlock()
	if e.draining.Load() {
		return nil
	}
	if _, err := e.queries.ExpireQueuedJobs(ctx); err != nil {
		return err
	}
	for range 16 {
		processed, err := e.recoverOne(ctx)
		if err != nil {
			return err
		}
		if !processed {
			break
		}
	}
	offers := e.Registry.ExecutableKeys()
	if len(offers) == 0 {
		return nil
	}
	for range 16 {
		processed, err := e.reconcileOne(ctx, offers)
		if err != nil || !processed || e.draining.Load() {
			return err
		}
	}
	return nil
}

func (e *Engine) PrunePermits(ctx context.Context) error { return e.queries.PruneQuotaPermits(ctx) }

func (e *Engine) Prune(ctx context.Context) error {
	if err := e.queries.PruneJobs(ctx); err != nil {
		return err
	}
	if err := e.queries.PruneWorkers(ctx); err != nil {
		return err
	}
	e.admission.RLock()
	defer e.admission.RUnlock()
	if e.policyUncertain {
		return nil
	}
	return e.queries.PruneQuotaBuckets(ctx, quotaKeys(e.network))
}
