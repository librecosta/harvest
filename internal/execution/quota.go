package execution

import (
	"bytes"
	"context"
	json "encoding/json/v2"
	"errors"
	"net/http"
	"slices"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/provider"
)

const unitsPerToken = int64(time.Minute / time.Microsecond)

type PermitRequest struct {
	Fence
	egress.Intent
}

type PermitFinish struct {
	Fence
	AttemptID uuid.UUID `json:"attempt_id"`
	egress.Usage
}

func (e *Engine) Permit(ctx context.Context, p config.Principal, id uuid.UUID, in PermitRequest) (egress.Grant, error) {
	if !UUIDv7(in.OperationID) || in.Origin < 0 || in.MaxBytes < 1 || (in.Method != http.MethodGet && in.Method != http.MethodHead) {
		return egress.Grant{}, ErrInvalid
	}
	e.admission.RLock()
	defer e.admission.RUnlock()
	if e.draining.Load() {
		return egress.Grant{}, ErrDraining
	}
	if e.policyUncertain {
		return egress.Grant{}, ErrPolicyUnavailable
	}
	return tx(ctx, e, func(db *q.Queries) (egress.Grant, error) {
		job, attempt, worker, err := lockAttempt(ctx, db, p, id, in.Fence)
		if err != nil {
			return egress.Grant{}, err
		}
		if worker.Draining {
			return egress.Grant{}, ErrDraining
		}
		if job.State == "cancel_requested" {
			return egress.Grant{}, ErrCancelled
		}
		cap, ok := e.Registry.Capability(job.Capability)
		if !ok || cap.Execute == nil || cap.Key() != job.ExecutionKey || cap.Network == nil || !slices.Contains(worker.Offers, job.ExecutionKey) {
			return egress.Grant{}, ErrForbidden
		}
		now, err := db.DBNow(ctx)
		if err != nil {
			return egress.Grant{}, err
		}
		if job.State != "running" || !validLease(job, attempt, now) {
			return egress.Grant{}, ErrFence
		}
		if attempt.NetworkRevision == 0 || attempt.NetworkRevision != e.policy.RoutingRevision {
			return egress.Grant{}, ErrPolicyChanged
		}
		hash, err := hashValue(in.Intent)
		if err != nil {
			return egress.Grant{}, err
		}
		old, err := db.QuotaPermitReplay(ctx, q.QuotaPermitReplayParams{AttemptID: id, OperationID: in.OperationID})
		replay := err == nil
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return egress.Grant{}, err
		}
		if replay && !bytes.Equal(old.RequestHash, hash) {
			return egress.Grant{}, ErrConflict
		}
		if in.Origin >= len(cap.Network.Origins) || in.MaxBytes != cap.Network.MaxBodyBytes {
			return egress.Grant{}, ErrForbidden
		}
		var scope provider.Scope
		if err := json.Unmarshal(job.Scope, &scope); err != nil {
			return egress.Grant{}, err
		}
		rule := e.source(job.Capability, job.Namespace)
		paths, err := decodePaths(worker.NetworkPaths)
		if err != nil {
			return egress.Grant{}, err
		}
		plan, _ := e.plan(p, rule, scope.EgressRegion, paths)
		if plan == nil || in.Profile != plan.Profile {
			return egress.Grant{}, ErrForbidden
		}
		i := slices.IndexFunc(plan.Paths, func(a egress.Advertisement) bool { return a.ID == in.PathID })
		if i < 0 {
			return egress.Grant{}, ErrForbidden
		}
		if plan.Paths[i].State != "healthy" && plan.Paths[i].State != "degraded" {
			return egress.Grant{}, ErrEgressUnavailable
		}
		path, _ := routePath(e.route(plan.Profile), plan.Paths[i])
		if replay {
			grant := egress.Grant{ID: old.ID, ExpiresAt: old.ExpiresAt.UTC()}
			if old.FinishedAt == nil && old.ExpiresAt.After(now) {
				grant.LeaseMS = old.ExpiresAt.Sub(now).Milliseconds()
			}
			return grant, nil
		}
		used, err := db.CountAttemptPermits(ctx, id)
		if err != nil {
			return egress.Grant{}, err
		}
		if used >= int64(cap.Network.MaxRequests) {
			return egress.Grant{}, ErrQuotaBudget
		}
		keys := []string{e.network.GlobalBucket, rule.ProviderBucket, rule.AccountBucket, path.Bucket}
		buckets, err := db.LockQuotaBuckets(ctx, keys)
		if err != nil {
			return egress.Grant{}, err
		}
		if len(buckets) != len(keys) {
			return egress.Grant{}, errors.New("configured quota buckets unavailable")
		}
		now, err = db.DBNow(ctx)
		if err != nil {
			return egress.Grant{}, err
		}
		expires := minTimeQuota(now.Add(egress.MaxPermitTime), attempt.LeaseUntil)
		if expires.Sub(now) < 250*time.Millisecond {
			return egress.Grant{}, ErrFence
		}
		counts, err := db.CountActiveQuota(ctx, q.CountActiveQuotaParams{NowAt: now, Keys: keys})
		if err != nil {
			return egress.Grant{}, err
		}
		if err := resetQuotaMetrics(ctx, db, buckets, now); err != nil {
			return egress.Grant{}, err
		}
		balances, blocked, delay := e.availableQuota(buckets, counts, now)
		if len(blocked) != 0 {
			return egress.Grant{RetryMS: max(1, min(delay, time.Minute).Milliseconds())}, db.DeferQuota(ctx, blocked)
		}
		if err := db.DebitQuota(ctx, q.DebitQuotaParams{Keys: keysInOrder(buckets), Balances: balances, NowAt: now}); err != nil {
			return egress.Grant{}, err
		}
		permit := uuid.NewV7()
		err = db.InsertQuotaPermit(ctx, q.InsertQuotaPermitParams{
			ID: permit, AttemptID: id, OperationID: in.OperationID, RequestHash: hash,
			BucketKeys: keys, AccountKey: rule.AccountBucket, MaxBytes: in.MaxBytes, ExpiresAt: expires,
		})
		return egress.Grant{ID: permit, ExpiresAt: expires.UTC(), LeaseMS: expires.Sub(now).Milliseconds()}, err
	})
}

func keysInOrder(buckets []q.HarvestQuotaBucket) []string {
	keys := make([]string, len(buckets))
	for i, b := range buckets {
		keys[i] = b.Key
	}
	return keys
}

func (e *Engine) availableQuota(buckets []q.HarvestQuotaBucket, counts []q.CountActiveQuotaRow, now time.Time) ([]int64, []string, time.Duration) {
	var balances []int64
	var blocked []string
	var delay time.Duration
	for _, b := range buckets {
		i := slices.IndexFunc(e.network.Quotas, func(c egress.QuotaConfig) bool { return c.ID == b.Key })
		cfg := e.network.Quotas[i]
		tokens := refillQuota(cfg, b, now)
		wait := b.CooldownUntil.Sub(now)
		if tokens < unitsPerToken {
			deficit := unitsPerToken - tokens
			wait = max(wait, time.Duration((deficit+cfg.PerMinute-1)/cfg.PerMinute)*time.Microsecond)
		}
		if slices.ContainsFunc(counts, func(c q.CountActiveQuotaRow) bool { return c.Key == b.Key && c.Active >= cfg.Concurrency }) {
			wait = max(wait, 500*time.Millisecond)
		}
		if wait > 0 {
			blocked = append(blocked, b.Key)
			delay = max(delay, wait)
		}
		balances = append(balances, tokens-unitsPerToken)
	}
	return balances, blocked, delay
}

func (e *Engine) FinishPermit(ctx context.Context, p config.Principal, id uuid.UUID, in PermitFinish) error {
	if !UUIDv7(in.AttemptID) || !UUIDv7(in.JobID) || !UUIDv7(in.WorkerID) || in.Generation < 1 || in.Bytes < 0 || in.LatencyMS < 0 || in.LatencyMS > egress.MaxPermitTime.Milliseconds() ||
		in.ThrottleSeconds < 0 || in.ThrottleSeconds > 300 || (in.Outcome != "throttled" && in.ThrottleSeconds != 0) {
		return ErrInvalid
	}
	var success, refusal, failure int64
	var cooldown time.Duration
	switch in.Outcome {
	case "success":
		success = 1
	case "transport", "upstream", "policy":
		failure = 1
	case "throttled":
		refusal, cooldown = 1, time.Duration(max(30, in.ThrottleSeconds))*time.Second
	case "denied":
		refusal, cooldown = 1, time.Minute
	default:
		return ErrInvalid
	}
	hash, err := hashValue(in.Usage)
	if err != nil {
		return err
	}
	_, err = tx(ctx, e, func(db *q.Queries) (struct{}, error) {
		var zero struct{}
		row, err := found(db.LockQuotaPermitForFinish(ctx, q.LockQuotaPermitForFinishParams{
			PermitID: id, WorkerID: in.WorkerID, Principal: p.Name, AttemptID: in.AttemptID, JobID: in.JobID, Generation: in.Generation,
		}))
		if err != nil {
			return zero, err
		}
		if row.FinishedAt != nil {
			if !bytes.Equal(row.FinishHash, hash) {
				return zero, ErrConflict
			}
			return zero, nil
		}
		if in.Bytes > row.MaxBytes {
			return zero, ErrInvalid
		}
		buckets, err := db.LockQuotaBuckets(ctx, row.BucketKeys)
		if err != nil {
			return zero, err
		}
		now, err := db.DBNow(ctx)
		if err != nil {
			return zero, err
		}
		if err := resetQuotaMetrics(ctx, db, buckets, now); err != nil {
			return zero, err
		}
		until := time.Unix(0, 0).UTC()
		if cooldown > 0 {
			until = now.Add(cooldown)
		}
		if err := db.RecordQuotaOutcome(ctx, q.RecordQuotaOutcomeParams{
			Keys: row.BucketKeys, AccountKey: row.AccountKey, Cooldown: until,
			SuccessCount: success, RefusalCount: refusal, FailureCount: failure, UsedBytes: in.Bytes, LatencyMs: in.LatencyMS,
		}); err != nil {
			return zero, err
		}
		return zero, db.FinishQuotaPermit(ctx, q.FinishQuotaPermitParams{ID: id, FinishHash: hash, NowAt: now})
	})
	return err
}

func resetQuotaMetrics(ctx context.Context, db *q.Queries, buckets []q.HarvestQuotaBucket, now time.Time) error {
	window := now.UTC().Truncate(24 * time.Hour)
	var keys []string
	for _, b := range buckets {
		if b.MetricsFrom.Before(window) {
			keys = append(keys, b.Key)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	return db.ResetQuotaMetrics(ctx, q.ResetQuotaMetricsParams{Keys: keys, WindowStart: window})
}

func minTimeQuota(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
