package execution

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"slices"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/egress"
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
	if !UUIDv7(in.OperationID) || in.Origin < 0 || in.RDAPStage < 0 || in.RDAPStage > egress.RDAPLookup || in.MaxBytes < 1 || (in.Method != http.MethodGet && in.Method != http.MethodHead) {
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
	if in.RDAPStage != 0 {
		e.rdap.mu.Lock()
		defer e.rdap.mu.Unlock()
	}
	grant, err := tx(ctx, e, func(db *q.Queries) (egress.Grant, error) {
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
		scope, err := webJobScope(job)
		if err != nil {
			return egress.Grant{}, err
		}
		var hop *int16
		var registryDomain, endpoint string
		switch {
		case cap.Network.RDAP:
			target, err := db.Target(ctx, q.TargetParams{ID: job.TargetID, Namespaces: []string{job.Namespace}})
			if err != nil {
				return egress.Grant{}, err
			}
			registryDomain, err = egress.RegistrableDomain(target.ExternalKey)
			if err != nil || in.Origin != 0 || in.TargetID != target.ID || target.State != "active" || target.Provider != "web" ||
				target.ResourceType != "domain" || in.Method != http.MethodGet || in.RDAPStage == 0 ||
				in.Previous != (uuid.UUID{}) || in.Destination != "" || in.RequestURI != "" {
				return egress.Grant{}, ErrForbidden
			}
		case cap.Network.WebMethod != "":
			target, err := db.Target(ctx, q.TargetParams{ID: job.TargetID, Namespaces: []string{job.Namespace}})
			if err != nil {
				return egress.Grant{}, err
			}
			if in.Origin != 0 || in.TargetID != target.ID || in.Destination != target.ExternalKey ||
				target.Provider != "web" || target.ResourceType != cap.Resource || target.State != "active" ||
				in.Method != cap.Network.WebMethod || in.RDAPStage != 0 || !cap.Network.WebTarget(target.ExternalKey) ||
				!egress.WebRequestURI(in.RequestURI) || cap.Network.WebPath != "" && !egress.DiscoveryPath(in.RequestURI) {
				return egress.Grant{}, ErrForbidden
			}
			switch {
			case in.Previous == (uuid.UUID{}):
				path, err := approveFirstWebPath(ctx, db, job, target, cap.Network, scope)
				if err != nil {
					return egress.Grant{}, err
				}
				if in.RequestURI != path {
					return egress.Grant{}, ErrForbidden
				}
				hop = new(int16)
			case in.Method != http.MethodGet || !UUIDv7(in.Previous):
				return egress.Grant{}, ErrForbidden
			case !replay:
				n, err := db.WebRedirectHop(ctx, q.WebRedirectHopParams{
					ParentID: in.Previous, AttemptID: id, Digest: egress.RedirectDigest(in.RequestURI),
				})
				if errors.Is(err, pgx.ErrNoRows) {
					return egress.Grant{}, ErrForbidden
				}
				if err != nil {
					return egress.Grant{}, err
				}
				if int32(n) >= cap.Network.MaxRequests {
					return egress.Grant{}, ErrQuotaBudget
				}
				hop = &n
			}
			if cap.Network.Robots {
				if err := authorizeRobots(ctx, db, job, scope, in.RequestURI); err != nil {
					return egress.Grant{}, err
				}
			}
		case in.Origin >= len(cap.Network.Origins) || in.TargetID != (uuid.UUID{}) ||
			in.Previous != (uuid.UUID{}) || in.Destination != "" || in.RequestURI != "" || in.RDAPStage != 0 ||
			cap.Network.FixedPath != "" && in.Method != http.MethodGet:
			return egress.Grant{}, ErrForbidden
		}
		if in.MaxBytes != cap.Network.MaxBodyBytes {
			return egress.Grant{}, ErrForbidden
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
			grant := egress.Grant{ID: old.ID, ExpiresAt: old.ExpiresAt.UTC(), Endpoint: old.RdapEndpoint}
			if old.FinishedAt == nil && old.ExpiresAt.After(now) {
				grant.LeaseMS = old.ExpiresAt.Sub(now).Milliseconds()
			}
			return grant, nil
		}
		if in.RDAPStage != 0 {
			endpoint, err = e.rdap.authorize(in.RDAPStage, registryDomain)
			if err != nil {
				return egress.Grant{}, err
			}
			used, err := db.HasRDAPStage(ctx, q.HasRDAPStageParams{AttemptID: id, RdapStage: in.RDAPStage})
			if err != nil {
				return egress.Grant{}, err
			}
			if used {
				return egress.Grant{}, ErrQuotaBudget
			}
			if in.RDAPStage == egress.RDAPBootstrap && time.Now().Before(e.rdap.leaderUntil) {
				return egress.Grant{RetryMS: 250}, nil
			}
		}
		used, err := db.CountAttemptPermits(ctx, id)
		if err != nil {
			return egress.Grant{}, err
		}
		if hop != nil && *hop == 0 && used != 0 {
			return egress.Grant{}, ErrForbidden
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
		var parent *uuid.UUID
		if in.Previous != (uuid.UUID{}) {
			parent = &in.Previous
		}
		err = db.InsertQuotaPermit(ctx, q.InsertQuotaPermitParams{
			ID: permit, AttemptID: id, OperationID: in.OperationID, RequestHash: hash,
			BucketKeys: keys, AccountKey: rule.AccountBucket, MaxBytes: in.MaxBytes, ExpiresAt: expires,
			WebHop: hop, PreviousPermit: parent, RdapStage: in.RDAPStage, RdapEndpoint: endpoint,
		})
		return egress.Grant{ID: permit, ExpiresAt: expires.UTC(), LeaseMS: expires.Sub(now).Milliseconds(), Endpoint: endpoint}, err
	})
	if err == nil && in.RDAPStage == egress.RDAPBootstrap && grant.LeaseMS > 0 && grant.ID != e.rdap.leader {
		e.rdap.leader, e.rdap.leaderUntil = grant.ID, time.Now().Add(time.Duration(grant.LeaseMS)*time.Millisecond)
	}
	return grant, err
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

const MaxPermitFinishBody = egress.RDAPMaxBytes + MaxBodySize

func (e *Engine) FinishPermit(ctx context.Context, p config.Principal, id uuid.UUID, in PermitFinish) error {
	if !UUIDv7(in.AttemptID) || !UUIDv7(in.JobID) || !UUIDv7(in.WorkerID) || in.Generation < 1 || in.Bytes < 0 || in.LatencyMS < 0 || in.LatencyMS > egress.MaxPermitTime.Milliseconds() ||
		in.ThrottleSeconds < 0 || in.ThrottleSeconds > 300 || (in.Outcome != "throttled" && in.ThrottleSeconds != 0) {
		return ErrInvalid
	}
	if len(in.Bootstrap) > egress.RDAPMaxBytes || len(in.Bootstrap) != 0 && (!in.BootstrapAttempt || in.Outcome != "success") ||
		in.BootstrapAttempt && in.RedirectDigest != "" {
		return ErrInvalid
	}
	var bootstrap egress.Bootstrap
	if in.BootstrapAttempt {
		bootstrap, _ = egress.ParseBootstrap(in.Bootstrap)
		e.rdap.mu.Lock()
		defer e.rdap.mu.Unlock()
	}
	if in.RedirectDigest != "" {
		b, err := hex.DecodeString(in.RedirectDigest)
		if in.Outcome != "success" || err != nil || len(b) != 32 || hex.EncodeToString(b) != in.RedirectDigest {
			return ErrInvalid
		}
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
	var timelyBootstrap bool
	_, err = tx(ctx, e, func(db *q.Queries) (struct{}, error) {
		var zero struct{}
		row, err := found(db.LockQuotaPermitForFinish(ctx, q.LockQuotaPermitForFinishParams{
			PermitID: id, WorkerID: in.WorkerID, Principal: p.Name, AttemptID: in.AttemptID, JobID: in.JobID, Generation: in.Generation,
		}))
		if err != nil {
			return zero, err
		}
		if (row.RdapStage == egress.RDAPBootstrap) != in.BootstrapAttempt {
			return zero, ErrInvalid
		}
		if row.FinishedAt != nil {
			if !bytes.Equal(row.FinishHash, hash) {
				return zero, ErrConflict
			}
			timelyBootstrap = in.BootstrapAttempt && row.FinishedAt.Before(row.ExpiresAt)
			return zero, nil
		}
		if in.Bytes > row.MaxBytes || (in.RedirectDigest != "" && row.WebHop == nil) {
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
		timelyBootstrap = in.BootstrapAttempt && now.Before(row.ExpiresAt)
		return zero, db.FinishQuotaPermit(ctx, q.FinishQuotaPermitParams{ID: id, FinishHash: hash, NowAt: now, RedirectDigest: in.RedirectDigest})
	})
	if err == nil && in.BootstrapAttempt && id == e.rdap.leader {
		e.rdap.leader, e.rdap.leaderUntil = uuid.Nil(), time.Time{}
		if timelyBootstrap && len(bootstrap) != 0 {
			e.rdap.services = bootstrap
			e.rdap.validUntil = time.Now().Add(24 * time.Hour)
			e.rdap.retryAfter = time.Time{}
		} else {
			e.rdap.retryAfter = time.Now().Add(15 * time.Second)
		}
	}
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
