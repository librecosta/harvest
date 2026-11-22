package execution

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/provider"
)

const (
	EvidenceSHA256Header     = "X-Harvest-Evidence-SHA256"
	EvidenceObservedHeader   = "X-Harvest-Observed-At"
	EvidenceJobHeader        = "X-Harvest-Job-ID"
	EvidenceWorkerHeader     = "X-Harvest-Worker-ID"
	EvidenceGenerationHeader = "X-Harvest-Generation"
)

var ErrSteamEvidenceSource = errors.New("invalid_evidence_source")

func (e *Engine) steamEvidenceTask(ctx context.Context, db *q.Queries, job q.HarvestJob, a q.HarvestAttempt) (provider.Task, error) {
	if job.Capability != "steam.details" || job.RequestSchema != 1 || a.EgressRegion == nil || *a.EgressRegion == "" {
		return provider.Task{}, ErrUnsupported
	}
	work, err := e.resolveStored(ctx, db, []string{job.Namespace}, job.TargetID, job.Capability, job.ExecutionKey, job.Params, job.Scope, job.Fingerprint)
	if err != nil {
		return provider.Task{}, err
	}
	if work.target.Provider != "steam" || work.target.ResourceType != "app" {
		return provider.Task{}, ErrUnsupported
	}
	task, err := work.task()
	if err != nil {
		return provider.Task{}, err
	}
	params, err := work.capability.NormalizeParams(task.Params)
	if err != nil {
		return provider.Task{}, err
	}
	task.Scope, err = provider.NormalizeSteamScope(job.Capability, params, task.Scope)
	return task, err
}

func (e *Engine) StageSteamEvidence(ctx context.Context, p config.Principal, id uuid.UUID, fence Fence, observed time.Time, digest, body []byte) (bool, error) {
	if p.Role != "worker" {
		return false, ErrForbidden
	}
	if len(body) > provider.MaxSteamEvidenceBytes {
		return false, ErrTooLarge
	}
	hash := sha256.Sum256(body)
	if !UUIDv7(id) || !bytes.Equal(hash[:], digest) || len(body) == 0 || observed.IsZero() {
		return false, ErrInvalid
	}
	return tx(ctx, e, func(db *q.Queries) (bool, error) {
		job, a, worker, err := lockAttempt(ctx, db, p, id, fence)
		if err != nil {
			return false, err
		}
		now, err := db.DBNow(ctx)
		if err != nil {
			return false, err
		}
		if !validLease(job, a, now) || job.State != "running" {
			return false, ErrFence
		}
		if !slices.Contains(worker.Offers, job.ExecutionKey) {
			return false, ErrUnsupported
		}
		task, err := e.steamEvidenceTask(ctx, db, job, a)
		if err != nil {
			return false, err
		}
		same, err := db.ExistingSteamEvidenceUpload(ctx, q.ExistingSteamEvidenceUploadParams{AttemptID: id, Sha256: hash[:], Body: body})
		if err == nil {
			if same == nil || !*same {
				return false, ErrConflict
			}
			return true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return false, err
		}
		const clockSkew = time.Minute
		if observed.Before(a.ClaimedAt.Add(-clockSkew)) || observed.After(now.Add(clockSkew)) ||
			!provider.ValidSteamEvidence(task, body) {
			return false, ErrSteamEvidenceSource
		}
		err = db.StageSteamEvidence(ctx, q.StageSteamEvidenceParams{
			AttemptID: id, PayloadSha256: hash[:], Payload: body, ObservedAt: observed.UTC(),
		})
		if err != nil {
			return false, err
		}
		now, err = db.DBNow(ctx)
		if err == nil && !validLease(job, a, now) {
			return false, ErrFence
		}
		return false, err
	})
}

// RecoverSteamEvidence runs after Completion's independent transaction. A failed
// archive leaves staging intact and moves the next attempt behind other work.
func (e *Engine) RecoverSteamEvidence(ctx context.Context, logger *slog.Logger) {
	if err := e.queries.ExpireStagedSteamEvidence(ctx); err != nil {
		logger.Warn("Steam evidence staging expiry unavailable")
	}
	candidates, err := e.queries.SteamEvidenceCandidates(ctx)
	if err != nil {
		logger.Warn("Steam evidence recovery unavailable")
		return
	}
	for _, row := range candidates {
		if ctx.Err() != nil {
			return
		}
		log := logger.With("job_id", row.JobID, "attempt_id", row.AttemptID)
		call, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
		outcome, err := e.archiveSteamEvidence(call, row)
		cancel()
		if err != nil {
			log.Warn("Steam evidence archive failed", "status", "pending_recovery")
			if err := e.queries.DelaySteamEvidence(ctx, row.AttemptID); err != nil {
				log.Warn("Steam evidence recovery delay unavailable")
			}
		} else if outcome != "" {
			log.Info("Steam evidence archive", "status", outcome)
		}
	}
}

func (e *Engine) archiveSteamEvidence(ctx context.Context, row q.SteamEvidenceCandidatesRow) (string, error) {
	e.admission.RLock()
	defer e.admission.RUnlock()
	return tx(ctx, e, func(db *q.Queries) (string, error) {
		job, err := db.LockJob(ctx, q.LockJobParams{ID: row.JobID, Namespaces: []string{row.Namespace}})
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil // Job pruning cascades to staging.
		}
		if err != nil {
			return "", err
		}
		a, err := db.AttemptForGeneration(ctx, q.AttemptForGenerationParams{JobID: job.ID, Generation: row.Generation})
		if err != nil {
			return "", err
		}
		staged, err := db.SteamEvidenceStagedAt(ctx, row.AttemptID)
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		if err != nil {
			return "", err
		}
		if a.State == "running" {
			return "", db.DelaySteamEvidence(ctx, row.AttemptID)
		}
		if a.ID != row.AttemptID || a.EndedAt == nil || len(a.ResultHash) != sha256.Size || a.ReceiptState == nil ||
			(a.State != "succeeded" && a.State != "partial") ||
			(*a.ReceiptState != a.State && !(a.State == "partial" && *a.ReceiptState == "retry_wait")) {
			return "discarded_without_success", db.DeleteStagedSteamEvidence(ctx, row.AttemptID)
		}
		task, err := e.steamEvidenceTask(ctx, db, job, a)
		if err != nil {
			return "", err
		}
		// Evidence lock order is Job, staging, maintenance, then archive rows.
		// Background pruning starts at maintenance and never locks a Job.
		state, err := db.LockSteamEvidenceMaintenance(ctx)
		if err != nil {
			return "", err
		}
		scope := q.ConfirmSteamEvidenceParams{
			AttemptID: a.ID, Namespace: task.Namespace, TargetID: task.TargetID, Locale: task.Scope.Locale,
			StoreCountry: strings.ToUpper(task.Scope.Region), EgressRegion: strings.ToLower(*a.EgressRegion),
			ConfirmedAt: *a.EndedAt,
		}
		id, err := db.ConfirmSteamEvidence(ctx, scope)
		outcome := "reused"
		if errors.Is(err, pgx.ErrNoRows) {
			id, outcome, err = e.admitSteamEvidence(ctx, db, state, scope, staged, job.ID)
			if err == nil && id == uuid.Nil() {
				return outcome, nil
			}
		}
		if err != nil {
			return "", err
		}
		if err := db.AttributeSteamEvidence(ctx, q.AttributeSteamEvidenceParams{
			AttemptID: a.ID, JobID: job.ID, EvidenceID: id, ObservedAt: staged.ObservedAt, ConfirmedAt: *a.EndedAt,
		}); err != nil {
			return "", err
		}
		return outcome, db.DeleteStagedSteamEvidence(ctx, a.ID)
	})
}
