package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

func (c client) uploadSteamEvidence(ctx context.Context, deadline time.Time, base string, claim execution.Claim, candidate provider.SteamEvidence, logger *slog.Logger) {
	logger = logger.With("job_id", claim.JobID, "attempt_id", claim.AttemptID, "bytes", len(candidate.Body))
	logger.Info("Steam evidence captured")
	hash := sha256.Sum256(candidate.Body)
	limit := deadline.Add(-time.Second) // Give Completion the remaining lease budget.
	for range 2 {
		call, cancel := context.WithDeadline(ctx, minTime(limit, time.Now().Add(750*time.Millisecond)))
		err := c.postEvidence(call, base+"/evidence", claim.Fence, candidate, hash)
		cancel()
		if err == nil {
			logger.Info("Steam evidence upload accepted")
			return
		}
		if !transient(err) || ctx.Err() != nil || !time.Now().Before(limit) {
			break
		}
	}
	logger.Warn("Steam evidence upload failed")
}

func (c client) postEvidence(ctx context.Context, path string, fence execution.Fence, candidate provider.SteamEvidence, hash [sha256.Size]byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(candidate.Body))
	if err != nil {
		return responseError{status: http.StatusBadRequest}
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(execution.EvidenceSHA256Header, hex.EncodeToString(hash[:]))
	req.Header.Set(execution.EvidenceObservedHeader, candidate.ObservedAt.Format(time.RFC3339Nano))
	req.Header.Set(execution.EvidenceJobHeader, fence.JobID.String())
	req.Header.Set(execution.EvidenceWorkerHeader, fence.WorkerID.String())
	req.Header.Set(execution.EvidenceGenerationHeader, strconv.FormatInt(fence.Generation, 10))
	_, err = c.send(req, nil)
	return err
}
