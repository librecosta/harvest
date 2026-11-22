package handlers

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"time"
	"uuid"

	"github.com/gofiber/fiber/v3"

	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

func (a *API) StageSteamEvidence(c fiber.Ctx) error {
	id, err := pathID(c)
	if err != nil {
		return a.respond(c, 204, nil, err)
	}
	body := c.BodyRaw()
	if len(body) > provider.MaxSteamEvidenceBytes {
		return a.respond(c, 204, nil, execution.ErrTooLarge)
	}
	hash, hashErr := hex.DecodeString(c.Get(execution.EvidenceSHA256Header))
	observed, timeErr := time.Parse(time.RFC3339Nano, c.Get(execution.EvidenceObservedHeader))
	job, jobErr := uuid.Parse(c.Get(execution.EvidenceJobHeader))
	worker, workerErr := uuid.Parse(c.Get(execution.EvidenceWorkerHeader))
	generation, genErr := strconv.ParseInt(c.Get(execution.EvidenceGenerationHeader), 10, 64)
	if c.Get(fiber.HeaderContentType) != "application/octet-stream" || c.Get(fiber.HeaderContentEncoding) != "" ||
		len(body) == 0 || len(hash) != sha256.Size || errors.Join(hashErr, timeErr, jobErr, workerErr, genErr) != nil {
		a.logger.Info("Steam evidence upload rejected", "attempt_id", id, "code", "invalid_request")
		return a.respond(c, 204, nil, execution.ErrInvalid)
	}
	duplicate, err := a.engine.StageSteamEvidence(c.Context(), request(c).principal, id,
		execution.Fence{JobID: job, WorkerID: worker, Generation: generation}, observed, hash, body)
	switch {
	case err != nil:
		code := "storage_unavailable"
		for _, known := range []error{execution.ErrInvalid, execution.ErrFence, execution.ErrConflict, execution.ErrNotFound, execution.ErrUnsupported, execution.ErrForbidden, execution.ErrSteamEvidenceSource} {
			if errors.Is(err, known) {
				code = known.Error()
				break
			}
		}
		a.logger.Warn("Steam evidence upload rejected", "job_id", job, "attempt_id", id, "code", code)
	case duplicate:
		a.logger.Info("Steam evidence upload duplicate", "job_id", job, "attempt_id", id)
	default:
		a.logger.Info("Steam evidence upload accepted", "job_id", job, "attempt_id", id)
	}
	return a.respond(c, 204, nil, err)
}
