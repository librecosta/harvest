package handlers

import (
	"time"
	"uuid"

	"github.com/gofiber/fiber/v3"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/execution"
)

type jobView struct {
	ID               uuid.UUID         `json:"id"`
	Namespace        string            `json:"namespace"`
	Request          execution.Request `json:"request"`
	State            string            `json:"state"`
	Cause            string            `json:"cause"`
	AvailableAt      time.Time         `json:"available_at"`
	DeadlineAt       time.Time         `json:"deadline_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
	AttemptCount     int32             `json:"attempt_count"`
	MaxAttempts      int32             `json:"max_attempts"`
	Generation       int64             `json:"generation"`
	Code             string            `json:"code,omitempty"`
	ScheduleID       *uuid.UUID        `json:"schedule_id,omitempty"`
	ScheduleRevision *int64            `json:"schedule_revision,omitempty"`
	ScheduledFor     *time.Time        `json:"scheduled_for,omitempty"`
	OriginJobID      *uuid.UUID        `json:"origin_job_id,omitempty"`
	AcceptedAttempt  *uuid.UUID        `json:"accepted_attempt_id,omitempty"`
}

type attemptView struct {
	ID          uuid.UUID  `json:"id"`
	WorkerID    uuid.UUID  `json:"worker_id"`
	Generation  int64      `json:"generation"`
	State       string     `json:"state"`
	Code        string     `json:"code,omitempty"`
	ClaimedAt   time.Time  `json:"claimed_at"`
	HeartbeatAt time.Time  `json:"heartbeat_at"`
	LeaseUntil  time.Time  `json:"lease_until"`
	EndedAt     *time.Time `json:"ended_at,omitempty"`
}

func showJob(j q.HarvestJob) jobView {
	return jobView{
		ID: j.ID, Namespace: j.Namespace, Request: requestView(j.TargetID, j.Capability, j.RequestSchema, j.Params, j.Scope),
		State: j.State, Cause: j.Cause, AvailableAt: j.AvailableAt, DeadlineAt: j.DeadlineAt, UpdatedAt: j.UpdatedAt,
		AttemptCount: j.AttemptCount, MaxAttempts: j.MaxAttempts, Generation: j.Generation, Code: j.LastCode,
		ScheduleID: j.ScheduleID, ScheduleRevision: j.ScheduleRevision, ScheduledFor: j.ScheduledFor,
		OriginJobID: j.OriginJobID, AcceptedAttempt: j.AcceptedAttemptID,
	}
}

func (a *API) Jobs(c fiber.Ctx) error {
	p, err := page(c)
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	rows, err := a.engine.Jobs(c.Context(), request(c).principal, p)
	return a.respond(c, 200, list(rows, p.Size, func(j q.HarvestJob) (jobView, uuid.UUID) { return showJob(j), j.ID }), err)
}

func (a *API) Job(c fiber.Ctx) error {
	id, err := pathID(c)
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	j, attempts, err := a.engine.Job(c.Context(), request(c).principal, id)
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	waiting, err := a.engine.JobWaitingReason(c.Context(), j)
	history := make([]attemptView, 0, len(attempts))
	for _, a := range attempts {
		history = append(history, attemptView{a.ID, a.WorkerID, a.Generation, a.State, a.ErrorCode, a.ClaimedAt,
			a.HeartbeatAt, a.LeaseUntil, a.EndedAt})
	}
	return a.respond(c, 200, struct {
		Job     jobView       `json:"job"`
		History []attemptView `json:"attempts"`
		Waiting string        `json:"waiting_reason,omitempty"`
	}{showJob(j), history, waiting}, err)
}

func (a *API) SubmitJob() fiber.Handler {
	return action(a, 202, func(c fiber.Ctx, p config.Principal, in execution.Submit) (any, error) {
		j, err := a.engine.SubmitJob(c.Context(), p, c.Get("Idempotency-Key"), in)
		return showJob(j), err
	})
}

func (a *API) CancelJob(c fiber.Ctx) error {
	id, err := pathID(c)
	if err != nil {
		return a.respond(c, 202, nil, err)
	}
	j, err := a.engine.CancelJob(c.Context(), request(c).principal, id)
	return a.respond(c, 202, showJob(j), err)
}

func (a *API) RetryJob(c fiber.Ctx) error {
	id, err := pathID(c)
	if err != nil {
		return a.respond(c, 202, nil, err)
	}
	j, err := a.engine.RetryJob(c.Context(), request(c).principal, c.Get("Idempotency-Key"), id)
	return a.respond(c, 202, showJob(j), err)
}

func (a *API) Operations(c fiber.Ctx) error {
	result, err := a.engine.Operations(c.Context(), request(c).principal, c.Query("namespace"))
	return a.respond(c, 200, result, err)
}
