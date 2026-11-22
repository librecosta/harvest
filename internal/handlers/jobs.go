package handlers

import (
	"slices"
	"strconv"
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
	CreatedAt        time.Time         `json:"created_at"`
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
	ID           uuid.UUID  `json:"id"`
	WorkerID     uuid.UUID  `json:"worker_id"`
	Generation   int64      `json:"generation"`
	State        string     `json:"state"`
	Code         string     `json:"code,omitempty"`
	ClaimedAt    time.Time  `json:"claimed_at"`
	HeartbeatAt  time.Time  `json:"heartbeat_at"`
	LeaseUntil   time.Time  `json:"lease_until"`
	EndedAt      *time.Time `json:"ended_at,omitempty"`
	EgressRegion *string    `json:"egress_region,omitempty"`
}

func (a *API) showJob(j q.HarvestJob) jobView {
	return jobView{
		ID: j.ID, Namespace: j.Namespace, Request: a.requestView(j.TargetID, j.Capability, j.ExecutionKey, j.RequestSchema, j.Params, j.Scope),
		State: j.State, Cause: j.Cause, AvailableAt: j.AvailableAt, DeadlineAt: j.DeadlineAt, UpdatedAt: j.UpdatedAt, CreatedAt: j.CreatedAt,
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
	return a.respond(c, 200, list(rows, p.Size, func(j q.HarvestJob) (jobView, uuid.UUID) { return a.showJob(j), j.ID }), err)
}

func (a *API) CollectionStatus(c fiber.Ctx) error {
	args := c.Request().URI().QueryArgs()
	if args.Len() != 1 || len(c.Query("namespace")) > 63 || c.Query("namespace") == "" {
		return a.respond(c, 200, nil, execution.ErrInvalid)
	}
	out, err := a.engine.CollectionStatus(c.Context(), request(c).principal, c.Query("namespace"))
	return a.respond(c, 200, out, err)
}

func (a *API) CollectionJobs(c fiber.Ctx) error {
	filter := q.ListCollectionJobsParams{PageSize: 21}
	seen, bad := make(map[string]bool), false
	c.Request().URI().QueryArgs().VisitAll(func(k, v []byte) {
		key, value := string(k), string(v)
		bad = bad || seen[key] || len(value) > 80
		seen[key] = true
		switch key {
		case "namespace":
			filter.Namespace = value
		case "capability":
			filter.Capability = value
		case "group":
			filter.JobGroup = value
			bad = bad || !slices.Contains([]string{"queue", "history"}, value)
		case "state":
			filter.State = value
			bad = bad || !slices.Contains([]string{"queued", "running", "retry_wait", "cancel_requested", "succeeded", "partial", "failed", "cancelled"}, value)
		case "before":
			parsed, err := queryID(value)
			bad = bad || err != nil
			filter.BeforeID = &parsed
		case "since", "until":
			parsed, err := time.Parse(time.RFC3339Nano, value)
			bad = bad || err != nil
			if key == "since" {
				filter.Since = &parsed
			} else {
				filter.Until = &parsed
			}
		default:
			bad = true
		}
	})
	if bad || filter.Namespace == "" || filter.Since != nil && filter.Until != nil && !filter.Since.Before(*filter.Until) {
		return a.respond(c, 200, nil, execution.ErrInvalid)
	}
	rows, err := a.engine.CollectionJobs(c.Context(), request(c).principal, filter)
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	out := struct {
		Items  []jobView  `json:"items"`
		Before *uuid.UUID `json:"before,omitempty"`
	}{Items: []jobView{}}
	if len(rows) == int(filter.PageSize) {
		rows = rows[:len(rows)-1]
		out.Before = &rows[len(rows)-1].ID
	}
	for _, row := range rows {
		out.Items = append(out.Items, a.showJob(row))
	}
	return a.respond(c, 200, out, nil)
}

func (a *API) ScheduleJobs(c fiber.Ctx) error {
	id, err := pathID(c)
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	var before *uuid.UUID
	since := time.Unix(0, 0).UTC()
	limit := int32(10)
	bad := false
	seen := make(map[string]bool)
	c.Request().URI().QueryArgs().VisitAll(func(k, v []byte) {
		key, value := string(k), string(v)
		bad = bad || seen[key]
		seen[key] = true
		switch key {
		case "since":
			parsed, err := time.Parse(time.RFC3339Nano, value)
			bad = bad || err != nil || parsed.Before(time.Unix(0, 0))
			since = parsed
		case "before":
			parsed, err := queryID(value)
			bad = bad || err != nil
			before = &parsed
		case "limit":
			size, err := strconv.ParseInt(value, 10, 32)
			bad = bad || err != nil || size < 1 || size > 20
			limit = int32(size)
		default:
			bad = true
		}
	})
	if bad {
		return a.respond(c, 200, nil, execution.ErrInvalid)
	}
	schedule, rows, lastFailure, err := a.engine.ScheduleJobs(c.Context(), request(c).principal, id, before, since, limit)
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	out := struct {
		Schedule    scheduleView `json:"schedule"`
		Items       []jobView    `json:"items"`
		Before      *uuid.UUID   `json:"before,omitempty"`
		LastFailure *jobView     `json:"last_failure,omitempty"`
	}{Schedule: a.showSchedule(schedule), Items: []jobView{}}
	if len(rows) > int(limit) {
		rows = rows[:limit]
		out.Before = &rows[len(rows)-1].ID
	}
	for _, row := range rows {
		out.Items = append(out.Items, a.showJob(row))
	}
	if lastFailure != nil {
		job := a.showJob(*lastFailure)
		out.LastFailure = &job
	}
	return a.respond(c, 200, out, nil)
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
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	results, err := a.engine.JobResults(c.Context(), j)
	history := make([]attemptView, 0, len(attempts))
	for _, a := range attempts {
		history = append(history, attemptView{a.ID, a.WorkerID, a.Generation, a.State, a.ErrorCode, a.ClaimedAt,
			a.HeartbeatAt, a.LeaseUntil, a.EndedAt, a.EgressRegion})
	}
	return a.respond(c, 200, struct {
		Job     jobView                     `json:"job"`
		History []attemptView               `json:"attempts"`
		Waiting string                      `json:"waiting_reason,omitempty"`
		Results []execution.ObservationView `json:"confirmed_results,omitempty"`
	}{a.showJob(j), history, waiting, results}, err)
}

func (a *API) SubmitJob() fiber.Handler {
	return action(a, 202, func(c fiber.Ctx, p config.Principal, in execution.Submit) (any, error) {
		j, err := a.engine.SubmitJob(c.Context(), p, c.Get("Idempotency-Key"), in)
		return a.showJob(j), err
	})
}

func (a *API) CancelJob(c fiber.Ctx) error {
	id, err := pathID(c)
	if err != nil {
		return a.respond(c, 202, nil, err)
	}
	j, err := a.engine.CancelJob(c.Context(), request(c).principal, id)
	return a.respond(c, 202, a.showJob(j), err)
}

func (a *API) RetryJob(c fiber.Ctx) error {
	id, err := pathID(c)
	if err != nil {
		return a.respond(c, 202, nil, err)
	}
	j, err := a.engine.RetryJob(c.Context(), request(c).principal, c.Get("Idempotency-Key"), id)
	return a.respond(c, 202, a.showJob(j), err)
}

func (a *API) Operations(c fiber.Ctx) error {
	result, err := a.engine.Operations(c.Context(), request(c).principal, c.Query("namespace"))
	return a.respond(c, 200, result, err)
}
