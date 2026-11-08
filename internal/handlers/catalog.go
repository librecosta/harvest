package handlers

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"slices"
	"time"
	"uuid"

	"github.com/gofiber/fiber/v3"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

func (a *API) Providers(c fiber.Ctx) error {
	descriptions := a.engine.Registry.Describe()
	allowed := request(c).principal.Capabilities
	if len(allowed) != 0 {
		for i := range descriptions {
			descriptions[i].Capabilities = slices.DeleteFunc(descriptions[i].Capabilities, func(cap provider.Info) bool {
				return !slices.Contains(allowed, cap.Capability)
			})
		}
		descriptions = slices.DeleteFunc(descriptions, func(d provider.Description) bool { return len(d.Capabilities) == 0 })
	}
	return a.respond(c, 200, descriptions, nil)
}

func (a *API) Targets(c fiber.Ctx) error {
	p, err := page(c)
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	rows, err := a.engine.Targets(c.Context(), request(c).principal, p)
	return a.respond(c, 200, list(rows, p.Size, func(t q.HarvestTarget) (q.HarvestTarget, uuid.UUID) { return t, t.ID }), err)
}

func (a *API) Target(c fiber.Ctx) error {
	id, err := pathID(c)
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	target, err := a.engine.Target(c.Context(), request(c).principal, id)
	return a.respond(c, 200, target, err)
}

func (a *API) RegisterTarget() fiber.Handler {
	return action(a, 201, func(c fiber.Ctx, p config.Principal, in execution.TargetInput) (any, error) {
		return a.engine.RegisterTarget(c.Context(), p, in)
	})
}

type targetPatch struct {
	Revision int64  `json:"revision"`
	State    string `json:"state"`
}

func (a *API) EditTarget() fiber.Handler {
	return action(a, 200, func(c fiber.Ctx, p config.Principal, in targetPatch) (any, error) {
		id, err := pathID(c)
		if err != nil {
			return nil, err
		}
		return a.engine.EditTarget(c.Context(), p, id, in.Revision, in.State)
	})
}

type scheduleView struct {
	ID         uuid.UUID            `json:"id"`
	Namespace  string               `json:"namespace"`
	Request    execution.Request    `json:"request"`
	Recurrence execution.Recurrence `json:"recurrence"`
	NextDueAt  time.Time            `json:"next_due_at"`
	Enabled    bool                 `json:"enabled"`
	Revision   int64                `json:"revision"`
	Missed     int32                `json:"missed_count"`
}

func requestView(id uuid.UUID, capability string, version int32, params, scope []byte) execution.Request {
	var s provider.Scope
	// The service writes the typed scope, so this is valid for all stored rows.
	_ = json.Unmarshal(scope, &s)
	return execution.Request{TargetID: id, Capability: capability, RequestSchema: version, Params: jsontext.Value(params), Scope: s}
}

func showSchedule(s q.HarvestSchedule) scheduleView {
	return scheduleView{
		ID: s.ID, Namespace: s.Namespace, Request: requestView(s.TargetID, s.Capability, s.RequestSchema, s.Params, s.Scope),
		Recurrence: execution.Recurrence{IntervalSeconds: s.IntervalSeconds, Cron: s.Cron, AnchorAt: s.AnchorAt, Policy: s.Policy},
		NextDueAt:  s.NextDueAt, Enabled: s.Enabled, Revision: s.Revision, Missed: s.MissedCount,
	}
}

func (a *API) Schedules(c fiber.Ctx) error {
	p, err := page(c)
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	rows, err := a.engine.Schedules(c.Context(), request(c).principal, p)
	return a.respond(c, 200, list(rows, p.Size, func(s q.HarvestSchedule) (scheduleView, uuid.UUID) { return showSchedule(s), s.ID }), err)
}

func (a *API) Schedule(c fiber.Ctx) error {
	id, err := pathID(c)
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	s, err := a.engine.Schedule(c.Context(), request(c).principal, id)
	return a.respond(c, 200, showSchedule(s), err)
}

func (a *API) CreateSchedule() fiber.Handler {
	return action(a, 201, func(c fiber.Ctx, p config.Principal, in execution.ScheduleInput) (any, error) {
		s, err := a.engine.CreateSchedule(c.Context(), p, in)
		return showSchedule(s), err
	})
}

func (a *API) EditSchedule() fiber.Handler {
	return action(a, 200, func(c fiber.Ctx, p config.Principal, in execution.SchedulePatch) (any, error) {
		id, err := pathID(c)
		if err != nil {
			return nil, err
		}
		s, err := a.engine.EditSchedule(c.Context(), p, id, in)
		return showSchedule(s), err
	})
}
