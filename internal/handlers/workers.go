package handlers

import (
	"context"

	"github.com/gofiber/fiber/v3"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

func (a *API) RegisterWorker() fiber.Handler {
	return action(a, 204, func(c fiber.Ctx, p config.Principal, in execution.Registration) (any, error) {
		return nil, a.engine.RegisterWorker(c.Context(), p, in)
	})
}

func (a *API) HeartbeatWorker() fiber.Handler {
	return action(a, 200, func(c fiber.Ctx, p config.Principal, in execution.Heartbeat) (any, error) {
		id, err := pathID(c)
		if err != nil {
			return nil, err
		}
		return a.engine.Heartbeat(c.Context(), p, id, in)
	})
}

func (a *API) Claim(c fiber.Ctx) error {
	id, err := pathID(c)
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	result, err := a.engine.Claim(c.Context(), request(c).principal, id)
	if result == nil {
		return a.respond(c, 204, nil, err)
	}
	return a.respond(c, 200, result, err)
}

func (a *API) Renew() fiber.Handler {
	return action(a, 200, func(c fiber.Ctx, p config.Principal, in execution.Fence) (any, error) {
		id, err := pathID(c)
		if err != nil {
			return nil, err
		}
		return a.engine.Renew(c.Context(), p, id, in)
	})
}

func (a *API) Complete() fiber.Handler {
	return boundedAction(a, 200, provider.MaxCompletionBody, func(c fiber.Ctx, p config.Principal, in execution.Completion) (any, error) {
		id, err := pathID(c)
		if err != nil {
			return nil, err
		}
		return a.engine.Complete(c.Context(), p, id, in)
	})
}

func (a *API) Permit() fiber.Handler {
	return action(a, 200, func(c fiber.Ctx, p config.Principal, in execution.PermitRequest) (any, error) {
		id, err := pathID(c)
		if err != nil {
			return nil, err
		}
		return a.engine.Permit(c.Context(), p, id, in)
	})
}

func (a *API) FinishPermit() fiber.Handler {
	return action(a, 204, func(c fiber.Ctx, p config.Principal, in execution.PermitFinish) (any, error) {
		id, err := pathID(c)
		if err != nil {
			return nil, err
		}
		return nil, a.engine.FinishPermit(c.Context(), p, id, in)
	})
}

func (a *API) WorkerReady(ready func(context.Context) bool) fiber.Handler {
	return func(c fiber.Ctx) error {
		err := a.engine.WorkerReadiness(request(c).principal)
		if err == nil && !ready(c.Context()) {
			return problem(c, 503, "internal_unavailable", request(c).id)
		}
		return a.respond(c, 204, nil, err)
	}
}
