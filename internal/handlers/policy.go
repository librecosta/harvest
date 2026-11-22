package handlers

import (
	"github.com/gofiber/fiber/v3"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/execution"
)

func (a *API) NetworkPolicy(c fiber.Ctx) error {
	out, err := a.engine.NetworkPolicy(request(c).principal)
	return a.respond(c, 200, out, err)
}

func (a *API) ReplaceNetworkPolicy() fiber.Handler {
	return boundedAction(a, 200, execution.MaxPolicyBody, func(c fiber.Ctx, p config.Principal, in execution.PolicyUpdate) (any, error) {
		return a.engine.ReplaceNetworkPolicy(c.Context(), p, in)
	})
}

func (a *API) EgressPolicy(c fiber.Ctx) error {
	namespace, _, err := workerNodeQuery(c, false)
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	out, err := a.engine.EgressPolicy(c.Context(), request(c).principal, namespace)
	return a.respond(c, 200, out, err)
}

func (a *API) EditEgressPolicy() fiber.Handler {
	return action(a, 200, func(c fiber.Ctx, p config.Principal, in execution.EgressPolicyEdit) (any, error) {
		namespace, _, err := workerNodeQuery(c, false)
		if err != nil {
			return nil, err
		}
		return a.engine.EditEgressPolicy(c.Context(), p, namespace, in)
	})
}
