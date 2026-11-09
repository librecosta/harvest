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
