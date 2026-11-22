package handlers

import (
	"github.com/gofiber/fiber/v3"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/execution"
)

func workerNodeQuery(c fiber.Ctx, allowCursor bool) (namespace, after string, err error) {
	seen := make(map[string]bool, 2)
	c.Request().URI().QueryArgs().VisitAll(func(k, v []byte) {
		key, value := string(k), string(v)
		switch {
		case seen[key] || value == "":
			err = execution.ErrInvalid
		case key == "namespace":
			namespace = value
		case key == "after" && allowCursor:
			after = value
		default:
			err = execution.ErrInvalid
		}
		seen[key] = true
	})
	return
}

func (a *API) WorkerNodes(c fiber.Ctx) error {
	namespace, after, err := workerNodeQuery(c, true)
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	result, err := a.engine.ListWorkerNodes(c.Context(), request(c).principal, namespace, after)
	return a.respond(c, 200, result, err)
}

func (a *API) WorkerNode(c fiber.Ctx) error {
	namespace, _, err := workerNodeQuery(c, false)
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	result, err := a.engine.GetWorkerNode(c.Context(), request(c).principal, namespace, c.Params("node"))
	return a.respond(c, 200, result, err)
}

func (a *API) EditWorkerNode() fiber.Handler {
	return action(a, 200, func(c fiber.Ctx, p config.Principal, edit execution.WorkerNodeEdit) (any, error) {
		namespace, _, err := workerNodeQuery(c, false)
		if err != nil {
			return nil, err
		}
		return a.engine.EditWorkerNode(c.Context(), p, namespace, c.Params("node"), edit)
	})
}
