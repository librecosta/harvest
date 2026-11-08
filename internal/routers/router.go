package routers

import (
	"context"

	"github.com/gofiber/fiber/v3"

	"github.com/librecosta/harvest/internal/handlers"
)

func Register(app *fiber.App, ready func(context.Context) bool, api *handlers.API) {
	registerHealth(app, ready)
	if api != nil {
		consumer := app.Group("/api/v1", api.Authenticate(false))
		worker := app.Group("/internal/v1", api.Authenticate(true))
		registerURLs(consumer, worker, api, ready)
	}
}
