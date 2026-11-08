package routers

import (
	"context"

	"github.com/gofiber/fiber/v3"

	"github.com/librecosta/harvest/internal/handlers"
)

func registerHealth(app *fiber.App, ready func(context.Context) bool) {
	app.Get(LivePath, handlers.Live)
	app.Get(ReadyPath, handlers.Ready(ready))
}
