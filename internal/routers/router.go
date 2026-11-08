package routers

import (
	"context"

	"github.com/gofiber/fiber/v3"
)

func Register(app *fiber.App, ready func(context.Context) bool) {
	registerHealth(app, ready)
}
