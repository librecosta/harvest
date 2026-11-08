package handlers

import (
	"context"

	"github.com/gofiber/fiber/v3"
)

type healthStatus struct {
	Status string `json:"status"`
}

func Live(c fiber.Ctx) error {
	return writeHealth(c, fiber.StatusOK, "ok")
}

func Ready(check func(context.Context) bool) fiber.Handler {
	return func(c fiber.Ctx) error {
		if !check(c.Context()) {
			return writeHealth(c, fiber.StatusServiceUnavailable, "unavailable")
		}
		return writeHealth(c, fiber.StatusOK, "ok")
	}
}

func writeHealth(c fiber.Ctx, code int, status string) error {
	c.Set(fiber.HeaderCacheControl, "no-store")
	return c.Status(code).JSON(healthStatus{Status: status})
}
