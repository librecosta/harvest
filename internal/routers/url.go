package routers

import (
	"context"

	"github.com/gofiber/fiber/v3"

	"github.com/librecosta/harvest/internal/handlers"
)

const (
	LivePath  = "/livez"
	ReadyPath = "/readyz"
)

func registerURLs(consumer, worker fiber.Router, h *handlers.API, ready func(context.Context) bool) {
	write := handlers.Role("writer", "operator")
	operator := handlers.Role("operator")
	consumer.Get("/providers", h.Providers)
	consumer.Get("/targets", h.Targets)
	consumer.Get("/targets/:id", h.Target)
	consumer.Post("/targets", write, h.RegisterTarget())
	consumer.Patch("/targets/:id", write, h.EditTarget())
	consumer.Get("/schedules", h.Schedules)
	consumer.Get("/schedules/:id", h.Schedule)
	consumer.Post("/schedules", write, h.CreateSchedule())
	consumer.Patch("/schedules/:id", write, h.EditSchedule())
	consumer.Get("/jobs", h.Jobs)
	consumer.Get("/jobs/:id", h.Job)
	consumer.Post("/jobs", write, h.SubmitJob())
	consumer.Post("/jobs/:id/cancel", operator, h.CancelJob)
	consumer.Post("/jobs/:id/retry", operator, h.RetryJob)
	consumer.Get("/operations/status", operator, h.Operations)

	worker.Get("/readyz", h.WorkerReady(ready))
	worker.Post("/workers/register", h.RegisterWorker())
	worker.Post("/workers/:id/heartbeat", h.HeartbeatWorker())
	worker.Post("/workers/:id/claim", h.Claim)
	worker.Post("/attempts/:id/renew", h.Renew())
	worker.Post("/attempts/:id/complete", h.Complete())
	worker.Post("/attempts/:id/permit", h.Permit())
	worker.Post("/permits/:id/finish", h.FinishPermit())
}
