package routers

import (
	"bytes"
	"context"
	"uuid"

	"github.com/gofiber/fiber/v3"
	"github.com/valyala/fasthttp"

	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/handlers"
	"github.com/librecosta/harvest/internal/provider"
)

func Register(app *fiber.App, ready func(context.Context) bool, api *handlers.API) {
	registerHealth(app, ready)
	if api != nil {
		app.Server().HeaderReceived = requestBodyLimit
		consumer := app.Group("/api/v1", api.Authenticate(false))
		worker := app.Group("/internal/v1", api.Authenticate(true))
		registerURLs(consumer, worker, api, ready)
	}
}

// Allow the larger body only on the fenced Worker completion route, before
// fasthttp reads it. Control, public and other Worker routes keep their limit.
func requestBodyLimit(h *fasthttp.RequestHeader) fasthttp.RequestConfig {
	if !h.IsPost() {
		return fasthttp.RequestConfig{}
	}
	path, _, _ := bytes.Cut(h.RequestURI(), []byte("?"))
	id, ok := bytes.CutPrefix(path, []byte("/internal/v1/attempts/"))
	if !ok {
		return fasthttp.RequestConfig{}
	}
	id, ok = bytes.CutSuffix(id, []byte("/complete"))
	if !ok || len(id) != 36 {
		return fasthttp.RequestConfig{}
	}
	parsed, err := uuid.Parse(string(id))
	if err != nil || !execution.UUIDv7(parsed) {
		return fasthttp.RequestConfig{}
	}
	return fasthttp.RequestConfig{MaxRequestBodySize: provider.MaxCompletionBody}
}
