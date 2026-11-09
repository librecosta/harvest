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

// Set the route-specific larger request budgets before fasthttp reads the body.
func requestBodyLimit(h *fasthttp.RequestHeader) fasthttp.RequestConfig {
	path, _, _ := bytes.Cut(h.RequestURI(), []byte("?"))
	if bytes.Equal(h.Method(), []byte("PUT")) && bytes.Equal(path, []byte("/api/v1"+networkPolicyPath)) {
		return fasthttp.RequestConfig{MaxRequestBodySize: execution.MaxPolicyBody}
	}
	if !h.IsPost() {
		return fasthttp.RequestConfig{}
	}
	for _, route := range []struct {
		prefix, suffix string
		limit          int
	}{
		{"/internal/v1/attempts/", "/complete", provider.MaxCompletionBody},
		{"/internal/v1/permits/", "/finish", execution.MaxPermitFinishBody},
	} {
		id, ok := bytes.CutPrefix(path, []byte(route.prefix))
		if !ok {
			continue
		}
		id, ok = bytes.CutSuffix(id, []byte(route.suffix))
		if !ok || len(id) != 36 {
			break
		}
		parsed, err := uuid.Parse(string(id))
		if err == nil && execution.UUIDv7(parsed) {
			return fasthttp.RequestConfig{MaxRequestBodySize: route.limit}
		}
	}
	return fasthttp.RequestConfig{}
}
