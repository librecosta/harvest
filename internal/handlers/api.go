package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	json "encoding/json/v2"
	"errors"
	"log/slog"
	"mime"
	"slices"
	"strconv"
	"strings"
	"time"
	"uuid"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/execution"
)

type API struct {
	engine *execution.Engine
	logger *slog.Logger
	keys   []principalKey
}

type principalKey struct {
	principal config.Principal
	hash      [32]byte
}

func NewAPI(e *execution.Engine, principals []config.Principal, logger *slog.Logger) *API {
	a := &API{engine: e, logger: logger}
	for _, p := range principals {
		key := sha256.Sum256([]byte(p.Token))
		p.Token = ""
		a.keys = append(a.keys, principalKey{p, key})
	}
	return a
}

type requestInfo struct {
	principal config.Principal
	id        string
}

type requestKey struct{}

func (a *API) Authenticate(worker bool) fiber.Handler {
	return func(c fiber.Ctx) error {
		id := uuid.NewV7().String()
		c.Set(fiber.HeaderCacheControl, "no-store")
		c.Set("X-Request-ID", id)
		token, ok := strings.CutPrefix(c.Get(fiber.HeaderAuthorization), "Bearer ")
		var principal config.Principal
		if ok && len(token) >= 32 && len(token) <= 128 {
			hash := sha256.Sum256([]byte(token))
			for _, key := range a.keys {
				if subtle.ConstantTimeCompare(hash[:], key.hash[:]) == 1 {
					principal = key.principal
				}
			}
		}
		if principal.Name == "" {
			c.Set("WWW-Authenticate", "Bearer")
			return problem(c, fiber.StatusUnauthorized, "unauthenticated", id)
		}
		if (principal.Role == "worker") != worker {
			return problem(c, fiber.StatusForbidden, "forbidden", id)
		}
		info := requestInfo{principal, id}
		ctx, cancel := context.WithTimeout(context.WithValue(c.Context(), requestKey{}, info), 3*time.Second)
		defer cancel()
		c.SetContext(ctx)
		return c.Next()
	}
}

func request(c fiber.Ctx) requestInfo {
	info, _ := c.Context().Value(requestKey{}).(requestInfo)
	return info
}

func Role(roles ...string) fiber.Handler {
	return func(c fiber.Ctx) error {
		if info := request(c); !slices.Contains(roles, info.principal.Role) {
			return problem(c, fiber.StatusForbidden, "forbidden", info.id)
		}
		return c.Next()
	}
}

func (a *API) respond(c fiber.Ctx, status int, value any, err error) error {
	if err == nil {
		if status == fiber.StatusNoContent {
			return c.SendStatus(status)
		}
		return c.Status(status).JSON(value)
	}
	info := request(c)
	var pgErr *pgconn.PgError
	switch {
	case errors.Is(err, execution.ErrInvalid):
		return problem(c, 400, err.Error(), info.id)
	case errors.Is(err, execution.ErrForbidden):
		return problem(c, 403, err.Error(), info.id)
	case errors.Is(err, execution.ErrNotFound):
		return problem(c, 404, err.Error(), info.id)
	case errors.Is(err, execution.ErrUnsupported), errors.Is(err, execution.ErrFence),
		errors.Is(err, execution.ErrCancelled), errors.Is(err, execution.ErrConflict), errors.Is(err, execution.ErrTerminal):
		return problem(c, 409, err.Error(), info.id)
	case errors.As(err, &pgErr) && pgErr.Code == "23505":
		return problem(c, 409, "conflict", info.id)
	case errors.Is(err, execution.ErrDraining):
		return problem(c, 503, "draining", info.id)
	default:
		a.logger.Warn("API dependency unavailable", "request_id", info.id, "route", c.Route().Path)
		return problem(c, 503, "internal_unavailable", info.id)
	}
}

func FiberError(c fiber.Ctx, err error) error {
	status := fiber.StatusInternalServerError
	var httpErr *fiber.Error
	if errors.As(err, &httpErr) {
		status = httpErr.Code
	}
	code := "invalid_request"
	switch status {
	case fiber.StatusNotFound:
		code = "not_found"
	case fiber.StatusMethodNotAllowed:
		code = "method_not_allowed"
	case fiber.StatusRequestEntityTooLarge:
		code = "request_too_large"
	}
	if status >= 500 {
		code = "internal_unavailable"
	}
	id := request(c).id
	if id == "" {
		id = uuid.NewV7().String()
		c.Set("X-Request-ID", id)
	}
	c.Set(fiber.HeaderCacheControl, "no-store")
	return problem(c, status, code, id)
}

func problem(c fiber.Ctx, status int, code, id string) error {
	return c.Status(status).JSON(struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
		Retryable bool   `json:"retryable"`
	}{code, strings.ReplaceAll(code, "_", " "), id, status == 503})
}

func action[T any](a *API, status int, run func(fiber.Ctx, config.Principal, T) (any, error)) fiber.Handler {
	return func(c fiber.Ctx) error {
		var in T
		media, _, err := mime.ParseMediaType(c.Get(fiber.HeaderContentType))
		body := bytes.TrimSpace(c.BodyRaw())
		if err != nil || media != "application/json" || c.Get("Content-Encoding") != "" ||
			len(body) > execution.MaxBodySize || len(body) == 0 ||
			body[0] != '{' ||
			json.Unmarshal(body, &in, json.RejectUnknownMembers(true)) != nil {
			return a.respond(c, status, nil, execution.ErrInvalid)
		}
		result, err := run(c, request(c).principal, in)
		return a.respond(c, status, result, err)
	}
}

func pathID(c fiber.Ctx) (uuid.UUID, error) {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil || !execution.UUIDv7(id) {
		return uuid.Nil(), execution.ErrInvalid
	}
	return id, nil
}

func page(c fiber.Ctx) (execution.Page, error) {
	p := execution.Page{Size: 50}
	var bad bool
	seen := make(map[string]bool)
	c.Request().URI().QueryArgs().VisitAll(func(k, v []byte) {
		key, val := string(k), string(v)
		bad = bad || seen[key]
		seen[key] = true
		switch key {
		case "namespace":
			p.Namespace = val
		case "after":
			id, err := uuid.Parse(val)
			bad = bad || err != nil || !execution.UUIDv7(id)
			p.After = id
		case "limit":
			size, err := strconv.ParseInt(val, 10, 32)
			bad = bad || err != nil || size < 1 || size > 200
			p.Size = int32(size)
		default:
			bad = true
		}
	})
	if bad || p.Namespace == "" {
		return p, execution.ErrInvalid
	}
	return p, nil
}

func list[T any, R any](rows []T, limit int32, view func(T) (R, uuid.UUID)) any {
	items := make([]R, 0, len(rows))
	var after *uuid.UUID
	for _, row := range rows {
		item, id := view(row)
		items = append(items, item)
		after = &id
	}
	if len(items) < int(limit) {
		after = nil
	}
	return struct {
		Items []R        `json:"items"`
		After *uuid.UUID `json:"after,omitempty"`
	}{items, after}
}
