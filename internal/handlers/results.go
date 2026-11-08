package handlers

import (
	"slices"
	"strconv"
	"time"
	"uuid"

	"github.com/gofiber/fiber/v3"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/execution"
)

func resultPage(c fiber.Ctx, extra ...string) (execution.ResultPage, map[string]string, error) {
	in := execution.ResultPage{Limit: 50}
	args := make(map[string]string, len(extra)+3)
	allowed := append([]string{"stream", "after", "limit"}, extra...)
	var invalid bool
	c.Request().URI().QueryArgs().VisitAll(func(k, v []byte) {
		key, val := string(k), string(v)
		if !slices.Contains(allowed, key) || args[key] != "" || len(val) == 0 || len(val) > 1024 {
			invalid = true
			return
		}
		args[key] = val
	})
	in.Stream, in.After = args["stream"], args["after"]
	if val := args["limit"]; val != "" {
		n, err := strconv.ParseInt(val, 10, 32)
		invalid = invalid || err != nil || n < 1 || n > execution.MaxResultPage
		in.Limit = int32(n)
	}
	if invalid || in.Stream == "" {
		return in, nil, execution.ErrInvalid
	}
	return in, args, nil
}

func queryID(raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil || !execution.UUIDv7(id) {
		return uuid.Nil(), execution.ErrInvalid
	}
	return id, nil
}

func (a *API) CurrentResults(c fiber.Ctx) error {
	in, _, err := resultPage(c)
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	out, err := a.engine.CurrentResults(c.Context(), request(c).principal, in)
	return a.respond(c, 200, out, err)
}

func (a *API) ObservationHistory(c fiber.Ctx) error {
	page, args, err := resultPage(c, "target", "capability", "scope", "since", "until")
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	in := execution.HistoryPage{ResultPage: page, Capability: args["capability"], ScopeKey: args["scope"]}
	if target := args["target"]; target != "" {
		id, err := queryID(target)
		if err != nil {
			return a.respond(c, 200, nil, err)
		}
		in.Target = &id
	}
	for key, dst := range map[string]*time.Time{"since": &in.Since, "until": &in.Until} {
		if s := args[key]; s != "" {
			t, err := time.Parse(time.RFC3339Nano, s)
			if err != nil || t.Year() < 1970 {
				return a.respond(c, 200, nil, execution.ErrInvalid)
			}
			*dst = t
		}
	}
	out, err := a.engine.ObservationHistory(c.Context(), request(c).principal, in)
	return a.respond(c, 200, out, err)
}

func (a *API) Changes(c fiber.Ctx) error {
	page, args, err := resultPage(c, "consumer", "reset", "through")
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	id, err := queryID(args["consumer"])
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	in := execution.ChangesPage{ResultPage: page, Consumer: id, Through: args["through"]}
	if token := args["reset"]; token != "" {
		id, err := queryID(token)
		if err != nil {
			return a.respond(c, 200, nil, err)
		}
		in.Reset = &id
	}
	out, err := a.engine.Changes(c.Context(), request(c).principal, in)
	return a.respond(c, 200, out, err)
}

func (a *API) Snapshot(c fiber.Ctx) error {
	page, args, err := resultPage(c, "consumer", "reset")
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	id, err := queryID(args["consumer"])
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	token, err := queryID(args["reset"])
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	out, err := a.engine.Snapshot(c.Context(), request(c).principal, execution.SnapshotPage{ResultPage: page, Consumer: id, Reset: token})
	return a.respond(c, 200, out, err)
}

func (a *API) EnrollConsumer() fiber.Handler {
	return action(a, 200, func(c fiber.Ctx, p config.Principal, in execution.ConsumerEnrollment) (any, error) {
		return a.engine.EnrollConsumer(c.Context(), p, in)
	})
}

func (a *API) StartReset() fiber.Handler {
	type reset struct {
		Stream string `json:"stream"`
	}
	return action(a, 200, func(c fiber.Ctx, p config.Principal, in reset) (any, error) {
		id, err := pathID(c)
		if err != nil {
			return nil, err
		}
		return a.engine.StartReset(c.Context(), p, id, in.Stream)
	})
}

func (a *API) Acknowledge() fiber.Handler {
	return action(a, 200, func(c fiber.Ctx, p config.Principal, in execution.ConsumerAck) (any, error) {
		id, err := pathID(c)
		if err != nil {
			return nil, err
		}
		return a.engine.Acknowledge(c.Context(), p, id, in)
	})
}
