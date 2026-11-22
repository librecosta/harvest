package handlers

import (
	"strconv"
	"uuid"

	"github.com/gofiber/fiber/v3"

	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/execution"
)

func (a *API) ListSteamEvidence(c fiber.Ctx) error {
	filter := q.ListSteamEvidenceParams{PageSize: 50}
	seen := make(map[string]bool)
	var invalid bool
	c.Request().URI().QueryArgs().VisitAll(func(k, v []byte) {
		key, value := string(k), string(v)
		if seen[key] || value == "" {
			invalid = true
			return
		}
		seen[key] = true
		switch key {
		case "namespace":
			filter.Namespace = value
		case "app_id":
			filter.AppID = value
		case "locale":
			filter.Locale = value
		case "store_country":
			filter.StoreCountry = value
		case "egress_region":
			filter.EgressRegion = value
		case "after", "evidence_id", "target_id":
			id, err := uuid.Parse(value)
			if err != nil || !execution.UUIDv7(id) {
				invalid = true
				return
			}
			switch key {
			case "after":
				filter.AfterID = id
			case "evidence_id":
				filter.EvidenceID = &id
			case "target_id":
				filter.TargetID = &id
			}
		case "limit":
			size, err := strconv.ParseInt(value, 10, 32)
			invalid = invalid || err != nil
			filter.PageSize = int32(size)
		default:
			invalid = true
		}
	})
	if invalid {
		return a.respond(c, 200, nil, execution.ErrInvalid)
	}
	rows, err := a.engine.ListSteamEvidence(c.Context(), request(c).principal, filter)
	return a.respond(c, 200, list(rows, filter.PageSize, func(row q.ListSteamEvidenceRow) (q.ListSteamEvidenceRow, uuid.UUID) {
		return row, row.ID
	}), err)
}

func (a *API) SteamEvidenceRaw(c fiber.Ctx) error {
	id, err := pathID(c)
	if err != nil || c.Request().URI().QueryArgs().Len() != 0 {
		return a.respond(c, 200, nil, execution.ErrInvalid)
	}
	payload, err := a.engine.SteamEvidenceRaw(c.Context(), request(c).principal, id)
	if err != nil {
		return a.respond(c, 200, nil, err)
	}
	c.Set(fiber.HeaderContentType, "application/octet-stream")
	c.Set(fiber.HeaderCacheControl, "no-store")
	c.Set("X-Content-Type-Options", "nosniff")
	c.Set(fiber.HeaderContentDisposition, "attachment; filename=steam-appdetails-"+id.String()+".json")
	return c.Send(payload)
}

func (a *API) SteamEvidenceStorage(c fiber.Ctx) error {
	args := c.Request().URI().QueryArgs()
	namespace := c.Query("namespace")
	if args.Len() != 0 && (args.Len() != 1 || namespace == "") {
		return a.respond(c, 200, nil, execution.ErrInvalid)
	}
	storage, err := a.engine.SteamEvidenceStorage(c.Context(), request(c).principal, namespace)
	return a.respond(c, 200, storage, err)
}
