package execution

import (
	"bytes"
	"context"
	"encoding/base64"
	json "encoding/json/v2"
	"errors"
	"slices"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/provider"
)

const (
	resetTTL       = 10 * time.Minute
	resetMaxEvents = 2048
	resetMaxBytes  = 32 << 20
	maxConsumers   = 128
)

var readPage = pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}

type streamCursor struct {
	Stream   string    `json:"stream"`
	Epoch    uuid.UUID `json:"epoch"`
	Position *int64    `json:"position"`
}

func encodeCursor(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		// All cursor values are internal, fixed wire structs.
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeCursor(token string, dst any) error {
	if len(token) == 0 || len(token) > 1024 {
		return ErrInvalid
	}
	data, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || json.Unmarshal(data, dst, json.RejectUnknownMembers(true)) != nil {
		return ErrInvalid
	}
	return nil
}

func cursorAt(h q.HarvestChangeHead, position int64) string {
	return encodeCursor(streamCursor{h.Stream, h.Epoch, &position})
}

func parseStreamCursor(token string, h q.HarvestChangeHead) (int64, error) {
	var cursor streamCursor
	if decodeCursor(token, &cursor) != nil || cursor.Stream != h.Stream || cursor.Position == nil || *cursor.Position < 0 || !UUIDv7(cursor.Epoch) {
		return 0, ErrInvalid
	}
	if cursor.Epoch != h.Epoch || *cursor.Position < h.MinPosition-1 {
		return 0, ErrCursorExpired
	}
	if *cursor.Position > h.LastPosition {
		return 0, ErrInvalid
	}
	return *cursor.Position, nil
}

func lockedHead(ctx context.Context, db *q.Queries, stream string) (q.HarvestChangeHead, error) {
	h, err := db.LockChangeHead(ctx, stream)
	if !errors.Is(err, pgx.ErrNoRows) {
		return h, err
	}
	if err := db.EnsureChangeHead(ctx, q.EnsureChangeHeadParams{Stream: stream, Epoch: uuid.NewV7()}); err != nil {
		return h, err
	}
	return db.LockChangeHead(ctx, stream)
}

func grantHash(p config.Principal) []byte {
	caps := slices.Clone(p.Capabilities)
	slices.Sort(caps)
	hash, _ := hashValue(struct {
		Name, Role   string
		Capabilities []string
	}{p.Name, p.Role, caps})
	return hash
}

func authorizeStream(p config.Principal, stream string) error {
	if !provider.Identifier(stream) {
		return ErrInvalid
	}
	if p.Role == "worker" || !canNamespace(p, stream) {
		return ErrForbidden
	}
	return nil
}

type ConsumerEnrollment struct {
	Stream    string `json:"stream"`
	Name      string `json:"name"`
	Principal string `json:"principal"`
}

type ConsumerView struct {
	ID            uuid.UUID `json:"id"`
	Stream        string    `json:"stream"`
	Name          string    `json:"name"`
	Epoch         uuid.UUID `json:"epoch"`
	ACK           string    `json:"ack"`
	ResetRequired bool      `json:"reset_required"`
	ResetReason   string    `json:"reset_reason,omitempty"`
}

func consumerView(c q.HarvestConsumer, h q.HarvestChangeHead) ConsumerView {
	return ConsumerView{c.ID, c.Stream, c.Name, h.Epoch, cursorAt(h, c.AckPosition), c.ResetRequired, c.ResetReason}
}

func (e *Engine) EnrollConsumer(ctx context.Context, p config.Principal, in ConsumerEnrollment) (ConsumerView, error) {
	if p.Role != "operator" {
		return ConsumerView{}, ErrForbidden
	}
	if err := authorizeStream(p, in.Stream); err != nil {
		return ConsumerView{}, err
	}
	if !provider.Identifier(in.Name) || !provider.Identifier(in.Principal) {
		return ConsumerView{}, ErrInvalid
	}
	owner, ok := e.consumerPolicies[in.Principal]
	if !ok || !canNamespace(owner, in.Stream) {
		return ConsumerView{}, ErrForbidden
	}
	return tx(ctx, e, func(db *q.Queries) (ConsumerView, error) {
		h, err := lockedHead(ctx, db, in.Stream)
		if err != nil {
			return ConsumerView{}, err
		}
		c, err := db.ConsumerByName(ctx, q.ConsumerByNameParams{Stream: in.Stream, Name: in.Name})
		if err == nil {
			if c.Principal != owner.Name {
				return ConsumerView{}, ErrConflict
			}
			switch {
			case c.Epoch != h.Epoch:
				c.ResetRequired, c.ResetReason = true, "epoch_changed"
			case !bytes.Equal(c.Policy, grantHash(owner)):
				c.ResetRequired, c.ResetReason = true, "grant_changed"
			case !c.ResetRequired && c.AckPosition < h.MinPosition-1:
				c.ResetRequired, c.ResetReason = true, "retention_floor"
			}
			return consumerView(c, h), nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return ConsumerView{}, err
		}
		consumers, err := db.StreamConsumers(ctx, in.Stream)
		if err != nil {
			return ConsumerView{}, err
		}
		if len(consumers) >= maxConsumers {
			return ConsumerView{}, ErrStoragePressure
		}
		c, err = db.InsertConsumer(ctx, q.InsertConsumerParams{ID: uuid.NewV7(), Stream: in.Stream, Name: in.Name, Principal: owner.Name, Epoch: h.Epoch, Policy: grantHash(owner)})
		if err != nil {
			return ConsumerView{}, err
		}
		return consumerView(c, h), nil
	})
}

type ResetView struct {
	Reset    uuid.UUID `json:"reset"`
	Epoch    uuid.UUID `json:"epoch"`
	Baseline string    `json:"baseline_cursor"`
	Deadline time.Time `json:"deadline"`
}

func (e *Engine) StartReset(ctx context.Context, p config.Principal, id uuid.UUID, stream string) (ResetView, error) {
	if err := authorizeStream(p, stream); err != nil {
		return ResetView{}, err
	}
	if !UUIDv7(id) {
		return ResetView{}, ErrInvalid
	}
	return tx(ctx, e, func(db *q.Queries) (ResetView, error) {
		h, err := found(db.LockChangeHead(ctx, stream))
		if err != nil {
			return ResetView{}, err
		}
		c, err := found(db.LockConsumer(ctx, q.LockConsumerParams{ID: id, Stream: stream, Principal: p.Name}))
		if err != nil {
			return ResetView{}, err
		}
		now, err := db.DBNow(ctx)
		if err != nil {
			return ResetView{}, err
		}
		token, deadline := uuid.NewV7(), now.Add(resetTTL)
		_, err = db.StartConsumerReset(ctx, q.StartConsumerResetParams{
			ID: c.ID, Epoch: h.Epoch, Policy: grantHash(p), ResetID: &token, ResetBase: h.LastPosition, ResetBytes: h.TotalEventBytes, ResetDeadline: &deadline,
		})
		return ResetView{token, h.Epoch, cursorAt(h, h.LastPosition), deadline.UTC()}, err
	})
}

func resetLive(c q.HarvestConsumer, h q.HarvestChangeHead, now time.Time) bool {
	return c.ResetID != nil && c.ResetDeadline != nil && c.ResetDeadline.After(now) &&
		c.Epoch == h.Epoch && c.ResetBase >= h.MinPosition-1 &&
		h.LastPosition-c.ResetBase <= resetMaxEvents && h.TotalEventBytes-c.ResetBytes <= resetMaxBytes
}

func checkConsumer(c q.HarvestConsumer, h q.HarvestChangeHead, p config.Principal, reset *uuid.UUID, now time.Time) error {
	if c.Epoch != h.Epoch || !bytes.Equal(c.Policy, grantHash(p)) {
		return ErrCursorExpired
	}
	if reset == nil {
		if c.ResetRequired || c.AckPosition < h.MinPosition-1 {
			return ErrCursorExpired
		}
		return nil
	}
	if !resetLive(c, h, now) || *reset != *c.ResetID {
		return ErrCursorExpired
	}
	return nil
}

type ConsumerAck struct {
	Stream string     `json:"stream"`
	Cursor string     `json:"cursor"`
	Reset  *uuid.UUID `json:"reset,omitempty"`
}

func (e *Engine) Acknowledge(ctx context.Context, p config.Principal, id uuid.UUID, in ConsumerAck) (ConsumerView, error) {
	if err := authorizeStream(p, in.Stream); err != nil {
		return ConsumerView{}, err
	}
	if !UUIDv7(id) || (in.Reset != nil && !UUIDv7(*in.Reset)) {
		return ConsumerView{}, ErrInvalid
	}
	return tx(ctx, e, func(db *q.Queries) (ConsumerView, error) {
		h, err := found(db.LockChangeHead(ctx, in.Stream))
		if err != nil {
			return ConsumerView{}, err
		}
		c, err := found(db.LockConsumer(ctx, q.LockConsumerParams{ID: id, Stream: in.Stream, Principal: p.Name}))
		if err != nil {
			return ConsumerView{}, err
		}
		position, err := parseStreamCursor(in.Cursor, h)
		if err != nil {
			return ConsumerView{}, err
		}
		now, err := db.DBNow(ctx)
		if err != nil {
			return ConsumerView{}, err
		}
		// A committed reset ACK can be retried after a lost response.
		if in.Reset != nil && position <= c.AckPosition && checkConsumer(c, h, p, nil, now) == nil {
			return consumerView(c, h), nil
		}
		if err := checkConsumer(c, h, p, in.Reset, now); err != nil {
			return ConsumerView{}, err
		}
		if in.Reset != nil && position < c.ResetBase {
			return ConsumerView{}, ErrInvalid
		}
		c, err = db.AckConsumer(ctx, q.AckConsumerParams{ID: id, Position: position})
		return consumerView(c, h), err
	})
}

func ownedConsumer(ctx context.Context, db *q.Queries, p config.Principal, stream string, id uuid.UUID, reset *uuid.UUID) (q.HarvestChangeHead, q.HarvestConsumer, error) {
	h, err := found(db.ChangeHead(ctx, stream))
	if err != nil {
		return h, q.HarvestConsumer{}, err
	}
	c, err := found(db.Consumer(ctx, q.ConsumerParams{ID: id, Stream: stream, Principal: p.Name}))
	if err != nil {
		return h, c, err
	}
	now, err := db.DBNow(ctx)
	if err != nil {
		return h, c, err
	}
	return h, c, checkConsumer(c, h, p, reset, now)
}
