package execution

import (
	"context"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
)

const (
	MaxResultPage    = 200
	pageDataBudget   = 2<<20 - 4096
	changeScanWindow = 4096
)

type ResultPage struct {
	Stream string
	After  string
	Limit  int32
}

type ResultIdentity struct {
	TargetID     uuid.UUID `json:"target_id"`
	Capability   string    `json:"capability"`
	OutputSchema int32     `json:"output_schema"`
	ScopeKey     string    `json:"scope_key"`
}

type ObservationView struct {
	ResultIdentity
	ID          uuid.UUID      `json:"id"`
	Scope       jsontext.Value `json:"scope"`
	State       string         `json:"state"`
	ObservedAt  time.Time      `json:"observed_at"`
	ReceivedAt  time.Time      `json:"received_at"`
	Data        jsontext.Value `json:"data,omitzero"`
	ContentHash string         `json:"content_hash,omitempty"`
	Source      string         `json:"source"`
	Evidence    string         `json:"evidence,omitempty"`
	Sample      bool           `json:"sample"`
	JobID       uuid.UUID      `json:"job_id"`
	AttemptID   uuid.UUID      `json:"attempt_id"`
}

func observationView(o q.HarvestObservation) ObservationView {
	return ObservationView{
		ResultIdentity: ResultIdentity{o.TargetID, o.Capability, o.OutputSchema, o.ScopeKey},
		ID:             o.ID, Scope: o.Scope, State: o.State, ObservedAt: o.ObservedAt.UTC(), ReceivedAt: o.ReceivedAt.UTC(),
		Data: o.Data, ContentHash: hex.EncodeToString(o.ContentHash), Source: o.Source, Evidence: o.Evidence,
		Sample: o.Sample, JobID: o.JobID, AttemptID: o.AttemptID,
	}
}

func validScopeKey(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func validatePage(p config.Principal, in ResultPage) error {
	if err := authorizeStream(p, in.Stream); err != nil {
		return err
	}
	if in.Limit < 1 || in.Limit > MaxResultPage {
		return ErrInvalid
	}
	return nil
}

// Bound the number of payloads fetched. A second, exact budget uses the same
// JSON encoder as Fiber to cap the wire size even for expanded JSONB values.
func pagePrefix[T any](rows []T, limit int32, cost func(T) int32) []T {
	var used int
	for i, row := range rows {
		used += int(cost(row))
		if i >= int(limit) || used > pageDataBudget {
			return rows[:i]
		}
	}
	return rows
}

func addPageItem[T any](items *[]T, item T, used *int) (bool, error) {
	data, err := json.Marshal(item)
	if err != nil {
		return false, err
	}
	*used += len(data) + 1
	if *used > pageDataBudget {
		if len(*items) == 0 {
			return false, ErrTooLarge
		}
		return false, nil
	}
	*items = append(*items, item)
	return true, nil
}

func loadObservationViews(ctx context.Context, db *q.Queries, ids []uuid.UUID) (map[uuid.UUID]ObservationView, error) {
	out := make(map[uuid.UUID]ObservationView, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := db.ObservationsByID(ctx, ids)
	if err != nil {
		return nil, err
	}
	if len(rows) != len(ids) {
		return nil, errors.New("missing observation reference")
	}
	for _, row := range rows {
		out[row.ID] = observationView(row)
	}
	return out, nil
}

type CurrentItem struct {
	ObservationView
	Position    int64     `json:"position"`
	ConfirmedAt time.Time `json:"confirmed_at"`
}

type CurrentView struct {
	Items []CurrentItem `json:"items"`
	After string        `json:"after,omitempty"`
	Epoch *uuid.UUID    `json:"epoch,omitempty"`
	Head  string        `json:"head_cursor,omitempty"`
}

type currentKey struct {
	ResultIdentity
	Stream string     `json:"stream"`
	Epoch  uuid.UUID  `json:"epoch"`
	Reset  *uuid.UUID `json:"reset,omitempty"`
}

type SnapshotPage struct {
	ResultPage
	Consumer uuid.UUID
	Reset    uuid.UUID
}

func (e *Engine) CurrentResults(ctx context.Context, p config.Principal, in ResultPage) (CurrentView, error) {
	if err := validatePage(p, in); err != nil {
		return CurrentView{}, err
	}
	return txOptions(ctx, e, readPage, func(db *q.Queries) (CurrentView, error) {
		h, err := db.ChangeHead(ctx, in.Stream)
		if errors.Is(err, pgx.ErrNoRows) && in.After == "" {
			return CurrentView{Items: []CurrentItem{}}, nil
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return CurrentView{}, ErrCursorExpired
		}
		if err != nil {
			return CurrentView{}, err
		}
		return currentResults(ctx, db, p, in, h, nil)
	})
}

func (e *Engine) Snapshot(ctx context.Context, p config.Principal, in SnapshotPage) (CurrentView, error) {
	if err := validatePage(p, in.ResultPage); err != nil {
		return CurrentView{}, err
	}
	if !UUIDv7(in.Consumer) || !UUIDv7(in.Reset) {
		return CurrentView{}, ErrInvalid
	}
	return txOptions(ctx, e, readPage, func(db *q.Queries) (CurrentView, error) {
		h, _, err := ownedConsumer(ctx, db, p, in.Stream, in.Consumer, &in.Reset)
		if err != nil {
			return CurrentView{}, err
		}
		return currentResults(ctx, db, p, in.ResultPage, h, &in.Reset)
	})
}

func currentResults(ctx context.Context, db *q.Queries, p config.Principal, in ResultPage, h q.HarvestChangeHead, reset *uuid.UUID) (CurrentView, error) {
	var key currentKey
	if in.After != "" {
		if decodeCursor(in.After, &key) != nil || key.Stream != in.Stream || !UUIDv7(key.TargetID) ||
			key.Capability == "" || key.OutputSchema < 1 || !validScopeKey(key.ScopeKey) {
			return CurrentView{}, ErrInvalid
		}
		if key.Epoch != h.Epoch || (key.Reset == nil) != (reset == nil) || (reset != nil && *key.Reset != *reset) {
			return CurrentView{}, ErrCursorExpired
		}
	}
	rows, err := db.CurrentResultPage(ctx, q.CurrentResultPageParams{
		Stream: in.Stream, Capabilities: p.Capabilities, AfterTarget: key.TargetID, AfterCapability: key.Capability,
		AfterSchema: key.OutputSchema, AfterScope: key.ScopeKey, PageSize: in.Limit + 1,
	})
	if err != nil {
		return CurrentView{}, err
	}
	prefix := pagePrefix(rows, in.Limit, func(r q.CurrentResultPageRow) int32 { return r.StoredBytes })
	ids := make([]uuid.UUID, 0, len(prefix))
	for _, r := range prefix {
		ids = append(ids, r.ObservationID)
	}
	observations, err := loadObservationViews(ctx, db, ids)
	if err != nil {
		return CurrentView{}, err
	}
	out := CurrentView{Items: make([]CurrentItem, 0, len(ids)), Epoch: &h.Epoch, Head: cursorAt(h, h.LastPosition)}
	var used int
	for _, r := range prefix {
		ok, err := addPageItem(&out.Items, CurrentItem{observations[r.ObservationID], r.Position, r.ConfirmedAt.UTC()}, &used)
		if err != nil {
			return CurrentView{}, err
		}
		if !ok {
			break
		}
	}
	if n := len(out.Items); n > 0 && n < len(rows) {
		out.After = encodeCursor(currentKey{out.Items[n-1].ResultIdentity, in.Stream, h.Epoch, reset})
	}
	return out, nil
}

type HistoryPage struct {
	ResultPage
	Target       *uuid.UUID
	Capability   string
	ScopeKey     string
	Since, Until time.Time
}

type HistoryView struct {
	Items []ObservationView `json:"items"`
	After string            `json:"after,omitempty"`
}

type historyKey struct {
	Stream string    `json:"stream"`
	Epoch  uuid.UUID `json:"epoch"`
	Time   time.Time `json:"time"`
	ID     uuid.UUID `json:"id"`
}

func (e *Engine) ObservationHistory(ctx context.Context, p config.Principal, in HistoryPage) (HistoryView, error) {
	if err := validatePage(p, in.ResultPage); err != nil {
		return HistoryView{}, err
	}
	if len(in.Capability) > 127 || (in.Target != nil && !UUIDv7(*in.Target)) || (in.ScopeKey != "" && !validScopeKey(in.ScopeKey)) {
		return HistoryView{}, ErrInvalid
	}
	if in.Capability != "" && !canCapability(p, in.Capability) {
		return HistoryView{}, ErrForbidden
	}
	if in.Since.IsZero() {
		in.Since = time.Unix(0, 0).UTC()
	}
	if in.Until.IsZero() {
		in.Until = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	if in.Since.After(in.Until) {
		return HistoryView{}, ErrInvalid
	}
	return txOptions(ctx, e, readPage, func(db *q.Queries) (HistoryView, error) {
		out := HistoryView{Items: []ObservationView{}}
		h, err := db.ChangeHead(ctx, in.Stream)
		if errors.Is(err, pgx.ErrNoRows) && in.After == "" {
			return out, nil
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return HistoryView{}, ErrCursorExpired
		}
		if err != nil {
			return HistoryView{}, err
		}
		key := historyKey{Time: pgTime(in.Until).Add(time.Microsecond)}
		if in.After != "" {
			if decodeCursor(in.After, &key) != nil || key.Stream != in.Stream || key.Time.IsZero() || !UUIDv7(key.ID) {
				return HistoryView{}, ErrInvalid
			}
			if key.Epoch != h.Epoch {
				return HistoryView{}, ErrCursorExpired
			}
		}
		rows, err := db.ObservationHistoryPage(ctx, q.ObservationHistoryPageParams{
			Stream: in.Stream, Capabilities: p.Capabilities, Target: in.Target, Capability: in.Capability, ScopeKey: in.ScopeKey,
			Since: in.Since, Until: in.Until, AfterTime: key.Time, AfterID: key.ID, PageSize: in.Limit + 1,
		})
		if err != nil {
			return HistoryView{}, err
		}
		prefix := pagePrefix(rows, in.Limit, func(r q.ObservationHistoryPageRow) int32 { return r.StoredBytes })
		ids := make([]uuid.UUID, 0, len(prefix))
		for _, r := range prefix {
			ids = append(ids, r.ID)
		}
		observations, err := loadObservationViews(ctx, db, ids)
		if err != nil {
			return HistoryView{}, err
		}
		var used int
		for _, r := range prefix {
			ok, err := addPageItem(&out.Items, observations[r.ID], &used)
			if err != nil {
				return HistoryView{}, err
			}
			if !ok {
				break
			}
		}
		if n := len(out.Items); n > 0 && n < len(rows) {
			last := rows[n-1]
			out.After = encodeCursor(historyKey{in.Stream, h.Epoch, last.ReceivedAt.UTC(), last.ID})
		}
		return out, nil
	})
}

type ChangesPage struct {
	ResultPage
	Consumer uuid.UUID
	Reset    *uuid.UUID
	Through  string
}

type ChangeView struct {
	ResultIdentity
	ID             uuid.UUID        `json:"id"`
	Position       int64            `json:"position"`
	Action         string           `json:"action"`
	CommittedAt    time.Time        `json:"committed_at"`
	Observation    *ObservationView `json:"observation,omitempty"`
	TombstoneScope jsontext.Value   `json:"tombstone_scope,omitempty"`
}

type ChangesView struct {
	Items              []ChangeView `json:"items"`
	Epoch              uuid.UUID    `json:"epoch"`
	Next               string       `json:"next_cursor"`
	Head               string       `json:"head_cursor"`
	MinAvailable       int64        `json:"min_available_position"`
	MinAvailableCursor string       `json:"min_available_cursor"`
	HasMore            bool         `json:"has_more"`
}

func (e *Engine) Changes(ctx context.Context, p config.Principal, in ChangesPage) (ChangesView, error) {
	if err := validatePage(p, in.ResultPage); err != nil {
		return ChangesView{}, err
	}
	if !UUIDv7(in.Consumer) || (in.Reset != nil && (!UUIDv7(*in.Reset) || in.Through == "")) {
		return ChangesView{}, ErrInvalid
	}
	return txOptions(ctx, e, readPage, func(db *q.Queries) (ChangesView, error) {
		h, c, err := ownedConsumer(ctx, db, p, in.Stream, in.Consumer, in.Reset)
		if err != nil {
			return ChangesView{}, err
		}
		after, err := parseStreamCursor(in.After, h)
		if err != nil {
			return ChangesView{}, err
		}
		through := h.LastPosition
		if in.Through != "" {
			through, err = parseStreamCursor(in.Through, h)
			if err != nil {
				return ChangesView{}, err
			}
		}
		if through < after || (in.Reset != nil && after < c.ResetBase) {
			return ChangesView{}, ErrInvalid
		}
		scanThrough := after + min(through-after, changeScanWindow)
		rows, err := db.ChangeEventPage(ctx, q.ChangeEventPageParams{
			Stream: in.Stream, AfterPosition: after, ThroughPosition: scanThrough, Capabilities: p.Capabilities, PageSize: in.Limit + 1,
		})
		if err != nil {
			return ChangesView{}, err
		}
		prefix := pagePrefix(rows, in.Limit, func(r q.HarvestChangeEvent) int32 { return r.StoredBytes })
		ids := make([]uuid.UUID, 0, len(prefix))
		for _, r := range prefix {
			if r.ObservationID != nil {
				ids = append(ids, *r.ObservationID)
			}
		}
		observations, err := loadObservationViews(ctx, db, ids)
		if err != nil {
			return ChangesView{}, err
		}
		out := ChangesView{Items: make([]ChangeView, 0, len(prefix)), Epoch: h.Epoch, Head: cursorAt(h, h.LastPosition), MinAvailable: h.MinPosition, MinAvailableCursor: cursorAt(h, h.MinPosition-1)}
		var used int
		for _, r := range prefix {
			item := ChangeView{ResultIdentity: ResultIdentity{r.TargetID, r.Capability, r.OutputSchema, r.ScopeKey},
				ID: r.ID, Position: r.Position, Action: r.Action, CommittedAt: r.CommittedAt.UTC(), TombstoneScope: r.TombstoneScope}
			if r.ObservationID != nil {
				o := observations[*r.ObservationID]
				item.Observation = &o
			}
			ok, err := addPageItem(&out.Items, item, &used)
			if err != nil {
				return ChangesView{}, err
			}
			if !ok {
				break
			}
		}
		next := scanThrough
		if n := len(out.Items); n > 0 && n < len(rows) {
			next = out.Items[n-1].Position
		}
		out.Next, out.HasMore = cursorAt(h, next), next < through
		return out, nil
	})
}
