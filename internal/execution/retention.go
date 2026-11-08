package execution

import (
	"bytes"
	"context"
	"errors"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
)

func (e *Engine) PruneResults(ctx context.Context) error {
	r := e.retention
	streams, err := e.queries.DueChangeHeads(ctx, q.DueChangeHeadsParams{
		EventRows: r.EventRows, EventBytes: r.EventBytes, HistoryRows: r.HistoryRows, HistoryBytes: r.HistoryBytes,
	})
	if err != nil {
		return err
	}
	for _, stream := range streams {
		if _, err := tx(ctx, e, func(db *q.Queries) (struct{}, error) {
			return struct{}{}, e.pruneResultBatch(ctx, db, stream)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) pruneResultBatch(ctx context.Context, db *q.Queries, stream string) error {
	head, err := db.TryLockChangeHead(ctx, stream)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	now, err := db.DBNow(ctx)
	if err != nil {
		return err
	}
	consumers, err := db.StreamConsumers(ctx, stream)
	if err != nil {
		return err
	}
	events, err := db.OldestChangeEvents(ctx, stream)
	if err != nil {
		return err
	}

	resetFloor := head.LastPosition
	for _, c := range consumers {
		owner, ok := e.consumerPolicies[c.Principal]
		if ok && canNamespace(owner, stream) && resetLive(c, head, now) && bytes.Equal(c.Policy, grantHash(owner)) {
			resetFloor = min(resetFloor, c.ResetBase)
		}
	}
	r := e.retention
	pruneThrough := head.MinPosition - 1
	var removedBytes int64
	for _, event := range events {
		pressure := head.LastPosition-pruneThrough > r.EventRows || head.EventBytes-removedBytes > r.EventBytes
		if !pressure && (event.Position > resetFloor || !event.CommittedAt.Before(now.AddDate(0, 0, -r.EventDays))) {
			break
		}
		pruneThrough = event.Position
		removedBytes += int64(event.StoredBytes)
	}
	if err := db.ExpireConsumers(ctx, q.ExpireConsumersParams{
		Stream: stream, Epoch: head.Epoch, FloorPosition: pruneThrough, NowAt: now,
		ResetFloor: head.LastPosition - resetMaxEvents, ResetBytesFloor: head.TotalEventBytes - resetMaxBytes,
	}); err != nil {
		return err
	}
	if removedBytes != 0 {
		if err := db.DeleteChangePrefix(ctx, q.DeleteChangePrefixParams{Stream: stream, Position: pruneThrough}); err != nil {
			return err
		}
	}

	prune := q.RecordChangePruneParams{Stream: stream, MinPosition: pruneThrough + 1, EventBytesRemoved: removedBytes}
	if head.HistoryRows != 0 {
		detailBefore, sampleBefore := now.AddDate(0, 0, -r.HistoryDays), now.AddDate(0, 0, -r.SampleDays)
		scanUntil := detailBefore
		if sampleBefore.After(scanUntil) {
			scanUntil = sampleBefore
		}
		pressure := head.HistoryRows > r.HistoryRows || head.HistoryBytes > r.HistoryBytes
		if pressure {
			scanUntil = now
		}
		scanAt, scanID := time.Unix(0, 0).UTC(), uuid.Nil()
		if head.HistoryScanAt != nil && head.HistoryScanID != nil {
			scanAt, scanID = *head.HistoryScanAt, *head.HistoryScanID
		}
		removed, err := db.PruneObservations(ctx, q.PruneObservationsParams{
			Stream: stream, ScanAt: scanAt, ScanID: scanID, ScanUntil: scanUntil, Pressure: pressure, DetailBefore: detailBefore, SampleBefore: sampleBefore,
		})
		if err != nil {
			return err
		}
		prune.HistoryRowsRemoved, prune.HistoryBytesRemoved = removed.Rows, removed.Bytes
		if removed.NextID != uuid.Nil() {
			prune.ScanAt, prune.ScanID = &removed.NextTime, &removed.NextID
		}
	}
	return db.RecordChangePrune(ctx, prune)
}

type ResultStorage struct {
	History []q.RetainedHistoryByCapabilityRow `json:"recent_history_by_capability"`
	Stream  *StreamStatus                      `json:"stream,omitempty"`
}

type StreamStatus struct {
	Head             int64      `json:"head"`
	MinAvailable     int64      `json:"min_available"`
	EventRows        int64      `json:"event_rows"`
	EventBytes       int64      `json:"event_bytes_estimate"`
	HistoryRows      int64      `json:"history_rows"`
	HistoryBytes     int64      `json:"history_bytes_estimate"`
	MaxConsumerLag   int64      `json:"max_consumer_lag"`
	ExpiredConsumers int        `json:"expired_consumers"`
	LastPrune        *time.Time `json:"last_prune,omitempty"`
	Warning          string     `json:"warning,omitempty"`
}

func (e *Engine) resultStorage(ctx context.Context, p config.Principal, stream string) (ResultStorage, error) {
	return txOptions(ctx, e, readPage, func(db *q.Queries) (ResultStorage, error) {
		history, err := db.RetainedHistoryByCapability(ctx, q.RetainedHistoryByCapabilityParams{Stream: stream, Capabilities: p.Capabilities})
		out := ResultStorage{History: history}
		// Full-stream totals include all capabilities.
		if err != nil || len(p.Capabilities) != 0 {
			return out, err
		}
		head, err := db.ChangeHead(ctx, stream)
		if errors.Is(err, pgx.ErrNoRows) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		now, err := db.DBNow(ctx)
		if err != nil {
			return out, err
		}
		consumers, err := db.StreamConsumers(ctx, stream)
		if err != nil {
			return out, err
		}
		s := &StreamStatus{
			Head: head.LastPosition, MinAvailable: head.MinPosition, EventRows: head.LastPosition - head.MinPosition + 1,
			EventBytes: head.EventBytes, HistoryRows: head.HistoryRows, HistoryBytes: head.HistoryBytes,
		}
		if head.LastPrunedAt.After(time.Unix(0, 0)) {
			s.LastPrune = &head.LastPrunedAt
		}
		for _, c := range consumers {
			s.MaxConsumerLag = max(s.MaxConsumerLag, head.LastPosition-c.AckPosition)
			owner, ok := e.consumerPolicies[c.Principal]
			if !ok || !canNamespace(owner, stream) || !bytes.Equal(c.Policy, grantHash(owner)) ||
				c.ResetRequired || c.AckPosition < head.MinPosition-1 || c.Epoch != head.Epoch ||
				(c.ResetID != nil && !resetLive(c, head, now)) {
				s.ExpiredConsumers++
			}
		}
		r := e.retention
		if s.EventRows > r.EventRows || s.EventBytes > r.EventBytes || s.HistoryRows > r.HistoryRows || s.HistoryBytes > r.HistoryBytes {
			s.Warning = "retention_pressure"
		}
		out.Stream = s
		return out, nil
	})
}
