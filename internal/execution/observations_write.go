package execution

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"slices"
	"strings"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"

	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/provider"
)

var (
	ErrTooLarge        = errors.New("result_too_large")
	ErrCursorExpired   = errors.New("cursor_expired")
	ErrStoragePressure = errors.New("retention_pressure")
)

func canonicalScope(scope provider.SegmentScope) (provider.SegmentScope, string, error) {
	if !provider.Identifier(scope.Namespace) || !scope.Scope.Valid() || len(scope.Dimensions) > 8 {
		return scope, "", ErrInvalid
	}
	scope.Locale = strings.ReplaceAll(strings.ToLower(scope.Locale), "_", "-")
	scope.Region = strings.ToLower(scope.Region)
	scope.EgressRegion = strings.ToLower(scope.EgressRegion)
	for k, v := range scope.Dimensions {
		if !provider.Identifier(k) || !provider.Identifier(v) {
			return scope, "", ErrInvalid
		}
	}
	if len(scope.Dimensions) == 0 {
		scope.Dimensions = nil
	}
	hash, err := hashValue(scope)
	return scope, hex.EncodeToString(hash), err
}

type acceptedSegment struct {
	provider.Segment
	key           string
	scopeJSON     []byte
	hash          []byte
	action        string
	observationID uuid.UUID
}

func normalizeSegments(cap provider.Capability, task provider.Task, result *provider.Result) ([]acceptedSegment, error) {
	policy := cap.Observation
	if len(result.Segments) > provider.MaxSegments {
		return nil, ErrTooLarge
	}
	declared := make(map[string]bool)
	scopes := policy.Scopes(task)
	if len(scopes) == 0 || len(scopes) > provider.MaxSegments {
		return nil, ErrUnsupported
	}
	for _, scope := range scopes {
		canonical, key, err := canonicalScope(scope)
		if err != nil || canonical.Namespace != task.Namespace ||
			canonical.EgressRegion != strings.ToLower(task.Scope.EgressRegion) ||
			(task.Scope.Region != "" && canonical.Region != strings.ToLower(task.Scope.Region)) || declared[key] {
			return nil, ErrUnsupported
		}
		declared[key] = true
	}
	segments := make([]acceptedSegment, 0, len(result.Segments))
	valid, normalizedBytes := 0, 0
	for _, segment := range result.Segments {
		scope, key, err := canonicalScope(segment.Scope)
		if err != nil || !declared[key] || segment.OutputSchema != cap.OutputSchema ||
			segment.Source != policy.Source || segment.ObservedAt.Year() < 1970 || segment.ObservedAt.Year() > 9999 {
			return nil, ErrInvalid
		}
		declared[key] = false
		segment.Scope, segment.ObservedAt = scope, pgTime(segment.ObservedAt)
		s := acceptedSegment{Segment: segment, key: key}
		if len(s.Data) > policy.MaxDataBytes {
			return nil, ErrTooLarge
		}
		switch s.State {
		case "present", "confirmed_empty":
			if s.State == "present" && (s.Evidence != "" || s.Withdraw) {
				return nil, ErrInvalid
			}
			if s.State == "confirmed_empty" {
				expected := policy.EmptyEvidence
				if s.Withdraw {
					expected = policy.WithdrawEvidence
				}
				if expected == "" || expected != s.Evidence || policy.Sample {
					return nil, ErrInvalid
				}
			}
			var semantic jsontext.Value
			if policy.NormalizeScopedData != nil {
				s.Data, semantic, err = policy.NormalizeScopedData(task, s.Scope, s.State, s.Data)
			} else {
				s.Data, err = policy.NormalizeData(s.State, s.Data)
				semantic = s.Data
			}
			if err != nil || !s.Data.IsValid() || bytes.Equal(s.Data, []byte("null")) {
				return nil, ErrInvalid
			}
			if len(s.Data) > policy.MaxDataBytes || len(semantic) > policy.MaxDataBytes {
				return nil, ErrTooLarge
			}
			if !semantic.IsValid() {
				return nil, ErrInvalid
			}
			s.hash, err = hashValue(struct {
				State string
				Data  any
			}{s.State, semantic})
			if err != nil {
				return nil, ErrInvalid
			}
			valid++
		case "not_modified", "unavailable", "invalid", "not_covered":
			if s.Withdraw || (s.Evidence != "" &&
				((s.State != "unavailable" && s.State != "invalid") || !slices.Contains(policy.DiagnosticEvidence, s.Evidence))) {
				return nil, ErrInvalid
			}
			if len(s.Data) != 0 {
				if s.Evidence == "" || policy.NormalizeDiagnosticData == nil {
					return nil, ErrInvalid
				}
				s.Data, err = policy.NormalizeDiagnosticData(s.Evidence, s.Data)
				if err != nil || len(s.Data) > policy.MaxDataBytes || !s.Data.IsValid() {
					return nil, ErrInvalid
				}
			}
			if s.State == "not_modified" {
				if policy.Sample {
					return nil, ErrInvalid
				}
				valid++
			}
		default:
			return nil, ErrInvalid
		}
		s.scopeJSON, err = json.Marshal(scope, json.Deterministic(true))
		if err != nil {
			return nil, ErrInvalid
		}
		normalizedBytes += len(s.Data)
		if normalizedBytes > provider.MaxCompletionBody {
			return nil, ErrTooLarge
		}
		segments = append(segments, s)
	}
	if (result.State == "succeeded" && valid != len(scopes)) ||
		(result.State == "partial" && (valid == 0 || valid == len(scopes))) ||
		(result.State == "failed" && valid != 0) {
		return nil, ErrInvalid
	}
	slices.SortFunc(segments, func(a, b acceptedSegment) int { return strings.Compare(a.key, b.key) })
	result.Segments = nil
	for _, s := range segments {
		result.Segments = append(result.Segments, s.Segment)
	}
	return segments, nil
}

func (e *Engine) writeObservations(ctx context.Context, db *q.Queries, job q.HarvestJob, attempt q.HarvestAttempt,
	segments []acceptedSegment, sample bool, now time.Time) (time.Time, error) {
	if err := db.LockObservationTarget(ctx, job.TargetID.String()+"/"+job.Capability); err != nil {
		return now, err
	}
	var appendCount, historyBytes, eventCount, eventBytes int64
	for i := range segments {
		s := &segments[i]
		if s.ObservedAt.After(now.Add(2 * time.Minute)) {
			return now, ErrInvalid
		}
		switch s.State {
		case "present", "confirmed_empty", "not_modified":
			prior, err := db.LockCurrentResult(ctx, q.LockCurrentResultParams{
				Stream: job.Namespace, TargetID: job.TargetID, Capability: job.Capability, OutputSchema: s.OutputSchema, ScopeKey: s.key,
			})
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return now, err
			}
			switch {
			case s.Withdraw:
				if prior.Position != 0 {
					s.action = "withdraw"
				}
			case s.State == "not_modified":
				if prior.Position == 0 {
					return now, ErrConflict
				}
				s.action = "confirm"
				s.observationID = prior.ObservationID
			case sample:
				s.action = "sample"
			case bytes.Equal(prior.ContentHash, s.hash):
				s.action = "confirm"
				s.observationID = prior.ObservationID
			default:
				s.action = "upsert"
			}
		default:
			s.action = "diagnostic"
		}
		switch s.action {
		case "diagnostic", "upsert", "sample":
			appendCount++
			historyBytes += int64(s.observationBytes())
		}
		switch s.action {
		case "withdraw", "upsert", "sample":
			eventCount++
			eventBytes += int64(s.eventBytes())
		}
	}
	var head q.HarvestChangeHead
	if appendCount != 0 || eventCount != 0 {
		var err error
		head, err = lockedHead(ctx, db, job.Namespace)
		if err != nil {
			return now, err
		}
		r := e.retention
		// A bounded ingress reserve lets maintenance catch up without letting
		// concurrent writers grow the log indefinitely. Confirmations still work.
		if (eventCount != 0 && (head.LastPosition-head.MinPosition+1+eventCount > r.EventRows+128 ||
			head.EventBytes+eventBytes > r.EventBytes+provider.MaxCompletionBody+(32<<10))) ||
			(appendCount != 0 && (head.HistoryRows+appendCount > r.HistoryRows+128 ||
				head.HistoryBytes+historyBytes > r.HistoryBytes+provider.MaxCompletionBody+(32<<10))) {
			return now, ErrStoragePressure
		}
	}
	now, err := db.DBNow(ctx)
	if err != nil {
		return now, err
	}
	if !validLease(job, attempt, now) {
		return now, ErrFence
	}
	position := head.LastPosition
	for i := range segments {
		s := &segments[i]
		if s.action == "upsert" || s.action == "sample" || s.action == "withdraw" {
			position++
		}
		if err := storeSegment(ctx, db, job, attempt, s, sample, now, position); err != nil {
			return now, err
		}
	}
	if job.Capability == provider.SteamCatalog || job.Capability == "steam.details" {
		ids := make([]uuid.UUID, 0, len(segments))
		for _, s := range segments {
			if s.observationID != uuid.Nil() {
				ids = append(ids, s.observationID)
			}
		}
		if len(ids) != 0 {
			if err := db.RecordAttemptResults(ctx, q.RecordAttemptResultsParams{ID: attempt.ID, ResultObservationIds: ids}); err != nil {
				return now, err
			}
		}
	}
	if appendCount != 0 || eventCount != 0 {
		err = db.AddChangeTotals(ctx, q.AddChangeTotalsParams{
			Position: position, EventBytesDelta: eventBytes, HistoryRowsDelta: appendCount, HistoryBytesDelta: historyBytes, Stream: job.Namespace,
		})
	}
	return now, err
}

func (s acceptedSegment) observationBytes() int32 {
	return int32(len(s.Data) + len(s.scopeJSON) + 768)
}

func (s acceptedSegment) eventBytes() int32 {
	if s.action == "withdraw" {
		return int32(len(s.scopeJSON) + 512)
	}
	return s.observationBytes() + 256
}

func storeSegment(ctx context.Context, db *q.Queries, job q.HarvestJob, attempt q.HarvestAttempt,
	s *acceptedSegment, sample bool, now time.Time, position int64) error {
	switch s.action {
	case "":
		return nil
	case "confirm":
		return db.ConfirmCurrentResult(ctx, q.ConfirmCurrentResultParams{
			Stream: job.Namespace, TargetID: job.TargetID, Capability: job.Capability, OutputSchema: s.OutputSchema, ScopeKey: s.key, ConfirmedAt: now,
		})
	}
	event := q.InsertChangeEventParams{Stream: job.Namespace, Position: position, ID: uuid.NewV7(), Action: s.action,
		TargetID: job.TargetID, Capability: job.Capability, OutputSchema: s.OutputSchema, ScopeKey: s.key, CommittedAt: now, StoredBytes: s.eventBytes()}
	if s.action == "withdraw" {
		event.TombstoneScope = s.scopeJSON
		if err := db.DeleteCurrentResult(ctx, q.DeleteCurrentResultParams{
			Stream: job.Namespace, TargetID: job.TargetID, Capability: job.Capability, OutputSchema: s.OutputSchema, ScopeKey: s.key,
		}); err != nil {
			return err
		}
		return db.InsertChangeEvent(ctx, event)
	}
	id := uuid.NewV7()
	if err := db.InsertObservation(ctx, q.InsertObservationParams{
		ID: id, Stream: job.Namespace, TargetID: job.TargetID, Capability: job.Capability, OutputSchema: s.OutputSchema, ScopeKey: s.key,
		Scope: s.scopeJSON, State: s.State, ObservedAt: s.ObservedAt, ReceivedAt: now, Data: s.Data, ContentHash: s.hash,
		Source: s.Source, Evidence: s.Evidence, Sample: sample && s.action == "sample", JobID: job.ID, AttemptID: attempt.ID, StoredBytes: s.observationBytes(),
	}); err != nil || s.action == "diagnostic" {
		return err
	}
	if err := db.PutCurrentResult(ctx, q.PutCurrentResultParams{
		Stream: job.Namespace, TargetID: job.TargetID, Capability: job.Capability, OutputSchema: s.OutputSchema, ScopeKey: s.key,
		ObservationID: id, ContentHash: s.hash, Position: position, ConfirmedAt: now,
	}); err != nil {
		return err
	}
	event.ObservationID = &id
	s.observationID = id
	return db.InsertChangeEvent(ctx, event)
}
