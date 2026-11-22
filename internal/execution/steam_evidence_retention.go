package execution

import (
	"bytes"
	"context"
	json "encoding/json/v2"
	"slices"
	"time"
	"uuid"

	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/provider"
)

func (e *Engine) admitSteamEvidence(ctx context.Context, db *q.Queries, state q.HarvestSteamEvidenceMaintenance,
	scope q.ConfirmSteamEvidenceParams, staged q.SteamEvidenceStagedAtRow, jobID uuid.UUID) (uuid.UUID, string, error) {
	for reclaimed := false; ; reclaimed = true {
		old, err := db.SteamEvidenceScopeVersions(ctx, q.SteamEvidenceScopeVersionsParams{
			Namespace: scope.Namespace, TargetID: scope.TargetID, Locale: scope.Locale,
			StoreCountry: scope.StoreCountry, EgressRegion: scope.EgressRegion,
		})
		if err != nil {
			return uuid.Nil(), "", err
		}
		rotated, released, eligible := rotateSteamEvidence(old, e.steamEvidence.MaxVersions, scope.ConfirmedAt, scope.AttemptID)
		if !eligible {
			return uuid.Nil(), "superseded", db.DeleteStagedSteamEvidence(ctx, scope.AttemptID)
		}
		usage, err := db.SteamEvidenceBudget(ctx, scope.Namespace)
		if err != nil {
			return uuid.Nil(), "", err
		}
		if fitsSteamEvidenceBudget(usage, e.steamEvidence.NamespaceBytes, e.steamEvidence.GlobalBytes, int64(staged.PayloadBytes)-released) {
			if len(rotated) > 0 {
				if err := db.DeleteSteamEvidenceVersions(ctx, rotated); err != nil {
					return uuid.Nil(), "", err
				}
			}
			id, err := db.ArchiveSteamEvidence(ctx, q.ArchiveSteamEvidenceParams{
				ID: uuid.NewV7(), Namespace: scope.Namespace, TargetID: scope.TargetID, Locale: scope.Locale,
				StoreCountry: scope.StoreCountry, EgressRegion: scope.EgressRegion, JobID: jobID,
				AttemptID: scope.AttemptID, ConfirmedAt: scope.ConfirmedAt,
			})
			if len(rotated) > 0 {
				return id, "rotated", err
			}
			return id, "stored", err
		}
		if reclaimed {
			break
		}
		cutoff, err := e.steamEvidenceCutoff(ctx, db)
		if err != nil {
			return uuid.Nil(), "", err
		}
		if _, err := e.pruneSteamEvidenceVersions(ctx, db, state, cutoff); err != nil {
			return uuid.Nil(), "", err
		}
	}
	if err := db.MarkSteamEvidenceCapacityLimited(ctx, scope.AttemptID); err != nil {
		return uuid.Nil(), "", err
	}
	if staged.CapacityLimited {
		return uuid.Nil(), "", nil
	}
	return uuid.Nil(), "capacity_limited", db.CountSteamEvidenceCapacityRejection(ctx, scope.Namespace)
}

func fitsSteamEvidenceBudget(usage q.SteamEvidenceBudgetRow, namespaceBudget, globalBudget, delta int64) bool {
	return usage.NamespaceBytes+delta <= namespaceBudget && usage.GlobalBytes+delta <= globalBudget
}

func rotateSteamEvidence(old []q.SteamEvidenceScopeVersionsRow, maxVersions int32, incomingAt time.Time, incomingID uuid.UUID) ([]uuid.UUID, int64, bool) {
	keep := int(maxVersions) - 1
	if len(old) <= keep {
		return nil, 0, true
	}
	// A delayed distinct result cannot evict a better-ranked version.
	boundary := old[keep]
	if incomingAt.Before(boundary.LastServerConfirmedAt) ||
		(incomingAt.Equal(boundary.LastServerConfirmedAt) && bytes.Compare(incomingID[:], boundary.LastServerAttemptID[:]) <= 0) {
		return nil, 0, false
	}
	ids := make([]uuid.UUID, 0, len(old)-keep)
	var size int64
	for _, row := range old[keep:] {
		ids = append(ids, row.ID)
		size += int64(row.PayloadBytes)
	}
	return ids, size, true
}

type SteamEvidencePrune struct {
	VersionsRemoved     int
	AttributionsRemoved int
	GlobalBytes         int64
	OverBudget          bool
}

func (e *Engine) PruneSteamEvidence(ctx context.Context) (SteamEvidencePrune, error) {
	e.admission.RLock()
	defer e.admission.RUnlock()
	return tx(ctx, e, func(db *q.Queries) (SteamEvidencePrune, error) {
		state, err := db.LockSteamEvidenceMaintenance(ctx)
		if err != nil {
			return SteamEvidencePrune{}, err
		}
		cutoff, err := e.steamEvidenceCutoff(ctx, db)
		if err != nil {
			return SteamEvidencePrune{}, err
		}
		versions, err := e.pruneSteamEvidenceVersions(ctx, db, state, cutoff)
		if err != nil {
			return SteamEvidencePrune{}, err
		}
		attributions, err := e.pruneSteamEvidenceAttributions(ctx, db, state, cutoff)
		if err != nil {
			return SteamEvidencePrune{}, err
		}
		usage, err := db.SteamEvidenceBudget(ctx, "")
		return SteamEvidencePrune{
			VersionsRemoved: versions, AttributionsRemoved: attributions, GlobalBytes: usage.GlobalBytes,
			OverBudget: usage.GlobalBytes > e.steamEvidence.GlobalBytes || usage.LargestNamespaceBytes > e.steamEvidence.NamespaceBytes,
		}, err
	})
}

var evidenceCursorStart = time.Unix(0, 0).UTC()

func (e *Engine) steamEvidenceCutoff(ctx context.Context, db *q.Queries) (time.Time, error) {
	now, err := db.DBNow(ctx)
	return now.UTC().AddDate(0, 0, -e.steamEvidence.HistoryDays), err
}

func (e *Engine) pruneSteamEvidenceVersions(ctx context.Context, db *q.Queries, state q.HarvestSteamEvidenceMaintenance, cutoff time.Time) (int, error) {
	if e.policyUncertain {
		return 0, nil
	}
	batch := e.steamEvidence.PruneBatch
	rows, err := db.SteamEvidenceExpiryCandidates(ctx, q.SteamEvidenceExpiryCandidatesParams{
		Cutoff: cutoff, ScanAt: state.VersionScanAt, ScanID: state.VersionScanID, BatchSize: batch,
	})
	if err != nil {
		return 0, err
	}
	protected, err := e.trackedSteamEvidence(ctx, db, rows)
	if err != nil {
		return 0, err
	}
	ids := make([]uuid.UUID, 0, len(rows))
	for _, row := range rows {
		if !protected[row.ID] {
			ids = append(ids, row.ID)
		}
	}
	if len(ids) > 0 {
		if err := db.DeleteSteamEvidenceVersions(ctx, ids); err != nil {
			return 0, err
		}
	}
	next := q.AdvanceSteamEvidenceVersionCursorParams{VersionScanAt: evidenceCursorStart}
	if len(rows) == int(batch) {
		last := rows[len(rows)-1]
		next.VersionScanAt, next.VersionScanID = last.LastServerConfirmedAt, last.ID
	}
	return len(ids), db.AdvanceSteamEvidenceVersionCursor(ctx, next)
}

type steamTrackingCandidate struct {
	q.SteamEvidenceExpiryCandidatesRow
	AllowedModes    []string `json:"allowed_modes"`
	InheritedRegion string   `json:"inherited_region"`
}

func (e *Engine) trackedSteamEvidence(ctx context.Context, db *q.Queries, rows []q.SteamEvidenceExpiryCandidatesRow) (map[uuid.UUID]bool, error) {
	capability, ok := e.Registry.Capability("steam.details")
	if !ok || capability.Network == nil || capability.Execute == nil {
		return nil, nil
	}
	candidates := make([]steamTrackingCandidate, 0, len(rows))
	for _, row := range rows {
		if !row.Latest {
			continue
		}
		rule := e.source("steam.details", row.Namespace)
		route := e.route(rule.Profile)
		candidate := steamTrackingCandidate{SteamEvidenceExpiryCandidatesRow: row}
		for _, mode := range []string{"", directFirst, egress.PreferProxy, egress.RequireProxy} {
			if allowedMode(route, capability.Network.AllowDirect, mode) &&
				slices.ContainsFunc(route.Paths, func(path egress.PathConfig) bool {
					return path.Region == row.EgressRegion && allowedTaskPath(rule, path, mode, capability.Network.AllowDirect)
				}) {
				candidate.AllowedModes = append(candidate.AllowedModes, mode)
			}
		}
		if len(candidate.AllowedModes) == 0 {
			continue
		}
		if inherited, valid := resolveEgressRegion(rule, route, ""); valid {
			candidate.InheritedRegion = inherited
		}
		candidates = append(candidates, candidate)
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	// Tracking edits take the shared side of this lock. Keep the eligibility
	// check valid through the deletion and commit without locking Schedule rows.
	if err := db.LockSteamEvidenceTracking(ctx); err != nil {
		return nil, err
	}
	data, err := json.Marshal(candidates)
	if err != nil {
		return nil, err
	}
	ids, err := db.TrackedSteamEvidenceIDs(ctx, q.TrackedSteamEvidenceIDsParams{
		ExecutionKey: capability.Key(), Candidates: data,
		PilotOffer: provider.SteamMediaV2Key, PilotNamespaces: e.steamMediaV2Namespaces,
	})
	if err != nil {
		return nil, err
	}
	protected := make(map[uuid.UUID]bool, len(ids))
	for _, id := range ids {
		protected[id] = true
	}
	return protected, nil
}

func (e *Engine) pruneSteamEvidenceAttributions(ctx context.Context, db *q.Queries, state q.HarvestSteamEvidenceMaintenance, cutoff time.Time) (int, error) {
	batch := e.steamEvidence.PruneBatch
	rows, err := db.SteamEvidenceAttributionExpiryCandidates(ctx, q.SteamEvidenceAttributionExpiryCandidatesParams{
		Cutoff: cutoff, ScanAt: state.AttributionScanAt, ScanID: state.AttributionScanID, BatchSize: batch,
	})
	if err != nil {
		return 0, err
	}
	ids := make([]uuid.UUID, 0, len(rows))
	for _, row := range rows {
		if row.Pinned != nil && !*row.Pinned {
			ids = append(ids, row.AttemptID)
		}
	}
	if len(ids) > 0 {
		if err := db.DeleteSteamEvidenceAttributions(ctx, ids); err != nil {
			return 0, err
		}
	}
	next := q.AdvanceSteamEvidenceAttributionCursorParams{AttributionScanAt: evidenceCursorStart}
	if len(rows) == int(batch) {
		last := rows[len(rows)-1]
		next.AttributionScanAt, next.AttributionScanID = last.ConfirmedAt, last.AttemptID
	}
	return len(ids), db.AdvanceSteamEvidenceAttributionCursor(ctx, next)
}
