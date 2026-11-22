package execution

import (
	"context"
	"strconv"
	"uuid"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/provider"
)

func allowSteamEvidenceRead(p config.Principal, namespace string) error {
	if p.Role != "operator" {
		return ErrForbidden
	}
	if !canCapability(p, "steam.details") || (namespace != "" && !canNamespace(p, namespace)) {
		return ErrNotFound
	}
	return nil
}

func (e *Engine) ListSteamEvidence(ctx context.Context, p config.Principal, filter q.ListSteamEvidenceParams) ([]q.ListSteamEvidenceRow, error) {
	if !provider.Identifier(filter.Namespace) || filter.PageSize < 1 || filter.PageSize > 100 ||
		(filter.AfterID != uuid.Nil() && !UUIDv7(filter.AfterID)) ||
		(filter.EvidenceID != nil && !UUIDv7(*filter.EvidenceID)) ||
		(filter.TargetID != nil && !UUIDv7(*filter.TargetID)) ||
		(filter.Locale != "" && filter.Locale != "en" && filter.Locale != "zh-cn") ||
		(filter.StoreCountry != "" && filter.StoreCountry != "CN" && filter.StoreCountry != "US" && filter.StoreCountry != "HK") ||
		(filter.EgressRegion != "" && (!provider.Identifier(filter.EgressRegion) || len(filter.EgressRegion) > 16)) {
		return nil, ErrInvalid
	}
	if filter.AppID != "" {
		id, err := strconv.ParseUint(filter.AppID, 10, 32)
		if err != nil || id == 0 || strconv.FormatUint(id, 10) != filter.AppID {
			return nil, ErrInvalid
		}
	}
	if err := allowSteamEvidenceRead(p, filter.Namespace); err != nil {
		return nil, err
	}
	return e.queries.ListSteamEvidence(ctx, filter)
}

func (e *Engine) SteamEvidenceRaw(ctx context.Context, p config.Principal, id uuid.UUID) ([]byte, error) {
	if err := allowSteamEvidenceRead(p, ""); err != nil {
		return nil, err
	}
	return found(e.queries.SteamEvidenceRaw(ctx, q.SteamEvidenceRawParams{ID: id, Namespaces: p.Namespaces}))
}

type SteamEvidenceStorageView struct {
	Namespace string `json:"namespace,omitempty"`
	q.SteamEvidenceStorageRow
	BudgetBytes int64 `json:"budget_bytes"`
	OverBudget  bool  `json:"over_budget"`
}

func (e *Engine) SteamEvidenceStorage(ctx context.Context, p config.Principal, namespace string) (SteamEvidenceStorageView, error) {
	if namespace != "" && !provider.Identifier(namespace) {
		return SteamEvidenceStorageView{}, ErrInvalid
	}
	if err := allowSteamEvidenceRead(p, namespace); err != nil {
		return SteamEvidenceStorageView{}, err
	}
	return txOptions(ctx, e, readPage, func(db *q.Queries) (SteamEvidenceStorageView, error) {
		budget := e.steamEvidence.NamespaceBytes
		if namespace == "" {
			visible, err := db.SteamEvidenceGlobalVisible(ctx, p.Namespaces)
			if err != nil {
				return SteamEvidenceStorageView{}, err
			}
			if !visible {
				return SteamEvidenceStorageView{}, ErrNotFound
			}
			budget = e.steamEvidence.GlobalBytes
		}
		stats, err := db.SteamEvidenceStorage(ctx, namespace)
		return SteamEvidenceStorageView{Namespace: namespace, SteamEvidenceStorageRow: stats,
			BudgetBytes: budget, OverBudget: stats.LogicalBytes > budget}, err
	})
}
