package execution

import (
	"context"
	"crypto/sha256"
	json "encoding/json/v2"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/provider"
)

type Engine struct {
	Registry                 *provider.Registry
	db                       *pgxpool.Pool
	queries                  *q.Queries
	admission                sync.RWMutex
	draining                 atomic.Bool
	workerPolicies           []config.Principal
	network                  egress.NetworkConfig // admission guards the effective policy and all dependent operations.
	policy                   q.HarvestOperationalPolicy
	quotaBindings            map[string]string
	policyUncertain          bool
	retention                config.Retention
	steamEvidence            config.SteamEvidence
	steamMediaV2Namespaces   []string
	steamEventsNamespaces    []string
	steamDetailsV3Namespaces []string
	steamAssetsNamespaces    []string
	consumerPolicies         map[string]config.Principal
	rdap                     rdapCache
}

func New(db *pgxpool.Pool, registry *provider.Registry, principals []config.Principal, retention config.Retention) *Engine {
	return NewWithSteamEvidence(db, registry, principals, retention, config.DefaultSteamEvidence())
}

func NewWithSteamEvidence(db *pgxpool.Pool, registry *provider.Registry, principals []config.Principal, retention config.Retention, evidence config.SteamEvidence) *Engine {
	return NewConfigured(db, registry, config.Config{Principals: principals, Retention: retention, SteamEvidence: evidence})
}

func NewConfigured(db *pgxpool.Pool, registry *provider.Registry, cfg config.Config) *Engine {
	retention, evidence := cfg.Retention, cfg.SteamEvidence
	if retention == (config.Retention{}) {
		retention = config.DefaultRetention()
	}
	consumers := make(map[string]config.Principal)
	var workers []config.Principal
	for _, p := range cfg.Principals {
		if p.Role == "worker" {
			workers = append(workers, config.Principal{Name: p.Name, Namespaces: slices.Clone(p.Namespaces), Capabilities: slices.Clone(p.Capabilities), EgressProfiles: slices.Clone(p.EgressProfiles)})
		} else {
			consumers[p.Name] = config.Principal{Name: p.Name, Role: p.Role, Namespaces: slices.Clone(p.Namespaces), Capabilities: slices.Clone(p.Capabilities)}
		}
	}
	if evidence == (config.SteamEvidence{}) {
		evidence = config.DefaultSteamEvidence()
	}
	return &Engine{Registry: registry, db: db, queries: q.New(db), workerPolicies: workers, retention: retention, steamEvidence: evidence,
		steamMediaV2Namespaces: slices.Clone(cfg.SteamMediaV2Namespaces), steamAssetsNamespaces: slices.Clone(cfg.SteamAssetsNamespaces),
		steamDetailsV3Namespaces: slices.Clone(cfg.SteamDetailsV3Namespaces), steamEventsNamespaces: slices.Clone(cfg.SteamEventsNamespaces), consumerPolicies: consumers}
}

func (e *Engine) offerEnabled(key, namespace string) bool {
	switch key {
	case provider.SteamMediaV2Key:
		return slices.Contains(e.steamMediaV2Namespaces, namespace)
	case provider.SteamDetailsV3Key:
		return slices.Contains(e.steamDetailsV3Namespaces, namespace)
	case provider.SteamEventsKey:
		return slices.Contains(e.steamEventsNamespaces, namespace)
	case provider.SteamAssetsKey:
		return slices.Contains(e.steamAssetsNamespaces, namespace)
	default:
		return true
	}
}

func (e *Engine) Drain() {
	e.draining.Store(true)
	// Wait for any already admitted transaction before checking active
	// leases. A claim must not become visible after the pre-drain sees zero.
	e.admission.Lock()
	e.admission.Unlock()
}

func (e *Engine) ActiveLeases(ctx context.Context) (int64, error) {
	return e.queries.ActiveLeases(ctx)
}

func tx[T any](ctx context.Context, e *Engine, fn func(*q.Queries) (T, error)) (T, error) {
	return txOptions(ctx, e, pgx.TxOptions{}, fn)
}

func txOptions[T any](ctx context.Context, e *Engine, options pgx.TxOptions, fn func(*q.Queries) (T, error)) (T, error) {
	var value T
	err := pgx.BeginTxFunc(ctx, e.db, options, func(t pgx.Tx) error {
		var err error
		value, err = fn(e.queries.WithTx(t))
		return err
	})
	return value, err
}

func found[T any](value T, err error) (T, error) {
	if errors.Is(err, pgx.ErrNoRows) {
		return value, ErrNotFound
	}
	return value, err
}

func canNamespace(p config.Principal, namespace string) bool {
	return slices.Contains(p.Namespaces, namespace)
}

func canCapability(p config.Principal, capability string) bool {
	// A consumer grant with no capability filter covers its granted namespaces.
	return (p.Role != "worker" && len(p.Capabilities) == 0) || slices.Contains(p.Capabilities, capability)
}

func terminal(state string) bool {
	switch state {
	case "succeeded", "partial", "failed", "cancelled":
		return true
	default:
		return false
	}
}

func pgTime(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }

func hashValue(value any) ([]byte, error) {
	data, err := json.Marshal(value, json.Deterministic(true))
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(data)
	return hash[:], nil
}

func UUIDv7(id uuid.UUID) bool { return id[6]>>4 == 7 && id[8]>>6 == 2 }
