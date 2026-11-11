package execution

import (
	"errors"
	"testing"
	"time"

	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/egress"
)

func TestSteamQuotaBindingSurvivesSourceRemoval(t *testing.T) {
	network := egress.NetworkConfig{Sources: []egress.SourceConfig{
		{Capability: "steam.details", Namespace: "first", ProviderBucket: "store", AccountBucket: "store_exit"},
		{Capability: "steam.news", Namespace: "first", ProviderBucket: "api", AccountBucket: "daily"},
	}}
	bound, err := extendQuotaBindings(network, nil)
	if err != nil {
		t.Fatal(err)
	}
	bound, err = extendQuotaBindings(egress.NetworkConfig{}, bound)
	if err != nil {
		t.Fatal(err)
	}
	network.Sources = []egress.SourceConfig{{Capability: "steam.players", Namespace: "second", ProviderBucket: "api", AccountBucket: "replacement"}}
	if _, err := extendQuotaBindings(network, bound); !errors.Is(err, ErrPolicyBinding) {
		t.Fatalf("renamed official account got %v, want %v", err, ErrPolicyBinding)
	}
	network.Sources[0].AccountBucket = "daily"
	if _, err := extendQuotaBindings(network, bound); err != nil {
		t.Fatalf("same official budget was rejected in a new namespace: %v", err)
	}
	network.Sources[0] = egress.SourceConfig{Capability: "steam.details", Namespace: "second", ProviderBucket: "replacement", AccountBucket: "other_exit"}
	if _, err := extendQuotaBindings(network, bound); !errors.Is(err, ErrPolicyBinding) {
		t.Fatalf("renamed Storefront provider got %v, want %v", err, ErrPolicyBinding)
	}
}

func TestDailyPermitQuotaUTCBoundary(t *testing.T) {
	midnight := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	engine := Engine{network: egress.NetworkConfig{Quotas: []egress.QuotaConfig{
		{ID: "account", Burst: 4, PerMinute: 60, Concurrency: 2, DailyLimit: 2},
	}}}
	for _, tc := range []struct {
		name   string
		now    time.Time
		issued int64
		wait   time.Duration
	}{
		{"remaining", midnight.Add(-time.Minute), 1, 0},
		{"exhausted", midnight.Add(-time.Minute), 2, time.Minute},
		{"no grant across midnight", midnight.Add(-time.Second), 1, time.Second},
		{"next day", midnight, 2, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := q.HarvestQuotaBucket{Key: "account", Balance: 4 * unitsPerToken, RefilledAt: tc.now,
				MetricsFrom: midnight.Add(-24 * time.Hour), Issued: tc.issued}
			_, blocked, wait := engine.availableQuota([]q.HarvestQuotaBucket{b}, nil, tc.now)
			if wait != tc.wait || (len(blocked) == 0) != (tc.wait == 0) {
				t.Fatalf("blocked=%v wait=%v, want %v", blocked, wait, tc.wait)
			}
		})
	}
}
