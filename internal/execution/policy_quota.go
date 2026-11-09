package execution

import (
	"context"
	"errors"
	"time"

	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/egress"
)

func quotaKeys(network egress.NetworkConfig) []string {
	keys := make([]string, 0, len(network.Quotas))
	for _, quota := range network.Quotas {
		keys = append(keys, quota.ID)
	}
	return keys
}

func refillQuota(cfg egress.QuotaConfig, bucket q.HarvestQuotaBucket, now time.Time) int64 {
	elapsed := min(max(now.Sub(bucket.RefilledAt), 0), 24*time.Hour)
	return min(cfg.Burst*unitsPerToken, bucket.Balance+cfg.PerMinute*elapsed.Microseconds())
}

func reconcileQuotas(ctx context.Context, db *q.Queries, old, next egress.NetworkConfig) error {
	before := make(map[string]egress.QuotaConfig, len(old.Quotas))
	after := make(map[string]egress.QuotaConfig, len(next.Quotas))
	for _, cfg := range old.Quotas {
		before[cfg.ID] = cfg
	}
	for _, cfg := range next.Quotas {
		after[cfg.ID] = cfg
	}
	var changed, added []string
	for k, cfg := range before {
		if cfg != after[k] {
			changed = append(changed, k)
		}
	}
	for k := range after {
		if _, ok := before[k]; !ok {
			added = append(added, k)
			changed = append(changed, k)
		}
	}
	if len(changed) == 0 {
		return nil
	}
	if len(added) != 0 {
		if err := db.EnsureQuotaBuckets(ctx, added); err != nil {
			return err
		}
	}
	buckets, err := db.LockQuotaBuckets(ctx, changed)
	if err != nil {
		return err
	}
	if len(buckets) != len(changed) {
		return errors.New("configured quota buckets unavailable")
	}
	now, err := db.DBNow(ctx)
	if err != nil {
		return err
	}
	balances := make([]int64, 0, len(buckets))
	for _, b := range buckets {
		balance := int64(0)
		if cfg, ok := before[b.Key]; ok {
			balance = refillQuota(cfg, b, now)
		}
		if cfg, ok := after[b.Key]; ok {
			balance = min(balance, cfg.Burst*unitsPerToken)
		}
		// Newly activated and reactivated quotas accrue from zero under the
		// new rate. The accounting row's cooldown and metrics remain intact.
		balances = append(balances, balance)
	}
	return db.SettleQuotaBalances(ctx, q.SettleQuotaBalancesParams{Keys: keysInOrder(buckets), Balances: balances, NowAt: now})
}
