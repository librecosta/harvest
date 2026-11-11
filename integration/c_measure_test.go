//go:build integration

package integration

import (
	"slices"
	"testing"
	"time"
)

type steamMetrics struct {
	calls, permits, bytes int64
	lsn                   string
}

func readSteamMetrics(t *testing.T, p *testPlane) steamMetrics {
	t.Helper()
	var m steamMetrics
	err := p.pool.QueryRow(t.Context(), `SELECT coalesce(sum(calls),0)::bigint,
coalesce(sum(calls) FILTER (WHERE query LIKE '%INSERT INTO harvest.quota_permits%'),0)::bigint,
(SELECT body_bytes FROM harvest.quota_buckets WHERE key='steam_store'),pg_current_wal_insert_lsn()::text
FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND query NOT LIKE '%pg_stat_statements%'`).
		Scan(&m.calls, &m.permits, &m.bytes, &m.lsn)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func recordSteamMetrics(t *testing.T, p *testPlane, before steamMetrics, m *permitMetrics, elapsed time.Duration, heaps []uint64, processes ...*child) {
	t.Helper()
	after := readSteamMetrics(t, p)
	var wal int64
	var connections int
	if err := p.pool.QueryRow(t.Context(), `SELECT pg_wal_lsn_diff($1::text::pg_lsn,$2::text::pg_lsn)::bigint,
(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database())`, after.lsn, before.lsn).Scan(&wal, &connections); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	calls, controlBytes := slices.Clone(m.permits), m.bytes
	m.mu.Unlock()
	if after.permits-before.permits != 6 || len(calls) < 6 {
		t.Fatal("six Storefront grants were not measured")
	}
	slices.Sort(calls)
	p95 := calls[(len(calls)*95+99)/100-1]
	t.Logf("C six Store scopes: %v, %d Permit RPCs p95=%v, SQL=%d WAL=%d B Store bodies=%d B control bodies=%d B PostgreSQL connections=%d",
		elapsed, len(calls), p95, after.calls-before.calls, wal, after.bytes-before.bytes, controlBytes, connections)
	t.Logf("C Go heap server/A/B bytes %v -> %v", heaps, sampleHeap(p, processes...))
}
