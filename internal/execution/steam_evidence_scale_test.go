package execution

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/database"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/provider"
)

func TestSteamEvidenceQueryScaleNative(t *testing.T) {
	path := os.Getenv("HARVEST_NATIVE_EVIDENCE_SCALE_DB_CONFIG")
	if path == "" {
		t.Skip("HARVEST_NATIVE_EVIDENCE_SCALE_DB_CONFIG is unset")
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Host != "127.0.0.1" || !strings.HasSuffix(cfg.Database.Name, "_test") {
		t.Fatal("evidence scale fixture requires isolated loopback PostgreSQL")
	}
	ctx := t.Context()
	db, err := database.Open(ctx, cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := database.CheckSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	const namespace = "evidence_scale"
	exec := func(t *testing.T, sql string, args ...any) {
		t.Helper()
		if _, err := db.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	queries := q.New(db)
	for _, size := range []int{100, 1000, 10000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			exec(t, "DELETE FROM harvest.steam_evidence WHERE namespace=$1", namespace)
			exec(t, "DELETE FROM harvest.targets WHERE namespace=$1", namespace)
			exec(t, `INSERT INTO harvest.targets (id,provider,resource_type,external_key,namespace)
SELECT uuidv7(),'steam','app',(440000 + n)::text,$1 FROM generate_series(1,$2::integer) n`, namespace, size/5)
			exec(t, `WITH expanded AS (
    SELECT t.id,t.namespace,uuidv7() AS attempt_id,uuidv7() AS job_id,
        convert_to(rpad(format('{"%1$s":{"success":true,"data":{"steam_appid":%1$s,"name":"Fixture %2$s"}}}',
            t.external_key,n),4096,' '),'UTF8') body
    FROM harvest.targets t CROSS JOIN generate_series(1,5) n WHERE t.namespace=$1
), archived AS (
    INSERT INTO harvest.steam_evidence (
        id,namespace,target_id,capability,locale,store_country,egress_region,payload_sha256,payload,payload_bytes,
        first_observed_at,last_confirmed_at,first_job_id,first_attempt_id,format_version,last_server_confirmed_at,last_server_attempt_id
    ) SELECT uuidv7(),namespace,id,'steam.details','en','US','cn',sha256(body),body,octet_length(body),now(),now(),
        job_id,attempt_id,1,now(),attempt_id FROM expanded
    RETURNING id,first_job_id,first_attempt_id
)
INSERT INTO harvest.steam_evidence_attribution (attempt_id,job_id,evidence_id,observed_at,confirmed_at)
SELECT first_attempt_id,first_job_id,id,now(),now() FROM archived`, namespace)
			exec(t, "ANALYZE harvest.steam_evidence, harvest.targets, harvest.steam_evidence_usage")
			filter := q.ListSteamEvidenceParams{Namespace: namespace, PageSize: 50}
			head, err := queries.ListSteamEvidence(ctx, filter)
			if err != nil || len(head) != 50 {
				t.Fatalf("metadata seed: %d rows, %v", len(head), err)
			}
			filter.AfterID = head[len(head)-1].ID
			raw := q.SteamEvidenceRawParams{ID: head[0].ID, Namespaces: []string{namespace}}
			measureSteamEvidenceQuery(t, "keyset_50", func(ctx context.Context) error {
				rows, err := queries.ListSteamEvidence(ctx, filter)
				if err == nil && len(rows) != 50 {
					return fmt.Errorf("got %d metadata rows", len(rows))
				}
				return err
			})
			appFilter := q.ListSteamEvidenceParams{Namespace: namespace, AppID: "440001", PageSize: 50}
			measureSteamEvidenceQuery(t, "app_id", func(ctx context.Context) error {
				rows, err := queries.ListSteamEvidence(ctx, appFilter)
				if err == nil && len(rows) != 5 {
					return fmt.Errorf("got %d app rows", len(rows))
				}
				return err
			})
			measureSteamEvidenceQuery(t, "raw_4KiB", func(ctx context.Context) error {
				value, err := queries.SteamEvidenceRaw(ctx, raw)
				if err == nil && len(value) != 4096 {
					return fmt.Errorf("got %d raw bytes", len(value))
				}
				return err
			})
			measureSteamEvidenceQuery(t, "budget_ledger", func(ctx context.Context) error {
				usage, err := queries.SteamEvidenceBudget(ctx, namespace)
				if err == nil && usage.NamespaceBytes != int64(size)*4096 {
					return fmt.Errorf("got %d logical bytes", usage.NamespaceBytes)
				}
				return err
			})
			stats, err := queries.SteamEvidenceStorage(ctx, namespace)
			if err != nil || stats.Versions != int64(size) || stats.Attributions != int64(size) {
				t.Fatalf("storage totals: %+v %v", stats, err)
			}
			t.Logf("logical_bytes=%d payload_column_bytes=%d", stats.LogicalBytes, stats.PayloadColumnBytes)
			if size == 10000 {
				exec(t, "UPDATE harvest.steam_evidence SET last_server_confirmed_at=now()-interval '91 days' WHERE namespace=$1", namespace)
				registry, err := provider.New(provider.Builtins()...)
				if err != nil {
					t.Fatal(err)
				}
				for range 2 {
					engine := New(db, registry, nil, config.Retention{})
					start := time.Now()
					result, err := engine.PruneSteamEvidence(ctx)
					if err != nil || result.VersionsRemoved != 32 {
						t.Fatalf("prune batch after restart: %+v %v", result, err)
					}
					t.Logf("prune_32_with_cascade=%s", time.Since(start))
				}
			}
		})
	}
}

func measureSteamEvidenceQuery(t *testing.T, label string, query func(context.Context) error) {
	t.Helper()
	const samples = 101
	elapsed := make([]time.Duration, 0, samples)
	for range samples {
		start := time.Now()
		if err := query(t.Context()); err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		elapsed = append(elapsed, time.Since(start))
	}
	slices.Sort(elapsed)
	t.Logf("%s median=%s p95=%s (%d queries)", label, elapsed[samples/2], elapsed[95], samples)
}
