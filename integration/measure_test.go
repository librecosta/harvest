//go:build integration

package integration

import (
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	json "encoding/json/v2"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

func measureIdle(t *testing.T, p *testPlane, server *child, workers map[string]*child) {
	t.Helper()
	ctx := t.Context()
	if _, err := p.pool.Exec(ctx, "CREATE EXTENSION pg_stat_statements"); err != nil {
		t.Fatal(err)
	}
	count := func() int64 {
		t.Helper()
		var calls int64
		err := p.pool.QueryRow(ctx, "SELECT coalesce(sum(calls), 0)::bigint FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) AND query NOT LIKE '%pg_stat_statements%'").Scan(&calls)
		if err != nil {
			t.Fatal(err)
		}
		return calls
	}
	first := count()
	start := time.Now()
	time.Sleep(6 * time.Second)
	calls := count() - first
	seconds := time.Since(start).Seconds()
	var connections int32
	if err := p.pool.QueryRow(ctx, "SELECT count(*)::int FROM pg_stat_activity WHERE application_name = 'harvest'").Scan(&connections); err != nil {
		t.Fatal(err)
	}
	connections -= p.pool.Stat().TotalConns()
	if calls < 1 || connections < 1 || connections > 4 {
		t.Fatalf("idle measurement saw %d SQL calls and %d control-plane connections", calls, connections)
	}
	var rss []string
	for _, name := range []string{"worker_a", "worker_b"} {
		rss = append(rss, processRSS(workers[name].cmd.Process.Pid))
	}
	t.Logf("idle %.2f tracked SQL statements/s (%d in %.2fs), pool %d/4, serve+embedded RSS %s, remote Worker RSS %s / %s",
		float64(calls)/seconds, calls, seconds, connections, processRSS(server.cmd.Process.Pid), rss[0], rss[1])
	pg := p.compose("ps", "-q", "postgres")
	t.Logf("PG18 memory (4 vCPU cap): %s",
		run(t, p.scratch, nil, "docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", pg))
}

func processRSS(pid int) string {
	if runtime.GOOS == "linux" {
		if data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid)); err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if value, ok := strings.CutPrefix(line, "VmRSS:"); ok {
					return strings.TrimSpace(value)
				}
			}
		}
	}
	if runtime.GOOS == "windows" {
		out, err := exec.Command("tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/FO", "CSV", "/NH").Output()
		if err == nil {
			fields, err := csv.NewReader(bytes.NewReader(out)).Read()
			if err == nil && len(fields) == 5 {
				return fields[4]
			}
		}
	}
	return "unavailable"
}

func measurePlans(t *testing.T, p *testPlane) {
	t.Helper()
	ctx := t.Context()
	e := execution.New(p.pool, fixtureRegistry(), nil, egress.NetworkConfig{})
	principal := config.Principal{
		Name: "benchmark", Role: "worker", Namespaces: []string{"benchmark"},
		MaxSlots: 16, Capabilities: []string{fixtureOffer.Capability},
	}
	target, err := e.RegisterTarget(ctx, principal, execution.TargetInput{
		Provider: "fixture", ResourceType: "key", ExternalKey: "query_plan", Namespace: "benchmark",
	})
	if err != nil {
		t.Fatal(err)
	}
	now, err := q.New(p.pool).DBNow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const history, queued, schedules = 20000, 200, 5000
	index := 0
	_, err = p.pool.CopyFrom(ctx, pgx.Identifier{"harvest", "jobs"}, []string{
		"id", "target_id", "namespace", "capability", "request_schema", "execution_key", "params", "scope",
		"fingerprint", "cause", "priority", "available_at", "deadline_at", "max_attempts", "state", "updated_at",
	}, pgx.CopyFromFunc(func() ([]any, error) {
		if index == history+queued {
			return nil, nil
		}
		i := index
		index++
		hash := sha256.Sum256([]byte(strconv.Itoa(i)))
		state, updated := "failed", now.Add(-time.Hour)
		available, deadline := now.Add(-2*time.Hour), now.Add(-time.Hour)
		if i < 200 {
			updated = now.Add(-31 * 24 * time.Hour)
		}
		if i >= history {
			state, updated, available, deadline = "queued", now, now, now.Add(5*time.Minute)
		}
		params, _ := json.Marshal(fixtureParams{Label: "bench_" + strconv.Itoa(i), Mode: "success"})
		return []any{uuid.NewV7(), target.ID, principal.Namespaces[0], fixtureOffer.Capability, int32(1), fixtureOffer.Key(),
			params, []byte("{}"), hash[:], "manual", int16(i % 2), available, deadline, int32(3), state, updated}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	index = 0
	_, err = p.pool.CopyFrom(ctx, pgx.Identifier{"harvest", "schedules"}, []string{
		"id", "target_id", "namespace", "capability", "request_schema", "execution_key", "params", "scope",
		"fingerprint", "interval_seconds", "cron", "anchor_at", "next_due_at", "policy", "enabled",
	}, pgx.CopyFromFunc(func() ([]any, error) {
		if index == schedules {
			return nil, nil
		}
		i := index
		index++
		hash := sha256.Sum256([]byte(strconv.Itoa(i)))
		next := now.Add(time.Hour)
		if i < 10 {
			next = now.Add(-time.Hour)
		}
		params, _ := json.Marshal(fixtureParams{Label: "bench_" + strconv.Itoa(i), Mode: "success"})
		return []any{uuid.NewV7(), target.ID, principal.Namespaces[0], fixtureOffer.Capability, int32(1), fixtureOffer.Key(),
			params, []byte("{}"), hash[:], int32(3600), "", next, next, "state_refresh", true}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.pool.Exec(ctx, "ANALYZE harvest.jobs, harvest.schedules, harvest.targets"); err != nil {
		t.Fatal(err)
	}
	t.Logf("EXPLAIN sample: %d historical Jobs, %d due Jobs, %d Schedules (10 due)", history, queued, schedules)
	explain(t, p, "jobs.sql", "LockClaimableJob", []string{"offers", "network_offers", "namespaces"}, []string{fixtureOffer.Key()}, []string{}, principal.Namespaces)
	explain(t, p, "catalog.sql", "LockDueSchedule", []string{"offers"}, []string{fixtureOffer.Key()})
	var latency []time.Duration
	for range 2 {
		worker := uuid.NewV7()
		if err := e.RegisterWorker(ctx, principal, execution.Registration{
			ID: worker, Offers: []provider.Offer{fixtureOffer}, Capacity: 16,
		}); err != nil {
			t.Fatal(err)
		}
		for range 16 {
			start := time.Now()
			claim, err := e.Claim(ctx, principal, worker)
			if err != nil || claim == nil {
				t.Fatalf("benchmark claim: %v", err)
			}
			latency = append(latency, time.Since(start))
		}
	}
	slices.Sort(latency)
	t.Logf("32 full claim transactions p50=%s p95=%s", latency[15], latency[30])
	if err := e.Prune(ctx); err != nil {
		t.Fatal(err)
	}
	var old, running int
	if err := p.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE updated_at < clock_timestamp() - interval '30 days'),
		count(*) FILTER (WHERE state = 'running') FROM harvest.jobs WHERE namespace = 'benchmark'`).Scan(&old, &running); err != nil {
		t.Fatal(err)
	}
	if old != 72 || running != 32 {
		t.Fatalf("retention did not limit the batch or preserve active work: old=%d running=%d", old, running)
	}
}

type planNode struct {
	Index    string     `json:"Index Name"`
	Hit      int        `json:"Shared Hit Blocks"`
	Read     int        `json:"Shared Read Blocks"`
	Children []planNode `json:"Plans"`
}

func planIndexes(n planNode) []string {
	var indexes []string
	if n.Index != "" {
		indexes = append(indexes, n.Index)
	}
	for _, child := range n.Children {
		indexes = append(indexes, planIndexes(child)...)
	}
	return indexes
}

func explain(t *testing.T, p *testPlane, file, name string, params []string, args ...any) {
	t.Helper()
	data, err := os.ReadFile("../db/queries/" + file)
	if err != nil {
		t.Fatal(err)
	}
	_, tail, ok := strings.Cut(strings.ReplaceAll(string(data), "\r\n", "\n"), "-- name: "+name+" :one\n")
	if !ok {
		t.Fatalf("missing SQL source %s", name)
	}
	sql, _, _ := strings.Cut(tail, "\n-- name:")
	for i, param := range params {
		sql = strings.ReplaceAll(sql, "sqlc.arg("+param+")", fmt.Sprintf("$%d", i+1))
	}
	var output []byte
	if err := p.pool.QueryRow(t.Context(), "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+strings.TrimSpace(sql), args...).Scan(&output); err != nil {
		t.Fatal(err)
	}
	var plan []struct {
		Root planNode `json:"Plan"`
		MS   float64  `json:"Execution Time"`
	}
	if err := json.Unmarshal(output, &plan); err != nil || len(plan) != 1 {
		t.Fatalf("decode EXPLAIN: %v", err)
	}
	root := plan[0].Root
	t.Logf("%s execution %.3f ms, shared buffers hit=%d read=%d, indexes=%v",
		name, plan[0].MS, root.Hit, root.Read, planIndexes(root))
}
