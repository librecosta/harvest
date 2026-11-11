//go:build integration

package integration

import (
	"context"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/librecosta/harvest/internal/execution"
)

type dDBSample struct {
	SQL, Heap, Index, TOAST int64
	Connections, LockWaits  int
	LSN                     string
}

func dDatabase(t *testing.T, p *testPlane) (s dDBSample) {
	t.Helper()
	err := p.pool.QueryRow(t.Context(), `/* d_observer */
SELECT (SELECT coalesce(sum(calls),0)::bigint FROM pg_stat_statements
   WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
     AND query NOT LIKE '%d_observer%'),
 coalesce(sum(pg_relation_size(c.oid)),0)::bigint, coalesce(sum(pg_indexes_size(c.oid)),0)::bigint,
 coalesce(sum(CASE WHEN c.reltoastrelid<>0 THEN pg_total_relation_size(c.reltoastrelid) ELSE 0 END),0)::bigint,
 (SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()),
 (SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'),
 pg_current_wal_insert_lsn()::text
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='harvest' AND c.relkind='r'`).
		Scan(&s.SQL, &s.Heap, &s.Index, &s.TOAST, &s.Connections, &s.LockWaits, &s.LSN)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func dP95(samples []time.Duration) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	values := slices.Clone(samples)
	slices.Sort(values)
	return values[(len(values)*95+99)/100-1]
}

type dRPCSample struct {
	permits, claims, completes int
}

func dRPC(m *permitMetrics) dRPCSample {
	m.mu.Lock()
	defer m.mu.Unlock()
	return dRPCSample{len(m.permits), len(m.claims), len(m.completes)}
}

type dBucket struct {
	Issued, Deferred, Succeeded, Refused, Failed, BodyBytes int64
}

func dBuckets(t *testing.T, p *testPlane) map[string]dBucket {
	t.Helper()
	rows, err := p.pool.Query(t.Context(), `/* d_observer */
SELECT key,issued,deferred,succeeded,refused,failed,body_bytes FROM harvest.quota_buckets`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]dBucket{}
	for rows.Next() {
		var key string
		var s dBucket
		if err := rows.Scan(&key, &s.Issued, &s.Deferred, &s.Succeeded, &s.Refused, &s.Failed, &s.BodyBytes); err != nil {
			t.Fatal(err)
		}
		out[key] = s
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func dProcess(t *testing.T, p *testPlane, phase string, processes []*child) {
	t.Helper()
	var cpu []float64
	var rss []string
	for _, process := range processes {
		pid := process.cmd.Process.Pid
		busy, err := processCPU(pid)
		if err != nil {
			t.Fatal(err)
		}
		cpu = append(cpu, busy)
		rss = append(rss, processRSS(pid))
	}
	t.Logf("D %s server/A/B OS CPU seconds=%v heap bytes=%v RSS=%v", phase, cpu, sampleHeap(p, processes...), rss)
	pg := p.compose("ps", "-q", "postgres")
	t.Logf("D %s PostgreSQL Docker CPU/RSS: %s", phase,
		run(t, p.scratch, nil, "docker", "stats", "--no-stream", "--format", "{{.CPUPerc}} / {{.MemUsage}}", pg))
}

func dMixedLoad(t *testing.T, p *testPlane, rpc *permitMetrics, consumer dConsumer, work []execution.Request,
	execute func(...execution.Request) []testJob, proxies []*fakeProxy, processes ...*child) {
	t.Helper()
	ctx := t.Context()
	idle := 4 * time.Second
	if os.Getenv("HARVEST_D_SOAK") == "1" {
		idle = 20 * time.Second
	}
	idleStart := dDatabase(t, p)
	idleAt := time.Now()
	time.Sleep(idle)
	dProcess(t, p, "before load", processes)
	before := dDatabase(t, p)
	t.Logf("D idle SQL/s %.2f connections=%d lock waits=%d", float64(before.SQL-idleStart.SQL)/time.Since(idleAt).Seconds(), before.Connections, before.LockWaits)
	var baseline execution.Operations
	statusPath := "/api/v1/operations/status?namespace=" + consumer.stream
	p.call("GET", statusPath, "operator", "", nil, &baseline, 200)
	buckets := dBuckets(t, p)
	startRPC := dRPC(rpc)
	proxyBytes := make([][2]int64, len(proxies))
	for i, p := range proxies {
		proxyBytes[i] = [2]int64{p.outbound.Load(), p.inbound.Load()}
	}
	start := time.Now()
	stop := start.Add(15 * time.Minute)
	long := os.Getenv("HARVEST_D_SOAK") == "1"
	perCap := map[string]int64{}
	var queue, runTime []time.Duration
	maxLag := int64(0)
	var status execution.Operations
	stopLocks := dSampleLocks(ctx, p.pool)
	t.Cleanup(func() { stopLocks() })
	for round := 0; ; round++ {
		roundAt := time.Now()
		jobs := execute(work...)
		for i, job := range jobs {
			var queued, elapsed float64
			var permits int64
			err := p.pool.QueryRow(ctx, `/* d_observer */
SELECT extract(epoch FROM a.claimed_at-j.created_at)::float8,
       extract(epoch FROM a.ended_at-a.claimed_at)::float8,
       (SELECT count(*) FROM harvest.quota_permits qp WHERE qp.attempt_id=a.id)
FROM harvest.jobs j JOIN harvest.attempts a ON a.job_id=j.id AND a.generation=j.generation WHERE j.id=$1`, job.ID).Scan(&queued, &elapsed, &permits)
			if err != nil {
				t.Fatal(err)
			}
			if permits == 0 {
				t.Fatalf("mixed %s produced no permit", work[i].Capability)
			}
			perCap[work[i].Capability] += permits
			queue = append(queue, time.Duration(queued*float64(time.Second)))
			runTime = append(runTime, time.Duration(elapsed*float64(time.Second)))
		}
		p.call("GET", statusPath, "operator", "", nil, &status, 200)
		maxLag = max(maxLag, status.Results.Stream.MaxConsumerLag)
		if err := consumer.drain(ctx); err != nil {
			t.Fatal(err)
		}
		if (!long && round == 2) || (long && !time.Now().Before(stop)) {
			break
		}
		if long {
			time.Sleep(max(0, time.Until(roundAt.Add(15*time.Second))))
		}
	}
	duration := time.Since(start)
	locks := stopLocks()
	if locks.samples == 0 {
		t.Fatal("no lock observations were collected")
	}
	for i, p := range proxies {
		t.Logf("D proxy %s tunnel bytes sent/received=%d/%d", p.name, p.outbound.Load()-proxyBytes[i][0], p.inbound.Load()-proxyBytes[i][1])
	}
	dProcess(t, p, "after load", processes)
	after := dDatabase(t, p)
	var wal int64
	if err := p.pool.QueryRow(ctx, "/* d_observer */ SELECT pg_wal_lsn_diff($1::text::pg_lsn,$2::text::pg_lsn)::bigint", after.LSN, before.LSN).Scan(&wal); err != nil {
		t.Fatal(err)
	}
	endBuckets := dBuckets(t, p)
	for _, key := range []string{"web_global", "web_provider", "steam_store", "steam_api", "steam_official", "web_connect_path", "web_socks_path", "web_local"} {
		left, right := buckets[key], endBuckets[key]
		t.Logf("D bucket %s issued=%d deferred=%d succeeded=%d refused=%d failed=%d response_body_bytes=%d", key,
			right.Issued-left.Issued, right.Deferred-left.Deferred, right.Succeeded-left.Succeeded, right.Refused-left.Refused,
			right.Failed-left.Failed, right.BodyBytes-left.BodyBytes)
	}
	rpc.mu.Lock()
	permit, claim, complete := dP95(rpc.permits[startRPC.permits:]), dP95(rpc.claims[startRPC.claims:]), dP95(rpc.completes[startRPC.completes:])
	rpc.mu.Unlock()
	t.Logf("D mixed %s %d jobs %v SQL=%d (%.2f/s) WAL=%d B heap/index/TOAST growth=%d/%d/%d B; connections=%d pool waits=%d",
		duration, len(queue), perCap, after.SQL-before.SQL, float64(after.SQL-before.SQL)/duration.Seconds(), wal,
		after.Heap-before.Heap, after.Index-before.Index, after.TOAST-before.TOAST, after.Connections, status.DBPressure.Waits-baseline.DBPressure.Waits)
	t.Logf("D lock samples=%d snapshots with any/stream-head waits=%d/%d", locks.samples, locks.any, locks.head)
	t.Logf("D p95 permit/claim/complete/queue/attempt=%s/%s/%s/%s/%s; max consumer lag=%d", permit, claim, complete, dP95(queue), dP95(runTime), maxLag)
	time.Sleep(idle)
	dProcess(t, p, "after idle", processes)
	tail := dDatabase(t, p)
	t.Logf("D post-load idle SQL=%d connections=%d lock waits=%d", tail.SQL-after.SQL, tail.Connections, tail.LockWaits)
}

type dLockSamples struct{ samples, any, head int }

func dSampleLocks(ctx context.Context, pool *pgxpool.Pool) func() dLockSamples {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan dLockSamples, 1)
	go func() {
		var counts dLockSamples
		defer func() { done <- counts }()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var all, heads int
				err := pool.QueryRow(ctx, `/* d_observer */
SELECT count(*), count(*) FILTER (WHERE query LIKE '%harvest.change_heads%')
FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'`).Scan(&all, &heads)
				if err == nil {
					counts.samples++
					if all > 0 {
						counts.any++
					}
					if heads > 0 {
						counts.head++
					}
				}
			}
		}
	}()
	return sync.OnceValue(func() dLockSamples {
		cancel()
		return <-done
	})
}
