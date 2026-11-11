//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/librecosta/harvest/internal/execution"
)

func sampleProcessHeap() {
	path := fmt.Sprintf("heap-%d.txt", os.Getpid())
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for range tick.C {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		_ = os.WriteFile(path, []byte(strconv.FormatUint(mem.HeapAlloc, 10)), 0o600)
	}
}

func sampleHeap(p *testPlane, processes ...*child) []uint64 {
	out := make([]uint64, 0, len(processes))
	for _, c := range processes {
		raw, _ := os.ReadFile(filepath.Join(p.scratch, fmt.Sprintf("heap-%d.txt", c.cmd.Process.Pid)))
		n, _ := strconv.ParseUint(string(raw), 10, 64)
		out = append(out, n)
	}
	return out
}

type permitMetrics struct {
	mu        sync.Mutex
	permits   []time.Duration
	claims    []time.Duration
	completes []time.Duration
	bytes     int64
}

type measureResponse struct {
	http.ResponseWriter
	status int
}

func (w *measureResponse) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *measureResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func measureControl(t *testing.T, p *testPlane, paths ...string) *permitMetrics {
	t.Helper()
	target, _ := url.Parse("http://" + p.address)
	proxy := httputil.NewSingleHostReverseProxy(target)
	transport := &http.Transport{MaxIdleConnsPerHost: 8}
	proxy.Transport = transport
	t.Cleanup(transport.CloseIdleConnections)
	metrics := &permitMetrics{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		response := &measureResponse{ResponseWriter: w}
		proxy.ServeHTTP(response, r)
		permit := strings.HasSuffix(r.URL.Path, "/permit")
		if response.status == 200 {
			metrics.mu.Lock()
			switch {
			case strings.HasSuffix(r.URL.Path, "/claim"):
				metrics.claims = append(metrics.claims, time.Since(start))
			case strings.HasSuffix(r.URL.Path, "/complete"):
				metrics.completes = append(metrics.completes, time.Since(start))
			}
			metrics.mu.Unlock()
		}
		if permit || strings.HasSuffix(r.URL.Path, "/finish") {
			metrics.mu.Lock()
			metrics.bytes += max(r.ContentLength, 0)
			if permit {
				metrics.permits = append(metrics.permits, time.Since(start))
			}
			metrics.mu.Unlock()
		}
	}))
	t.Cleanup(srv.Close)
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		data := strings.Replace(string(content), fmt.Sprintf("control_url: %q", target.String()), fmt.Sprintf("control_url: %q", srv.URL), 1)
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return metrics
}

func measureA2(t *testing.T, p *testPlane, lab *networkLab, control *permitMetrics, submit networkSubmit, state networkState, server, workerA, workerB *child) {
	t.Helper()
	ctx := t.Context()
	if _, err := p.pool.Exec(ctx, "CREATE EXTENSION pg_stat_statements"); err != nil {
		t.Fatal(err)
	}
	type stats struct {
		statements, inserted, heap, policySQL int64
		lsn                                   string
	}
	get := func() stats {
		t.Helper()
		var s stats
		err := p.pool.QueryRow(ctx, `SELECT coalesce(sum(calls),0)::bigint,
			coalesce(sum(calls) FILTER (WHERE query LIKE '%INSERT INTO harvest.quota_permits%'),0)::bigint,
			coalesce(sum(calls) FILTER (WHERE query LIKE '%harvest.operational_policies%'),0)::bigint,
			pg_total_relation_size('harvest.quota_permits')+pg_total_relation_size('harvest.quota_buckets'),
			pg_current_wal_insert_lsn()::text
			FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
				AND query NOT LIKE '%pg_stat_statements%' AND query NOT LIKE '%pg_stat_activity%'`).Scan(&s.statements, &s.inserted, &s.policySQL, &s.heap, &s.lsn)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	poolStats := func() execution.DBPressure {
		var status execution.Operations
		p.call("GET", "/api/v1/operations/status?namespace=net_direct", "operator", "", nil, &status, 200)
		if status.DBPressure.Max != 4 {
			t.Fatal("missing control plane pool diagnostics")
		}
		return status.DBPressure
	}
	base := get()
	idleStart := time.Now()
	time.Sleep(5 * time.Second)
	before := get()
	idle := time.Since(idleStart).Seconds()
	basePool := poolStats()
	baseHeap := sampleHeap(p, server, workerA, workerB)
	control.mu.Lock()
	control.permits = nil
	control.bytes = 0
	control.mu.Unlock()
	stop, cancel := context.WithCancel(ctx)
	var poll sync.WaitGroup
	var lockSamples, samples, peakActive atomic.Int64
	poll.Go(func() {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop.Done():
				return
			case <-tick.C:
				var n, active int64
				if p.pool.QueryRow(stop, "SELECT count(*) FILTER (WHERE wait_event_type='Lock'), count(*) FILTER (WHERE state='active') FROM pg_stat_activity WHERE application_name='harvest' AND pid<>pg_backend_pid()").Scan(&n, &active) == nil {
					samples.Add(1)
					lockSamples.Add(n)
					for old := peakActive.Load(); old < active && !peakActive.CompareAndSwap(old, active); old = peakActive.Load() {
					}
				}
			}
		}
	})
	defer func() { cancel(); poll.Wait() }()
	start := time.Now()
	const waves = 4
	var payload int
	for i := range waves {
		l1, l2 := fmt.Sprintf("bench_direct_%d", i), fmt.Sprintf("bench_socks_%d", i)
		a := submit("net_direct", l1, "bench", "cn")
		b := submit("net_socks", l2, "bench", "cn")
		state(a, "succeeded")
		state(b, "succeeded")
		assertPermits(t, p, a, 12)
		assertPermits(t, p, b, 12)
		payload += 2 * (len(lab.hit(l1)) + len(lab.hit(l2)))
	}
	after := get()
	seconds := time.Since(start).Seconds()
	if after.policySQL != before.policySQL || before.policySQL != base.policySQL {
		t.Fatal("network path queried the durable policy after startup")
	}
	t.Log("policy SQL during idle and loaded request phases: 0")
	cancel()
	poll.Wait()
	var wal int64
	if err := p.pool.QueryRow(ctx, "SELECT pg_wal_lsn_diff($1::text::pg_lsn,$2::text::pg_lsn)::bigint", after.lsn, before.lsn).Scan(&wal); err != nil {
		t.Fatal(err)
	}
	control.mu.Lock()
	latency, controlBytes := slices.Clone(control.permits), control.bytes
	control.mu.Unlock()
	if len(latency) < waves*24 || after.inserted-before.inserted != waves*24 || payload != waves*24*2 {
		t.Fatalf("network/permit accounting diverged: %d RPC, %d debits, %d payload bytes", len(latency), after.inserted-before.inserted, payload)
	}
	slices.Sort(latency)
	loadedPool := poolStats()
	t.Logf("A2 reference: idle %.2f SQL/s; loaded %.2f SQL/s, %d permits in %.2fs (%.2f/s), permit RPC p95 %v, DB pool %d/%d open (%d peak SQL-active, %d additional pool waits), %d lock waits over %d 10ms samples",
		float64(before.statements-base.statements)/idle, float64(after.statements-before.statements)/seconds, len(latency), seconds, float64(len(latency))/seconds,
		latency[(len(latency)*95+99)/100-1], loadedPool.Open, loadedPool.Max, peakActive.Load(), loadedPool.Waits-basePool.Waits, lockSamples.Load(), samples.Load())
	t.Logf("A2 storage/traffic: %d WAL bytes, quota tables + indexes delta %d bytes, %d control request-body bytes, %d origin response-body bytes",
		wal, after.heap-before.heap, controlBytes, payload)
	t.Logf("A2 Go HeapAlloc idle/load by process (bytes, server then Workers): %v / %v", baseHeap, sampleHeap(p, server, workerA, workerB))
	t.Logf("A2 process RSS: control plane %s, Workers %s / %s", processRSS(server.cmd.Process.Pid), processRSS(workerA.cmd.Process.Pid), processRSS(workerB.cmd.Process.Pid))
	t.Logf("A2 PostgreSQL CPU/RSS snapshot: %s", run(t, p.scratch, nil, "docker", "stats", "--no-stream", "--format", "{{.CPUPerc}} / {{.MemUsage}}", p.compose("ps", "-q", "postgres")))
}
