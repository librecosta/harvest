//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

func TestA3Measure(t *testing.T) {
	if os.Getenv("HARVEST_A3_MEASURE") != "1" {
		t.Skip("set HARVEST_A3_MEASURE=1 for the isolated performance run")
	}
	p := startFixture(t)
	p.a3Config()
	server := p.serve()
	a3Exec(t, p, "CREATE EXTENSION pg_stat_statements")
	who := []string{"worker_a", "worker_b"}
	workers := make([]uuid.UUID, len(who))
	for i, name := range who {
		workers[i] = uuid.NewV7()
		p.call("POST", "/internal/v1/workers/register", name, "", execution.Registration{
			ID: workers[i], Offers: []provider.Offer{fixtureOffer, sampleOffer}, Capacity: 1,
		}, nil, 204)
	}
	const jobs = 64
	targets := make([]uuid.UUID, jobs)
	for i := range targets {
		targets[i] = p.register("fixture", "key", fmt.Sprintf("measure-a3-%d", i), "public")
	}
	p95 := func(samples []time.Duration) time.Duration {
		slices.Sort(samples)
		return samples[(len(samples)*95+99)/100-1]
	}
	type counters struct {
		statements, hits, reads, dirtied, written int64
		lsn                                       string
	}
	readCounters := func() counters {
		t.Helper()
		var n counters
		err := p.pool.QueryRow(t.Context(), `SELECT coalesce(sum(calls),0)::bigint,
			coalesce(sum(shared_blks_hit),0)::bigint,coalesce(sum(shared_blks_read),0)::bigint,
			coalesce(sum(shared_blks_dirtied),0)::bigint,coalesce(sum(shared_blks_written),0)::bigint,pg_current_wal_insert_lsn()::text
			FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
			AND query NOT LIKE '%pg_stat_statements%' AND query NOT LIKE '%pg_stat_activity%'`).Scan(
			&n.statements, &n.hits, &n.reads, &n.dirtied, &n.written, &n.lsn)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	pool := func() execution.DBPressure {
		var out execution.Operations
		p.call("GET", "/api/v1/operations/status?namespace=public", "operator", "", nil, &out, 200)
		return out.DBPressure
	}
	type phase struct {
		offer                                   provider.Offer
		duration                                time.Duration
		sql, wal, reads, hits, dirtied, written int64
		submitP95, completeP95                  time.Duration
	}
	runPhase := func(offer provider.Offer) phase {
		t.Helper()
		waitsBefore := pool().Waits
		before := readCounters()
		stop, cancel := context.WithCancel(t.Context())
		defer cancel()
		var poll sync.WaitGroup
		var lockSamples, samples, peakActive atomic.Int64
		poll.Go(func() {
			tick := time.NewTicker(5 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-stop.Done():
					return
				case <-tick.C:
					var locks, active int64
					err := p.pool.QueryRow(stop, `SELECT count(*) FILTER (WHERE wait_event_type='Lock' AND query LIKE '%LockChangeHead%'),
						count(*) FILTER (WHERE state='active') FROM pg_stat_activity
						WHERE application_name='harvest' AND pid<>pg_backend_pid()`).Scan(&locks, &active)
					if err == nil {
						lockSamples.Add(locks)
						samples.Add(1)
						if active > peakActive.Load() {
							peakActive.Store(active)
						}
					}
				}
			}
		})
		defer func() { cancel(); poll.Wait() }()
		var submits, completions []time.Duration
		start := time.Now()
		for i := 0; i < jobs; i += len(workers) {
			claims := make([]execution.Claim, len(workers))
			payloads := make([]execution.Completion, len(workers))
			for slot, worker := range workers {
				req := requestFor(t, targets[i+slot], "bench", "success", 0)
				params := observationParams{Mode: "present", Value: i + slot, Padding: 256}
				if offer == sampleOffer {
					req = a3Request(t, targets[i+slot], offer, params)
				}
				started := time.Now()
				job := p.submit(uuid.NewV7().String(), req)
				submits = append(submits, time.Since(started))
				p.call("POST", "/internal/v1/workers/"+worker.String()+"/claim", who[slot], "", nil, &claims[slot], 200)
				if claims[slot].JobID != job.ID {
					t.Fatal("unexpected performance fixture claim")
				}
				result := provider.Result{State: "succeeded", Data: []byte(`{"label":"bench","count":1}`)}
				if offer == sampleOffer {
					result = a3Result(claims[slot].Task, params, time.Now())
				}
				payloads[slot] = execution.Completion{Fence: claims[slot].Fence, OutputSchema: 1, Result: result}
			}
			type measurement struct {
				duration time.Duration
				status   int
				err      error
			}
			done := make(chan measurement, len(workers))
			for slot := range workers {
				go func() {
					started := time.Now()
					status, _, err := p.request("POST", completePath(claims[slot]), who[slot], "", payloads[slot])
					done <- measurement{time.Since(started), status, err}
				}()
			}
			for range workers {
				m := <-done
				if m.err != nil || m.status != 200 {
					t.Fatalf("performance completion %d: %v", m.status, m.err)
				}
				completions = append(completions, m.duration)
			}
			time.Sleep(20 * time.Millisecond)
		}
		dur := time.Since(start)
		after := readCounters()
		cancel()
		poll.Wait()
		pressure := pool()
		wal := a3Count(t, p, "SELECT pg_wal_lsn_diff($1::text::pg_lsn,$2::text::pg_lsn)::bigint", after.lsn, before.lsn)
		out := phase{offer, dur, after.statements - before.statements, wal, after.reads - before.reads,
			after.hits - before.hits, after.dirtied - before.dirtied, after.written - before.written, p95(submits), p95(completions)}
		t.Logf("%s: %d jobs in %v, %d SQL (%.2f/s), %d WAL B, buffers hit/read/dirtied/written=%d/%d/%d/%d; submit/complete p95=%v/%v; pool=%d/%d, +%d waits, SQL-active peak=%d, head wait samples=%d/%d",
			out.offer.Capability, jobs, out.duration.Round(time.Millisecond), out.sql, float64(out.sql)/out.duration.Seconds(), out.wal,
			out.hits, out.reads, out.dirtied, out.written, out.submitP95, out.completeP95,
			pressure.Open, pressure.Max, pressure.Waits-waitsBefore, peakActive.Load(), lockSamples.Load(), samples.Load())
		t.Logf("%s server RSS %s; PG CPU/RSS after phase %s", offer.Capability, processRSS(server.cmd.Process.Pid),
			run(t, p.scratch, nil, "docker", "stats", "--no-stream", "--format", "{{.CPUPerc}} / {{.MemUsage}}", p.compose("ps", "-q", "postgres")))
		return out
	}

	// Use the unmodified A2 completion path in the same process and DB to
	// isolate incremental result persistence from transport/scheduler costs.
	c := a3Enroll(t, p, "public", "measure", "public_reader")
	reset := a3Reset(p, c, "public_reader")
	a3Ack(p, c, "public_reader", reset.Baseline, &reset.Reset)
	baseline := runPhase(fixtureOffer)
	loaded := runPhase(sampleOffer)
	t.Logf("A3 incremental SQL %.2f/job; WAL %.0f B/job; completion p95 delta %v",
		float64(loaded.sql-baseline.sql)/jobs, float64(loaded.wal-baseline.wal)/jobs, loaded.completeP95-baseline.completeP95)
	for _, path := range []string{
		"/api/v1/observations/current?stream=public",
		"/api/v1/observations/history?stream=public",
		"/api/v1/changes?stream=public&consumer=" + c.ID.String() + "&after=" + reset.Baseline,
	} {
		latencies := make([]time.Duration, 32)
		for i := range latencies {
			started := time.Now()
			p.call("GET", path, "public_reader", "", nil, nil, 200)
			latencies[i] = time.Since(started)
		}
		t.Logf("%s, 50 items, HTTP p95 %v", strings.SplitN(path, "?", 2)[0], p95(latencies))
	}
	measureA3Plans(t, p)
}
