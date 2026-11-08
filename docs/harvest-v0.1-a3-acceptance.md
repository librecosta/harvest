# Harvest V0.1-A3 acceptance

PostgreSQL 18.6 in disposable Docker Compose projects, Go 1.27.3 on Windows
amd64. Each PostgreSQL container was capped at 4 CPUs and 8 GiB, and test
server and Worker processes used `GOMAXPROCS=4`. The test binary compiles
the fixture Provider. Production discovery still reports Web and Steam as
`not_implemented`.

## Recovery protocol

All routes use the authenticated `/api/v1` prefix. `stream` is the
authorization namespace. A reader's capability grants apply to each page.

1. The operator calls `POST /consumers` with `stream`, `name` and
   `principal`. The named principal owns the consumer. Enrolling the same
   identity again returns the existing consumer.
2. The owner calls `POST /consumers/:id/reset` with `{"stream":"public"}`.
   Keep the returned `reset`, `epoch`, `baseline_cursor` and deadline.
   Stage `GET /snapshots/current?stream=...&consumer=...&reset=...` pages by
   canonical entity identity. Follow `after` until absent. Keep each row's
   `position` and the final page's `head_cursor`.
3. Read `GET /changes?stream=...&consumer=...&after=...&reset=...&through=...`
   starting with the baseline as `after` and the final snapshot head as
   `through`. Follow `next_cursor` until `has_more` is false. Apply a
   change only when its position is newer than the staged version of that
   entity. Retain versioned tombstones during staging. The cursor can
   advance across events outside the consumer's capability grants.
4. In one consumer database transaction, activate the staged projection
   and persist its cursor. Then call `POST /consumers/:id/ack` with
   `stream`, `cursor` and `reset`. Continue normal polling and ACKs using
   the durable cursor. Normal polling omits `reset` and can omit `through`.
   If Harvest returns HTTP 410 `cursor_expired`, start a fresh staging
   snapshot. `min_available_position - 1` is the oldest valid after-position,
   encoded as `min_available_cursor` in a successful changes response.

Use `/observations/current?stream=...` for last-known-good reads and
`/observations/history?stream=...` for history. Both accept opaque
`after` and `limit`. History also accepts `target`, `capability`, `scope`
(scope key), and RFC3339 `since` and `until` on receipt time. Pages default
to 50 items, with maxima of 200 items and 2 MiB. Only Worker completion has
a 2 MiB request limit, with up to 16 segments and 512 KiB per segment
(or less by capability). Details are ordered by accepted stream position.
Identical detail confirmations refresh current timestamps without emitting
events. Authoritative withdrawals live in the delivery log as tombstones.

The optional YAML `retention` block configures per-stream horizons and
budgets. Defaults are 14 days, 100,000 rows and 256 MiB of estimated replay
data for events, and 30 days, 200,000 rows and 512 MiB of estimated retained
data for history, including samples. Resets have a 10-minute deadline and
may pin at most 2,048 events or 32 MiB of replay data. Age cleanup honors
live reset pins. Hard retention pressure invalidates crossed resets and
lagging cursors before advancing the floor. Current and event references
keep their observations pinned. If pinned data exhausts the history budget
and its bounded ingress reserve, result writes that add history return
503 `retention_pressure` until capacity is available.

## Executed checks

- Fresh schema and explicit upgrade from populated A2 preserved Targets,
  Schedules, Jobs, Attempts, Workers, quota balances and live permits.
  Startup before migration rejected the incompatible schema.
- Independent scopes preserved last-known-good during partial failure.
  Confirmed empty required evidence, repeated zero samples stayed distinct,
  and unchanged detail scans retained the same observation and head.
  Replays returned the same receipt. Conflicting, stale and cancelled
  completions and invalid or oversized segments left results unchanged.
- A PostgreSQL gate held a completion after event insertion. A same-stream
  writer waited, a different stream committed, and reads saw only the
  previous head. Forcing SQL rollback left the receipt and results
  uncommitted. The next writer reused the rolled-back position.
- Two independent Worker processes completed changes between snapshot
  pages. Recovery included an insertion before the page cursor, updates,
  a withdrawal and a recreated identity. It converged through position 18
  and then 20, replaying 14 events and skipping eight older versions. The
  fixture committed the projection and cursor in a separate PostgreSQL
  database before ACK.
- Age and pressure pruning preserved current and replay references, live
  leases and permits. Older cursors, reset expiry, reset budget exhaustion
  and changed grants produced explicit expiry. Capability filters hid
  payloads and still advanced the cursor. A 2 MiB page bound was verified
  with 450 KiB sample payloads.
- `task check:containers` passed A0 through A3 in 188 seconds. It covers
  native and Docker readiness, loss and recovery, SIGTERM, A1 fencing and
  A2 proxy and permit regression. `task check` and `go test -race ./...`
  passed, including reproducible Goose and sqlc output. The A3 integration
  tests also passed with the race detector. Docker built the Linux ARM64 image.

## Measured increment

`HARVEST_A3_MEASURE=1` enables `TestA3Measure` under the `integration`
build tag. The paired phases used the same server and database, two Worker
protocol clients with one slot each, 64 jobs per phase, and concurrent
completions in pairs. The baseline exercises legacy A2 completion. A3
adds one sample with 256 bytes of padding per Job.

| Metric | Legacy completion | A3 sample |
| --- | ---: | ---: |
| Tracked SQL statements | 1,613 | 2,262 |
| WAL insert bytes | 276,728 | 418,752 |
| Shared buffers hit/read/dirtied/written | 6,995/3/41/34 | 10,070/0/42/39 |
| Submit / completion HTTP p95 | 5.9 / 7.2 ms | 6.7 / 24.5 ms |
| Open connections / cap, additional pool waits | 2/4, 1 | 3/4, 1 |
| Stream-head waits in 5 ms samples | 0/330 | 35/410 |
| Server RSS | 20,064 KiB | 20,944 KiB |
| PostgreSQL CPU / memory snapshot after phase | 0.27% / 55.48 MiB | 6.04% / 60.95 MiB |

This run added 10.14 SQL calls and 2,219 WAL bytes per sample Job.
For 50-item pages, current/history/changes HTTP p95 was 5.3/5.4/8.1 ms.
`EXPLAIN (ANALYZE, BUFFERS)` over 8,192 seeded entities used the scoped
current primary key, history index and stream primary key. Execution
times were 0.128/0.062/0.038 ms respectively. The earlier
[A2 network run](harvest-v0.1-a2-acceptance.md) exercised a separate workload.

Sustained hot-stream throughput and long-running disk/autovacuum behavior
remain unmeasured. The retention numbers track estimated encoded bytes.
