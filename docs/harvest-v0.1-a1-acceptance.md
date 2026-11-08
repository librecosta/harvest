# V0.1-A1 acceptance

Run `task check` for Go, Goose syntax and reproducible sqlc output.
Run `task check:containers` for the A0 lifecycle regression and A1 execution
suite. Each suite creates a uniquely named Compose project and disposable
PostgreSQL 18 volume, then removes both. The volume survives PostgreSQL
stop/start within the test. The A1 suite re-executes the test binary as one
control plane and two independent Worker processes, with an additional
embedded worker scoped to its own namespace. Only that binary registers the
deterministic fixture Provider.

| Gate | Executed verification |
| --- | --- |
| Migrations and startup | Fresh PG18 and upgrade from the empty A0 schema; pending status and refused startup before explicit up; repeat up; expected tables and read-only compatibility checks. |
| Registry and auth | Steam uint32 and Web IDNA canonicalization, domain/origin separation, UUIDv7 round trips, isolated namespaces, strict request and offer schemas, no executable Web/Steam discovery, role and Worker slot restrictions. |
| Scheduling | Anchored interval and UTC cron against real PostgreSQL; two concurrent reconcilers and restart, one catch-up slot, missed point-in-time coverage, Run Now phase and pause/CAS. |
| Execution | Workers race one Job; the first renews and is killed, then the control plane is killed and restarted. The surviving Worker takes over the expired Job. Stale renew and completion fail, identical replay returns the same receipt, and altered replay conflicts. |
| State and lifecycle | Bounded retries and backoff, partial outcome, operator retry lineage, concurrent cancel/complete, explicit Worker cancellation acknowledgement, cancellation recovered after expiry, pre-drain completion, DB outage/recovery, checkout stop and Docker SIGTERM. |
| Database plans and limits | `EXPLAIN (ANALYZE, BUFFERS)` on source claim and reconciliation SQL with 20,000 history Jobs, 200 due Jobs and 5,000 Schedules. Retention prunes a bounded batch and preserves active leases. |

## Wire and operational decisions

- `POST /api/v1/targets` takes `provider`, `resource_type`, `external_key`,
  and `namespace`. Target PATCH takes `revision` and `state`. Schedule POST
  takes `request`, `recurrence`, and optional `enabled`; PATCH takes the
  expected `revision` plus the fields being edited.
- A request contains `target_id`, `capability`, `request_schema`,
  a typed `params` object, and optional `scope`. The scope includes locale,
  source region and semantic egress region. Egress profile declarations are
  metadata for A1. Job submission takes either `{"request": ...}` or
  `{"schedule_id": ...}` (Run Now). It and `POST /jobs/:id/retry` require a
  caller-scoped `Idempotency-Key`. Job detail includes a bounded Attempt
  history and a waiting reason for queued work.
- A recurrence has an explicit UTC-normalized `anchor_at`, `policy`, and
  either `interval_seconds` or five-field UTC `cron`. An interval must
  meet the capability's minimum, up to 30 days. A1 permits cron for
  capabilities with a minimum of at most one minute. A point-in-time slot
  expires five seconds after its scheduled time. Missed counts saturate
  cumulatively at one million. A cron advance examines at most 256 missed
  slots for the count and locates the latest slot separately.
- Normal Jobs expire after five minutes and allow up to three Attempts.
  Explicit retryable outcomes and lost leases use one- and two-second
  backoffs. Workers poll once per slot about every 1–1.25 seconds when idle.
  Presence heartbeats are five seconds. Attempt leases are at most 12
  seconds with renewal every three seconds. Claims promote work waiting 30
  seconds above newly available priority Jobs.
- The server allows five seconds for pre-drain, then uses Fiber's ten-second
  graceful shutdown. Results during pre-drain store a typed outcome code,
  digest and receipt. A1 keeps the digest and execution history; Observations
  and Change Stream belong to A3. Proxy routing and global permits belong
  to A2.

## Measured reference

The local Windows/Docker Desktop run capped PostgreSQL at four vCPUs and
8 GiB and set `GOMAXPROCS=4` per Go process. In a 6.00-second idle window
with two remote Workers and one embedded Worker, PostgreSQL tracked 135 SQL
statements (22.49/s). The control plane had four open connections of four
configured. Working sets were 18,716 KiB for serve with the embedded Worker
and 15,084 / 15,096 KiB for the two remote Workers. PostgreSQL used 61.28 MiB.

| Measurement | Observed |
| --- | --- |
| Claim selection | 1.124 ms, 212 shared-buffer hits, zero reads, `jobs_deadline_idx` |
| Due Schedule selection | 0.140 ms, five shared-buffer hits, zero reads, `schedules_due_idx` |
| 32 complete claim transactions | p50 6.54 ms, p95 8.00 ms |

Idle polling drives the SQL count and the connection pool reached its
configured open-connection ceiling. The claim selector examined more
buffers than reconciliation and used the deadline index on this warm sample.
These results cover local idle and short transactions. Host-wide 4C8G
co-tenancy, sustained mixed load and cold-cache behavior remain unmeasured.
