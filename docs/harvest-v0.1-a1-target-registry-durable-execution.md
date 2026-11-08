# Harvest V0.1-A1 — Target Registry & Durable Execution

Status: **Ready for Codex implementation** (2026-11-08). Depends on successful A0 foundation at `develop` commit `262c74e`. Work only in `librecosta/harvest` `develop`. No production deployment.

## Objective

Make Harvest's **first real persistent control plane**: canonical targets, compiled provider/capability discovery, recurring schedules, manual jobs, atomic claim, worker registration, attempts, leases, retry/cancel, and bounded diagnostics. Demonstrate durable, authenticated, two-Worker execution against a disposable PostgreSQL 18 database.

A1 is **not** real Web/Steam fetching, proxy routing, global request permits, Observation persistence or incremental Change Stream. Those belong to V0.1-A2/A3/B/C. This boundary must be real: no dummy production endpoint claiming data was collected.

Read before coding:
- `docs/harvest-a1-architecture-engineering-baseline.md`
- `docs/harvest-a1-a-provider-resource-transport.md`
- `docs/harvest-a1-b-distributed-scheduling-execution.md`
- `docs/harvest-a1-c-storage-api-operations.md`
- `docs/harvest-a1-proxy-egress-contract.md`
- `docs/harvest-v0.1-a0-acceptance.md`

## 1. Preserve A0

- Keep Go 1.27.3, native `uuid.NewV7()`, Fiber v3 + JSON v2, Cobra/Viper one explicit YAML, Router split (`internal/routers/router.go`, `url.go`, `health.go`), Task/Docker behavior, PostgreSQL pgx + sqlc + explicit Goose. Do not introduce ORM, Redis, Kafka, duplicate configuration or another service/repository.
- Reuse A0's runtime owner, Fiber hooks and startup rollback; extend it for scheduling/worker draining, not a second shutdown manager. Ensure `/livez`, `/readyz`, `healthcheck`, and safe `task stop` still work.
- The repo's existing A1 documents are normative. Where a concrete implementation decision is missing, pick the **simplest implementation satisfying the invariants** and document it in this file's acceptance report; do not speculatively scaffold future modules.
- No destructive migrations against existing shared/production databases. First real migrations require disposable fresh-DB and upgrade-from-A0 tests.

## 2. Provider/resource registry — only metadata and normalization in A1

Introduce a small compiled, typed Provider/Capability registration interface with manifest, request/output schema identifiers, strict per-capability parameter validation, resource canonicalization and worker compatibility requirements. **No GoFurry business identifiers**.

Target natural identity: `(provider, resource_type, canonical_external_key, access_namespace)`; PK UUIDv7. Canonical rules:
- `steam.app`: positive uint32 decimal AppID, reject overflow; canonical decimal identity.
- `web.domain`: validated normalized IDNA lowercase hostname, preserve `www`/subdomains.
- `web.origin`: validated HTTP(S) origin, canonical scheme/host/port, preserve nondefault port and distinction from domain.
- Unknown provider/resource/capability/schema/params are errors, not silently accepted.

**Capability advertising rule**: production discovery must not claim execution support before a real Provider implementation exists. It may report future metadata explicitly as `not_implemented` but must never enqueue it as executable. Supply a deterministic **test-only** Provider (compiled/registered only in test harness) to prove job execution end to end. Do not ship a synthetic success Provider as a production feature.

Canonical typed parameters + schema version, auth namespace and semantic scope form a stable fingerprint independent of request JSON key ordering/whitespace. Never collapse jobs across private scopes or incompatible semantics. Do not build a generic arbitrary-JSON ETL runtime.

## 3. PostgreSQL 18 — A1's first real schema

Goose migrations in `db/migrations`; stable runtime SQL via sqlc/pgx. Create only necessary A1 tables in the Harvest-owned schema:

- `targets`: UUID PK, provider/resource/key/access_namespace, lifecycle, stable UNIQUE natural identity.
- `schedules`: UUID PK, target FK, capability/request schema, canonical params/fingerprint/scope, UTC fixed-interval or cron, anchor/next_due, policy `state_refresh|point_in_time`, enable/pause, revision CAS and bounded missed coverage counters.
- `jobs`: UUID PK, immutable canonical request, cause, schedule/revision/slot lineage, priority/available/deadline, state, attempt counters, lease generation, latest accepted receipt/hash, optional originating Job; unique schedule slot and bounded active-equivalent dedupe.
- `attempts`: UUID PK, Job FK, Worker FK/instance, fencing generation, lease/heartbeat/end, sanitized status/errors and replay-safe terminal receipt. Unique(Job, lease_generation).
- `workers`: UUID PK **per process boot**, authenticated worker principal, allowed capability/schema/egress metadata (nonsecret), capacity, last_seen and draining state; heartbeat UPDATE, not an append-only event table.

Add only a small minimal durable idempotency/authorization lookup table if provably necessary. **Do not pre-create** `quota_buckets`, `quota_permits`, `current_results`, `observations`, `change_heads`, `change_events`, `consumers` or speculative business tables: A2/A3 own them.

Use PostgreSQL time for schedule slots, lease deadlines and state changes. Lock Job rows for transitions, `FOR UPDATE SKIP LOCKED` for claims, fixed SQL lock order and correct unique constraints. Avoid SQL injection and unjustified direct dynamic SQL. For UUID, verify sqlc generated mapping to the **Go standard-library `uuid.UUID`** or provide a documented, tested boundary conversion (not another application UUID allocator). Any numeric revision/lease generation is **not an ID**.

Migrations must work fresh and upgrade-from-A0, be safe on repeated explicit `migrate up`, and never apply at startup. Index due Jobs, expiring leases, due Schedules and canonical target keys. Benchmark realistic claim/reconcile queries with `EXPLAIN (ANALYZE, BUFFERS)`.

## 4. Job + Attempt state machine

Job:
`queued -> running -> succeeded|partial|failed`, or `running -> retry_wait -> running`; `queued/retry_wait -> cancelled`; `running -> cancel_requested -> cancelled`. Terminal status is immutable; operator retry creates a new Job referencing the old. A1 test-only results may reach a terminal success for the fixture execution only; production capability availability must remain truthful.

- Every claim inserts a new Attempt, increments the Job's fencing generation, sets lease using **DB clock**, and commits before handing work to Worker.
- Renew requires matching job/attempt/worker/fencing, current running state and **unexpired** lease; expired heartbeat never revives authority.
- Completion validates current fencing/lease. Duplicate same-attempt same-hash post-commit replay returns the same durable receipt; conflicting or stale attempt cannot commit.
- Cancellation/completion race is resolved by transactional Job lock, with deterministic winner. A cancelled Job is not retroactively rolled back at the external provider.
- Retry only on explicitly classified retryable errors, bounded attempt counts and backoff with deterministic test clock. Queue age/deadline and absence of eligible Worker must be explainable, not busy-looped.
- Fixture partial/failure responses should prove state logic, without writing a fake public Observation or declaring actual Steam/Web data collected.

## 5. Scheduling rules

Implement anchored fixed intervals and UTC Cron with documented minimum interval per capability/worker policy. Do **not** use an in-memory process-relative timer as durable truth.

- Unique `(schedule_id, schedule_revision, scheduled_for)`, with transactional due Job creation and `next_due_at` advancement; Run Now does not change phase.
- `state_refresh`: on downtime materialize at most one catch-up job for most recent eligible missed slot, record bounded missed coverage, resume anchored phase.
- `point_in_time`: missed slots recorded as missed counts/coverage; no retroactive fabricated measurements.
- Pause/disable stops future enqueue but does not silently cancel active jobs.
- Multiple reconcile calls or process restart cannot duplicate the same slot; concurrency and absence of work do not poll PostgreSQL in a tight loop.

## 6. Worker protocol and service authentication

Add `harvest worker --config <worker-yaml>` using the **same binary**. Worker configuration is a different explicitly validated YAML shape from Server mode: control-plane address, configured credential, supported/local capabilities, concurrency and (only placeholder declared) egress profile metadata. **No PostgreSQL credential or pool in worker mode**.

- Server has an explicit YAML registry of service principals, with consumer reader/operator/writer and Worker permissions distinctly scoped. Service credentials remain in private files; no web user/OAuth system or silent env auth.
- Worker authenticates/registers a fresh UUIDv7 instance, announces implementation/schema compatibility and allowed capacity. Server *verifies* it against principal permissions; self-advertising does not grant authority.
- `/internal/v1/workers/register`, `/:id/heartbeat`, `/:id/claim`, `/internal/v1/attempts/:id/renew`, `/:id/complete` are private authenticated endpoints with bounded request/response sizes, sensible timeouts and structured errors.
- Consumer/operator `/api/v1/providers`, `/targets`, `/schedules`, `/jobs` (including cancel and retry as separate new Job) and read-only operational summary are versioned/authenticated. Put full method/path mappings in `internal/routers/url.go`, health in `health.go`, handlers outside router. No placeholder `/observations` or `/changes` endpoints until A3.
- Embedded local Worker follows the **same lease/attempt rules**, even if it uses in-process calls. Long polling is bounded; Worker heartbeat/attempt renewals are distinct, and a worker may not claim outside its slot capacity.
- The production network service must be bound to loopback/private/Tailscale by default. Do not expose `/internal/v1` through public Nginx or treat Tailscale itself as authentication.

**A1 has no real network Provider execution**. Therefore worker-side egress/credential code should only have interface seams necessary for A2; do not fake a functioning proxy pool or issue unmetered real upstream requests.

## 7. Lifecycle, Task and Docker

- On startup: config validation, DB + migration compatibility, registry and auth, scheduler, Fiber, readiness; start claims only after completion API is listening.
- On SIGTERM: readiness false; stop new schedule creation/claims; allow current Attempt renew and result submission during bounded pre-drain; cancel remaining local fixture work, Fiber graceful shutdown, close pool exactly once; force-expired attempts recover on restart.
- Keep A0's `task dev/start/stop/build/test/check/sql:generate/sql:check/migrate/check:docker/check:containers`. Add `task dev:worker` or equivalent, no duplicate process supervisor. Ensure Windows and Linux CI and local native workflow.
- Use existing Dockerfile + Compose, support same image `worker` mode, and test two real independent Worker runtimes against the disposable PostgreSQL 18 deployment. No deployment to live CN/INTL VPS.

## 8. Acceptance gates

1. Fresh/upgrade migrations, actual `sqlc` generated queries reproducible, no Go runtime DDL or missing schema applied automatically.
2. Canonical target registration/upsert is idempotent; web domain vs origin remain distinct; Steam AppID normalization/rejections correct; service namespaces enforce isolation.
3. Cron/anchor correctness across restart; concurrent reconcilers produce one slot; Run Now does not shift phase; `state_refresh` catch-up only once; point-in-time missing coverage is not fabricated.
4. Two independent Worker processes race one Job -> one valid lease; expired holder cannot commit after takeover; late heartbeat invalid; retries bounded.
5. Worker crash and server restart recover work without manual DB edits. Same-attempt identical completion replay returns same receipt; different replay conflicts.
6. Cancellation racing completion yields one consistent terminal decision; no resurrection; no active work left orphaned by graceful shutdown.
7. Worker registrations reject incompatible capability/schema/credentials and attempts to claim unauthenticated/private work; Worker secret is not stored in central request/observation rows or logs.
8. `/livez`, `/readyz`, Task process ownership and Docker health still pass through DB outage, startup failure and SIGTERM.
9. Bounded idle polling/heartbeat DB IO on the 4C8G deployment reference; record actual qps, memory, pool connections, claim latency and observed bottlenecks; do not invent fixed performance success.
10. A test-only deterministic fixture Provider proves full claim -> execute -> renew -> complete/fail -> history; production API does not claim Web/Steam collection already exists.

All CI integration tests run against disposable PG18 with deterministic clocks where possible and isolated generated secrets; no live Steam, proxy, production credentials or GoFurry database.

## Completion reporting

Update README/architecture and a compact `docs/harvest-v0.1-a1-acceptance.md` with actual execution evidence, limitations and next steps. Commit only to `develop`, push, and report commit SHA. No meaningless test-log dumps, no release or production deployment. On success continue V0.1-A2 (Proxy & Egress / Global Quota), then A3 (Observation/Change Stream).

## Codex execution instruction

**Complete A1 end to end** within this defined scope. Choose minimal necessary Go files and SQL; prioritize robust transaction semantics, realistic two-Worker tests and maintainable Router/service separation. Do not ask to rebuild A0. Preserve branch protections, all existing documentation, and the unchanged GoFurry/steam-go repositories. Before implementation, check the current Go 1.27 UUID/sqlc type support and Fiber v3 APIs; adjust narrowly if verified behavior differs from the draft. 
