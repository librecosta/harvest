# Harvest V0.1-A4 — Operational Policy Persistence

Status: **Implementation-ready** (2026-11-09). Repository: **librecosta/harvest**, only **develop**. A3 completed at 71e712252e0918e3c61c7092ade47c4f212dbd33; retain A0–A3 functionality. This is a narrow prerequisite to production Web/Steam Provider development, **not** a new service or generic configuration platform.

## 0. Objective and contract references

The existing A2 implementation loads network source routing, quota limits and path policies from server YAML into `execution.Engine.network`, while `harvest.quota_buckets` / `quota_permits` persist **runtime accounting**. Make PostgreSQL the authoritative source for **operational network policy** and provide authenticated, version-controlled **online read/replace** APIs. Running servers must use current DB policy, not silently reapply an old YAML on restart.

Read `AGENTS.md`, `docs/harvest-a1-architecture-engineering-baseline.md`, `docs/harvest-a1-b-distributed-scheduling-execution.md`, `docs/harvest-a1-c-storage-api-operations.md`, `docs/harvest-a1-proxy-egress-contract.md`, `docs/harvest-v0.1-a2-acceptance.md`, and `docs/harvest-v0.1-a3-acceptance.md`. Code points: `internal/config/network.go`, `internal/egress/config.go`, `internal/execution/{engine,network,quota,workers}.go`, `internal/server/server.go`, `internal/routers/url.go`, `db/queries/quota.sql`.

**Database-owned policy:** global/provider/account/egress quota definitions (`per_minute`, `burst`, `concurrency`), source Capability+namespace to quota/profile associations, server-approved route mode, proxy path IDs/types/regions/bucket mappings, and explicit direct-fallback policy.

**Remain in explicit local YAML:** PostgreSQL connection, Fiber listeners, service principals and tokens, Worker-local actual proxy URLs/usernames/passwords, local proxy pool capacities/health/credentials, local Transport policy, and retention budgets. No central proxy credentials, no runtime environment-variable settings, no web-based secret editor. The Worker advertises only existing nonsecret logical profiles/paths and must never gain permission because the central policy changed.

Existing Target, Schedule, Job, Attempt, Result and Consumer data remain in their own existing tables; do not mix business content or GoFurry data into policy storage.

## 1. Schema and initial policy lifecycle

Use **one bounded authoritative policy document**, not many boilerplate tables per Provider/path:

- `harvest.operational_policies`: one named singleton row for `network` (stable text PK), `revision bigint > 0`, validated canonical `policy jsonb`, checksum, `updated_at timestamptz`, nonsecret `updated_by`. JSON shape is the existing small typed `egress.NetworkConfig` (global_bucket/routes/sources/quotas), strictly decoded and validated; no secret-bearing fields.
- Optional compact `harvest.operational_policy_revisions` containing UUIDv7 revision ID, prior/new revision, timestamp, service principal, safe before/after checksum and bounded previous nonsecret document (only if needed for usable audit/restore). At most a short configured history (e.g. last 20 revisions); no unbounded audit or duplicate persistent runtime metrics. Keep this a tiny implementation detail rather than building an event-sourced settings platform.
- Preserve `harvest.quota_buckets` and `harvest.quota_permits` as runtime accounting; do not reset available balance, upstream cooldown, daily metrics or in-flight permits on restart. No entity auto-increment PK, no ORM; SQLC/pgx and explicit Goose migrations. Test upgrading a populated A3 database.

**One-time import:** support an explicit operator command such as `harvest policy init --config config.yaml --file network-policy.yaml`, or an equally simple one-shot import of the existing server YAML `network:` block, **only when the DB policy row is absent**. It must reject a repeat unless an operator goes through the versioned update path. Never automatically import during `serve`, at startup or after a restart. A3 test fixtures relying on YAML must explicitly bootstrap their isolated DB, not silently retain two active policy sources.

A fresh database without an active network-capable Provider may run with a clear `not_configured` policy status and no network execution. If any compiled network Capability requires operational policy to function, refuse to start the network execution path without an initialized valid policy; do not allow unmetered fallback. The normal management/control API must remain explainable and safe. If the initial deploy needs a policy, the documented sequence is migrate -> explicit import -> serve.

On subsequent starts **only PostgreSQL policy** is loaded. The old server-side YAML `network` data may be accepted strictly as a one-time migration input during a documented compatibility period, but is NEVER an active automatic override. Update the example YAML and README to make precedence unmistakable.

## 2. Operator API (no new dashboard)

Use the existing operator service-principal role, private-first API, audit identity and Router separation:

- `GET /api/v1/operations/network-policy`: return nonsecret canonical effective policy, revision, checksum, updated time/actor and policy status; scoped/authorized operator only.
- `PUT /api/v1/operations/network-policy`: full-policy replacement with `expected_revision` and `policy`; validate the complete document and reject unknown fields. Optimistic CAS; stale revision => 409 `policy_revision_conflict`; semantically identical content => idempotent no-op without manufacturing a revision.
- Initial creation must be explicit and operator-authorized (one-time CLI init is sufficient); no unauthenticated bootstrap endpoint.
- Include effective policy revision/status in the **existing** sanitized operations status API; do not expose passwords, proxy URLs, raw credentials or private request data.
- No public browser access, no arbitrary `PATCH` matrix, no cross-region sync service, no human identity system or new Web UI. GoFurry Admin can later use these stable APIs.

Keep route lists in `internal/routers/url.go`, groups in `router.go`, health in `health.go`. Handlers should delegate validation/transactions to the owning service layer.

## 3. Validation and strict safety boundaries

Move/refactor the existing `validateNetwork` rules so **the same validator** applies to imported policy, DB-loaded policy and operator PUT. It must verify:
- correct complete shape and unique IDs; valid rates, bursts, concurrency, path kinds, regions, profile modes and cross-references;
- source Capability+namespace exists and is executable with an explicit network contract, and its direct/region/host constraints cannot be weakened by policy;
- configured principal scope allows the source/capability and Worker path offers; worker self-report never grants new privileges;
- a `require_proxy` route cannot silently become direct, even during edits, active work or missing endpoints. Explicit `prefer_proxy.allow_direct` remains two-sided opt-in, bounded by Provider contract and Worker YAML;
- do not accept unknown or secret-bearing fields in DB policy, arbitrary host/proxy URL edits, raw credentials, an unrestricted arbitrary fetch endpoint or remote Worker policy escalation.

Allow an operator to intentionally change routing mode or availability **only** when the newly supplied complete policy passes authorization and Provider safety checks. If a Worker does not actually offer the new route, jobs remain queued with an explainable waiting reason.

## 4. Consistent live activation, quota state and in-flight jobs

**One authoritative Control Plane** is the V0.1 deployment contract. Do not implement multi-leader watchers, Redis pub/sub, file polling, or a background cluster-wide configuration service.

Every online update must be atomic and race-safe:
1. Serialize policy updates and concurrent claim/permit admissions using the existing `Engine.admission` barrier (or an equivalently proven mechanism). Convert `Engine.network` to a safely published immutable policy snapshot, or place ALL reads behind the same lock. No data races or mixed policy versions across `source()`, `route()`, `plan()`, dispatch, permit, operator status and Worker registration.
2. In one DB transaction lock the policy row, validate expected revision, validate the new complete policy and referenced compiled capabilities, and reconcile affected quota bucket state. Commit new revision and document **before** acknowledging the PUT; publish one consistent in-memory snapshot while the admission barrier still prevents stale admissions. Failed DB commit or failed validation must preserve the old effective version everywhere.
3. When modifying quotas, settle refill under the **old** rate up to DB time, then clamp tokens to the new `burst`, reset refill timestamp and apply the new rate. Never gift accumulated old-rate tokens after a rate increase, resurrect an upstream cooldown, reset daily counters, or refund uncertain requests. Lowering concurrency cannot invalidate an already-issued network request, but must block **new** permits until active reservations fall below the new ceiling.
4. Keep old quota bucket keys/permits while referenced or cooling down; reject unsafe repurposing/renaming of a bucket identity that could bypass an active throttle. Removing a route/source or reducing privileges does not authorize old claimed jobs to contact a formerly allowed path.
5. Existing Job claims contain an egress plan created at claim time. Add a narrow policy revision/eligibility check to the permit path (and claim contract if needed). After route policy changes, an old claim must **fail closed** and move through a bounded, clearly classified retry/cancel path when it cannot satisfy the new rules. A permit already issued may finish and release safely; a fresh permit may only be issued under the new effective policy.
6. If only numeric quota values change and routing remains compatible, allow in-flight attempts to continue under **new** accounting and limits, without pointless task cancellation. Prevent A1 retry amplification or high-frequency polling.

A policy change applies to both local and remote Workers **without restart** because Server authoritative claim/permit checks use the updated document. Worker-local proxy endpoints, credentials, transport pool makeup and enable/disable state remain YAML-defined and require worker restart to change in V0.1. The two scopes must not be confused.

Performance: No per-request DB read of the large policy JSON. Read DB policy on startup and on mutation, use an immutable in-memory snapshot for hot-path evaluation, and keep token/permit state in existing small SQLC transactions. Retain strict 4C8G shared-VPS budgets.

## 5. Test with real PostgreSQL 18 and two Workers

Use only disposable A0–A3 fixture databases, real Worker processes and fake HTTP/HTTPS CONNECT/SOCKS5 providers/proxies. No live Steam, GoFurry DB or production.

1. Fresh A4 and populated A3 upgrade preserve targets, jobs, observations, change streams, quotas, cooldown, old active permits and migration lineage.
2. Explicit one-time import initializes DB. No policy is silently imported on serve/restart; an old YAML file cannot replace a newer operator edit.
3. Operator GET/PUT valid, no-op revision, stale simultaneous CAS, unauthorized reader/writer/worker mutation, invalid references/schema, and secret-bearing fields rejected without mutation.
4. Two independent Workers share a quota bucket. Live lower/raise `per_minute`, `burst`, `concurrency`; after API success, no new permit can use old limits. Confirm old cooldown remains, balances clamp, no phantom token or double-debit, active permit finishes.
5. Change route from allowed proxy to another approved profile; old claimed attempt never leaks to direct or wrong region. If no healthy authorized Worker path exists, Job waits instead of uncontrolled retries. Changing back resumes safe new claims.
6. Deliberate `require_proxy` outage remains zero direct attempts; 429/401/403 still cool down rather than rotate to evade upstream denial.
7. Restart the server with **stale original YAML** and confirm DB revision and routing/limit remain effective; remote Worker stays running and receives new policy decisions without restart.
8. Data-race/concurrency tests for policy read/update against claim, permit, registration, heartbeat, finish and metrics. Post-commit status and in-memory policy always agree.
9. No extra per-network-request SQL reads or unbounded write amplification merely because policy is database-owned. Compare baseline/A4 idle SQL/sec, permit p95, DB connection/wait, WAL and policy update latency on the documented constrained test.
10. All A0/A1/A2/A3 suites, Go race tests, Windows/Linux CI, Docker, Fiber graceful shutdown, and PostgreSQL readiness remain green.

## 6. Implementation limits and handoff

Preserve Go 1.27.3, Fiber v3 lifecycle, Router split, Task, Docker/Compose, `sqlc + pgx`, Goose explicit migrations, UUIDv7 for entity IDs, strict YAML, private Worker API and existing A3 retention. No production deployments and no edits to steam-go or GoFurry.

Update a compact `docs/harvest-v0.1-a4-acceptance.md` with actual executed tests and numbers, `contracts/database.md` for policy/quotas, and README/config examples for one-time import and operator API. Keep release prose terse. All A4 implementation code goes only to `develop`; do not touch master or pre-master.

**Next stage** after A4 acceptance: V0.1-B Web Provider, then V0.1-C Steam Provider using the already maintained steam-go SDK.

Codex: Implement the full A4 scope in one focused delivery. Favor two small PostgreSQL policy tables (main + bounded revision history if useful), one validated Policy owner and minimal new HTTP handlers. No config-center microservice or UI. Run disposable Postgres concurrency tests; do not weaken A0–A3 tests for convenience. Commit/push develop; report SHA, measured verification and genuine unresolved issues only.
