# Harvest A1-B — Distributed Scheduling & Execution Contract

Status: **Detailed design, accepted direction; implementation pending** (2026-11-08).
Depends on [A1 Architecture & Engineering Baseline](./harvest-a1-architecture-engineering-baseline.md), [A1-A Provider & Resource Contract](./harvest-a1-a-provider-resource-transport.md) and [A1-P Proxy & Egress](./harvest-a1-proxy-egress-contract.md). A1-C will freeze SQL DDL and HTTP request/response shapes. This document governs the V0.1 execution semantics.

## 0. Scope and invariants

Harvest has **one authoritative control plane per deployment**, a PostgreSQL 18 database as the **single durable source of truth**, and one or many pull-based Worker processes. `harvest serve` can embed a local Worker; `harvest worker` uses the same binary and an authenticated private control-plane API, never direct SQL. Multiple Control Plane leaders/HA and multi-database distribution are not V0.1 requirements. The DB-backed algorithms must still survive process restart and accidental concurrent scheduler executions.

Hard invariants:

1. No app-level model or business IDs from GoFurry. All entity primary keys are Go 1.27 native UUIDv7 and PostgreSQL `uuid`. An incremented numeric **fencing generation** is permitted as a version, not as an entity ID.
2. A Job is a **logical acquisition request**, not a process invocation. Each execution/re-execution is a distinct Attempt with its own UUIDv7.
3. At most one **current valid lease generation** per Job. An expired worker may still physically be running; only the current generation can commit. No claim of exactly-once external HTTP effects.
4. Every Job state transition and result acceptance is serialized through its Job row in PostgreSQL transactions. Late or duplicate HTTP submissions cannot silently overwrite observations.
5. Providers do not manipulate SQL, schedule transitions, global quota state or network egress routing.
6. No ambient environment proxy, secret-bearing central task payload or unauthenticated remote Worker API. If global quota/authorization cannot be obtained, outbound requests fail closed (no local unlimited fallback).
7. Source 429/auth denial and policy refusal never trigger uncontrolled proxy rotation. Successful scoped observations are not erased by failures or absent/partial responses.
8. Unbounded schedule catch-up, retries, queue growth and historical data growth are forbidden. A1-C specifies concrete retention and operational alerts.

## 1. Durable entities (logical, not yet SQL DDL)

- `Target`: canonical Provider/Resource/external key and access namespace (A1-A).
- `Schedule`: Target + Capability/version + canonical parameters/semantic scope + UTC recurrence, anchored start, policy (`state_refresh` or `point_in_time`), enabled/paused state, revision and next due time.
- `Job`: UUIDv7, immutable request fingerprint and caller scope, cause (`scheduled`, `manual`, `retry_requested`), priority, due time, current state, deadline/max queue age, attempt count, current lease_generation, latest accepted outcome, and optional originating Job/Schedule reference.
- `Attempt`: UUIDv7, Job ID, lease generation, Worker instance ID, claimed/started/last heartbeat/lease expiration/ended timestamps, terminal attempt error classification and safe summary. One row per actual claim; no separate 'Run' hierarchy in V0.1.
- `WorkerInstance`: UUIDv7 per process startup, authenticated principal/reference, registered Provider+Capability+schema versions, egress profile names and regions, parallel slot capacity, heartbeat/last seen/draining state. Operator identity and credentials are managed independently of instance IDs.
- `QuotaBucket`/`Permit`: server-authoritative network request/rate and concurrency accounting. Numeric refill tokens, active counters and fencing metadata are **not entity IDs**. Permit IDs may be UUIDv7 when persisted.
- `Result/Observation` and delivery change records: A1-A and A1-C own the payload/storage and commit-safe consumer stream; accepted result + Job/Attempt finalization + applicable change notification must commit atomically.

Avoid unneeded historical worker heartbeat rows and a universal workflow/DAG schema. Active endpoint proxy health belongs to Worker memory; only sanitized bounded diagnostics may be persisted later by A1-C.

## 2. Job state and transitions

Job states:
- nonterminal `queued`: eligible when `available_at <= database_now()` and not cancelled/expired;
- nonterminal `running`: owned by an Attempt with a current lease_generation;
- nonterminal `retry_wait`: previous attempt failed or lease expired, next attempt not due;
- nonterminal `cancel_requested`: cancellation accepted while current attempt may still run; cannot accept new result writes;
- terminal `succeeded`, `partial`, `failed`, `cancelled`.

`partial` is a terminal, accepted partial Outcome only when the declared scope permits partial success **and** the retry policy is exhausted or no further retries are appropriate. A retryable partial result can commit successfully validated scoped segments in the same attempt, then transition to `retry_wait` for the remaining eligible scopes. Never silently report full success for incomplete declared scope.

Transitions:
- create -> `queued`;
- `queued` / due `retry_wait` -> `running` on atomic claim, create Attempt, increment fencing generation;
- `running` -> `succeeded` on complete scoped result;
- `running` -> `partial` on accepted terminal incomplete scoped result;
- `running` -> `retry_wait` on bounded retryable outcome/expired lease with attempts remaining;
- `running` -> `failed` on terminal error/deadline/attempts exhausted;
- `queued`/`retry_wait` -> `cancelled` immediately when requested;
- `running` -> `cancel_requested` on cancellation; then `cancelled` after cancellation acknowledgement or expiry;
- terminal states have no transitions. A human 'retry' creates a fresh Job referencing the earlier Job; never rewrites terminal history.

A `queued` Job without eligible Workers or proxy profile remains waiting with a safe 'why blocked' diagnostic; it is not repeatedly claimed and failed. Max queue age and deadline create a bounded `failed` outcome when configured, never an infinite silent backlog. Quota exhaustion defers issuance rather than creating useless failures on every poll.

Cancellation-vs-completion race: whichever transaction first locks and commits the Job row decides. If a completion committed first, cancellation returns 'already terminal'. If cancellation commits first, subsequent result mutations are rejected; safe cancellation acknowledgment is allowed. A cancelled task may have already contacted the external source; no claim of rollback.

## 3. Atomic claim, lease and fencing

Worker poll passes authenticated Worker instance and advertised capacity. Control Plane selects eligible jobs according to provider/capability+schema support, network/egress profile, authorization and available worker slots.

Claim transaction, using PostgreSQL clock as sole time authority:
1. Select a due, eligible Job using `FOR UPDATE SKIP LOCKED` and a stable order (priority, `available_at`, UUID tie-breaker); do not hold this transaction while doing external I/O.
2. Recheck terminal/cancel/deadline, per-request dedupe/concurrency-lane rules and admitted Worker capabilities.
3. Increment `lease_generation` on that Job, insert Attempt, assign `lease_until=database_now()+lease_ttl`, move Job to `running`, commit.
4. Return Job UUID, Attempt UUID, fencing generation, lease deadline, canonical scoped request and only nonsecret egress capability requirements.

At most one generation is current in DB; stale workers can run concurrently until their cancellation observes lease loss, so we cannot promise exactly one *physical* HTTP execution.

Lease renewal is a separate privileged operation. It matches Job/Attempt/Worker identity + fencing generation, requires `running` and a not-expired current lease, and extends using **database time**. Late heartbeats do not resurrect expired leases. Distinguish Worker-instance heartbeat (online presence) from per-Attempt lease renewal.

Suggested test defaults, subject to measured acceptance: 60s lease, 15s heartbeat, up to 15s long poll, all configurable with enforced heartbeat/lease and upstream deadline bounds. If a task can run for minutes, repeated renewals are mandatory. A remote Worker must cancel its context and stop new network calls as soon as it knows its lease has been revoked or cannot renew within the safe window.

Recovery pass:
- expired `running` Attempt -> mark expired, schedule retry with backoff if allowed else `failed`;
- expired `cancel_requested` -> `cancelled`;
- retry eligibility/deadline checked before a new claim;
- do not rewrite historical attempt outcomes, and do not manufacture a historical Observation.

Commit result transaction:
- lock Job first, verify current Attempt UUID, Worker ID, lease generation, state and not-expired lease;
- validate schema, scope, size and result digests; apply only valid per-scope segments;
- atomically commit Job/Attempt status, latest good scoped values, immutable accepted observations and applicable delivery changes.
- On HTTP response lost *after* a committed acceptance, identical same-attempt replay is acknowledged with the original receipt, even if the Job is now terminal. Different payload hash for an already accepted attempt is rejected. A previous generation never gains authority to write.
- Keep commit lock order stable (`Job -> Attempt -> scoped current/result -> delivery event`) and enforce only one atomic acceptance per Attempt, even across API retries.

## 4. Scheduling semantics

Server owns recurrence; Workers cannot add their own timers or alter Schedule. Scheduler uses short periodic DB reconciliation, not in-memory 'since process start' timers as schedule truth.

- Fixed interval is anchored to an explicit `anchor_at` UTC timestamp and period (slot = anchor + n * period). Cron runs in UTC only for V0.1. Validate minimum allowed interval per Capability/tenant and a maximum backlog. DST ambiguity is eliminated by UTC-only recurrence.
- `scheduled_for` is the canonical slot. Uniqueness on (`schedule_id`, `schedule_revision`, `scheduled_for`) prevents repeated materialization. Schedule creation, due Job insertion and advancement of `next_due_at` are transactional. Run Now creates a manual Job with optional Schedule lineage and must never shift recurring phase.
- Schedule revisions change on recurrence/parameter/scoping edits. Disable or pause stops **future** issuance; it does not automatically cancel an active Job. A separate action can explicitly cancel affected pending/running Jobs.
- `state_refresh`: when many slots passed during downtime, enqueue **at most one** catch-up Job for the latest eligible missed slot, record a bounded missed-slot count/summary, and then resume normal anchored phase. Never flood N catch-up requests.
- `point_in_time`: missed slots are recorded as missed coverage; do not query the current upstream state and label it as historical state. Resume from the next valid slot; no synthetic past player count.
- Job request fingerprint includes Target, Capability/version, canonical parameters, visibility/credential scope and semantic egress region. Concurrent *active* equivalent work may be joined only when allowed by privacy/source semantics. Different time slots still have separate scheduling lineage, and coalescing cannot falsely report unperformed point-in-time samples.
- The schedule engine creates bounded batches and uses a deterministic reconciliation watermark. After prolonged downtime, it computes skipped slot counts without creating one row for every historical gap.

## 5. Worker registration, dispatch and execution ownership

- A Worker registers a new instance UUIDv7, authenticating via a private credential scoped to that operator-managed worker (stored in worker YAML). API is bound to private/Tailscale ingress, protected by service authentication, and never exposes worker credentials to callers.
- Worker advertises Provider/Capability **and schema versions**, maximum parallel slots, actual egress profile names/regions, and transport types. The server verifies these against an authorized Worker policy; worker self-report alone cannot grant privileged source/credential rights.
- V0.1 uses authenticated pull/long polling: no WebSocket broker or externally exposed message bus. Poll returns at most a bounded number of leases within capacity; cancellation/lease decisions are observed on heartbeat. Slot counts are tracked conservatively by the Worker and Server.
- An embedded local Worker uses the **same execution protocol/lease rules**, even if it may call an in-process adapter rather than serialize every local HTTP hop. Avoid a hidden 'single-node shortcut' that bypasses fencing or quotas.
- Queued Job priority has a tiny fixed set of bands (e.g. operator/manual, normal, maintenance) with fair/aging admission so manual work does not starve scheduled tasks. No generic workflow dependency graph or speculative dynamic scheduler.
- Worker is responsible for per-Attempt context deadline, acquired Egress Client, bounded Provider execution, result serialization and sanitization, and submitting result/ack. Provider never receives SQL credentials.

## 6. Global quotas, egress and retry budgets

A1-P owns Worker-local proxy endpoints. Control Plane owns **global eligibility and quota grants** so total requests remain bounded across CN/INTL Workers.

Quotas are keyed as needed by Provider, upstream host/traffic class, upstream credential identity, authorized source access scope, and known shared egress IP/profile. Avoid assuming two Worker instances with the same proxy endpoint have independent upstream quota. Provider manifest supplies policy bucket definitions; active operator configuration sets limits.

V0.1 authoritative admission:
- A Provider HTTP request must obtain a short-lived, server-issued **request permit** through the Worker transport just before outbound execution; a Job claim alone is not a license for unlimited internal HTTP calls. This includes all SDK retries and redirects that create additional upstream requests.
- Implement a PostgreSQL-backed token bucket and concurrency reservations with short critical sections; serialize each contended bucket row, use DB time, and do **not** hold SQL locks while network requests are in flight. Conservatively consume uncertain request tokens; do not refund merely because response status was unknown. Concurrency reservations have expiry/fencing so crashed Workers eventually release capacity.
- Use a deterministic ordering when reserving several applicable buckets to avoid deadlocks. Permit cannot be issued if any required bucket would exceed its bound; no partial quota spending on a failed multi-bucket admission.
- A permit accounts for a bounded request/response budget, is valid only for the correct Worker/Attempt/bucket and short TTL, and is not transferable. If the control plane is unreachable, the Worker **fails closed for new outgoing HTTP operations**, keeps its Job/lease recovery semantics, and does not silently use an unlimited local fallback.
- Expired permits do not license extra traffic. Count network-level attempts, not just logical Jobs. An SDK's internal automatic retry is either disabled or is fully intercepted by the common permit-aware RoundTripper. For non-HTTP transports, Provider must obtain an equivalent operation permit at the dial/probe boundary.
- Per-worker/endpoint concurrency and bandwidth caps are additional local bounds. For V0.1 favor simple authoritative per-request permits over large distributed token leasing. Optimize round-trip overhead only if measured.
- Respect upstream `Retry-After`, auth failures, source-origin 429/403 and written terms. Source rejection should cool down appropriate Provider/credential/source buckets, **not** rotate proxy identities to bypass limits. Transport connectivity failure may permit a bounded same-profile proxy failover per explicit A1-P policy.
- Set separate budgets for: network operations inside an Attempt, Attempt count per Job, queue delay/deadline, total bytes and retries. Defaults must be conservative and configurable by Capability; avoid multiplicative nested retries.

Possible outcome categories:
`success`, `partial`, `upstream_unavailable`, `upstream_throttled`, `upstream_auth`, `transport_error`, `proxy_unavailable`, `quota_deferred`, `invalid_payload`, `policy_denied`, `worker_lost`, `cancelled`, `internal_unavailable`.
Only a subset are retryable, and each error has a bounded retry/backoff policy. Avoid unbounded retries on malformed data or permanent auth denial.

## 7. Graceful lifecycle under Fiber v3 and Docker

Runtime Owner, not Fiber handlers, owns **all** initialization/shutdown of DB, control engine, worker(s), proxy pools and HTTP app. Fiber hooks are used consistently, but not as the only way to stop long-running work.

Startup:
1. Validate one explicit YAML configuration, authorize worker/egress scope and validate the compiled Provider registry before external actions.
2. Initialize logger, network clients/proxy pools, PostgreSQL and migrations compatibility check (**never run Goose automatically**), quota/scheduler/Worker runtime.
3. Register router groups, worker/private API, `/livez` and `/readyz`; register hooks. Start Fiber `Listen` in managed concurrency; `OnListen` marks readiness after listener and essential dependencies are actually healthy. Startup failures rollback in reverse order, not relying on shutdown hooks.
4. Start/resume scheduling and Worker polling only when dependencies are ready; avoid a startup gap in which jobs are claimed before the API can accept results.

SIGTERM/Task stop/Docker shutdown coordination:
1. Set readiness false and mode `draining`, stop creation/claims of new Jobs, stop Worker polling, stop issuing new quota permits for **new** work; preserve permitted completion paths and existing-lease renewals while draining.
2. Keep private endpoints alive for a bounded **pre-drain window**: allow current Attempts to complete/commit and receive acknowledgements; notify remote Workers to stop new activity via heartbeat responses. Do not call Fiber shutdown immediately if this would make result submission impossible.
3. Cancel remaining local attempt contexts when grace runs out; remote Attempts may safely expire and be recovered on next boot. Do not fabricate successful completion. Flush safe state without trusting an unlimited shutdown timeout.
4. Invoke Fiber `ShutdownWithContext` and let `OnPreShutdown` idempotently enforce non-admission, HTTP connection drain, and `OnPostShutdown` post-HTTP finalization. Close remaining proxy transports and DB pool **after** HTTP/result pathways are stopped. Fiber hooks execute in their documented order; the Runtime Owner is responsible for cleanup on every path including failed startup.
5. Container grace (`stop_grace_period`) must exceed Harvest internal deadlines plus a small margin; `task stop` verifies process ownership.

A failed optional remote Worker does not fail Server readiness. DB outage makes Server not ready and prevents new claims/permits. Provider-side 429 does not fail Harvest readiness. Worker readiness reflects authenticated control-plane connectivity and internal ability to handle jobs, not task availability.

## 8. Observability and security

At minimum expose safe counters and timestamps via authenticated status/diagnostics: queued/running/retry_wait/cancel_requested counts, oldest due Job age, missed schedule coverage, current Worker last-seen/capacity, lease recovery count, permit denials, per-bucket cooldown, classified upstream/transport failures, source latency, proxy health summaries. V0.1 needs no Prometheus server dependency; a future metrics endpoint may be added only when justified.

Logs include request/trace IDs, Job/Attempt/Worker UUIDs and stable provider/capability identifiers. Never log raw payloads, token-bearing URLs, proxy credentials, OAuth tokens, cookies or entire HTTP errors. Distinguish operator diagnostics from the public consumer API. Limit heartbeat/progress write rate; history is bounded by A1-C.

Tailscale/private networking is not authorization. Worker credentials bind to allowed Provider and egress privileges, and consumer authentication cannot claim or submit work. SSRF and proxy-side DNS enforcement from A1-P continue to apply in distributed execution.

## 9. Test and acceptance matrix

Use deterministic fake clock, local mock upstream servers, fake proxies, disposable PostgreSQL 18 and two independent Worker processes. No live Steam credentials or public crawling are required for core CI.

1. Same schedule slot raced by two reconcilers -> exactly one Job; app restart preserves anchored phase.
2. Manual Run Now does not change recurring next_due; pause/resume does not silently cancel running work.
3. State-refresh downtime catches up once; point-in-time downtime records missing coverage and does **not** invent past samples.
4. Two Workers race one Job: exactly one valid current lease; expired holder cannot overwrite newer results. Stress claim/renew/complete/cancel races.
5. Commit succeeds but HTTP response is dropped: replay of the same accepted Attempt yields same receipt without duplicate observations/change events; different replay payload rejected.
6. Partial Steam locale/region outcome updates valid scoped results while keeping failed scopes last-known-good; real zero player count is kept, not replaced with failure.
7. Multi-request Steam SDK task cannot exceed global network request permits, even with internal retry/redirect; multiple Workers sharing one upstream account/egress quota honor combined limits.
8. Control-plane/DB loss causes new outbound requests to fail closed; no silent direct proxy fallback, no unlimited local quota.
9. `require_proxy` Worker without healthy eligible proxy cannot claim tasks requiring that profile; bounded transport failover works only within allowed routes.
10. Provider 429/401/403 is classified and cooled down appropriately; no uncontrolled proxy rotation and no repeated retry multiplication.
11. Operator cancellation wins or completion wins under row-locked deterministic transaction order; late result after cancellation cannot mutate data.
12. SIGTERM pre-drain allows an in-flight remote Worker to commit before HTTP listener closure; forced timeout leaves recoverable leases, and DB/pool cleanup occurs exactly once.
13. Worker capability/schema/authorization mismatches never claim tasks; absent workers leave bounded queued diagnostics, not false failure attempts.
14. Short polling/heartbeats, recovery, quota rows and observation writes remain within measured PostgreSQL connection/CPU/IO budgets; no fast busy loops, ever-growing heartbeat tables, or unbounded scheduled catch-up.

## 10. Explicit V0.1 non-goals and A1-C handoff

No worker marketplace, DAG/workflow engine, multi-controller consensus/automatic HA, global traffic optimizer, Kafka/Redis queue, arbitrary third-party executable plugins or exactly-once promises. No automatic bypass of provider source limits or licensing.

A1-C will freeze SQL constraints, indexes, transactions and sqlc query patterns; worker route request/response shape; input validation/auth scopes; precise default timeouts; raw/attempt/lease/permit retention; repair CLI; and the integration/deployment acceptance.

References:
- https://www.postgresql.org/docs/18/sql-select.html
- https://docs.gofiber.io/api/hooks/
- https://docs.gofiber.io/blog/fiber-v3-graceful-shutdown/
