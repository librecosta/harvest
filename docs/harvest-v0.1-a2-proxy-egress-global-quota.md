# Harvest V0.1-A2 — Proxy, Egress Routing & Global Quotas

Status: **Implementation-ready scope** (2026-11-08). A1 control plane completed at `develop` commit `a3288310f2e13591a796b288cf1f4919d7f5faf3`; A2 extends it without rebuilding A0/A1. Work only in `librecosta/harvest` `develop`. No deployment, real upstream acquisition, database/data migration or modifications to `steam-go` are authorized in A2.

Normative docs:
- `docs/harvest-a1-architecture-engineering-baseline.md`
- `docs/harvest-a1-a-provider-resource-transport.md`
- `docs/harvest-a1-b-distributed-scheduling-execution.md`
- `docs/harvest-a1-c-storage-api-operations.md`
- `docs/harvest-a1-proxy-egress-contract.md`
- `docs/harvest-v0.1-a1-acceptance.md`

## Goal and boundaries

Turn proxy-aware egress into a **real Worker execution capability** and enforce **server-authoritative, cross-Worker outbound request quotas** using the existing single PostgreSQL 18 control plane. Demonstrate correctness with deterministic test-only HTTP mock Providers, local mock upstream targets, a trusted fake proxy, and at least two independent Workers. Use the same single Go binary; production Web and Steam remain explicitly `not_implemented` until V0.1-B/C.

Do not turn Harvest into a forward proxy, dynamic provider plugin system, VPN/proxy marketplace, or general traffic bypass tool. Provider/source conditions and authorization remain effective regardless of proxy.

## 1. Preserve and extend the A1 runtime

- Retain `uuid.NewV7()`, pgx/sqlc/Goose, Fiber v3 route separation, typed JSON v2, strict explicit YAML config, Task/Compose, database schema ownership, worker protocol, fencing and idempotent completion.
- The current `provider.Capability.Execute(context.Context, Task) Result` cannot enforce egress or quotas. Add the narrowest execution-services argument, e.g. `Execute(ctx, task, services) Result`, providing ONLY authorized protocol-specific HTTP Client/transport or network dialer, operation-permit gate, cancellation/deadline/size budget, and sanitized metrics. Adapt the test-only executor; do not expose raw server SQL, proxy credentials or an unrestricted network client to Provider code. Keep logical capability/result schemas and existing Job/Attempt external APIs compatible unless change is essential.
- Worker owns the pool lifetime and closes clients/transports **once on drain**, not per request. Different proxy endpoints and credential/session isolation must not accidentally share a wrong connection. Shutdown may have valid in-flight operations; permit finish and result submission remain available during the existing bounded pre-drain. Avoid a new lifecycle manager.
- A1 already advertises `egress_profiles` but does not route; A2 makes them real. Do not confuse a worker-advertised name with server-authorized permission.

## 2. Proxy pool: Worker-local, YAML-owned

Extend the **same explicit worker YAML** (and embedded local_worker config when applicable) with a compact `egress` configuration:
- named profiles; each profile has an explicit route mode: `direct`, `prefer_proxy`, `require_proxy`;
- named endpoint IDs with HTTP/HTTPS/SOCKS5 proxy URLs, enable/disable status and optional declared region, bounded concurrency, health check target, cooldown and traffic/byte budget;
- whether `prefer_proxy` may **explicitly** fall back to direct; default **false**;
- optional source/capability matching policy from Control Plane; Worker may further constrain but **never broaden** server authorization.

Strictly validate type, URI scheme, hostname, port, credentials, profile references, duplicate IDs, timeout/concurrency bounds and region. Reject unsupported proxy or protocol combinations *before* claiming jobs. Credentials stay in Worker-local ignored YAML, not in Target/Job/Attempt, central DB, work assignment, public discovery or logs. Avoid automatic `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, `NO_PROXY`, `.env` imports and config file search.

For each named profile Worker advertises **nonsecret** capacity, supported transport kinds, region and coarse health state to the control plane through registration/heartbeat. The Control Plane uses these as hints plus its own principal policy and request requirements; it does not treat self-report as privilege escalation. If no eligible/healthy path exists, Job remains explainably blocked/deferred rather than being repeatedly claimed then failed.

A2 supports explicit HTTP(S) and SOCKS5 proxy endpoints for HTTP(S) Provider requests. Raw TCP/DNS forwarding is **not universally supported** merely because a proxy exists; declare only protocol modes actually implemented. Avoid speculative browser/headless integration. Reuse Go standard `net/http.Transport` and audited SOCKS5 support; avoid rebuilding protocols.

## 3. Routing and retry policy

Implement Egress Router below Provider-specific request logic:
- `direct`: no proxy;
- `require_proxy`: must use an eligible endpoint; on exhaustion **never** secretly fall back to direct;
- `prefer_proxy`: selects an eligible endpoint first, may use direct only when explicitly authorized by that profile and the source/Task.
- Selection is stable per required session/attempt and traffic class, with a small bounded healthy-candidate selection scheme (e.g., least-inflight and deterministic tie-break). Do not make rotation itself the resilience strategy.
- Verify Worker is eligible for semantic `egress_region` and task scope: if source response differs by region, do not collect via an arbitrary alternate region and report it as the requested region. The actual proxy endpoint is **not** Target identity. Authorization namespace/credential scope remains mandatory for deduplication and transport sharing.
- Maintain local health states `healthy/degraded/cooldown/disabled`, with conservative active checks to a configured **trusted** probe endpoint, passive transport failures, cooldown then controlled recovery. Distinguish broken proxy transport from origin HTTP failure. Keep health probes bounded and avoid calling third-party targets at scale.
- Retry/failover on **eligible network-connect or proxy-handshake errors** only, with strictly bounded attempts and Job deadline; preserve request cancellation and avoid repeating unsafe operations. 429, authorization denial, policy blocks, or other upstream refusal must cool down appropriate upstream/provider credential buckets and must **not** trigger uncontrolled proxy cycling. A request may have a sticky session: keep route stable unless explicit replay permission exists.
- Transport reuse is essential: no new `http.Transport` per request. Prove shared-connection ownership and close/drain behavior.

## 4. Server-side global quota contract

An A1 Job may create multiple actual HTTP operations, retries or redirects. **Quota accounting is on outgoing network attempts, not Jobs**. Add minimal PostgreSQL tables (A1-C design): `harvest.quota_buckets` and `harvest.quota_permits` plus narrowly necessary constraints/indexes. No auto-increment entity PK. Persist only required active permits, compact counters and bounded aggregate diagnostics; no unbounded one-row-per-request lifetime history.

Quota dimensions as actually needed for V0.1:
- Provider + capability/traffic class;
- upstream host/API service and credential/account reference (nonsecret stable ID);
- shared known egress route / proxy endpoint / egress IP as appropriate;
- worker-local endpoint concurrency and byte limits (additional, not substitutes).

The control plane declares policy via server YAML (explicit minimum configuration and secure defaults), and derives the applicable bucket set from **trusted task + manifest + authorized Worker route**; Worker may not choose an arbitrary bucket key/limit to grant itself more tokens. Sort bucket lock order deterministically. Within one short DB transaction use PostgreSQL clock, SELECT FOR UPDATE bucket rows, refill safely and debit all applicable buckets **atomically**, and reserve bounded concurrency capacity with expiry; do not hold locks during network IO. If any bucket blocks, do not partially charge others; respond with a bounded retry-after/defer indication, not a busy polling loop. Use pgx/sqlc query contracts; no dynamic SQL injection.

Add separately authenticated worker-only endpoints under `/internal/v1`:
- `POST /attempts/:id/permit` for one authorized outbound operation with worker ID + Attempt UUID/fence + stable operation idempotency ID (UUIDv7), upstream class + approved egress selection, and bounded byte/cost intent. Reject stale lease/attempt, wrong principal, wrong profile/host/namespace and drained/failed authorization.
- `POST /permits/:id/finish` for idempotent conclusion/release and sanitized usage counters. Never return/store credentials, full source URLs, raw bodies or proxy URLs.

Permit issuance must be **idempotent** by Worker+Attempt+operation ID and payload fingerprint; a retried HTTP control request must not burn a second token, and a changed replay conflicts. Expiry releases concurrency safely but a previously uncertain on-wire request is still counted against rate quota; no false token refund. Strict TTL/lease binding and cancellation/fencing checks apply. If control plane/DB is unavailable, **new outbound network requests fail closed** without local unlimited bypass. On graceful drain, allow already accepted in-flight work to finish/ack according to bounded policy, never admit arbitrary new Job work.

Place permit acquisition **at each actual HTTP RoundTrip**, including redirects and SDK-internal retries. At worker transport boundary, do not let an injected alternate HTTP Client bypass the gate. Permit must be obtained before dialing, and concurrency accounting must be finished on terminal transport failure or once the response body has been closed/fully consumed (bounded timeout on leaking bodies), not immediately after response headers. Preserve cancellation throughout. Optional non-HTTP operations need their own audited permit-aware dialer in a later stage, not a fake universal gate.

### Cost / efficiency bound

A1 measured an idle ~22.49 SQL statements/s across three Workers and a four-connection Server pool at capacity. A2 must log actual incremental permit/QPS, pool pressure, lock wait, WAL/DB bytes, heap and network overhead. Prefer short SQLC transactions and bounded local backoff/circuit state; no permanent per-request SQL log. If per-roundtrip permit requests are too expensive, document evidence and propose a later safe *fenced token-lease* optimization, not an unsafe automatic bypass.

## 5. Safety, limits and source policy

- Prevent SSRF across **every** hop: validate source/target, redirects and resolved IP at connect time, including reserved/private/link-local/metadata/DNS rebinding addresses. Arbitrary-domain requests through a proxy **must not** be allowed unless the trusted egress proxy (or equivalent exit control) can enforce destination-IP/CIDR restrictions at actual remote DNS resolution/connect time. A client precheck that a proxy later re-resolves is insufficient. For trusted fixed-source provider APIs, enforce a fixed host allowlist and permitted redirect scope.
- Do not create a public arbitrary URL fetch/proxy API in A2. In test fixtures permit loopback/local mock traffic only through an isolated explicit test boundary; production configuration must not allow that behavior.
- Enforce method, destination/host, timeout, body bytes, content type/response-size and max network requests per Attempt. No automatic environment proxies or TLS verification disablement. Strip sensitive headers on cross-host redirects and do not disclose credentials in error text.
- Do not use proxy rotation to evade origin throttling or access policies. Respect upstream authorization, request budgets and usage rules.
- Source/credential scope, geographic restrictions and data-use constraints are explicit; the single CN collector does not automatically license international transfer of all results.

## 6. Router, lifecycle, observability, Task and deployment

- `internal/routers/router.go` owns groups, `url.go` exact method/path inventory, `health.go` remains probes; Handler/Engine/SQL separated. Worker routes are protected by worker-only service principal, never publicly exposed. No new external consumer API for proxy credentials or arbitrary URLs.
- Extend authenticated `/api/v1/operations/status` with sanitized pool/profile eligibility, quota deferrals, per-provider/egress success/failure, traffic bytes, bounded bucket/permit counts, DB pressure; aggregate data with bounded retention, avoid raw per-request tables/log spam. Do not turn `/readyz` into a proxy/upstream health assertion: an unavailable optional proxy does not make the entire control plane unhealthy.
- Use one Fiber Runtime Owner with `OnListen`, `OnPreShutdown`, `OnPostShutdown`; Worker pool transport shutdown is idempotent, and existing A1 pre-drain keeps result/permit completion endpoints accessible for bounded active work. Do not copy another scheduler or goroutine control loop.
- Extend explicit YAML, `config.example.yaml`, documented worker example (ignored secrets), Task's existing `dev/start/stop/dev:worker/build/check/check:containers`, and existing Compose. No new repo or separate proxy service; no second PostgreSQL. Prevent `task stop` regression and keep Windows/Linux CI.
- If the existing A1 `MaxBodySize=16KiB` or execution timeout conflicts with genuinely bounded diagnostic/permit metadata, revise minimally with tests; do not introduce arbitrary large control messages.
- New schema changes through explicit Goose; regenerate sqlc and prove fresh + A1-upgrade migration. Do not perform destructive historical changes.

## 7. Deterministic acceptance / test matrix

Use disposable PostgreSQL 18, test-only compiled networking Provider, two **independent Worker processes**, local deterministic HTTP upstream + local fake HTTP and SOCKS5 proxies, and fixture-only safe-loopback network policy. No external steam requests, API credentials or public scraping in normal CI.

1. One CN Worker with `steam-like` test task routes through specified proxy while a direct-designated task uses direct; verify proxy was actually traversed.
2. All endpoints down: `require_proxy` never uses direct; `prefer_proxy` uses direct only when explicitly opted in; same-region and scope restrictions cannot be bypassed.
3. At least two Workers with different profile advertisements: only eligible Worker may claim; unauthorized profile announcement or profile mismatch is rejected.
4. HTTP and SOCKS5 fake proxies really carry requests. Pool state transition, endpoint disable/cooldown/recovery, connection reuse and sticky routing are observable.
5. Per-actual-roundtrip permit gate counts SDK-style retry and redirect attempts. Two Workers sharing one bucket never exceed the configured combined cap. Cancelled/stale-lease jobs cannot obtain fresh permits.
6. Duplicate permit request same operation ID produces same receipt; conflicting replay rejects; completion duplicate is safe; expired permit restores concurrency but does not refund spent rate tokens.
7. Control-plane/DB outage fails closed for fresh outbound requests, local pool does not invent unlimited fallback; no fast retry/DB busy loop.
8. Upstream 429, 401, 403 and policy denials do not trigger uncontrolled proxy rotation; network connection errors alone cause bounded eligible failover.
9. DNS rebinding, redirect-to-private/metadata, remote proxy DNS bypass and secret/header leakage are blocked; unsafe proxy path is refused for arbitrary targets if remote enforcement cannot be proven.
10. Response-body leaks, task context cancellation, SIGTERM, server drain and Worker shutdown cleanly release local connections/permits and preserve A1 Job/Attempt fencing.
11. Retention and storage pressure: no indefinite request/permit logs, bounded expired-permit cleanup, safe aggregate results; no impact on A1 history/lease pruning.
12. Re-measure baseline vs A2 under realistic 4C8G-like constraints: SQL statements/s, DB pool saturation, lock contention, quota p95, CPU/RSS, outbound traffic bytes and WAL growth. Report numbers, not fabricated SLAs.
13. Windows, Linux, Docker tests and AMD64/ARM64 image build remain green, without public credentials or production DB access.

## 8. Explicit non-goals and next handoff

V0.1-A2 does NOT implement real Web/Steam/Bilibili Provider fetching, Observation/current_results, incremental Change Stream, business CN/INTL synchronization, proxy purchasing, automatic proxy-source discovery, arbitrary proxy chaining, Headless browser farm, dynamic plugins or public user accounts.

After acceptance: V0.1-A3 owns persisted scoped Observations, commit-safe Change Stream, consumer snapshot/ACK and retention; V0.1-B/C implement actual Web/Steam collectors with Harvest-managed egress. If `steam-go` needs a transport ownership fix, open a **separate compatible SDK change** after real integration proof, not proactively within A2.

## Codex execution and reporting

Complete the **entire A2 stage** in `librecosta/harvest` `develop`. Preserve A0/A1 architecture, keep new abstractions minimal and tested. Submit commits and push develop; do not touch `master`/`pre-master`, other repositories, or production. Record concise `docs/harvest-v0.1-a2-acceptance.md` with actual evidence, key performance measurements, limitations and commit SHA. Do not claim simulated traffic as real Steam acquisition.
