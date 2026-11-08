# Harvest A1 — Architecture & Engineering Baseline

Status: **Accepted baseline** (2026-11-08). A1-A/B/C are design workstreams, not implementation authorization.

## Product boundary

Harvest is an independent, BSD-3-Clause Go data acquisition engine. It has no dependency on GoFurry models, the legacy GoFurry schema, or the GoFurry CN → INTL sync service. Consumers call versioned APIs and own their business projections. Harvest owns acquisition targets, schedules, execution lineage, and structured observations.

One repository (`librecosta/harvest`), one Go module/binary, one independently owned PostgreSQL database, distinct runtime modes: `serve` (control/API/scheduler + optional local worker) and `worker` (remote execution through a private API). The first deployment is one node; the implementation must still support multiple workers. No Kubernetes, mandatory Redis, Kafka, or shared business database.

## Engineering baseline

- Go **1.27.3**; native `uuid.NewV7()` for application entity identities and PostgreSQL `uuid` columns. Provider-native IDs are separate natural identities. No autoincrement primary keys, Snowflake allocator, or generated machine IDs. UUID ordering is **not** a durable commit-order cursor.
- Fiber **v3** with explicit router split: `routers/router.go` (groups), `routers/url.go` (method/path ownership), `routers/health.go` (probes), handlers outside routers. Public consumer API and privileged worker API are separate auth scopes. No SQL/Provider execution inside router packages.
- `/livez` and `/readyz` supported from day one for LibreCosta Uptime. Liveness is process responsiveness; readiness reflects essential dependencies for the current role, not upstream provider availability or whether a task is queued. An absent optional remote worker does not fail the control-plane readiness.
- A single runtime owner orchestrates configuration, logging, DB pool, Fiber application, scheduler, worker and shutdown. Read configuration from one explicit `--config` YAML via Cobra/Viper; unknown keys fail. No implicit env overlay or `.env` runtime resolution.
- Respect Fiber v3 lifecycle: `OnListen` for listening readiness transition, `OnPreShutdown` for draining and stopping new scheduling/claims, `OnPostShutdown` for final post-HTTP cleanup. Use idempotent resource closure and bounded contexts. Startup failures and listener binding failures need explicit rollback because shutdown hooks need not run when listening never starts. Long-running work must not be made dependent on HTTP shutdown hooks alone. Do not enable Fiber prefork.
- Go stdlib `encoding/json/v2`, PostgreSQL 18, `sqlc + pgx`, Goose explicit migrations. Handwritten SQL limited to documented cases where sqlc cannot represent the operation. No ORM, magic query builder or application-start DDL.
- Development uses Task: `dev`, `start`, `stop`, `build`, `test`, `check`, `sql:generate`, `sql:check`, `migrate`, `check:docker`. `stop` may terminate only a process/container that the current checkout started and verified, never an arbitrary process bound to a port. No PowerShell-only scripts.
- Native binary for development/install; production-first Docker Compose and GHCR image, nonroot runtime, explicit YAML read-only mount, healthcheck through `harvest healthcheck`, Linux amd64/arm64, SIGTERM graceful drain. Existing PostgreSQL instance may host a separate Harvest database: no second PostgreSQL server is required.
- No unbounded collection or unbounded storage. Low-frequency defaults, bounded response bodies and result size, request quotas, bounded attempts, tiered retention, batched pruning, and visibility into storage usage. Do not discard sampled time-series evidence necessary for the consumers' chosen analytics. A source failure never erases last-known-good data.
- Secrets, user-authorized payloads and restricted-source data may not be shared across consumer scopes by default. Public-source reuse and distribution are permitted only when access and licensing conditions allow. Public/worker HTTP APIs remain private-network bound by default; prevent SSRF including redirect/DNS rebinding paths.

## A1-A — Provider, Resource & Result Contract (design scope)

- Provider identifies a source (`web`, `steam`, later `bilibili`); Resource is its object class (`domain`, `origin`, `app`); Capability identifies one versioned acquisition operation (`http`, `dns`, `tls`, `details`, `players`).
- Target identity is `provider + resource_type + normalized external key`, plus an authorization/visibility scope when necessary. Keep acquisition parameters (locale, region, probe target endpoint) out of unrelated business identity. Resource-specific normalization is mandatory.
- Each Provider registers a compiled manifest: resource types, capabilities, typed request schema, typed output schema/version, credential requirements, rate-limit domain, timeout/result constraints, and worker capabilities. No arbitrary dynamic executable plugins.
- Core owns scheduling, retry, storage, credential access mediation and transport policy; provider implementation owns source-specific validation/fetch/normalization and returns structured results with completeness, provenance, scope and typed errors. Providers cannot mutate core tables.
- First built-in proof providers: Web and Steam. Review legacy collector as a feature/test reference, not as a schema to carry forward.

## A1-B — Durable Scheduling & Distributed Execution (design scope)

- Persist Target, Schedule, Job, Attempt, Worker/lease, Observation and consumer delivery references. The scheduler uses stable fixed intervals or cron in UTC, explicit late/missed-slot policies and idempotent schedule creation.
- PostgreSQL owns the queue and atomically claims due jobs. Remote workers use private polling/lease/heartbeat/result endpoints; no direct database privileges. Execution is at least once; result commits are idempotent and validated with fencing generation + attempt identity.
- A worker may only claim tasks that match its declared capabilities and network/credential constraints. Server-side global provider/API-credential quotas apply across all workers, not only per worker. Retries use bounded exponential backoff and source-aware error classes.
- Shutdown means stop issuing work, retain availability for in-flight result commits during the drain window, terminate jobs with bounded time, and allow crashed leases to be recovered. Worker loss does not make the API process unhealthy.

## A1-C — Storage, Consumer API, Operations & Budget (design scope)

- Harvest stores latest successful result per scope, bounded historical observations, limited task/attempt diagnostics, and opt-in bounded raw evidence. Time series with numerical meaning may be sampled/aggregated rather than deduplicated into a latest-only record.
- Initial budget candidates, to be tested rather than blindly frozen: 30-day job/attempt history; 30-day high-frequency raw samples; bounded snapshots of changed details; target/schedule lifetime explicit; recent observations and aggregate history have separately specified horizons.
- Versioned REST consumer API for Targets, Schedules, Jobs, Observations, incremental changes and capability discovery. Privileged worker API is distinct. Consumers never access Harvest PostgreSQL tables.
- Incremental delivery must have a **commit-safe** monotonic cursor, not a UUIDv7 or ordinary database sequence used as an assumed commit order. Consider a small serialized per-stream transactional ledger and prove concurrent commits cannot skip events. Consumer resumption after expiration requires explicit reset/snapshot recovery, never silent data loss.
- Minimal CLI/health diagnostics precede any optional web console. Automated acceptance covers clean start, failed start rollback, SIGTERM drain, DB outage/readiness, two concurrent workers, lost lease/fencing, retry/duplicate result, partial provider success, storage compaction, and consumption recovery.

## A1-P — Proxy Pool & Egress Routing (cross-cutting baseline)

- Proxy-aware egress is a **first-class core capability**, not Provider-specific ad hoc client configuration. Each Worker owns explicit YAML-defined direct/proxy transports and local pool state; Control Plane owns job routing constraints and aggregate quotas.
- Support HTTP, HTTPS and SOCKS5 proxies where the Provider transport permits; require explicit `direct`, `prefer_proxy` or `require_proxy` route policy. Never silently downgrade `require_proxy` to direct, import environment proxy settings or rotate around upstream policy denials/429 responses.
- Worker capability matching, per-endpoint health/cooldown, bounded failure fallback, secret isolation, destination/redirect/SSRF protection (including proxy-side DNS resolution) and storage/bandwidth/cost metrics are required in V0.1.
- See [A1-P Proxy Pool & Egress Routing Contract](./harvest-a1-proxy-egress-contract.md). Its detailed implementation is refined across A1-A (Provider transport contract), A1-B (worker routing/quotas) and A1-C (storage/API/operations).

## Scope exclusions

No GoFurry catalog, content moderation, cross-region sync logic, human user system, generic ETL/workflow engine, distributed browser farm, third-party code loader, or indefinite raw-content archive within Harvest V0.1.

## References

- Fiber v3 hooks: https://docs.gofiber.io/api/hooks/
- Fiber v3 services: https://docs.gofiber.io/api/services/
- Go 1.27 UUID: https://pkg.go.dev/uuid
- Existing GoFurry patterns: https://github.com/librecosta/gofurry/tree/develop/services/api
