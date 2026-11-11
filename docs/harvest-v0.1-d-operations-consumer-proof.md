# Harvest V0.1-D — Operations, Resource Hardening & Consumer Proof

Status: **Implementation-ready design** (2026-11-11). Repository: **librecosta/harvest**, branch **develop** only. Baseline: Steam V0.1-C implementation `2c8e0a25788960356f42774812dbdeeaaadbe7c2`; GitHub Actions successful. This is the **last V0.1 engineering/operations gate**, not a new business service or a new collection Provider.

## 0. Read first; preserve the architecture

Read `AGENTS.md`, `.agents/architecture.md`, all relevant A1 contracts and A0–C acceptance reports, `contracts/database.md`, `docs/harvest-v0.1-c-steam-provider.md`, `docs/harvest-v0.1-c-acceptance.md`, the live `internal/egress`, `internal/execution`, `internal/provider`, `internal/server` and Task/Compose/CI configs. Review the current source and **actual passing tests**, not only staged contracts.

Harvest is an independent BSD-3-Clause Go 1.27.3 Provider-based collection product with Fiber v3, explicit router and lifecycle, PostgreSQL 18/pgx/sqlc/Goose, UUIDv7, durable jobs, multi-Worker leasing, proxy-aware egress, PostgreSQL-managed operational quotas (including daily hard limits), scoped observations, change delivery and bounded retention. Web B1/B2 and Steam C are implemented.

**Do not change** `librecosta/gofurry` (new site), `gofurry/gofurry` (old site), or `gofurry/steam-go` (maintained SDK). Do not add Redis/Kafka, a new consumer microservice, frontend/admin UI, data warehouse, new Provider, notification engine, UGC or GoFurry catalog schema. No master/pre-master/release changes. Do not access production VPS, production PostgreSQL, real private keys or real user data.

## 1. Success definition

A clean operator should be able to provision an isolated Harvest database, bootstrap one valid DB-owned policy, run Server and one local/remote Worker via Task or Docker, register a Web Origin and a Steam App, create conservative schedules, understand quota and reason codes, read Observations and initialize/recover an independent consumer, and stop/update/restore safely. Everything else in D is **verification and narrow corrections**, not feature expansion.

At the end we have a concise go/no-go V0.1 operational readiness report **with explicit limitations**. A successful D does not prove unrestricted production crawling or that Steam grants any particular stable rate.

## 2. Correct the operator-facing documentation/config story

- Fix the outdated README introduction (it still says `Steam remains planned`) and ensure all implemented Web/Steam capability names and schema/implementation versions are accurate. Keep README concise; detailed deployment procedure in one useful `docs/operations.md` or equivalent, not many documents.
- Provide **small, annotated and independently valid** Server/Worker example configurations for (a) direct-only local/private-network deployment, and (b) one explicit, operator-owned regional proxy profile; use placeholders, no committed credentials. Current `config.example.yaml` and `config.worker.example.yaml` demonstrate Web primarily; expand the sample to show the Steam Details, News and Players grants without turning it into an unreadable catalog. Show that actual proxy URL/credentials and service tokens stay in protected Worker/local YAML, while quotas/routes live in PostgreSQL and are editable by API.
- Document migration/init order, expected readiness, one-time `policy init` and revisioned API updates; **never** instruct users to restart with old YAML expecting it to override DB policy. State explicitly how to update a policy in an already initialized DB and how to use `task stop` and Compose safely.
- Document traffic accounting: public Storefront appdetails is **observed/volatile**; recommendation `per_minute:30, burst:1, concurrency:1` is an operator choice based on one experiment, not an official Valve guarantee. Official News and Players use a **different** bucket, with an explicit daily hard cap chosen from the operator's real allowance (the source's quota may change). Proxy nodes sharing the same physical outbound IP or another app/SDK using the same upstream account must be accounted for in the budget **outside this Harvest instance**.
- Explain the limitations of the current 5-second Permit, the one-active-Control-Plane assumption, one Google DoH resolver, 24h in-memory IANA bootstrap, short-lived results/events, and the source-specific limitations of Web/Steam. Do not promise comprehensive availability or accuracy for unreachable/blocked sources.
- Keep source/key/region/access scopes visible only through the correct authenticated APIs. Keep `/livez` and `/readyz` compatible with LibreCosta Uptime, without using upstream Steam reachability as readiness.

## 3. Operations: bounded metrics, data growth and fault recovery

Do a focused audit of **long-running costs**. Avoid new SQL tables or public metric endpoints unless an actual gap is demonstrated.

- Run a repeatable **bounded mixed-workload soak** against local fixtures and isolated PostgreSQL 18, exercising representative Web HTTP/DNS/TLS/B2 and Steam Details/News/Players, a consumer poll/ACK and deliberately idle periods. A short CI fixture remains fast; offer an opt-in 15–30 minute Task/check for more meaningful soak, run it where resources permit, and accurately state what was/was not executed.
- Measure (a) idle SQL statements/sec and connection/pool waits, (b) requests/Permit deferrals by Provider/traffic class/egress, (c) Job claim/finish p95 and queue age, (d) CPU/RSS/heap during mixed load and after idle, (e) PostgreSQL WAL/data/index/TOAST growth and maintenance time, (f) Change Stream head contention/consumer lag, and (g) proxy outbound bytes/error/cooldown. Provide workload/fixture details and a direct comparison to A2/A3/B1/B2/C only when work mixes are comparable. **No made-up 4C8G throughput SLA**.
- Verify 30-day Job/Attempt, 14-day Change Stream, 30-day history and count/estimated-byte budgets actually behave as documented; current results/event refs stay pinned. Test hard retention pressure, consumer reset expiry, DB outage/recovery, Worker crash/restart, repeated Compose stop/start, graceful Fiber pre-drain and permit finish without leaked connections/queued orphans. Provide actionable warnings if consumers are approaching replay expiry; avoid new notification transports.
- Validate policy-init/restart, daily hard quota with UTC day boundary, A4 CAS updates while requests are active, Storefront 429/403 cooldown and permit accounting, proxy `require_proxy` never falling back to direct and A1 fencing after lease takeover. Preserve that quota issued outside Harvest is not in its DB.
- Tune **only clear hot spots supported by measurements** (e.g., busy polling, repetitive DB reads, overlarge payloads, avoidable SQL contention, excessive log volume). Preserve correctness and scope; do not invent a distributed quota cache or change the DB-owned configuration model.

## 4. Proof of independent consumer (without GoFurry business dependency)

The user intends **CN business data as the primary content source** and later CN -> INTL publish projection, but that architecture is **not built inside Harvest**. D should prove only Harvest-to-consumer acquisition result delivery.

- Add a small, **test-only or `examples/`** consumer adapter (not a production service) that runs against the authenticated Harvest API and an **independent disposable PostgreSQL database**. Store typed generic source identity + namespace + scoped result version, not GoFurry Site/Game tables. No direct read from Harvest PostgreSQL. No source-specific SQL schema assumptions.
- Demonstrate: enroll -> reset baseline -> paged current snapshot -> replay changes while writes continue -> atomically activate staged projection and durable cursor **in the consumer DB** -> server ACK -> restart -> resume -> idempotent apply -> withdrawal tombstone -> cursor expiry -> full reset and convergence.
- Explicitly test concurrent multi-scope Steam region/language updates, News page changes, point-in-time zero player samples and independently scoped Web observations. Partial/failed acquisitions must not erase last-known-good. Client-side materialization of sampled history must be opt-in and bounded; a latest-state projection by itself is not a historical analytics system.
- The example is a **proof of the public consumption protocol**, not a new always-on `sync` service and not the GoFurry CN -> INTL Sync. This prevents the domain model being prematurely determined by old data.

## 5. Controlled real-source smoke-test path (opt-in, no default traffic)

C/B fixture correctness is good; **real Steam, real public websites and production proxy behavior have not yet been measured**. Provide an operator-led small smoke test plan and, if practical, a narrow opt-in CLI/Task fixture with **no hard-coded public test targets** and zero live requests by default.

- Any live smoke requires explicit operator-supplied allowed Web Origin/Steam AppID, authorization, existing production-like proxy profile, a disposable or purpose-built test DB, quotas and an invocation flag (default off), plus a small hard request ceiling and time budget. No stored/provided production secrets in CI, artifact, logs or code.
- Test at most a few requests per selected source: connectivity and latency, source-specific failures/429/403, SSRF checks, locale/region results, safe LKG, and Web source regions. Do **not** conduct a burst search for Steam limits, submit arbitrary public targets or bypass terms. If a live check is not run, clearly mark **not run**; tests against local fixtures are **not** proof of public reachability.
- In particular, a short real Steam run cannot establish a universal `228/5min` limit or an enduring daily allowance. Retain conservative quotas and operator review.

## 6. Security and code hygiene

- Ensure Worker-only auth and consumer/operator roles remain separate, and sensitive query keys, credentials, cookies, proxy URL userinfo, personal RDAP contacts and raw Steam payloads are absent from logs, API error bodies, docs and fixtures. No debug endpoint with raw request logs.
- Confirm B1/B2 dynamic target validation, numeric IP pinning, host/SNI validation and proxy-side enforcement boundary remain intact. Unknown/partial states never translate into authoritative downstream site down/deletion; `HTTP/2` negotiated diagnostics must not become adoption metrics or new active probes. No port scan, WAF canary, JARM or VHost enumeration.
- Review any SDK transitive dependency/license notices for the open-source repository, CI pinning and reproducible builds. Do not silently fork steam-go.
- Keep Go UUIDv7, `sqlc+pgx`, explicit Goose migrations, Fiber lifecycle, Router mapping, YAML secrets and Task cross-platform safe stop. Fix only actual issues; avoid speculative refactors.

## 7. Verification gates

Use disposable PostgreSQL 18, **two independent Worker processes**, full HTTP/DoH/Steam fixtures, direct and fake HTTP CONNECT/SOCKS5, and a separate disposable consumer DB.

Mandatory:
1. Fresh DB and upgrade from C preserve all existing Targets/Schedules/Jobs/Observations/quotas/policy/Change Stream; no implicit schema migration.
2. Independent consumer proof includes baseline/replay/ACK, restart, duplicate event, concurrent insert/update/withdraw, expired cursor and partial Steam/Web scopes; no direct Harvest DB queries.
3. Policy change and quota/egress under load: no permit budget overspend or direct fallback; daily reset/expiry works, expired Attempt cannot update the consumer.
4. Storage retention/pressure: historical data bounded, current and unexpired event refs preserved, honest 410 + reset, no silent gaps or deletion.
5. DB/Worker/proxy outage and recoveries: liveness/readiness, pause/drain/resume and no resource leak or infinite tight retries.
6. Short normal CI runs remain deterministic and public-network-free. Unit/race/integration: Windows/Linux, disposable PG18, Docker amd64/arm64, `task check` / `task check:containers` pass.
7. Run and report at least one measurable mixed workload with per-capability requests, SQL/sec, WAL/data growth, permit p95, process memory, queue delays and evidence for any tuning. If long soak cannot run, say so.
8. Release readiness: valid current README and examples, concise operations runbook, honest limitations, and a one-page checklist showing **ready for limited operator deployment** vs **not yet production-validated**.

## 8. Completion and boundaries

Submit `docs/harvest-v0.1-d-acceptance.md` with actual executed evidence (pass/fail/not run), small before/after resource table, verified limitations and a concrete minimal deploy/runbook. Update README/config examples/Task only as necessary. Implement fixes narrowly within Harvest develop; do not touch other repos/production or publish a GitHub Release/tag automatically.

Codex should finish D in one pass, not split it into a swarm of small issues. If source-code defects or integration gaps are found, fix and regression-test them. Keep the report and release prose short. Provide final develop SHA and any genuinely blocking limitation.

**After D**: Harvest can be considered V0.1 release-candidate for a controlled deployment. New GoFurry **librecosta/gofurry** separately designs its content domain, the Harvest consumer mapper, CN/INTL publication Sync and historical migration. No old GoFurry schema becomes Harvest's canonical data model.
