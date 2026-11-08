# Harvest A1-P — Proxy Pool & Egress Routing Contract

Status: **Accepted cross-cutting design direction** (2026-11-08). Detailed fields, SQL and API shapes are subject to A1-A/B/C refinement. A1-P is not a new service, repository or implementation stage.

## Why this is core infrastructure

Proxy-aware acquisition is a first-class Harvest capability, independent of any individual Provider. It must be useful for Web, Steam and future API platforms such as Bilibili. Its purpose is controlled egress routing, better availability and measurable latency/cost, not automatic evasion of source limits or access policies. A CN worker's Steam requests may perform better via an explicit proxy; verify with real measurements rather than hardcoding the assumption.

## Execution ownership

- **Provider** declares the transport requirements and support for an egress profile (HTTP(S), SOCKS5-over-TCP, direct-only protocol operations), data/authorization scope and provider-specific request handling. It must not independently read proxy configuration, build a secret-bearing proxy pool or decide regional fallback.
- **Control Plane** owns routing policy, durable task admission, worker eligibility and cross-worker provider/account/egress quotas. It matches the request to an eligible Worker based on Provider/Capability, egress profiles/regions, authorization and available pool health.
- **Worker** owns network connections and the local **Egress Router + Proxy Pool**. The router chooses a permitted route and specific healthy endpoint, returns a scoped transport/client to the Provider, and reports only sanitized egress metadata and metrics to the control plane.
- **Pool endpoints** and credentials are declared in the Worker's one explicit YAML configuration, protected with filesystem permissions. Credentials are not stored in Target, Job or Observation, not returned by APIs, and not uploaded to the central DB. The control plane learns stable logical endpoint/pool identifiers, capabilities, effective egress region, health, available capacity and aggregate counters. Config changes initially use a bounded worker restart; no mutable browser-based credential store in V0.1.
- **Egress route** is an execution decision, not part of a Source Resource's core identity. However when source responses vary by egress region, this region and locale/authorization scope must be represented in the collection result scope and deduplication key. A changed egress route must not silently overwrite a semantically different last-known-good record.

## Routing policy

Three modes:
- `direct`: direct only.
- `prefer_proxy`: use configured pool; fallback to direct is a separate **explicit opt-in** and can occur only on eligible transport failures.
- `require_proxy`: only the configured pool. If no eligible proxy exists, do not secretly use direct or another region; wait/defer/fail according to bounded job policy.

Rules match Provider, Capability, Worker region, optional target class or authorization scope, and select a named egress profile. The same source may have different rules for `steam.details`, `web.http`, `web.dns` and so on. Explicit default is direct only for capabilities that declare direct support; no environment-derived `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` or `NO_PROXY` override. If a task requires a profile not available on a worker, it is ineligible for that worker.

Do not assume every Provider transport can use an HTTP proxy. HTTP(S) supports native Go `net/http.Transport` HTTP/HTTPS/SOCKS5 routing; raw TCP probes may use a separately audited SOCKS5 dialer if supported; DNS/UDP and RDAP must declare capability-specific behavior rather than imply universal proxy compatibility. Keep TLS server certificate verification on.

## Pool lifecycle and resilience

- Pool is a named group of explicit endpoint IDs, transport URLs and region/cost metadata. Start with health-qualified least-inflight selection (simple deterministic tie-break); avoid gratuitous rotation and per-request transport recreation. Maintain connection reuse keyed by endpoint + identity/transport scope, bounded idle conns and timeouts.
- Health states: `healthy`, `degraded`, `cooldown`, `disabled`; bounded active checks to a configured trusted health target plus passive transport error feedback. Separate proxy connectivity health from a particular upstream's policy/availability. Repeated transport errors open circuit cooldown, then limited half-open probes. Restart does not mutate the YAML-defined enable/disable choice.
- Global quota accounting must cover relevant upstream account/token, provider and known egress-IP buckets across workers where policy requires it; local per-endpoint concurrency is additional, not a substitute. Provider HTTP 429, auth failure, bans/denials and API quota errors must not trigger uncontrolled proxy rotation; apply source-aware cooldown and policy-compliant handling.
- Switching another endpoint within an allowed pool may occur for eligible connect/handshake/network failures only, with bounded retries/backoff. Preserve request cancellation, failure attribution and Job Attempt fencing. If one logical acquisition spans multiple requests requiring consistent session or egress geography, pin an endpoint for that attempt unless the Provider explicitly supports replay.
- Never log endpoint credentials, full proxy URLs, request bodies, Authorization headers or private source data. External consumer API receives no proxy addresses or credentials; privileged diagnostics may expose only endpoint ID, pool, effective region, state and aggregate metrics.

## Security and compliance

- Reject unsafe targets before dispatch and at each redirect. For direct requests, validate resolved destinations against public-address policy with DNS rebinding defenses. For proxy-resolved destinations, client-side DNS validation alone is insufficient: arbitrary Web targets may use only **trusted egress proxies** that enforce IP/CIDR, metadata-address and internal-service denial at the actual proxy-side resolution/connect point (or an equivalent verifiable network enforcement). Reject an egress path that cannot enforce the target safety policy. Provider-specific fixed/allowlisted APIs can use narrower restrictions.
- Default internal worker/proxy-control HTTP endpoints to loopback/private/Tailscale access and authenticate separately from consumer APIs. Do not expose an unauthenticated forward proxy. Proxy traffic, credentials and user-authorized data remain within allowed regional/legal boundaries.
- A proxy does not grant permission to scrape restricted APIs or circumvent provider rate limits, authentication or terms. Source-specific policy is enforceable at scheduling and runtime.

## Storage and cost

- Measure per logical endpoint and Provider/Capability: request attempts, successes, transport failures, HTTP upstream statuses (sanitized), bytes in/out, latency p50/p95 or bounded histograms, total traffic and estimated/declared proxy costs where available. Never assert precise billed costs without billing source data.
- Support configured per-endpoint concurrency, global bandwidth/traffic ceilings or budgets, request and response body limits, and safe defer/fail behavior when limits are reached. Avoid unnecessary active health probes that themselves add cost.
- Retain aggregate metrics for a bounded window, not a permanent per-request proxy event log. Observation stores only necessary, nonsecret provenance (e.g. egress region/route class when it materially changes facts); detailed failure diagnostics belong to bounded Attempt history.

## First-version scope / Acceptance

V0.1 must support local Direct and a Worker-local pool of explicitly configured HTTP, HTTPS, SOCKS5 proxies; per-Provider/Capability route policies; health qualification and cooldown; bounded fallback/retries; safe transport reuse; Worker capability publication; secure result/diagnostics; and health/status via internal API/CLI. First operator control is explicit YAML plus CLI/status, not a separate proxy management web service. Optional mutable pool UI, paid-provider purchasing, proxy discovery, large-scale reputation scoring and sophisticated automatic optimizer are deferred.

Acceptance must cover:
1. CN Steam request uses the explicitly assigned pool; direct-only Web stays direct.
2. `require_proxy` with all endpoints down never leaks to direct; `prefer_proxy` falls back only when authorized.
3. Two workers with different egress profiles route only eligible tasks; quotas and leases remain safe.
4. A failed endpoint cools down; valid fallback succeeds without unbounded retries or transport churn.
5. 429/auth denial does not cause uncontrolled proxy rotation.
6. DNS rebinding, redirects, private/metadata addresses and untrusted proxy DNS bypass attempts are blocked.
7. Sensitive credentials do not appear in API responses, logs, DB, result data or test artifacts.
8. Endpoint disable/restart, graceful SIGTERM and Task/Docker deployment preserve explainable behavior.
9. Latency/availability/traffic and storage impact can be measured against direct mode without assuming proxy always wins.

Implementation should first audit whether `github.com/gofurry/steam-go` supports injection of a custom HTTP transport/client. Never modify provider libraries speculatively or bypass centralized egress solely for expediency.

## External references

- Go standard HTTP transport: https://pkg.go.dev/net/http#Transport
- Existing A1 baseline: ./harvest-a1-architecture-engineering-baseline.md
