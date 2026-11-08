# Harvest A1-A — Provider, Resource Identity & Transport Contract

Status: **Design proposal for review** (2026-11-08); implementation is not authorized by this document. Inherits [A1 Architecture & Engineering Baseline](./harvest-a1-architecture-engineering-baseline.md) and [A1-P Proxy & Egress](./harvest-a1-proxy-egress-contract.md). A1-B owns distributed job and quota state machines; A1-C owns tables and HTTP API.

## 1. Separation and source of truth

Harvest Core owns the Provider registry, Target identity, canonical request, scheduling, job execution, worker control, quotas, egress routing, result persistence and delivery. A Provider is an independently testable **source-specific acquisition adapter**, not a service or a database owner. GoFurry models, tags, moderation, publication and CN/INTL sync remain entirely outside Harvest.

Model distinct concepts:

- **Provider:** source/system name, e.g. `web`, `steam` (later `bilibili`).
- **Resource type:** stable source identity, e.g. `web.domain`, `web.origin`, `steam.app`.
- **Capability:** one collection operation, e.g. `web.dns`, `web.http`, `web.tls`, `steam.details`, `steam.news`, `steam.players`.
- **Target:** canonical source identity plus access namespace; may have multiple schedules and subscribers.
- **Request:** one capability + version + canonical parameters and result scope against a Target.
- **Result segment:** one independently verifiable scoped observation, with explicit complete/empty/missing/error status.
- **Execution context:** attempt ID, deadline and an authorized, scoped Egress/credential view; never a Provider-owned DB handle.

Provider packages are compiled into the Harvest binary through explicit registration. V0.1 has no dynamic `.so`/WASM/script loading, no generic ETL graph and no third-party code execution.

## 2. Target identity

Stable natural identity is `(provider, resource_type, canonical_external_key, access_namespace)`; Harvest assigns a separate Go 1.27 `uuid.NewV7()` ID. `access_namespace=public` is permitted only for data actually safe and licensed for shared public acquisition. Private/OAuth-account scope must be isolated and may never be deduplicated with `public` by accident. Subscription/business entity IDs are *not* part of source identity.

Canonicalization is **Provider/Resource-owned and deterministic**, performed before persistence and again on the Worker:
- `steam.app`: positive uint32 decimal AppID; strip any leading zeroes by numeric parse; reject overflow, nondecimal, negatives and whitespace-only input. Preserve separate raw external key only for diagnostics when safe.
- `web.domain`: IDNA-normalized lowercase DNS hostname, no trailing dot, validate length/labels; do **not** automatically merge `www.` with apex or discard subdomains. Reject URL path/scheme and non-public/reserved targets under the explicit security policy.
- `web.origin`: normalized `scheme://host[:port]` for supported HTTP(S); remove default port, retain nondefault port, normalize IDNA and IPv6 syntax; reject URL userinfo, path other than `/`, query/fragment. `http` and `https` are different origins.
- `web.page` and IP-specific probing are deferred unless a concrete capability requires them.

Never equate origin identity with redirect destination. SSRF protections are executed at outbound connect time, not satisfied merely by a syntactic Target validation.

## 3. Provider manifest and lifecycle

Each Provider declares an immutable, code-registered Manifest with:
- stable Provider ID and implementation version;
- supported Resource types and their canonicalizers;
- Capability IDs, independent major request/output schema versions and strict parameter validators;
- required transport classes (`http`, `https`, `tcp`, `dns`; declare only what is implemented), supported egress modes, possible source regions;
- upstream host allowlist where applicable, credential scope, rate-limit/quota buckets and timeout/body/result size bounds;
- expected result segments and whether the payload is egress-country/locale/session sensitive.

Unknown Provider, Resource, Capability, schema version or parameter MUST fail with a typed validation error (not silently ignored). Manifest is discovery metadata, not a runtime-editable plugin script. Changing a schema's meaning requires a new version, not repurposing existing fields.

Runtime interface sketch (conceptual, not final Go types):

```go
type Capability interface {
    Describe() CapabilitySpec
    Canonicalize(RequestInput) (CanonicalRequest, error)
    Execute(context.Context, CanonicalRequest, ExecutionServices) (Outcome, error)
}
```

At REST/storage boundaries, `RequestInput.Params` and `Outcome.Segments[].Data` are bounded JSON bytes with an explicit schema ID. Inside each Provider, decode them into **typed per-capability Go structs**, perform strict validation, and emit typed output encoded through the Harvest JSON boundary. Do not make Core recursively interpret arbitrary `map[string]any`. Keep Provider interfaces minimal; do not prematurely promise binary compatibility to external Go plugins.

A Provider must not open a DB pool, own Scheduler/Lease transitions, instantiate its own unmanaged proxy pool, read the process environment for credentials/proxies, or start uncontrolled background loops. Worker runtime owns the lifetime of injected resources.

## 4. Canonical request, deduplication and scope

Canonical request contains `target_id`, `capability_id`, `request_schema_version`, canonical parameters, authorization namespace and declared semantic content scope. Core computes a stable digest from a deterministic typed canonical encoding, not the caller's JSON formatting or map iteration order. Hash is for active deduplication and cache lookup, not a user-visible ID.

Parameters may contain locale, storefront country, source edition or requested data portions. They are not baked into Steam AppID or web domain identity.

Do not collapse requests across different credential owners, locales/countries, incompatible output versions or relevant network-origin scope. The **actual proxy endpoint** is execution metadata and normally not part of Target identity. When the upstream output can change with egress geography, the request must declare a semantic egress region or an explicit `region-neutral` contract; observations for different meaningful regions must not overwrite each other.

Active jobs with equivalent fingerprints may be coalesced only if freshness, authorization and source usage restrictions allow. Scheduling and Job idempotency remain A1-B decisions.

## 5. Typed outcomes and evidence

Each completed attempt yields a bounded `Outcome` containing `Segments`, sanitized diagnostics and explicit attempt status `succeeded | partial | failed`. Each segment identifies its **complete scope** (for Steam, source + language + store region + data-kind when applicable), output schema version, `observed_at`, source classification, optional upstream entity/version marker and result state:
- `present`: a successful, validated value; zero and false remain meaningful values.
- `confirmed_empty`: the source definitively reported an empty collection in this scope.
- `not_modified`: source confirms no change to a previously valid scoped result; no replacement payload required.
- `unavailable` / `invalid`: this segment failed or could not be interpreted; preserve last-known-good.
- `not_covered`: scope was not attempted; never interpreted as deletion.

A partial multi-language or multi-country run may commit only valid independent segments; absence, malformed payload, transport error or HTTP 429 never authorizes deleting or replacing previous successful facts. `confirmed_empty` may replace a prior list only when source semantics explicitly establish completeness, not from a missing field. Preserve raw upstream values where their normalization is uncertain, subject to a strict payload/retention budget.

Public observation envelope, illustrative only:

```json
{
  "schema": "harvest.observation.v1",
  "provider": "steam",
  "resource": {"type": "app", "key": "123456"},
  "capability": "details",
  "capability_schema": 1,
  "scope": {"language": "zh-CN", "store_country": "CN", "access": "public"},
  "observed_at": "2026-11-08T10:00:00Z",
  "state": "present",
  "source": {"tier": "observed_structured", "sdk": "steam-go"},
  "data": {}
}
```

Stored observation identity is assigned by Core (UUIDv7), and `received_at` is a distinct Core commit time. Source URLs are sanitized; do not store cookies, proxy URLs with credentials or token-containing query strings. JSON envelope/schema are not GoFurry business schemas. Failure events live primarily in bounded Attempt diagnostics, not as an overwrite of the latest-good Observation.

## 6. ExecutionServices and transport injection

Core supplies a scoped `ExecutionServices` with:
- `Egress`: an allowed HTTP client/transport or protocol-aware dialer for this execution and traffic class;
- `Credentials`: only the authorized Worker-local credential scope (never the raw central registry); 
- `Limits`: remaining deadline, bounded body/result bytes, retry budget/quotas and network policy;
- `Observe`: nonsecret request metrics and structured failure signals.

Providers must use the supplied transport, preserving context cancellation. Avoid global HTTP clients, `http.DefaultClient` and ambient `HTTP_PROXY/HTTPS_PROXY/ALL_PROXY/NO_PROXY`. The route resolver enforces `direct`, `prefer_proxy` (direct fallback is a separate explicit permission) and `require_proxy` (strict never-direct). Endpoint/session pinning, safe redirect checks, TLS verification and SSRF egress enforcement belong below Provider. DNS, raw TCP and HTTP are distinct transport capabilities; do not advertise HTTP proxying as universal protocol support.

A network attempt is bounded by *both* Harvest Job/Attempt policy and SDK-level operations. **One owner of retry orchestration**: Harvest controls cross-attempt backoff and egress switching; Provider SDKs should normally be constructed without automatic outer retries in Harvest and any explicitly retained internal retry must be accounted for in the global request budget. HTTP 429, auth denial or source-policy refusal must not cause proxy rotation.

Do not create or tear down Transport per request. Pool connections by endpoint/credential/traffic scope, and give the Worker a clear close/drain owner. Shared transport must not be closed by one per-job SDK client while other jobs still use it.

## 7. steam-go integration audit (main branch, 2026-11-08)

The maintained SDK `github.com/gofurry/steam-go` **already exposes**:
- `WithHTTPClient(*http.Client)`;
- `WithProxySelector(ProxySelector)`, `WithProxySessionKey(...)`, routing/sticky/health-checked selectors;
- `WithTrafficPolicy(class, TrafficPolicy)` and `ProxyAwareTransportHook`, with Official API/Public Store/Community/Market classes;
- `WithRetry`, `WithRateLimit`, `WithRequestObserver`, configurable timeout/response-body caps;
- `Client.Close()`, which closes idle connections of its assembled HTTP clients. `buildHTTPClient` clones supplied `*http.Client` and clones a concrete `*http.Transport`.

Therefore **no immediate SDK API change is required**. Start with one Harvest-supplied HTTP client/selector and explicit per-class policies; do not instantiate a second autonomous SDK proxy pool or blindly enable SDK safe defaults. Verify that cloning, proxy selectors, traffic-class wrapper order, redirects, retries, connection ownership and sanitization meet the contract in integration tests. If and only if a real gap exists, propose a small additive change upstream under its [v1 compatibility contract](https://github.com/gofurry/steam-go/blob/main/contracts/compatibility.md) and run SDK regression tests; no silent breaking fork.

In particular, test whether `Client.Close()` on a per-attempt SDK client closes a shared Harvest Transport's idle connections. Prefer SDK clients scoped to a stable Worker pool/egress profile, or make ownership explicit with a backward-compatible injection API if tests demonstrate a need.

A `steam.details` task may generate multiple official/web requests; its output scope must distinguish successful locales/regions and not treat a failed Storefront request as evidence to clear prior data. `steam.players` must distinguish real 0 from unavailable. `steam.news` must handle stable upstream event identity and source attribution; the business decision about furry-game inclusion remains in GoFurry.

## 8. Security and provider compliance

Upstream API keys, cookies and OAuth tokens are supplied only for authorized scope, never included in public manifests, queues or logs. Public source data is shareable only when permitted; customer/private account observations require independent scope. Fixed-provider host allowlists and private-DNS/redirect protections are mandatory. For arbitrary web targets routed through proxies, trusted proxy-side destination-IP enforcement is required because the client may not perform the final DNS lookup. No open-proxy endpoint or bypassing upstream rate limits/permission restrictions.

## 9. First version acceptance (deterministic, mocked unless explicitly opted in)

1. Two Providers (Web + Steam) register from code with independent resources and capability versions; unsupported combinations and unknown parameters reject.
2. Canonicalization distinguishes web domain/origin and preserves `www`/scheme/port; Steam numeric AppIDs deduplicate correctly.
3. Deterministic canonical request hash respects locale, store country and private authorization scope; identical public requests may coalesce.
4. Multi-scope result: one successful Steam locale and one failed locale; only valid scope updates last-known-good. Genuine 0 players is not failure.
5. Worker-issued HTTP clients route Steam through a named CN proxy and Web direct in tests; `require_proxy` cannot fall back even with a second SDK policy.
6. Distinct Steam traffic classes obey their assigned transport, quota, cancellation and bounded result-body limits; no ambient proxy variables are honored.
7. Unavailable proxy, 429, auth failure and transport timeouts produce distinct sanitized errors and correct retry classification; no uncontrolled rotation.
8. Multiple concurrent SDK calls reuse pools; closing one SDK client cannot unexpectedly corrupt in-flight work by another; shutdown drains without leaking goroutines/connections.
9. Arbitrary Web target redirects, DNS rebinding and remote-proxy resolution cannot reach private/metadata networks; reject a proxy path that cannot enforce that guarantee.
10. Provider errors, diagnostics and captured fixtures contain no credentials, cookies, proxy passwords or secret-bearing URLs.

## 10. Deferred and next steps

- A1-B specifies Job/Attempt/Lease, scheduling, worker eligibility, global quotas and retry/cancel state machines.
- A1-C specifies SQL DDL, outbox/change stream, consumer API, retention and Docker/Task operational acceptance.
- V0.1 starts with `web.domain` / `web.origin` and `steam.app`; Bilibili is a future contract stress case, not a requirement to build now.
- Do not port legacy GoFurry schemas or migrate old collectors before the new contracts and their test harness are accepted.

## Source review

- https://github.com/gofurry/steam-go/blob/main/options.go
- https://github.com/gofurry/steam-go/blob/main/proxy.go
- https://github.com/gofurry/steam-go/blob/main/client.go
- https://github.com/gofurry/steam-go/blob/main/traffic_policy.go
- https://github.com/gofurry/steam-go/blob/main/contracts/upstream.md
