# Harvest V0.1-B1 — Core Web Acquisition & Passive Fingerprinting

Status: **Implementation-ready Web Provider design** (2026-11-09).
Target repository: **librecosta/harvest**, **develop** only. Based on A0–A4 engineering, runtime, results, egress, quota and DB-owned policy contracts. This is **not** the GoFurry business schema, not a security scanner, and not a generic crawler.

Read before implementation:
- AGENTS.md and .agents/architecture.md
- docs/harvest-a1-architecture-engineering-baseline.md
- docs/harvest-a1-a-provider-resource-transport.md
- docs/harvest-a1-b-distributed-scheduling-execution.md
- docs/harvest-a1-c-storage-api-operations.md
- docs/harvest-a1-proxy-egress-contract.md
- docs/harvest-v0.1-a4-acceptance.md
- contracts/database.md
- current code in internal/provider, internal/egress, internal/execution, internal/config, internal/worker.

## 0. Product goal and hard exclusions

Build a **reliable, low-frequency, non-intrusive, general-purpose Web Provider** for site metadata and network facts. Reuse Harvest's actual Target Registry, Scheduler, Worker, proxy pool, global permits, scoped Observation/current value, commit-safe Change Stream and database-backed operator policy. Do **not** port old GoFurry data models or its health/adoption analytics.

V0.1-B1 must implement real compiled capabilities:
- `web.http` on `web.origin`: HTTP observation, bounded homepage metadata, canonical/OG/Twitter/JSON-LD fields, content/semantic fingerprints and **passive technology detection** from the same response.
- `web.dns` on `web.domain`: bounded DNS record observation, resolver identity and error semantics.
- `web.tls` on HTTPS `web.origin`: verified TLS/certificate evidence, preferably reuse evidence already obtained during HTTP analysis and, when explicitly scheduled, a low-frequency **permit-gated HTTPS HEAD** probe; do not falsely claim to implement an arbitrary raw-TCP TLS scanner.

A successful `web.http` HTTPS response may include TLS handshake metadata obtained from that very connection without further network calls. A separate `web.tls` Job is **optional and low-frequency**, not an automatic extra request on every HTTP collection. `web.tls` must not require HTTP 2xx for reporting a valid TLS handshake; a 405/403 response can still contain valid TLS evidence. When cert validation fails, return a sanitized specific verification error; **never silently disable TLS verification to manufacture a certificate result**.

V0.1-B1 does **not** implement active port scans, WAF canaries, exploit/vulnerability checks, JARM, HTTP pipelining probes, virtual-host enumeration, HTTP/2 or HTTP/3 *support/adoption* probing, TLS cipher sweeps, JS execution, headless, external JS fetching, admin-path guessing or multi-page crawling. HTTP actual `resp.Proto` / TLS ALPN may be retained **only as connection diagnostics, not a statement of server capability**.

The existing Harvest HTTP Transport currently explicitly enables HTTP/1 only to avoid auto-replay outside its permit gate. Do not turn that artificially negotiated HTTP/1 result into a negative HTTP/2 observation. Do not enable HTTP/2 without independently proving correct per-request/stream metering and replay safety; it is not required in B1.

## 1. Provider identity and request contract

Preserve current `web.domain` and `web.origin` canonicalization. Do not rewrite Target identity, merge apex with www or derive a GoFurry Site ID. Explicit compiled manifest/schema versions. Replace the planned non-executable metadata entries with real implementations only after their complete tests pass.

`web.http` reads the root URL of the exact registered HTTP(S) Origin. No caller-controlled arbitrary request path, method, headers, body, IP, Host or proxy destination. Permit immutable typed input such as an optional locale and bounded requested metadata scope; no generic URL request builder.

`web.dns` requires a canonical public domain and validated type set, fixed bounded work (candidate A, AAAA, CNAME, MX, NS, SOA, TXT, CAA; explicitly cap RRs, names, response bytes and per-Job DNS requests). Preserve accurate NoError/no-answer, NXDOMAIN, SERVFAIL and timeout distinctions. TXT/CAA may contain sensitive or unexpectedly large strings: limit, redact/omit unsafe values as required and hash/truncate with explicit metadata. No recursive traversal of arbitrary subdomains or zone transfer.

`web.tls` rejects non-HTTPS Origin; certificate evidence relates only to the hostname actually connected with verified SNI. Do not confuse the proxy's TLS certificate with the source site's certificate.

Request Scope includes authorization namespace and, where semantically relevant, egress region and resolver vantage. Region-specific results do not overwrite other regions' last-known-good. Proxy endpoint IDs are diagnostics, not resource identity.

## 2. The B1 blocking change: safe dynamic destination egress

A4's `egress.Source` only supports **fixed compiled Origins** and A2's permit refers to an integer Origin index. Web Provider needs **authorized dynamic destinations**, not a general unrestricted fetch interface.

Add one **narrow target-derived dynamic mode** for the compiled `web.http` and `web.tls` capabilities while preserving exact fixed-Origin behavior for test providers, DoH resolvers and future Steam. The Control Plane derives/authorizes destination semantics from the **stored canonical Target + Job + capability + namespace**, never trusts a worker-supplied arbitrary host.

- Extend the Worker permit protocol minimally to attest Target identity and **canonical effective destination** (or a bounded claim-bound destination descriptor/identity hash). Revalidate against stored Job/Target, compiled Provider policy, egress route, request method, semantic region, current policy routing revision and active Attempt fence. No unapproved cross-target capability.
- HTTPS root GET/HEAD only as required; redirect follow policy must be explicit and bounded (candidate max 3). Default same-origin; optionally allow same registrable-domain apex/www change based on the public suffix list and safety checks; **external-domain redirect** is a reported link, not automatically followed. No HTTPS -> HTTP downgrade. Every followed hop needs independent policy evaluation, DNS safety check and Permit.
- Resolve the hostname at the outbound connect boundary; validate **all** returned A/AAAA answers against public-address policy. Pin the permitted numeric destination through the actual dial or proxy CONNECT/SOCKS request while retaining the original URL Host and TLS SNI. Never allow remote proxy DNS re-resolution of an unchecked hostname. Keep private/loopback/link-local/metadata/reserved/documentation addresses blocked including DNS rebinding and mixed private/public answers. Do not copy A2 fixture-only private-address allowances to production.
- Validate both initial and redirected destination, protocol, port, bound response size and request headers. Ports should be explicitly constrained to standard HTTP/HTTPS (80/443) initially for dynamic Web; configurable non-standard ports require a separately justified reviewed allowlist, not arbitrary port scanning.
- Keep `direct` / `prefer_proxy` / `require_proxy` guarantees, Worker region compatibility, shared bucket accounting, request-scoped proxy selection and connection reuse. `require_proxy` must never escape to direct; route change forces safe reclaim under A4 fencing.
- A Proxy/transport failure is **not** proof the target website is down. Report path/source error categories separately; do not store raw proxy URL/credentials as Observation. If the required proxy cannot enforce public destination policy (including remote DNS), fail closed.
- No new public API to fetch arbitrary URLs, no unchecked ProxySelector passed into a Provider, and no bypass of Permit by `net/http.DefaultClient`, `net.Dial` or direct SDK connections.

B1 must test authentic direct, HTTP CONNECT, HTTPS CONNECT and SOCKS5 *dynamic origin* requests where relevant. Host/SNI/numeric destination correctness and redirect permit accounting are mandatory.

## 3. DNS transport: metered and explicit

Current `egress.Client` is HTTP-only. For B1, choose a **small metered DNS-over-HTTPS (DoH) implementation** that uses the existing guarded HTTP path to one or two **fixed, validated resolver origins**, with an explicit compiled resolver profile and configurable allowed region/network policy. Use RFC 8484 wire-format DoH GET against a strictly known resolver path, with no user-supplied resolver URL; keep the trusted endpoint list short and document its terms/availability. Mock resolvers locally in integration tests. A narrow reviewed DNS parser dependency is acceptable; do not write a fragile ad-hoc DNS wire parser.

The domain being resolved is the DNS **question**, never the outbound HTTP destination. Each DoH request, redirect (if permitted; normally none) and retry consumes its own global request Permit. Cap type fanout and use conservative low-frequency schedules.

State the exact resolver and vantage in the Observation. A recursive DoH answer is not automatically an authoritative nameserver measurement, and the AD flag is **not** proof that Harvest performed its own DNSSEC verification. Failure of one record type must not erase good results of other types. Do not misstate transient NXDOMAIN as permanent site deletion.

B1 does not require arbitrary raw UDP/TCP sockets or SOCKS5 UDP transport. If DoH is unavailable on the first deployment network, fail with explicit `resolver_unavailable`; do not silently fall back to unmetered system DNS for the `web.dns` capability.

For HTTP transport hostname resolution, the narrow internal resolver needed for safe numeric dialing is a sub-operation of the Permit-gated HTTP connection; do not present it as an independently sampled `web.dns` Observation.

## 4. HTTP result and parsing contract

Perform one bounded, permit-gated GET for the canonical homepage. No automatic `HEAD` plus `GET` double work. Stop on finite redirects. Use context deadlines and limited decompression; guard compressed response expansion and ensure the body reader is closed so the active Permit concurrency reservation releases.

Candidate production bounds (tune against fixtures/measurement): main HTML body <= 512 KiB, final normalized metadata <= 64 KiB, redirects <=3, response headers <=16 KiB, one normal page request plus approved redirects, 5-second currently supported per-permit budget. Larger timeouts require a deliberate compatible A2 Permit/deadline enhancement, not hidden unmetered retries.

Typed output:
- target origin, final URL, redirect summary, HTTP status, content-type, measured latency & timing completeness, response/body byte count, truncated flag and network/egress geography. Distinguish source response vs proxy/TLS/timeout failure and avoid treating 403/WAF as evidence of site shutdown.
- HTML: title, description, language, canonical, Open Graph, Twitter Card, allowed icon/manifest **links** (no fetch in B1), limited JSON-LD website/organization names/URLs/descriptions and safely normalized text snippets if useful. Read and sanitize without JS execution. Bound all strings, arrays and HTML parser depth/resource use. Support charset where measured necessary and prevent UTF-8 assumption corruption.
- Selective headers: relevant cache, content and security headers, server tech hints. Never store Set-Cookie values, Authorization, session tokens, or raw unbounded response headers.
- Hashing: record separately a bounded **response-byte fingerprint** (mark partial if truncated) and a **normalized semantic fingerprint** based on a documented stable subset of page metadata. Use `ETag`/`Last-Modified` conservatively; `304` means `not_modified` only with a matching previous valid scoped result. A dynamic footer/nonce alone must not trigger a semantic change event.
- Use A3's independently scoped segments when parsing/header/TLS evidence differ in completeness; successful HTTP status must not clear last-known-good page metadata because HTML parsing failed or a response was non-HTML. Do not invent empty metadata, website titles or a negative technology finding.

TLS evidence from an actual valid HTTPS response includes verified state, certificate not-before/not-after, issuer, SAN summary, SHA256/SPKI digest when accessible, TLS protocol version and safe verification errors. Do not store private keys or full cert chains by default. Handshake failure must not be represented as a healthy website connection.

## 5. Technology fingerprinting — first-class, passive, small rule set

The first release includes **passive technology identification** from the *same* HTTP response, with **zero incremental outbound requests**. The parser sees bounded already-fetched headers, meta tags, known HTML markers and asset **URLs**; it must not request admin/login paths, source maps or external scripts.

Initial candidate catalog of approximately 15–20 (not a forced recognition count):
- CMS and communities: WordPress, Ghost, Drupal, Discourse, XenForo, MediaWiki.
- Application frameworks: Next.js, Nuxt, Astro.
- Hosting/edge: Vercel, Netlify, GitHub Pages, Cloudflare, Fastly, AWS CloudFront.
- HTTP server self-disclosure: Nginx, Apache httpd, Caddy.
- Consider an additional technology **only** if a narrowly testable reliable passive rule exists; do not infer React/Vue from one inconclusive bundle name.

Rules have stable ID, version, category, one or more bounded matchers, confidence (`high`, `medium`, `low`), sanitized evidence type/value, and optional disambiguation/negative conditions. The rule-set version is recorded in the result; do not force the full Wappalyzer fingerprint catalog into Harvest. Do not copy third-party fingerprint databases or code without reviewing licenses and provenance.

Multiple evidence signals may increase confidence. A single ambiguous signal cannot be promoted to a verified technology; unknown means **not observed**, not "technology absent". CDN, reverse proxy and hosting evidence do not imply the application's origin server technology. Server versions are not proactively guessed or probed. Bounded output count and size; no automatic CVE lookup, exploit checks or vulnerabilities.

Include negative/control fixture cases where generic framework-looking HTML, reverse-proxy headers, copied asset URLs and template strings would otherwise produce false positives. Results are informative evidence, not GoFurry publishing decisions.

The technology detector is a reusable internal submodule of `web.http`, not a separate network Job or new microservice.

## 6. Scheduling / Observation / data budget

Use A1/A3 existing durable schedule, typed segments, current result, Change Stream, last-good and retention. Do not port old GoFurry Redis/DB tables, derived site health, public adoption ratios or incorrect legacy HTTP/2 facts.

Default low-frequency candidate intervals:
- web.http: 24h initial;
- web.dns: 24h initial, record-type work capped;
- web.tls: 48h–7d, or on-demand; do not automatically duplicate every HTTPS GET.
All schedules remain operator-managed **in PostgreSQL**. A4 network route/quota policies also remain in PostgreSQL, not newly introduced static provider YAML. Worker actual proxy credentials/endpoints remain local YAML. No automatic schedule created for each new Target without consumer/operator intent.

Every upstream operation must be counted under the real global, capability, account and egress bucket rules, including redirect and retry. Additional per-origin courtesy/concurrency protections may be introduced **only in a bounded and centrally enforceable way**, not by silently adding an unbounded table per hostname or unmetered local requests. Use conservative host defaults and staged launch verification. No personal data scraping or unrestricted visitor tracking.

Provider output must remain domain-neutral and consumer-agnostic. GoFurry CN interprets observations to maintain site content; it does not receive a fabricated 'site should be published' verdict.

## 7. Architecture and deps

- One Provider implementation within Harvest. Prefer small packages under existing `internal/provider` / `internal/web` (exact names chosen by existing repository organization), with clear separation of HTTP transport adapter, HTML/metadata parser, passive fingerprint rules, DNS-over-HTTPS adapter and TLS evidence.
- Use Go stdlib `net/http`, `crypto/tls`, `crypto/x509`, `httptrace`, `golang.org/x/net/html` and a reviewed small DNS parser if needed. Use `httpx`, `katana`, `tlsx` and `dnsx` as **reference/test-case sources**, not default top-level executables or opaque network engines.
- Keep Fiber lifecycle owner, Router map, JSON v2, SQLC/pgx, PostgreSQL 18, Task and Docker; a new Provider should not force a new database table per technology or protocol. Small additive schema tweaks only if empirically necessary for correctness.
- Do not add a built-in Web UI or GoFurry business mapper. No new repository or mandatory Redis/Kafka/Playwright/Chromium.

## 8. Deterministic acceptance (before marking capabilities executable)

Use disposable PostgreSQL 18, two independent Workers, local fake HTTP/HTTPS origin and proxy servers and a local test DoH resolver; no public live crawling/third-party credentials in CI.

1. Authorized registered public web.origin can be fetched via direct and permit-gated proxy; an arbitrary URL/Host/path not derived from the canonical Job is rejected.
2. SSRF: mixed private/public DNS, loopback/metadata, DNS rebinding, cross-host redirect bypass, bad ports, proxy-side DNS re-resolution and TLS SNI mismatch all fail closed. `require_proxy` emits zero direct requests under outage.
3. Every real HTTP, redirect and DoH operation consumes one Permit; quota exhaustion produces backpressure rather than direct/unmetered fallback. A4 live quota/routing edits affect already-running B1 tasks correctly.
4. HTTP outputs status/timing/final-url/redirect and bounded HTML/OG/JSON-LD/semantic hashes. Non-HTML/partial/truncated/304 response does not overwrite still-valid unrelated metadata. Headers/cookies/secrets are sanitized.
5. 15–20 initial technology rule fixtures prove positives and ambiguity/false positives; no additional network calls, no active fingerprint/vulnerability probes. Ruleset version, evidence and confidence are present.
6. DNS A/AAAA/CNAME/MX/NS/SOA/TXT/CAA from fixed metered DoH; NXDOMAIN/NODATA/SERVFAIL/dnssec AD semantics, type-specific partial failures and resolver vantage are distinguished. No direct unauthorized DNS socket fallback.
7. Verified TLS certificate extraction works for normal HTTPS; expired/mismatched/untrusted certificates yield correct sanitized failure, not successful facts. Explicit web.tls uses only approved permit-gated operations; invalid HTTP status alone does not invalidate a successful TLS handshake.
8. HTTPS->HTTP downgrade, external redirect and excessive redirect chains cannot escape configured policy. Connection protocol diagnostic does **not** become HTTP/2 availability/adoption metric. Explicitly prove no such output fields.
9. A3 Observer: unchanged semantic results do not flood events; changed title/technology/cert/DNS facts update their scoped current and Change Stream exactly once. Network failure preserves last-known-good, zero/absence/unknown semantics are correct.
10. Worker crash/lease, bounded retry, proxy cooldown, A4 DB policy revision and graceful shutdown regressions stay green.
11. Measure total requests per Target, bytes/CPU/heap, HTTP/DNS/TLS p95, SQL statements, WAL growth and safe proxy latency against A4. No invented production SLA. Document known shortcomings and whether any requested source-specific capability remains non-executable.
12. Windows/Linux unit/CI, ARM64 image build, full existing A0–A4 integration suite and Go race checks pass; optional opt-in internet smoke test is never required for CI.

## 9. B2 / future scope

Separate low-frequency `web.robots`, `web.sitemap`, `web.rdap`, `web.assets`, `web.feed_discovery` (RSS/Atom) and additional passive enrichment may follow in **B2**. Katana-like multi-page `web.discover` is deferred until a concrete use case with strict request/depth/time/robots budgets exists. No dynamic browser farm or JS execution.

Do not create an HTTP/2 adoption or JARM/port/WAF/VHost output family even as a placeholder.

## 10. Delivery / implementation

Deliver B1 once, without speculative micro-phases. If safe dynamic egress or DNS/TLS metering requires a narrow, justified A2 transport/protocol extension, implement and regression-test that extension **inside Harvest**; do not bypass it or mislabel unimplemented capabilities as successful. Reuse and validate A4 PostgreSQL policy management; no return to YAML-owned network quotas.

Update README, provider/observation contracts if schema changes, and a concise `docs/harvest-v0.1-b1-acceptance.md` with executed evidence, request/cost metrics, known limitations and final SHA. Commit and push **develop only**. Do not modify other repositories, existing GoFurry data or production systems.

Helpful public references:
- https://github.com/projectdiscovery/httpx
- https://github.com/projectdiscovery/katana
- https://github.com/projectdiscovery/tlsx
- https://github.com/projectdiscovery/dnsx
