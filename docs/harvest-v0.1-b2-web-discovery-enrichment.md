# Harvest V0.1-B2 — Web Discovery & Low-Frequency Enrichment

Status: **Implementation-ready design** (2026-11-09). Work in **librecosta/harvest**, **develop** only. B1 implementation is commit `aaefc0b82ded0541b2c7527054c56b08c275c038`; its full CI succeeded. No production deployment or changes to GoFurry, steam-go or legacy repositories.

Before coding read `AGENTS.md`, `docs/harvest-v0.1-b1-core-web-acquisition.md`, `docs/harvest-v0.1-b1-acceptance.md`, A1-A/B/C/P, A4 acceptance and the real `internal/provider/web*.go`, `internal/egress/{policy,session,dial}.go`, `internal/execution/{quota,network,observations_write}.go`. They own Target/Job/Permit/fencing/observations; B2 **extends** them, not builds a new crawler or scheduler.

## 1. Product scope

Build **low-frequency, non-intrusive discovery of explicitly public site metadata**, not whole-site crawling, security testing, website mirroring or a content ingestion engine. Consumer-independent normalized results; GoFurry decides whether links are useful, published or automatically accepted.

B2 capabilities:
- `web.robots` on `web.origin`: GET exactly `/robots.txt`, parse applicable policy and bounded `Sitemap:` hints, return clear availability/parse status.
- `web.sitemap` on `web.origin`: GET canonical `/sitemap.xml` (and optionally a bounded, source-proven same-origin sitemap candidate from robots). Parse a bounded XML Sitemap or Sitemap Index. **Discover URLs**, do not visit them or recursively crawl.
- `web.rdap` on `web.domain`: derive registrable domain, resolve official RDAP service via validated IANA bootstrap, GET bounded public domain registration metadata only.
- `web.assets` on `web.origin`: use already discovered icon/manifest hints and/or exactly `/favicon.ico`; optionally retrieve a **small verified same-origin icon / manifest** with strict MIME and byte limits. Return canonical source links, MIME, hash, basic image dimensions and safe manifest fields, **not the image bytes**.
- RSS/Atom: extend **existing `web.http` HTML extraction** to return a small set of `link[rel~="alternate"][type="application/rss+xml"|"application/atom+xml"]` feed candidates, with source and `verified=false`, **without another HTTP request**. If a verified-feed capability is valuable, add narrowly scoped optional `web.feed` which GETs **one previously discovered and server-validated same-origin candidate** and verifies its minimal RSS/Atom root/channel metadata (no item-body archiving). Do not create a permanent feed polling/harvesting service in B2.

Do not invent arbitrary `web.page`, `web.fetch` or browser crawler APIs. No recursive sitemap visits, JS rendering, port scan, WAF canary, HTTP/2 adoption, JARM, HTTP pipeline or VHost enumeration. `security.txt`, `llms.txt`, expanded CDN fingerprint catalog and Katana-style depth crawling remain future optional capabilities, not blockers for B2.

## 2. One unified scoped-path network extension (critical)

B1's dynamic Web egress is restricted to the **registered canonical Origin root**; the A2/B1 internal `egress.Source` and Permit protocol must be extended narrowly for B2's safe known-path fetches, without exposing arbitrary worker-provided URLs.

1. Only compiled `web.*` capabilities and an active, authorized `web.origin`/domain Target can request a path. Immutable claim and current policy control allowed method/path, max hops and request budget. `web.robots` permits only `/robots.txt`; `web.sitemap` normally only `/sitemap.xml`; `web.assets` normally only `/favicon.ico` and **at most one** qualified stored icon/manifest candidate; verified `web.feed` only a validated stored candidate. `web.rdap` has its own controlled registry-service discovery.
2. For an HTML-discovered candidate, the Control Plane binds it to an already accepted `web.http` Observation for the **same Target+namespace+scope**, and includes a bounded immutable approved path in Job execution. A user-supplied path alone is never authority. The Worker revalidates the complete URL, path, origin and Permit; read-only observation-based candidate binding is acceptable, but Provider must never receive Harvest SQL credentials. Candidates with userinfo, secret-like query values, non-default ports not matching Target, fragments, opaque or unsafe encodings are not eligible for automatic fetch.
3. Default **same-origin only**. Do not treat an absolute external icon, Sitemap or feed link as permission to fetch another website. Retain a sanitized external URL as an unverified hint if permitted; do not follow it. HTTPS-to-HTTP downgrade refused. Cross-origin redirects are recorded as unresolved references, not followed. For `robots.txt`, RFC 9309 advises following more redirects, sometimes across origins; document the deliberate stricter B2 security behavior and conservatively mark unresolved external redirects rather than pretending full RFC-compliant crawling.
4. Each actual GET, redirect, retry, bootstrap and registry call requires a separate A4 server-authorized **Permit** tied to the Job/Attempt/fencing, route/region and policy revision. Preserve `direct/prefer_proxy/require_proxy`, no hidden default client or local unlimited fallback. Destination actual A/AAAA answers, numeric dial to trusted proxy and source SNI/Host revalidation from B1 remain mandatory. No worker-chosen arbitrary egress or origin. Exit proxy must enforce the same public-address policy.
5. Keep per-Job operations bounded and distinct by capability. Egress route compatibility, TTL, 5-second permit limit and Provider timeout must be tested together; if a narrow longer task/permit bound is necessary for XML/RDAP, change with regression tests and clear hard maxima, not bypass.
6. Always close response bodies, enforce **both compressed and decompressed** size limits, streaming parsing depth/count limits and read deadlines. Avoid gzip bombs, entity expansion, filesystem access and parsers that fetch referenced URLs. Do not store raw XML/HTML/icon binaries as observations.

**Source discovery policy:** A resource merely linked in HTML, robots or XML is a **candidate**, not a validated site resource. Keep `discovered`, `verified`, `unavailable`, `blocked`, `incomplete` distinguishable. Never interpret absent resource or one failed request as website offline.

## 3. Robots: standard-aware, but not a crawler

- Fetch `/robots.txt` with a clear identifiable Harvest UA and a fixed normal GET, using B1 network/permit policy; at most one initial request plus bounded same-origin redirects. No robots path guessing.
- Implement RFC 9309 group and rule selection for **Harvest's documented crawler product token**, including case-insensitive UA match, most-specific group, multiple matching groups, allow/disallow and longest-match precedence. An existing, small, well-maintained parser may be adopted after license and behavioral tests; no ad hoc split-on-colon behavior that fails comments/encoded paths. `Sitemap:` is supplementary, not an authorization rule.
- The protocol recommends parsing **at least 500 KiB**; allow up to 512 KiB of text for robots (B1's 512 KiB transfer ceiling is a usable baseline). Report truncation/parse incompleteness; never mark a partially parsed policy as complete. Keep normalized concise summaries and a capped list of sitemap links (e.g. 8).
- Distinguish 200 parseable, 404 missing, 401/403 forbidden, 429 throttled, and 5xx/unreachable or malformed. For optional non-root follow-up fetches: if robots is unreachable or unknown, defer rather than assume allow; missing 404 may permit, per standard. Observe source `Retry-After` through A2. Don't mislabel all 4xx as proof of site shutdown.
- B2 does not implement a full REP-compliant multi-origin crawling agent; the strict same-origin redirect cap is a documented security limitation. This matters for `web.sitemap`, assets and feeds: apply conservative permission and origin policy.

References: RFC 9309 https://www.rfc-editor.org/rfc/rfc9309.html.

## 4. Sitemap: discovery, not page fetching

- Default URL: `/sitemap.xml`, not arbitrary XML endpoints. Optional same-origin Sitemap URL referenced by the most recent verified robots Observation may be validated and bound as described in §2; **no automatic broad sitemap index recursion**.
- Support the standard `urlset` and `sitemapindex` XML structures; validate bounded `loc`, `lastmod` with explicit invalid/missing semantics, count and truncation. Return at most a small sample (e.g. 200 URLs and 20 sitemap-index child references), summary counts and conservative origin-host distribution. Do not treat `lastmod` as proof the content actually changed.
- Stream parse XML with DTD/external-entity resolution disabled; reject pathological nesting and huge elements. For `.xml.gz` candidates, compressed and decompressed budgets separately enforced; never uncap decompression. Do not blindly fetch all entries (the external protocol allows up to 50,000 URLs / 50 MiB per sitemap; Harvest intentionally retains only a bounded, partial view).
- Retain off-origin links only as safe, bounded **unverified hints** if useful; never automatically visit them. Sitemaps may point to images/video/pages: do not ingest them. A missing Sitemap is not evidence of a broken site.
- Default schedule weekly, operator-controlled; a large or incomplete XML is `partial/truncated`, not complete success.

Reference: https://www.sitemaps.org/protocol.html.

## 5. RDAP: official lookup and minimum public metadata

- Target is `web.domain`; derive effective registrable domain using a reviewed public suffix mechanism, distinguish a subdomain from the registrable domain and refuse unsupported/ambiguous public-suffix cases. The bootstrap service is **IANA Domain Space RDAP Bootstrap** (RFC 9224), selected by validated TLD/longest-match rule, not hardcoded untrusted web page links.
- Fetch/update the bootstrap from its pinned official HTTPS endpoint via a trusted **fixed-origin, quota-gated** network path with bounded TTL, size, schema and origin validation. Cache for a modest period with startup/restart behavior explained; no bootstrap request per Target. Bootstrap failures do not silently redirect to an unverified aggregator.
- The actual RDAP service base comes exclusively from a validated bootstrap entry and is bound to the domain Target and one authorized Job as an **RDAP-service-derived destination**, with separate public IP, hostname, HTTPS, redirect and permit checks. Reject localhost/private/metadata, unknown scheme, userinfo, unsafe path/query and arbitrary redirects; do not let external JSON turn into an open proxy.
- Bound one domain lookup and small redirects if safe, upper-limit response JSON (candidate 256 KiB; oversized yields partial/incomplete, not silently truncated facts), rate-limit by **registry host/account as appropriate** in addition to existing global/provider/egress buckets. Do not flood a registrar; retries on 429/503 are bounded.
- Return only normalized registry service, registrar (when reliable), registrable domain, authoritative source, lifecycle statuses, registration/updated/expiration events **when explicitly present**, nameservers and validity timestamps. Do **not** retain registrant names, addresses, personal email/phone, contact vCards, raw responses or unneeded entity records. `expires_at` may be absent; no invented 0/empty date. Registry-side missing data is not a domain-deletion verdict. Use an established RDAP parser if it fits the small scope.
- Suggested default every 14–30 days; B2 is not a domain renewal monitoring or notification product.

References: RFC 9224 https://www.rfc-editor.org/rfc/rfc9224.html and RFC 9082 https://www.rfc-editor.org/rfc/rfc9082.html.

## 6. Favicon, manifest and RSS/Atom

**Icon/manifest:** B1 already parses up to four icon references and one manifest URL from homepage HTML. Reuse those observations; no extra root GET by default. `web.assets` checks exactly `/favicon.ico` or **one** operator/consumer-selected validated same-origin discovered candidate. Optional manifest is an independently bounded small GET within that job budget. Parse raster dimensions using a safe config-only decoder; icon data size cap candidate 128 KiB; retain URL, MIME, byte size, digest, declared sizes and inspected dimensions. SVG may return URL/MIME/digest only and must not execute or dereference XML/script/external resources. Manifest parsing extracts only stable `name`, `short_name`, `start_url` (safe normalized link) and icon candidate declarations under a small JSON limit. No binary image retention or uploading to COS/R2. Consumers decide which asset becomes an official icon.

**RSS/Atom discovery:** Extend B1 HTML parsing to collect up to four `rel=alternate` RSS/Atom links with MIME, title, canonical sanitized URL and discovered/verified state; it costs zero new network requests. If implementing optional `web.feed`, use only an approved previously discovered same-origin candidate with a bounded single GET, validate RSS2 or Atom root/feed metadata and a handful of entry IDs/timestamps **without storing entire item content** or creating a periodic news aggregator. Off-origin feeds are hints only. A feed validation error does not make the website unavailable. Do not guess /feed, /rss or framework-specific endpoints.

Do not automatically mutate B1's existing valid Observation contract incompatibly. Introduce a compatible version when fields change in meaning; add tests that old current results remain readable across migration.

HTML feed discovery reference: https://html.spec.whatwg.org/multipage/links.html.

## 7. Data/operations and budget

- Use existing Provider/Resource/Capability compiled registration. B2 capabilities are `not_implemented` until real code and tests are present. Existing `web_v1` offers remain compatible; choose a distinct implementation/schema version only if wire meaning changes. No new core DB tables by default and absolutely no separate database per protocol.
- A3 independently scoped observations/last-known-good/change stream are authoritative. A failure to fetch robots/sitemap/RDAP/icon/feed must not wipe prior good, and unverified discovered links must never masquerade as verified bytes. Semantic change hash ignores TTL, transient fetch timestamps, random nonce and reordered irrelevant XML. Relevant changes (new RSS URL, robots policy, icon digest, registrar/expiry) emit bounded events.
- A4 PostgreSQL operational network policy owns capability routes and rate/burst/concurrency. Explicit Worker YAML owns proxy endpoints/credentials only. Provider manifests advertise conservative network op budgets, content type/size and minimum cadence. All network attempts, redirects and retry calls consume permits; no unsafe uncontrolled parallel target fanout.
- **Suggested initial schedules**, not auto-created: robots 7d; sitemap 7d; rdap 14–30d; assets 7d or on-demand; feed links extracted every existing 24h homepage run; optional feed validation 3–7d. No automatic schedule on Target registration. No source-specific unlimited bootstrap fetches.
- The current B1 release measured 88 upstream grants over 32 simulated Targets; B2 must report actual incremental requests/Target, bytes, DB SQL/WAL, parse heap/CPU and latency. Existing A0–B1 idle/load baselines are comparison points, but test mixes differ; do not claim production SLA on local fixtures.
- Do not expand the passive tech fingerprint catalog just because B2 exists; keep the 18 B1 rules unless a proven correction is required. No HTTP/2 adoption metric.

## 8. Mandatory acceptance (executed)

1. Real compiled B2 capabilities with strict correct Target resources, input validation and honest discovery/availability statuses; old Web and Steam schema discovery remains correct.
2. Robots fixtures: groups, nested rules, percent escapes, UA precedence, comments, same-origin redirects, 404 vs 403 vs 429 vs 503 vs timeout, truncated 512 KiB and Sitemap hints. Unreachable policy cannot be silently treated as allow for follow-up requests.
3. Sitemap XML urlset/index, namespace, relative/absolute host validation, malformed/overlarge/gzip-bomb/external-entity inputs and record caps. **Zero automatic visits to listed pages**; every downloaded sitemap is a distinct metered operation.
4. RDAP test IANA-bootstrap -> selected registry -> domain with mocked endpoints and strict egress authorization; wrong TLD, missing registry, bootstrap failures, private service IP, huge response, 429, redirected host and sensitive contact fields. Bootstrap cache prevents per-domain IANA requests. No real registry traffic in CI.
5. Icon/manifest: B1-discovered same-origin source binding, fallback favicon path, content-type/size, image dimensions, SVG no active content, external URL never fetched, zero image bytes in PostgreSQL.
6. RSS/Atom: HTML alternate discovery without extra GET, relative URL/base resolution and untrusted query redaction; optional validation only with bound original candidate; no feed item-body archival or guessed endpoints.
7. Every actual outbound GET, redirect, bootstrap/registry/icon/feed GET consumes a global Permit. HTTP CONNECT/SOCKS5/required proxy safety, region rules and A4 live policy changes remain enforced; no remote DNS rebinding bypass.
8. A3 current/Observation/change subscription sees correct bounded changes and preserves previous good on partial/unknown/blocked; no pointless events for identical documents. Long offline consumers still recover from snapshots.
9. Two independent Workers and disposable PostgreSQL 18 prove retry, cancellation, fencing, transaction/replay semantics, graceful SIGTERM and storage retention. No GoFurry business dependencies.
10. All A0–B1 native/integration/race tests plus new B2 tests pass on Windows/Linux; Docker and Linux ARM64 image build work. Record actual measurements and known scope limitations in a short `docs/harvest-v0.1-b2-acceptance.md`.

## 9. Delivery and next stage

Complete B2 in a focused change to Harvest `develop` only. Prefer Go stdlib, existing bounded HTML parser, `encoding/xml`, `image.DecodeConfig`, maintained publicsuffix/RDAP helper only if justified. Document any accepted dependency/license. Keep Router audit, Fiber lifecycle, sqlc, UUIDv7, Task/Docker and explicit DB migration intact. Do not touch production systems or modify any GoFurry/steam-go repository.

Update README for new capabilities, inputs and actual request budgets; keep report concise and evidence-based, commit and push develop, state SHA and genuinely unresolved issues. After B2 verification, move to **V0.1-C — Steam Provider** using the maintained `gofurry/steam-go`; Web deep crawling is not part of this release.
