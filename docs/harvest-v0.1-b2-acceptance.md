# V0.1-B2 acceptance

## Behavior

- Migration 6 preserved the B1 homepage current result. The compiled catalog
  exposes Robots, Sitemap, assets and RDAP with request and output schema 1.
  Schedule creation enforces the 24-hour Robots, Sitemap and asset minimum
  and the seven-day RDAP minimum.
- HarvestBot selects the most specific applicable Robots group, merges matching
  groups and applies longest-rule precedence. An observed 404 allows gated
  paths. An unavailable, malformed, stale or superseded policy blocks Sitemap
  and assets until a successful refresh. Robots current results remain
  readable after a failure. B2 accepts up to two same-origin redirects and
  stores sanitized unresolved redirect hints in diagnostic history.
- Sitemap parses a bounded URL set or index. Counts distinguish same-origin,
  external and invalid entries. The sample contains up to 40 URLs or 20 child
  sitemap hints. XML with external entities, oversized input or oversized gzip
  output is rejected. The fixture saw zero visits to listed pages or indexes.
- Icon and manifest selection uses the exact accepted HTML Observation,
  Target, namespace and scope. A forged path and cross-scope selection are
  refused. The fixture covered fallback ICO, PNG dimensions, SVG metadata and
  trimmed Web Manifest data. RSS and Atom discovery used the homepage
  response with zero extra GETs. Queries, fragments and userinfo are excluded
  from persisted links.
- The IANA fixture served one failed refresh and one valid refresh. The valid
  cold-cache pair used three grants for one bootstrap and two registry GETs
  across two Worker processes. Registry destinations came from the compiled
  bootstrap path. The fixture covered private answers, missing registries,
  mismatched domains, redirect, oversized JSON and a shared 429 cooldown.
  Retained facts exclude contact records and include expiry only when supplied.
- Real HTTP CONNECT, SOCKS5 and direct DoH all used metered operations.
  A live A4 routing edit fenced the old Attempt's follow-up. Cancellation
  released the permit. A killed Worker left an expired Attempt that a new
  process reclaimed. Scoped results, snapshot, Change Stream and ACK showed
  semantic changes and preserved prior good data on unavailable sources.

The bounded parsers use Go's standard library and the existing BSD-licensed
`golang.org/x/net/publicsuffix` dependency. Protocol references include
[RFC 9309](https://www.rfc-editor.org/rfc/rfc9309.html),
[Sitemap XML](https://www.sitemaps.org/protocol.html),
[RFC 9224](https://www.rfc-editor.org/rfc/rfc9224.html) and
[RFC 9082](https://www.rfc-editor.org/rfc/rfc9082.html).

## Verification

Windows used Go 1.27.3, isolated Docker PostgreSQL 18.6 and two separate
one-slot Workers. `task check`, `go test -race ./...`, the A0–B2 container
suite, Compose validation and the Linux ARM64 image build passed. The suite
covered older cursor recovery and bounded retention with A3's existing tests.
All B2 upstream requests in the suite terminated in local fixtures.

## Performance

Six origin/domain pairs ran 24 Jobs in 21.18 s with a warm RDAP cache.
Each pair added four grants, one each for Robots, Sitemap, favicon and RDAP.
The cold-cache pair separately added one shared bootstrap GET. The warm run
counted 7,074 response-body bytes, 15,325 permit/finish request-body bytes,
4,895 tracked PostgreSQL statements and 496,832 WAL insert bytes. SQL and
WAL include polling, scheduler and fixture instrumentation.

| Local measurement | Result |
| --- | ---: |
| Permit RPC p95 | 10.75 ms |
| Attempt p95, Robots / Sitemap / favicon / warm RDAP | 27.15 / 28.85 / 30.03 / 29.40 ms |
| Go heap before → after, control plane / Worker A / Worker B | 3.12 → 2.55 / 2.88 → 1.43 / 1.13 → 1.97 MB |
| Robots parser | 3.32–3.37 µs, 1,128 B allocated |
| Sitemap parser, 200 records / 15 KiB | 0.83–0.87 ms, 282 kB allocated |
| RDAP / Manifest parser | 4.74–4.78 / 4.35–4.38 µs, 1,049 / 1,523 B allocated |

The six Attempt samples per capability and the heap snapshots describe this
fixture. The B1 measurement used 88 grants across 32 targets with a different
protocol mix. B2 adds zero network operations to B1 homepage acquisition.

Reproduce by setting `HARVEST_B2_MEASURE=1` and running
`go test -tags=integration -run '^TestB2$' -count=1 -v ./integration`.
Run the parser benchmark with
`go test ./internal/provider -run '^$' -bench '^BenchmarkWebDiscovery$' -benchmem`.

## Limits

Sitemap uses only the canonical entry path. Sitemap hints and RSS/Atom links
remain unverified. Response ceilings are 512 KiB for Robots and Sitemap,
256 KiB for IANA and RDAP, 128 KiB for icons and 32 KiB for manifests. An
incomplete response records a diagnostic while preserving prior good data.

The 24-hour IANA cache lives in the single active control plane and starts
cold on restart. Registry redirects are unresolved. A conservative shared
RDAP account bucket makes a registry cooldown apply to all configured RDAP
sources. The measured latencies are local and public upstream behavior
remains unmeasured.
