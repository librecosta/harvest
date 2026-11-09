# Harvest V0.1-B1 acceptance

## Runtime and result contract

Apply migration 5 with the explicit Goose CLI before starting the upgraded
control plane. It extends existing Permit receipts. Enable the Web offers
and source routes explicitly, with independent `egress_region` scopes.
The Worker offer version is `web_v1` with request and output schema 1.

- `web.http` issues one root GET and up to three same-origin redirects.
  Each hop uses its own permit. Its `http`, `html`, and, for HTTPS, `tls`
  sections have independent current observations. HTTP retains status,
  timing through response headers, selected headers, sanitized redirect
  links and the SHA-256 of up to 512 KiB of response bytes. The 16 KiB
  header limit and the 512 KiB decoded-body limit also apply to parsing.
- HTML semantic hashes cover normalized title, description, language,
  links, Open Graph, Twitter Card, selected JSON-LD and the 18 passive
  technology rules. Evidence is a fixed identifier with confidence.
  Links have user information, query values and fragments removed.
  Response cookie values and raw cache validators are omitted. A
  non-HTML, incomplete or unsolicited 304 response preserves HTML LKG.
- `web.dns` uses RFC 8484 GET at `https://dns.google/dns-query`. It has a
  16 KiB reply limit, up to eight queries per Attempt and 32 records per
  type. Its scopes identify the recursive resolver and each record type.
  NXDOMAIN and NODATA are separate empty answers. SERVFAIL, REFUSED,
  timeout and invalid wire data produce diagnostics. TXT and CAA values
  are represented by their digest and byte count. TTL and AD are
  resolver diagnostics.
- `web.tls` uses one HTTPS HEAD. TLS evidence includes verification, the
  bounded DNS SAN list, validity dates, issuer and leaf/SPKI digests.
  HTTP/1 and negotiated TLS/ALPN fields describe the connection. Invalid
  chains, names and dates produce sanitized diagnostics.
- Body digests and lengths, connection timing, route kind, DNS TTL and
  AD remain available on the observation created at a semantic change.
  Equivalent later observations advance freshness without adding events.
  HTTP body completeness, DNS record content and certificate identity
  can independently trigger changes.

## Executed checks

B1 uses disposable PostgreSQL 18.6, local HTTP/HTTPS and wire-format DoH
fixtures, and two independent authenticated Worker processes. A separate
transport test exercises a real TLS-protected CONNECT proxy.

- Direct, HTTP CONNECT, HTTPS CONNECT and SOCKS5 preserved origin Host and
  used validated numeric destinations. HTTPS origin tests checked SNI.
  Mixed public/private answers, metadata addresses and DNS rebinding were
  blocked. Cross-origin redirects and downgrades produced zero follow-up
  requests. A long chain stopped after four permits.
- The control plane rejected a foreign Target, arbitrary first path,
  forged or reused redirect receipt and wrong method. A repeat operation ID
  replayed the same grant. A4 policy edits fenced an in-flight redirect and
  applied a global concurrency reduction across both Workers. Cancellation
  released its reservation. A 429 cooled the source and required-proxy outage
  produced zero origin requests.
- HEAD 403 and 405 both produced verified certificate evidence. The expired,
  wrong-name and untrusted fixtures produced no TLS facts. DoH covered all
  eight types, partial failures and resolver status distinctions.
- The consumer established a snapshot baseline, replayed paged changes and
  ACKed a converged projection. Nonce, TTL and AD changes emitted no events.
  Title, technology, certificate and DNS record changes emitted scoped
  updates. Truncation, 304, non-HTML and DNS failure preserved LKG.
- A Worker crash after a real request left its old Attempt fenced. A new
  process reclaimed the Job after expiry and completed it with fresh permits.

The parsing, scoping and error fixtures were reviewed against
[httpx bounded-response tests](https://github.com/projectdiscovery/httpx/blob/main/common/httpx/response_memory_test.go),
[Katana scope tests](https://github.com/projectdiscovery/katana/blob/main/pkg/utils/scope/scope_test.go),
[tlsx TLS fixtures](https://github.com/projectdiscovery/tlsx/blob/main/pkg/tlsx/tls/tls_test.go),
and [dnsx response handling](https://github.com/projectdiscovery/dnsx/blob/main/libs/dnsx/dnsx.go).

## Performance

The Windows fixture used Go 1.27.3, two one-slot Workers with
`GOMAXPROCS=4`, and disposable PostgreSQL 18.6 limited to 4 CPUs and
8 GiB. One run collected 32 targets in 29.63 s. Each HTTP or TLS target
used one permit and each DNS target used eight. The 88 grants accounted
for 13,864 response-body bytes and 45,582 permit/finish request-body bytes.
PostgreSQL tracked 7,945 statements, including polling and the fixture's
diagnostic reads, and generated 931,376 WAL insert bytes.

| Fixture measurement | Result |
| --- | ---: |
| HTTP Attempt p95, direct / SOCKS5 | 33.02 / 45.25 ms |
| HTTP time through response headers p95, direct / SOCKS5 | 14 / 18 ms |
| DNS Attempt p95, all eight types via SOCKS5 | 332.38 ms |
| TLS HEAD Attempt p95 via SOCKS5 | 56.60 ms |
| Permit RPC p95 | 19.00 ms |

The control plane and Workers A/B sampled 2.23 / 2.64 / 1.08 MB of Go heap
before the run and 3.48 / 1.41 / 1.05 MB afterward. The parser benchmark
took 59–76 µs at 8 KiB and 0.91–1.11 ms at 256 KiB. It allocated 6.7
and 22.6 kB per parse respectively. The table uses eight samples per
capability/route. These measurements include fixture and scheduler costs.

The A4 record measured 12.20 ms Permit RPC p95 and 315,976 WAL insert
bytes across 96 fixed-origin calls. B1 adds Target validation and scoped
result/event writes. Its mixed workload includes more claims and
completions per grant, so the runs measure different work. Reproduce B1
by setting `HARVEST_B1_MEASURE=1` and running
`go test -tags=integration -run '^TestB1$' -count=1 -v ./integration`.

Local checks passed for `task check`, `go test -race ./...`, B1 with the
race detector, the full A0–B1 container suite, Compose validation and
the Linux ARM64 image build.

## Limits

The deployment has one fixed recursive resolver. Its reachability, public
site behavior and production proxy latency remain unmeasured. The fixture
selects a verified numeric address on each new connection. The runtime
uses the first safe address when a hostname has multiple public answers.
Public-address validation covers the destination supplied to a trusted
operator-configured proxy. Network enforcement at that proxy remains part
of its deployment boundary.
