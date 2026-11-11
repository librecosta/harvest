# Changelog

## V0.1-D

- Documented direct and regional-proxy deployments for Web and Steam.
- Added mixed local load and independent PostgreSQL consumer recovery checks.
- Added an operator-visible replay-age warning and an opt-in public smoke task.

## V0.1-C

- Enabled Steam Storefront details, Steam News and Current Players with
  `steam-go` over Attempt-scoped egress and permits.
- Added scoped Steam price, details, media, news and sample observations.
- Added shared, persistent UTC-day Permit budgets to PostgreSQL network policy.

## V0.1-B2

- Enabled bounded Robots, Sitemap, RDAP and asset discovery. Added RSS and
  Atom candidates to the existing homepage Observation with zero extra GETs.
- Extended the shared Permit boundary for known paths, stored asset candidates
  and IANA-selected registry endpoints. Added migration 6 for replay receipts.
- Retained privacy-filtered scoped results and last known good across source
  failures. Added isolated dual-Worker HTTP CONNECT and SOCKS5 verification.

## V0.1-B1

- Enabled homepage metadata and 18 passive Web fingerprints, eight recursive
  DoH record types and verified TLS evidence from HTTP or an explicit HEAD.
- Bound dynamic destinations to stored Targets. Each same-origin redirect
  uses its own quota permit and a single-use receipt from the prior hop.
- Persisted independent Web result scopes with stable semantic hashes and
  preserved their last-known-good values across partial failures.

## V0.1-A4

- Persisted operational network policy in PostgreSQL with explicit import
  and versioned operator replacement.
- Preserved quota accounting across edits and restart. Routing revisions
  fence old network claims while issued permits can finish.

## V0.1-A3

- Added typed multi-scope observations, last-known-good results and
  commit-ordered changes in the existing fenced completion transaction.
- Added scoped consumer enrollment, monotonic ACK, keyset snapshots and
  bounded retention with explicit cursor expiry.

## V0.1-A2

- Added scoped Worker HTTP execution through direct, HTTP CONNECT, HTTPS
  CONNECT and SOCKS5 routes, with endpoint health, region matching, verified
  destination IPs and bounded failover.
- Added PostgreSQL-backed per-round-trip permits with atomic rate and
  concurrency accounting, idempotent finish, source cooldown and bounded
  diagnostics. Exercised the protocol with two isolated Workers and mock
  upstreams and proxies.

## V0.1-A1

- Added authenticated Target, Schedule and Job APIs with typed Provider
  metadata, canonical identity, namespace isolation and durable idempotency.
- Added the first PostgreSQL migration, sqlc bindings, anchored/UTC
  scheduling, fenced Worker leases, result receipts and crash recovery.
- Added the remote worker command and disposable two-Worker acceptance.
  Web and Steam discovery identify their capabilities as unimplemented.

## V0.1-A0

- Established the Go 1.27.3 server and CLI with explicit YAML, Fiber v3 probes
  and bounded shutdown.
- Added PostgreSQL 18 connectivity, embedded explicit Goose migrations and
  pinned sqlc tooling. A0 shipped an empty migration and query set.
- Added checkout-owned Task lifecycle commands, nonroot Docker packaging,
  Windows and Linux CI, and disposable PostgreSQL acceptance coverage.
