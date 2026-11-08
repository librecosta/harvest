# Changelog

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
