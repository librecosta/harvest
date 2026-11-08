# Changelog

## V0.1-A0

- Established the Go 1.27.3 server and CLI with explicit YAML, Fiber v3 probes
  and bounded shutdown.
- Added PostgreSQL 18 connectivity, embedded explicit Goose migrations and
  pinned sqlc tooling. A0 ships an empty migration and query set.
- Added checkout-owned Task lifecycle commands, nonroot Docker packaging,
  Windows and Linux CI, and disposable PostgreSQL acceptance coverage.
