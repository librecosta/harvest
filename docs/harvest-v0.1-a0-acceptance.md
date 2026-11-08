# V0.1-A0 acceptance

Run `task check` with Go 1.27.3 and Task 3. Run
`task check:docker` to validate Compose and build the local image, then
`task check:containers` for end-to-end verification. The latter needs a
running Linux Docker engine and Docker Compose v2 or newer.

| Gate | Verification |
| --- | --- |
| Explicit configuration | Unit tests reject absent files, unknown or duplicate keys, invalid types, aliases and extra documents. Integration starts with hostile libpq and HTTP proxy settings and checks for sanitized failures. |
| Startup rollback | Invalid DB credentials and an occupied HTTP address fail startup. Verify no pool sessions or local-run ownership are left after the bind failure. |
| Database outage | Stop the fixture database. Verify readiness 503 and liveness 200 in both native and container runtimes. Restart PostgreSQL and verify both recover. |
| Explicit SQL workflow | Run `task sql:generate` and `task sql:check` against the empty query set. Invoke Goose status and up twice and verify the fixture has no Harvest schema or Goose version table. |
| Process ownership | Exercise repeat stop, startup cancellation, an unrelated listener, a second working directory and stale ownership state. |
| Docker lifecycle | Exercise the deployment Compose service against disposable PG18. Verify the nonroot user, read-only filesystem and config mount, built-in health probe and clean SIGTERM exit. |

The harness picks a unique Compose project, image tag and loopback ports.
PostgreSQL data lives on a temporary filesystem. The harness supplies both
YAML files and cleans up its project and image on exit.

A0's SQL acceptance covers the empty migration set. A1 must validate the first
real migration, generated queries and schema compatibility on fresh and
upgraded databases. ARM64 image build is part of CI. Runtime acceptance runs
on the host's Docker architecture.
