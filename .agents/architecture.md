# Architecture map

`cmd/harvest` owns the Cobra command boundary, config path and signal context.
`internal/config` decodes strict YAML. `internal/server` owns the pgx pool
and Fiber app. `internal/routers` registers probe paths and delegates their
responses to `internal/handlers`. PostgreSQL setup and explicit Goose
commands live in `internal/database`.

Database connectivity is established before binding HTTP. Fiber's OnListen
enables readiness, OnPreShutdown clears it, and OnPostShutdown closes the
database pool. The same idempotent pool closure covers startup rollback. The
runtime waits until the listener is registered before requesting shutdown
if a signal arrives during startup.

`internal/localrun` holds a checkout lock while serving in local mode. Both
sides authenticate the loopback stop request with fresh challenges bound to
the checkout. The request cancels the same context used for OS signals.

## Design for the next milestones

- [A1 baseline](../docs/harvest-a1-architecture-engineering-baseline.md)
- [A1-A provider, resource and transport](../docs/harvest-a1-a-provider-resource-transport.md)
- [A1-B durable scheduling and execution](../docs/harvest-a1-b-distributed-scheduling-execution.md)
- [A1-C storage, API and operations](../docs/harvest-a1-c-storage-api-operations.md)
- [A1-P proxy and egress](../docs/harvest-a1-proxy-egress-contract.md)

V0.1-A1 begins Target Registry & Durable Execution. Schema and protocol changes
belong to that milestone and follow the identity, transaction and fencing
rules in those contracts.
