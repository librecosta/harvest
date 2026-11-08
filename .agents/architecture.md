# Architecture map

`cmd/harvest` loads the production Provider registry and invokes the CLI in
`internal/command`. `internal/config` validates distinct server and worker
YAML shapes. `internal/routers` owns the Fiber groups and route mapping.
`internal/handlers` authenticates requests and handles the wire protocol.
`internal/execution` owns the state machine and transactions through
generated `internal/database/queries`. `internal/provider` owns compiled
metadata, canonicalization, and typed capability validation. Workers use
`internal/worker` to pull and finish attempts over authenticated HTTP.
The integration harness adds a Provider when it re-executes its test binary.

`internal/egress` owns Worker-local transports, route selection, pinned DNS,
endpoint health and the scoped HTTP client. Each source round trip acquires
an Attempt-fenced permit before dialing. `internal/execution` derives quota
buckets from server YAML, the stored Job, compiled capability and authorized
Worker path. The Worker releases a permit after closing the body.

`internal/execution` also owns scoped Observation acceptance, last-known-good,
change-stream cursors and retention. It commits acquisition results under the
same Attempt fence. `internal/server` runs bounded result pruning with the
other maintenance loops.

`internal/server` owns the pool, Scheduler, optional embedded worker and
Fiber. Startup checks schema compatibility before binding. OnListen enables
readiness and starts managed loops. Shutdown closes admission, stops
reconciling and permits renew/complete during bounded pre-drain. Fiber
OnPreShutdown enforces draining and OnPostShutdown closes the pool after
HTTP. Startup rollback shares the same idempotent cleanup.

`internal/localrun` holds a checkout lock while serving in local mode. Both
sides authenticate the loopback stop request with fresh challenges bound to
the checkout. The request cancels the same context used for OS signals.

## Contracts

- [Engineering baseline](../docs/harvest-a1-architecture-engineering-baseline.md)
- [A1-A Provider, resource and transport](../docs/harvest-a1-a-provider-resource-transport.md)
- [A1-B durable scheduling and execution](../docs/harvest-a1-b-distributed-scheduling-execution.md)
- [A1-C storage, API and operations](../docs/harvest-a1-c-storage-api-operations.md)
- [A1-P proxy and egress](../docs/harvest-a1-proxy-egress-contract.md)
- [A1 implementation scope](../docs/harvest-v0.1-a1-target-registry-durable-execution.md)
- [A1 acceptance](../docs/harvest-v0.1-a1-acceptance.md)
- [A2 implementation scope](../docs/harvest-v0.1-a2-proxy-egress-global-quota.md)
- [A2 acceptance](../docs/harvest-v0.1-a2-acceptance.md)
- [A3 implementation scope](../docs/harvest-v0.1-a3-observation-change-stream-retention.md)
- [A3 acceptance and recovery](../docs/harvest-v0.1-a3-acceptance.md)
