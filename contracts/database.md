# Database ownership

Harvest connects to its own database on PostgreSQL 18. An existing instance
can host it alongside independently owned databases. Provision the database
and login outside Harvest. Keep application tables in the `harvest` schema
when the first migration introduces it. Consumers access Harvest through its
versioned API.

Serve performs connectivity and PostgreSQL major-version checks using pgx.
Only an explicit `harvest migrate up --config ...` applies DDL. Goose
serializes migration application with a PostgreSQL session advisory lock.
A0 has no schema version requirement because it ships no migrations.
When the first schema lands, the service must check compatibility without
mutating it before becoming ready.

Goose SQL in `db/migrations/` is the schema authority. Handwritten SQL in Go
is reserved for infrastructure operations that need no generated query,
such as the read-only Goose metadata existence check. Define application
queries in `db/queries/` and generate pgx bindings with
`task sql:generate`. Review the migration and generated diff together.
`task sql:check` compares checked-in bindings with regenerated output.

Entity primary keys use Go 1.27 `uuid.NewV7()` and PostgreSQL `uuid`.
Provider-native keys are separate identities. A delivery cursor has the
commit-order semantics of the [A1-C stream contract](../docs/harvest-a1-c-storage-api-operations.md#3-commit-safe-incremental-stream-critical-correctness-contract).

Use `task check:containers` for failure injection. It supplies disposable
credentials and creates its own PostgreSQL instance for each invocation.
