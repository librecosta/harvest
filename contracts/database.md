# Database ownership

Harvest connects to its own database on PostgreSQL 18. An existing instance
can host it alongside independently owned databases. Provision the database
and login outside Harvest. Consumers and workers use versioned HTTP APIs.
Application tables live in the `harvest` schema.

Only an explicit `harvest migrate up --config ...` applies DDL. Goose
serializes migration application with a PostgreSQL session advisory lock.
Startup and readiness check the PostgreSQL major version and expected
migration. The read-only checks for Goose metadata use handwritten SQL
because Goose owns that table independently of application migrations.

Goose SQL in `db/migrations/` is the schema authority. Define application
queries in `db/queries/` and generate pgx bindings with
`task sql:generate`. Review the migration and generated diff together.
`task sql:check` validates migrations and checks the generated bindings.
sqlc maps UUIDs to the Go standard-library `uuid.UUID` and nullable IDs to
`*uuid.UUID`. Use `uuid.NewV7()` for application identities. PostgreSQL
assigns state transition times. Revisions and lease generations are counters.

Claims serialize on the Worker row to enforce slot capacity, then lock a
Job with `FOR UPDATE SKIP LOCKED`. Completion, renewal, cancellation and
recovery serialize on the Job row before touching its Attempt. A valid lease
requires the current generation, a matching Job/Attempt/Worker and an
unexpired database deadline. Receipt and Job state commit together.
Schedules lock their own row and commit slot insertion and phase advancement
together. The slot unique constraint is scoped to Schedule and revision.

Manual active-equivalent Jobs are coalesced within their canonical
fingerprint and Run Now schedule lineage. The scoped submission table lets
multiple request keys replay the same Job. Terminal Job and Attempt history
is retained for 30 days and pruned in batches of 128. Expired history also
releases its submission keys. Schedule phase and Target identity survive
Job history expiry.

Use `task check:containers` for failure injection. It provisions disposable
credentials and a separate PostgreSQL instance for each test project.
