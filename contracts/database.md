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

Permit issuance first locks the Job and verifies the Worker, Attempt and
fencing generation. It locks applicable quota buckets in key order, then
uses the PostgreSQL clock to check cooldown, refill and reserve capacity.
Debit and the operation receipt commit together. Finish locks the permit and
then the same ordered buckets. Replay reads the original receipt without
another debit. Closing or expiring a permit releases only concurrency.
Active counts ignore expired permits even before cleanup.

The configured global bucket limits issuance across all Workers. Permit rows
remain for six minutes after expiry for bounded replay protection, then the
server removes batches of 256. Bucket usage aggregates reset at UTC midnight.
Bucket balances and source cooldowns survive that reset. Startup may ensure
configured bucket rows after schema compatibility succeeds.

A3 completion locks the Job and verifies its Attempt fence, then takes a
transaction-scoped lock on the Target and capability. It reads current rows
in canonical scope order, which also serializes writes to a previously
absent scope. Result writes that add history or events then lock the
namespace's stream head. Each completion touches one namespace. The writer
allocates contiguous positions under that lock and commits the Attempt, Job,
Observation, current reference and events together. A rollback releases all
reserved positions. Positions determine delivery order. `committed_at`
records the database clock while the writer holds the stream head lock.

Current results reference immutable observations. Identical details and
`not_modified` update confirmation freshness only. Event references also pin
observations. Job and Attempt lineage consists of UUIDs independent of
execution retention. Authoritative withdrawals carry a scoped tombstone and
remove current data. Each completed sample has its own observation and event.

The maintenance loop processes short transactions per stream. It locks the
head, marks lagging consumers and invalidates any reset protection crossed
by hard retention pressure, then removes a prefix of at most 128 events.
It scans up to 512 observations per batch and removes at most 128 unpinned
rows. The history scan checkpoint advances over pinned rows. Physical disk
use also depends on indexes, TOAST, dead tuples, WAL and autovacuum. The
configured budgets use estimated encoded row and replay sizes. Restore of a
stream to an earlier head requires a new epoch and fresh consumer baselines.

Use `task check:containers` for failure injection. It provisions disposable
credentials and a separate PostgreSQL instance for each test project.
