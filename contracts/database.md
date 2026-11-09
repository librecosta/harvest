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

For a dynamic Web permit the Job lock also serializes validation of the
stored Target and the hop chain. The first permit is for the compiled entry
path. B2 asset selection may bind a same-origin path from an immutable HTML
Observation for the same Target, namespace and scope. Sitemap and asset paths
require a fresh compatible Robots result at each permit. A redirect needs a finished parent from the same Attempt with a matching
request URI digest. A unique parent reference makes each continuation
single-use. An operation ID can still replay its original grant. The
quota ledger stores digests and hop numbers, with the same six-minute
retention. It keeps redirect URLs and query values out of the ledger. Unresolved
B2 redirects can appear as sanitized, unverified links in diagnostic history.
They do not advance current results or the change stream.

RDAP uses a distinct permit for the pinned IANA bootstrap and one for its
validated registry endpoint. Migration 6 enforces one of each stage per
Attempt and preserves the derived endpoint for idempotent grant replay. A
successful, timely bootstrap finish publishes the shared in-memory cache after
commit. The database retains the stage and receipt, with no raw bootstrap
payload or registry contact records. Both stages debit the same RDAP account
bucket across Workers.

The configured global bucket limits issuance across all Workers. Permit rows
remain for six minutes after expiry for bounded replay protection, then the
server removes batches of 256. Bucket usage aggregates reset at UTC midnight.
Bucket balances and source cooldowns survive that reset.

The explicit `policy init` command creates the singleton network policy and
missing quota rows atomically. Existing rows retain balances, timestamps,
cooldown and metrics. Startup loads and verifies the typed document and
checksum. The operator API uses full replacement and revision CAS.

A single active control plane owns live policy. Its admission barrier covers
policy reads, claims, permit issuance and affected pruning. Replacement
locks the policy row and affected bucket rows in key order. It settles
elapsed refill under the previous rates at PostgreSQL time, clamps to the
new bursts and preserves existing cooldown and counters. Added or
reactivated quotas accrue from zero at activation. A bounded ledger
preserves logical quota bindings across removal. Permit finish uses the
original reservation's bucket identities.

An Attempt records the routing revision in its claim transaction. A
routing edit invalidates previous network Attempts for new grants and
grant replay. Numeric edits keep the routing revision. The replacement
publishes its effective in-memory state after commit while admission is
still held. An ambiguous commit is checked against PostgreSQL before
admission resumes. A failed recheck leaves network admission closed until
an operator retries replacement or the server reloads the policy.

A3 completion locks the Job and verifies its Attempt fence, then takes a
transaction-scoped lock on the Target and capability. It reads current rows
in canonical scope order, which also serializes writes to a previously
absent scope. Result writes that add history or events then lock the
namespace's stream head. Each completion touches one namespace. The writer
allocates contiguous positions under that lock and commits the Attempt, Job,
Observation, current reference and events together. A rollback releases all
reserved positions. Positions determine delivery order. `committed_at`
records the database clock while the writer holds the stream head lock.

Current results reference immutable observations. Identical semantic details
and `not_modified` update confirmation freshness only. The Web capability's
compiled schema projects out volatile diagnostics before calculating the
content hash. The completion receipt still binds the full normalized data.
 Event references also pin
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
