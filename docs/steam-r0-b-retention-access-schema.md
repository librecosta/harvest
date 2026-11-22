# Steam evidence retention and controlled access

## Configuration and upgrade

Apply Goose migration 13 explicitly on the existing Harvest PostgreSQL
database after validating a restorable backup. It backfills the latest
Server receipt from v12 attribution, adds indexes and a logical usage ledger,
and initializes the durable pruning cursors. The archive time of a v12 row
remains unknown. Migration 13 retains the original bytes, IDs, observations
and attribution, and a rollback requires restoring a verified backup.

The Server reads the following defaults from the existing explicit YAML
configuration. Existing Worker AppID opt-in still controls capture.

```yaml
steam_evidence:
  max_versions_per_scope: 5
  history_days: 90
  namespace_budget_bytes: 268435456
  global_budget_bytes: 1073741824
  prune_batch_size: 32
```

Version count accepts 1–5 and the age window 1–365 days. The namespace
budget must allow at least 512 KiB, the global budget must cover the
configured namespace budget, and the global maximum is 64 GiB. The
prune batch accepts 1–128.

## Version confirmation and tracking

The version scope is namespace, Target, capability, locale, Store Country
and pinned egress region. Matching raw SHA-256 and bytes re-confirm a retained
version without writing a second payload. The source's `first_observed_at`
and R0-A `last_confirmed_at` continue representing Worker observation
times. New `last_server_confirmed_at` records the maximum Server Completion
receipt time. The ordered key is Server receipt time followed by Attempt UUID
as tie breaker. `archived_at` separately records the actual insert time.
A delayed archive cannot become latest on account of its insert time.

For incoming distinct content, retain the highest-ranked configured number
of versions across existing entries and the incoming receipt. Admission
deletes replaced versions, inserts the new version, records its attribution
and removes staging in a single transaction. A staged result already older
than the retained boundary is recorded as superseded. Absent a new distinct
admission, changes to a lower version limit do not initiate bulk deletion.

The age window uses the Server confirmation clock. After the cutoff, past
versions can expire, and the latest is protected when its Target is active
and an enabled Steam AppDetails Schedule with a supported execution key can
produce that precise market, locale and actual egress region. The Schedule's
explicit regions can protect more than one region. An inherited region
uses the same resolver as dispatch. A source pin and a path allowed by the
current profile and Schedule mode still apply. Evaluate configured paths
independent of transient Worker or proxy availability. An inactive Target,
disabled Schedule or incompatible source scope leaves the version subject
to the age window. Track only Harvest Target, Schedule and egress policy.

## Capacity, deletion and recovery

PostgreSQL holds an archive maintenance row lock and authoritative namespace
logical-byte counters. Admission uses the prospective logical byte delta
from insertion and same-scope replacements against both budgets. The
global total is the sum of namespace counters. PostgreSQL triggers update
the ledger transactionally with each insertion or deletion. Storage
inspection reports `pg_column_size(payload)` separately from logical
bytes. TOAST heap, indexes, free space and WAL are separate from both
measures.

On pressure, admission scans one bounded age-pruning batch and reevaluates
capacity. If it still cannot fit, it retains the staged attempt with the
`capacity_limited` marker and schedules a retry after 60 seconds. The
first denial for an Attempt increments its namespace counter and emits a
scoped diagnostic. Shrinking budgets reports overage and waits for ordinary
expiry or an admissible rotation. The capacity check preserves recent
histories in other scopes. Existing payload confirmations remain available
at capacity. Staging retains the R0-A per-payload cap and 24-hour expiry.
Its row count and logical bytes are accounted separately from archive budgets.

Deleting a version cascades exclusively to its Evidence Attribution.
For retained versions, independent age pruning preserves first observation
lineage and the latest Server receipt, and removes older intermediate
attributions beyond the age window. The existing Scheduler, Job, Consumer
and Observation stores keep their own lifecycle.

The existing maintenance loop advances ordered cursors over versions and
attribution. Each cursor scans at most one configured batch per regular
pass and wraps at the end. Archive pressure may advance the version cursor
in an archive transaction. The transaction locks Job, staging, then evidence
maintenance before modifying archive rows. Pruning starts at evidence
maintenance and reads tracking state under an advisory transaction lock.
Steam Schedule and Target edits take its shared side, so pruning can
select and delete under a stable tracking decision. Ordinary Completion
and Consumer reads need neither evidence lock. Errors roll back evidence
and cursors together. The R0-A recovery worker can retry from staging after
restart.

## Operator-only HTTP interface

The routes run under existing Bearer authentication and require an Operator
whose capability grant covers `steam.details`. Namespace and capability
denials for scoped Operator resources use the same 404 as missing evidence.
The common authentication layer returns 403 for an authenticated principal
of another role.

- `GET /api/v1/operations/steam-evidence?namespace=public` lists metadata.
  Optional filters are `target_id`, `app_id`, `locale`, `store_country`,
  `egress_region` and `evidence_id`. AppID is a canonical decimal string
  resolved via a registered Steam app Target. Page size defaults to 50 and
  accepts 1–100. Use the returned `after` UUID with the same filters for the
  next page. The immutable UUID order remains stable as timestamps update.
  The metadata includes both latest receipt and latest Worker observation
  times, logical bytes, SHA-256, first Job and Attempt and format version.
- `GET /api/v1/operations/steam-evidence/{id}/raw` serves one archived body
  as an octet-stream attachment with `no-store` and `nosniff`. The body is
  bounded to 512 KiB by admission and the database constraint. This route
  inherits the ordinary request-body limit. The returned bytes may contain
  untrusted HTML.
- `GET /api/v1/operations/steam-evidence/storage?namespace=public` reports
  logical bytes, stored payload column bytes, versions, attributions,
  staging rows and bytes, capacity-limited Attempt count, budget and overage.
  Omit namespace for global statistics when the Operator covers every
  namespace represented in the ledger or staging. Auth and statistics
  share one database read snapshot.

The list, raw and storage endpoints read archived or aggregate data as
specified. Staged bodies have no download endpoint. See the
[Steam v2 contract](steam-r0-b-steam-v2-contract.md) for R1 schema and
[acceptance](steam-r0-b-acceptance.md) for the verified rollout.
