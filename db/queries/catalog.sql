-- name: DBNow :one
SELECT clock_timestamp()::timestamptz;

-- name: UpsertTarget :one
INSERT INTO harvest.targets (id, provider, resource_type, external_key, namespace)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (provider, resource_type, external_key, namespace)
DO UPDATE SET external_key = EXCLUDED.external_key
RETURNING *;

-- name: Target :one
SELECT * FROM harvest.targets WHERE id = $1 AND namespace = ANY(sqlc.arg(namespaces)::text[]);

-- name: LockTarget :one
SELECT * FROM harvest.targets WHERE id = $1 AND namespace = ANY(sqlc.arg(namespaces)::text[]) FOR SHARE;

-- name: ListTargets :many
SELECT * FROM harvest.targets
WHERE namespace = $1 AND id > sqlc.arg(after_id)::uuid ORDER BY id LIMIT sqlc.arg(page_size);

-- name: UpdateTarget :one
UPDATE harvest.targets SET state = sqlc.arg(state), revision = revision + 1
WHERE id = $1 AND revision = sqlc.arg(expected_revision) AND namespace = ANY(sqlc.arg(namespaces)::text[])
RETURNING *;

-- name: InsertSchedule :one
INSERT INTO harvest.schedules
    (id, target_id, namespace, capability, request_schema, execution_key, params, scope, fingerprint,
     interval_seconds, cron, anchor_at, next_due_at, policy, enabled)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
ON CONFLICT (fingerprint, interval_seconds, cron, anchor_at, policy)
DO UPDATE SET id = harvest.schedules.id
RETURNING *;

-- name: Schedule :one
SELECT * FROM harvest.schedules WHERE id = $1 AND namespace = ANY(sqlc.arg(namespaces)::text[]);

-- name: LockSchedule :one
SELECT * FROM harvest.schedules WHERE id = $1 AND namespace = ANY(sqlc.arg(namespaces)::text[]) FOR UPDATE;

-- name: ListSchedules :many
SELECT * FROM harvest.schedules
WHERE namespace = $1 AND id > sqlc.arg(after_id)::uuid
  AND (coalesce(cardinality(sqlc.arg(capabilities)::text[]), 0) = 0 OR capability = ANY(sqlc.arg(capabilities)::text[]))
ORDER BY id LIMIT sqlc.arg(page_size);

-- name: UpdateSchedule :one
UPDATE harvest.schedules
SET capability = $2, request_schema = $3, execution_key = $4, params = $5, scope = $6, fingerprint = $7,
    interval_seconds = $8, cron = $9, anchor_at = $10, next_due_at = $11, policy = $12,
    enabled = $13, revision = revision + 1
WHERE id = $1
RETURNING *;

-- name: LockDueSchedule :one
SELECT s.* FROM harvest.schedules s
JOIN harvest.targets t ON t.id = s.target_id
WHERE s.enabled AND s.next_due_at <= statement_timestamp() AND t.state = 'active'
  AND s.execution_key = ANY(sqlc.arg(offers)::text[])
ORDER BY s.next_due_at, s.id LIMIT 1 FOR UPDATE OF s SKIP LOCKED;

-- name: AdvanceSchedule :exec
UPDATE harvest.schedules SET next_due_at = $2, missed_count = least(missed_count + sqlc.arg(missed)::int, 1000000)
WHERE id = $1;
