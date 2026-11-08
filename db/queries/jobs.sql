-- name: InsertJob :one
INSERT INTO harvest.jobs
    (id, target_id, namespace, capability, request_schema, execution_key, params, scope, fingerprint, cause,
     schedule_id, schedule_revision, scheduled_for, origin_job_id, priority, available_at, deadline_at, max_attempts)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
RETURNING *;

-- name: InsertManualJob :one
INSERT INTO harvest.jobs
    (id, target_id, namespace, capability, request_schema, execution_key, params, scope, fingerprint, cause,
     schedule_id, schedule_revision, priority, available_at, deadline_at, max_attempts)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'manual', $10, $11, 1, $12, $13, $14)
ON CONFLICT (fingerprint, schedule_id) WHERE cause = 'manual' AND state IN ('queued', 'running', 'retry_wait', 'cancel_requested')
DO UPDATE SET id = harvest.jobs.id
RETURNING *;

-- name: ReserveSubmission :execrows
INSERT INTO harvest.submissions (principal, key, request_hash) VALUES ($1, $2, $3)
ON CONFLICT (principal, key) DO NOTHING;

-- name: Submission :one
SELECT * FROM harvest.submissions WHERE principal = $1 AND key = $2;

-- name: AttachSubmission :exec
UPDATE harvest.submissions SET job_id = $3 WHERE principal = $1 AND key = $2;

-- name: Job :one
SELECT * FROM harvest.jobs WHERE id = $1 AND namespace = ANY(sqlc.arg(namespaces)::text[]);

-- name: ListJobs :many
SELECT * FROM harvest.jobs
WHERE namespace = $1 AND id > sqlc.arg(after_id)::uuid
  AND (coalesce(cardinality(sqlc.arg(capabilities)::text[]), 0) = 0 OR capability = ANY(sqlc.arg(capabilities)::text[]))
ORDER BY id LIMIT sqlc.arg(page_size);

-- name: LockJob :one
SELECT * FROM harvest.jobs WHERE id = $1 AND namespace = ANY(sqlc.arg(namespaces)::text[]) FOR UPDATE;

-- name: LockClaimableJob :one
SELECT j.* FROM harvest.jobs j
JOIN harvest.targets t ON t.id = j.target_id
WHERE j.state IN ('queued', 'retry_wait') AND j.available_at <= statement_timestamp()
  AND j.deadline_at > statement_timestamp() AND j.attempt_count < j.max_attempts
  AND (j.execution_key = ANY(sqlc.arg(offers)::text[]) OR
    j.execution_key || '|' || j.namespace || '|' || coalesce(j.scope->>'egress_region','') = ANY(sqlc.arg(network_offers)::text[]))
  AND j.namespace = ANY(sqlc.arg(namespaces)::text[])
  AND t.state = 'active'
ORDER BY CASE WHEN j.available_at <= statement_timestamp() - interval '30 seconds' THEN 2 ELSE j.priority END DESC,
    j.available_at, j.id
LIMIT 1 FOR UPDATE OF j SKIP LOCKED;

-- name: MarkJobClaimed :one
UPDATE harvest.jobs
SET state = 'running', generation = generation + 1, attempt_count = attempt_count + 1, updated_at = $2
WHERE id = $1 RETURNING *;

-- name: SetJobState :one
UPDATE harvest.jobs SET state = $2, last_code = $3, available_at = $4, updated_at = $5,
    accepted_attempt_id = coalesce(sqlc.narg(accepted_attempt_id)::uuid, accepted_attempt_id),
    accepted_hash = coalesce(sqlc.narg(accepted_hash)::bytea, accepted_hash)
WHERE id = $1 RETURNING *;

-- name: ExpireQueuedJobs :execrows
WITH due AS (
    SELECT id FROM harvest.jobs WHERE state IN ('queued', 'retry_wait') AND deadline_at <= statement_timestamp()
    ORDER BY deadline_at, id LIMIT 32 FOR UPDATE SKIP LOCKED
)
UPDATE harvest.jobs j SET state = 'failed', last_code = 'deadline_exceeded', updated_at = clock_timestamp()
FROM due WHERE j.id = due.id;

-- name: LockExpiredJob :one
SELECT j.* FROM harvest.attempts a JOIN harvest.jobs j ON j.id = a.job_id AND j.generation = a.generation
WHERE a.state = 'running' AND a.lease_until <= statement_timestamp() AND j.state IN ('running', 'cancel_requested')
ORDER BY a.lease_until, a.id LIMIT 1 FOR UPDATE OF j SKIP LOCKED;

-- name: ActiveLeases :one
SELECT count(*) FROM harvest.attempts WHERE state = 'running' AND lease_until > statement_timestamp();

-- name: JobSummary :many
SELECT state, count(*) AS total, min(available_at)::timestamptz AS oldest_available_at
FROM harvest.jobs WHERE namespace = $1
  AND (coalesce(cardinality(sqlc.arg(capabilities)::text[]), 0) = 0 OR capability = ANY(sqlc.arg(capabilities)::text[]))
GROUP BY state ORDER BY state;

-- name: PruneJobs :exec
DELETE FROM harvest.jobs WHERE id IN (
    SELECT id FROM harvest.jobs
    WHERE state IN ('succeeded', 'partial', 'failed', 'cancelled') AND updated_at < statement_timestamp() - interval '30 days'
    ORDER BY updated_at, id LIMIT 128 FOR UPDATE SKIP LOCKED
);

-- name: ActivitySummary :one
SELECT
    (SELECT coalesce(sum(missed_count), 0)::bigint FROM harvest.schedules s
     WHERE s.namespace = sqlc.arg(scope_namespace)::text AND (coalesce(cardinality(sqlc.arg(capabilities)::text[]), 0) = 0
        OR capability = ANY(sqlc.arg(capabilities)::text[]))) AS missed_slots,
    (SELECT count(*) FROM harvest.attempts a JOIN harvest.jobs j ON a.job_id = j.id
     WHERE j.namespace = sqlc.arg(scope_namespace)::text AND a.state = 'expired'
        AND (coalesce(cardinality(sqlc.arg(capabilities)::text[]), 0) = 0
        OR j.capability = ANY(sqlc.arg(capabilities)::text[]))) AS expired_attempts;
