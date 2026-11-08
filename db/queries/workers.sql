-- name: InsertWorker :exec
INSERT INTO harvest.workers (id, principal, offers, egress_profiles, capacity, network_paths)
VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (id) DO NOTHING;

-- name: Worker :one
SELECT * FROM harvest.workers WHERE id = $1 AND principal = $2;

-- name: LockWorker :one
SELECT * FROM harvest.workers WHERE id = $1 AND principal = $2 FOR UPDATE;

-- name: HeartbeatWorker :one
UPDATE harvest.workers SET last_seen = clock_timestamp(), draining = draining OR sqlc.arg(draining)::boolean,
    network_paths = coalesce(sqlc.narg(paths)::jsonb, network_paths)
WHERE id = $1 AND principal = $2 RETURNING *;

-- name: WorkerLeaseCount :one
SELECT count(*) FROM harvest.attempts
WHERE worker_id = $1 AND state = 'running' AND lease_until > statement_timestamp();

-- name: InsertAttempt :one
INSERT INTO harvest.attempts (id, job_id, worker_id, generation, claimed_at, heartbeat_at, lease_until)
VALUES ($1, $2, $3, $4, $5, $5, $6)
RETURNING *;

-- name: Attempt :one
SELECT * FROM harvest.attempts WHERE id = $1 AND job_id = $2 AND worker_id = $3 AND generation = $4;

-- name: AttemptForGeneration :one
SELECT * FROM harvest.attempts WHERE job_id = $1 AND generation = $2;

-- name: JobAttempts :many
SELECT * FROM harvest.attempts WHERE job_id = $1 ORDER BY generation;

-- name: RenewAttempt :one
UPDATE harvest.attempts SET heartbeat_at = $2, lease_until = $3 WHERE id = $1 RETURNING *;

-- name: FinishAttempt :exec
UPDATE harvest.attempts SET ended_at = $2, state = $3, error_code = $4, result_hash = $5, receipt_state = $6
WHERE id = $1;

-- name: WorkerSummary :many
SELECT id, capacity, last_seen, draining, network_paths FROM harvest.workers
WHERE principal = ANY(sqlc.arg(principals)::text[]) AND last_seen >= statement_timestamp() - interval '30 seconds'
ORDER BY last_seen DESC, id LIMIT 100;

-- name: PruneWorkers :exec
DELETE FROM harvest.workers WHERE id IN (
    SELECT w.id FROM harvest.workers w
    WHERE w.last_seen < statement_timestamp() - interval '30 days'
      AND NOT EXISTS (SELECT 1 FROM harvest.attempts a WHERE a.worker_id = w.id)
    ORDER BY w.last_seen, w.id LIMIT 128 FOR UPDATE SKIP LOCKED
);

-- name: NetworkWorkers :many
SELECT * FROM harvest.workers WHERE principal = ANY(sqlc.arg(principals)::text[]) AND NOT draining
  AND last_seen > statement_timestamp() - interval '30 seconds'
  AND sqlc.arg(execution_key)::text = ANY(offers)
ORDER BY last_seen DESC LIMIT 100;

-- name: EligibleWorker :one
SELECT EXISTS (
    SELECT 1 FROM harvest.workers
    WHERE principal = ANY(sqlc.arg(principals)::text[]) AND NOT draining
      AND last_seen > statement_timestamp() - interval '30 seconds'
      AND sqlc.arg(execution_key)::text = ANY(offers)
);
