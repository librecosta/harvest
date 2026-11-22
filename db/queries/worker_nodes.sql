-- name: InsertWorkerNode :exec
INSERT INTO harvest.worker_nodes (node_id, principal, namespaces, capabilities, current_worker_id)
VALUES ($1, $2, $3, $4, $5) ON CONFLICT (node_id) DO NOTHING;

-- name: LockWorkerNode :one
SELECT * FROM harvest.worker_nodes WHERE node_id = $1 FOR UPDATE;

-- name: SetWorkerNodeRuntime :exec
UPDATE harvest.worker_nodes SET current_worker_id = $2, namespaces = $3, capabilities = $4
WHERE node_id = $1;

-- name: WorkerNodes :many
SELECT n.*, w.capacity, w.last_seen, w.draining, w.offers, w.egress_profiles, w.network_paths,
       w.last_seen > statement_timestamp() - interval '30 seconds' AS online,
       (SELECT count(*) FROM harvest.attempts a WHERE a.worker_id = w.id
        AND a.state = 'running' AND a.lease_until > statement_timestamp()) AS active_tasks
FROM harvest.worker_nodes n JOIN harvest.workers w ON w.id = n.current_worker_id
WHERE NOT (n.principal = ANY(sqlc.arg(excluded_principals)::text[]))
  AND n.namespaces <@ sqlc.arg(namespaces)::text[]
  AND (cardinality(sqlc.arg(capabilities)::text[]) = 0 OR n.capabilities <@ sqlc.arg(capabilities)::text[])
  AND (sqlc.arg(namespace)::text = '' OR n.namespaces <@ ARRAY[sqlc.arg(namespace)::text])
  AND n.node_id > sqlc.arg(after_id)::text
  AND (sqlc.arg(exact_id)::text = '' OR n.node_id = sqlc.arg(exact_id))
ORDER BY n.node_id
LIMIT sqlc.arg(page_size)::integer;

-- name: WorkerNodeTasks :many
SELECT j.id AS job_id, j.capability, a.claimed_at
FROM harvest.attempts a JOIN harvest.jobs j ON j.id = a.job_id
WHERE a.worker_id = sqlc.arg(worker_id) AND a.state = 'running' AND a.lease_until > statement_timestamp()
  AND j.namespace = ANY(sqlc.arg(namespaces)::text[])
  AND (cardinality(sqlc.arg(capabilities)::text[]) = 0 OR j.capability = ANY(sqlc.arg(capabilities)::text[]))
ORDER BY a.claimed_at DESC, j.id
LIMIT 16;

-- name: SetWorkerNodeState :one
UPDATE harvest.worker_nodes SET desired_state = $2, revision = revision + 1
WHERE node_id = $1 AND revision = $3 RETURNING revision;

-- name: WorkerNodeAction :one
SELECT * FROM harvest.worker_node_actions WHERE request_id = $1;

-- name: InsertWorkerNodeAction :exec
INSERT INTO harvest.worker_node_actions
    (request_id, node_id, principal, actor_id, actor_name, actor_role, previous_state, desired_state, revision)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
