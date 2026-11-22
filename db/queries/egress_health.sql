-- name: AcceptWorkerHealth :execrows
UPDATE harvest.workers SET health_sequence = sqlc.arg(sequence)::bigint
WHERE id = sqlc.arg(worker_id) AND health_sequence < sqlc.arg(sequence)::bigint;

-- name: RecordWorkerHealth :exec
WITH reports AS (
    SELECT jsonb_array_elements(sqlc.arg(reports)::jsonb) AS report
), stamped AS (
    SELECT sqlc.arg(node_id)::text AS node_id, statement_timestamp() AS last_at,
           report ->> 'profile' AS profile, report ->> 'path_id' AS path_id, report ->> 'kind' AS kind,
           report ->> 'region' AS region, report ->> 'check_type' AS check_type, report ->> 'last_outcome' AS last_outcome,
           (report ->> 'successes')::bigint AS successes, (report ->> 'failures')::bigint AS failures,
           (report ->> 'transport_failures')::bigint AS transport_failures, (report ->> 'latency_ms')::bigint AS latency_ms,
           (report ->> 'last_latency_ms')::integer AS last_latency_ms,
           (report ->> 'trailing_failures')::bigint AS trailing_failures, (report ->> 'recoveries')::bigint AS recoveries
    FROM reports
)
INSERT INTO harvest.worker_egress_health AS dst
    (node_id, profile, path_id, kind, region, check_type, minute_at, last_at, last_outcome,
     successes, failures, transport_failures, latency_ms, last_latency_ms, consecutive_failures, recoveries)
SELECT s.node_id, s.profile, s.path_id, s.kind, s.region, s.check_type, date_trunc('minute', s.last_at), s.last_at,
       s.last_outcome, s.successes, s.failures, s.transport_failures, s.latency_ms, s.last_latency_ms,
       s.trailing_failures + CASE WHEN s.successes = 0 THEN coalesce(prior.consecutive_failures, 0) ELSE 0 END, s.recoveries
FROM stamped s LEFT JOIN LATERAL (
    SELECT h.consecutive_failures FROM harvest.worker_egress_health h
    WHERE (h.node_id, h.profile, h.path_id, h.kind, h.region, h.check_type) =
          (s.node_id, s.profile, s.path_id, s.kind, s.region, s.check_type)
      AND h.minute_at >= s.last_at - interval '24 hours'
    ORDER BY h.minute_at DESC LIMIT 1
) prior ON true
ON CONFLICT (node_id, profile, path_id, kind, region, check_type, minute_at) DO UPDATE SET
    last_at = excluded.last_at, last_outcome = excluded.last_outcome,
    successes = dst.successes + excluded.successes, failures = dst.failures + excluded.failures,
    transport_failures = dst.transport_failures + excluded.transport_failures, latency_ms = dst.latency_ms + excluded.latency_ms,
    last_latency_ms = excluded.last_latency_ms, consecutive_failures = excluded.consecutive_failures,
    recoveries = dst.recoveries + excluded.recoveries;

-- name: WorkerHealth :many
SELECT profile, path_id, kind, region, check_type,
       (array_agg(last_outcome ORDER BY minute_at DESC))[1]::text AS last_outcome,
       max(last_at)::timestamptz AS last_at,
       (array_agg(last_latency_ms ORDER BY minute_at DESC))[1]::integer AS last_latency_ms,
       (array_agg(consecutive_failures ORDER BY minute_at DESC))[1]::bigint AS consecutive_failures,
       sum(successes)::bigint AS successes, sum(failures)::bigint AS failures,
       sum(transport_failures)::bigint AS transport_failures, sum(latency_ms)::bigint AS latency_ms,
       sum(recoveries)::bigint AS recoveries
FROM harvest.worker_egress_health
WHERE node_id = sqlc.arg(node_id) AND minute_at >= statement_timestamp() - interval '24 hours'
GROUP BY profile, path_id, kind, region, check_type
ORDER BY max(last_at) DESC, profile, path_id, kind, region, check_type
LIMIT 128;

-- name: PruneWorkerHealth :exec
DELETE FROM harvest.worker_egress_health
WHERE (node_id, profile, path_id, kind, region, check_type, minute_at) IN (
    SELECT node_id, profile, path_id, kind, region, check_type, minute_at
    FROM harvest.worker_egress_health WHERE minute_at < statement_timestamp() - interval '24 hours'
    ORDER BY minute_at LIMIT 4096
);

-- name: InsertEgressPolicyAction :exec
INSERT INTO harvest.egress_policy_actions
    (request_id, principal, actor_id, actor_name, actor_role, namespace, capability, revision, old_profile, old_region, profile, region)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12);

-- name: PolicyWorkerPaths :many
SELECT w.id, w.principal, w.node_id, w.offers, w.network_paths, w.capacity,
       (w.last_seen > statement_timestamp()-interval '30 seconds') AS online,
       (w.draining OR coalesce(n.desired_state <> 'active', false)) AS blocked,
       (SELECT count(*) FROM harvest.attempts a WHERE a.worker_id = w.id AND a.state = 'running'
        AND a.lease_until > statement_timestamp()) AS active_tasks
FROM harvest.workers w LEFT JOIN harvest.worker_nodes n ON n.current_worker_id = w.id
WHERE w.principal = ANY(sqlc.arg(principals)::text[]) AND (w.node_id IS NULL OR n.node_id IS NOT NULL)
ORDER BY (w.last_seen > statement_timestamp()-interval '30 seconds') DESC, w.last_seen DESC, w.id
LIMIT 201;
