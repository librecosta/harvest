-- name: LockObservationTarget :exec
SELECT pg_advisory_xact_lock(hashtextextended(sqlc.arg(identity)::text, 293013));

-- name: LockCurrentResult :one
SELECT * FROM harvest.current_results
WHERE stream = $1 AND target_id = $2 AND capability = $3 AND output_schema = $4 AND scope_key = $5
FOR UPDATE;

-- name: ConfirmCurrentResult :exec
UPDATE harvest.current_results SET confirmed_at = $6
WHERE stream = $1 AND target_id = $2 AND capability = $3 AND output_schema = $4 AND scope_key = $5;

-- name: PutCurrentResult :exec
INSERT INTO harvest.current_results (stream,target_id,capability,output_schema,scope_key,observation_id,content_hash,position,confirmed_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT (stream,target_id,capability,output_schema,scope_key) DO UPDATE
SET observation_id = EXCLUDED.observation_id, content_hash = EXCLUDED.content_hash,
    position = EXCLUDED.position, confirmed_at = EXCLUDED.confirmed_at;

-- name: DeleteCurrentResult :exec
DELETE FROM harvest.current_results
WHERE stream = $1 AND target_id = $2 AND capability = $3 AND output_schema = $4 AND scope_key = $5;

-- name: InsertObservation :exec
INSERT INTO harvest.observations
    (id,stream,target_id,capability,output_schema,scope_key,scope,state,observed_at,received_at,data,content_hash,
    source,evidence,sample,job_id,attempt_id,stored_bytes)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18);

-- name: ObservationsByID :many
SELECT * FROM harvest.observations WHERE id = ANY(sqlc.arg(ids)::uuid[]);

-- name: CurrentRobotsPolicy :one
SELECT o.data FROM harvest.current_results c
JOIN harvest.observations o ON o.id = c.observation_id
WHERE c.stream = $1 AND c.target_id = $2 AND c.capability = 'web.robots'
  AND c.output_schema = 1 AND c.scope_key = $3 AND c.confirmed_at >= statement_timestamp() - interval '8 days'
  AND NOT EXISTS (
    SELECT 1 FROM harvest.jobs j WHERE j.namespace = c.stream AND j.target_id = c.target_id
      AND j.capability = 'web.robots' AND j.scope = sqlc.arg(job_scope)::jsonb
      AND j.state NOT IN ('succeeded', 'queued') AND j.updated_at > c.confirmed_at
  );

-- name: CurrentResultPage :many
SELECT c.target_id,c.capability,c.output_schema,c.scope_key,c.observation_id,c.position,c.confirmed_at,o.stored_bytes
FROM harvest.current_results c
JOIN harvest.observations o ON o.id = c.observation_id
WHERE c.stream = sqlc.arg(stream)
  AND (coalesce(cardinality(sqlc.arg(capabilities)::text[]),0) = 0 OR c.capability = ANY(sqlc.arg(capabilities)))
  AND (c.target_id,c.capability,c.output_schema,c.scope_key) >
      (sqlc.arg(after_target)::uuid,sqlc.arg(after_capability)::text,sqlc.arg(after_schema)::int,sqlc.arg(after_scope)::text)
ORDER BY c.target_id,c.capability,c.output_schema,c.scope_key
LIMIT sqlc.arg(page_size);

-- name: ObservationHistoryPage :many
SELECT id,received_at,stored_bytes FROM harvest.observations
WHERE stream = sqlc.arg(stream)
  AND (coalesce(cardinality(sqlc.arg(capabilities)::text[]),0) = 0 OR capability = ANY(sqlc.arg(capabilities)))
  AND (sqlc.narg(target)::uuid IS NULL OR target_id = sqlc.narg(target))
  AND (sqlc.arg(capability)::text = '' OR capability = sqlc.arg(capability))
  AND (sqlc.arg(scope_key)::text = '' OR scope_key = sqlc.arg(scope_key))
  AND received_at >= sqlc.arg(since)::timestamptz AND received_at <= sqlc.arg(until)::timestamptz
  AND (received_at,id) < (sqlc.arg(after_time)::timestamptz,sqlc.arg(after_id)::uuid)
ORDER BY received_at DESC,id DESC LIMIT sqlc.arg(page_size);

-- name: RetainedHistoryByCapability :many
SELECT capability,count(*) AS rows,sum(stored_bytes)::bigint AS bytes
FROM (
    SELECT capability,stored_bytes FROM harvest.observations o WHERE o.stream = sqlc.arg(stream)
    ORDER BY received_at DESC,id DESC LIMIT 8192
) AS recent
WHERE coalesce(cardinality(sqlc.arg(capabilities)::text[]),0) = 0 OR capability = ANY(sqlc.arg(capabilities))
GROUP BY capability ORDER BY capability;

-- name: PruneObservations :one
WITH candidates AS MATERIALIZED (
    SELECT h.id,h.received_at,h.sample FROM harvest.observations h WHERE h.stream = sqlc.arg(stream)
      AND (received_at,id) > (sqlc.arg(scan_at)::timestamptz,sqlc.arg(scan_id)::uuid)
      AND received_at < sqlc.arg(scan_until)::timestamptz
    ORDER BY received_at,id LIMIT 512
), batch AS MATERIALIZED (
    SELECT o.id FROM candidates c JOIN harvest.observations o ON o.id = c.id
    WHERE (sqlc.arg(pressure)::boolean OR (NOT c.sample AND c.received_at < sqlc.arg(detail_before)::timestamptz)
        OR (c.sample AND c.received_at < sqlc.arg(sample_before)::timestamptz))
      AND NOT EXISTS (SELECT 1 FROM harvest.current_results r WHERE r.observation_id = c.id)
      AND NOT EXISTS (SELECT 1 FROM harvest.change_events e WHERE e.observation_id = c.id)
    ORDER BY c.received_at,c.id LIMIT 128
    FOR UPDATE OF o SKIP LOCKED
), deleted AS (
    DELETE FROM harvest.observations old WHERE old.id IN (SELECT b.id FROM batch b) RETURNING old.id,old.received_at,old.stored_bytes
), checkpoint AS (
    SELECT id,received_at FROM candidates WHERE (SELECT count(*) FROM candidates) = 512
      AND (SELECT count(*) FROM deleted) < 128
    UNION ALL
    SELECT id,received_at FROM deleted WHERE (SELECT count(*) FROM deleted) = 128
    ORDER BY received_at DESC,id DESC LIMIT 1
)
SELECT count(*) AS rows,coalesce(sum(stored_bytes),0)::bigint AS bytes,
       coalesce((SELECT received_at FROM checkpoint),'epoch'::timestamptz)::timestamptz AS next_time,
       coalesce((SELECT id FROM checkpoint),'00000000-0000-0000-0000-000000000000'::uuid)::uuid AS next_id FROM deleted;
