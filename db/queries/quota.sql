-- name: EnsureQuotaBuckets :exec
INSERT INTO harvest.quota_buckets (key, balance, refilled_at)
SELECT k, 0, 'epoch' FROM unnest(sqlc.arg(keys)::text[]) AS k ORDER BY k COLLATE "C"
ON CONFLICT (key) DO NOTHING;

-- name: LockQuotaBuckets :many
SELECT * FROM harvest.quota_buckets WHERE key = ANY(sqlc.arg(keys)::text[])
ORDER BY key COLLATE "C" FOR UPDATE;

-- name: CountActiveQuota :many
SELECT k.key::text AS key, count(*) AS active
FROM harvest.quota_permits p
CROSS JOIN LATERAL unnest(p.bucket_keys) AS k(key)
WHERE p.finished_at IS NULL AND p.expires_at > sqlc.arg(now_at)::timestamptz
  AND p.bucket_keys && sqlc.arg(keys)::text[] AND k.key = ANY(sqlc.arg(keys)::text[])
GROUP BY k.key;

-- name: QuotaPermitReplay :one
SELECT * FROM harvest.quota_permits WHERE attempt_id = $1 AND operation_id = $2;

-- name: CountAttemptPermits :one
SELECT count(*) FROM harvest.quota_permits WHERE attempt_id = $1;

-- name: DebitQuota :exec
UPDATE harvest.quota_buckets b SET balance = (sqlc.arg(balances)::bigint[])[d.ord], refilled_at = sqlc.arg(now_at)::timestamptz,
    issued = issued + 1
FROM unnest(sqlc.arg(keys)::text[]) WITH ORDINALITY AS d(key, ord)
WHERE b.key = d.key;

-- name: ResetQuotaMetrics :exec
UPDATE harvest.quota_buckets SET metrics_from = sqlc.arg(window_start)::timestamptz,
    issued = 0, deferred = 0, succeeded = 0, refused = 0, failed = 0, body_bytes = 0,
    latency_le_25 = 0, latency_le_100 = 0, latency_le_1000 = 0
WHERE key = ANY(sqlc.arg(keys)::text[]);

-- name: CoolingQuotaKeys :many
SELECT key FROM harvest.quota_buckets
WHERE key = ANY(sqlc.arg(keys)::text[]) AND cooldown_until > statement_timestamp();

-- name: DeferQuota :exec
UPDATE harvest.quota_buckets SET deferred = deferred + 1 WHERE key = ANY(sqlc.arg(keys)::text[]);

-- name: InsertQuotaPermit :exec
INSERT INTO harvest.quota_permits (id, attempt_id, operation_id, request_hash, bucket_keys, account_key, max_bytes, expires_at, web_hop, previous_permit)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, sqlc.narg(web_hop), sqlc.narg(previous_permit));

-- name: WebRedirectHop :one
SELECT (parent.web_hop + 1)::smallint AS hop FROM harvest.quota_permits parent
WHERE parent.id = sqlc.arg(parent_id) AND parent.attempt_id = sqlc.arg(attempt_id)
    AND parent.finished_at IS NOT NULL AND parent.redirect_digest = sqlc.arg(digest)
    AND parent.web_hop < 3
    AND NOT EXISTS (SELECT 1 FROM harvest.quota_permits child WHERE child.previous_permit = parent.id);

-- name: LockQuotaPermitForFinish :one
SELECT p.* FROM harvest.quota_permits p
JOIN harvest.attempts a ON a.id = p.attempt_id
JOIN harvest.workers w ON w.id = a.worker_id
WHERE p.id = sqlc.arg(permit_id) AND w.id = sqlc.arg(worker_id) AND w.principal = sqlc.arg(principal)
  AND a.id = sqlc.arg(attempt_id) AND a.job_id = sqlc.arg(job_id) AND a.generation = sqlc.arg(generation)
FOR UPDATE OF p;

-- name: FinishQuotaPermit :exec
UPDATE harvest.quota_permits SET finished_at = sqlc.arg(now_at)::timestamptz, finish_hash = $2, redirect_digest = $3 WHERE id = $1;

-- name: RecordQuotaOutcome :exec
UPDATE harvest.quota_buckets SET succeeded = succeeded + sqlc.arg(success_count)::bigint,
    refused = refused + sqlc.arg(refusal_count)::bigint, failed = failed + sqlc.arg(failure_count)::bigint,
    body_bytes = body_bytes + sqlc.arg(used_bytes)::bigint,
    latency_le_25 = latency_le_25 + (sqlc.arg(latency_ms)::bigint <= 25)::int,
    latency_le_100 = latency_le_100 + (sqlc.arg(latency_ms)::bigint <= 100)::int,
    latency_le_1000 = latency_le_1000 + (sqlc.arg(latency_ms)::bigint <= 1000)::int,
    cooldown_until = CASE WHEN key = sqlc.arg(account_key)::text
        THEN greatest(cooldown_until, sqlc.arg(cooldown)::timestamptz) ELSE cooldown_until END
WHERE key = ANY(sqlc.arg(keys)::text[]);

-- name: PruneQuotaPermits :exec
DELETE FROM harvest.quota_permits WHERE id IN (
    SELECT id FROM harvest.quota_permits WHERE expires_at <= statement_timestamp() - interval '6 minutes'
    ORDER BY expires_at, id LIMIT 256 FOR UPDATE SKIP LOCKED
);

-- name: PruneQuotaBuckets :exec
DELETE FROM harvest.quota_buckets WHERE key IN (
    SELECT key FROM harvest.quota_buckets
    WHERE NOT (key = ANY(sqlc.arg(configured)::text[]))
      AND refilled_at < statement_timestamp() - interval '30 days'
      AND cooldown_until <= statement_timestamp()
      AND NOT EXISTS (SELECT 1 FROM harvest.quota_permits p WHERE p.bucket_keys @> ARRAY[quota_buckets.key])
    ORDER BY key LIMIT 32 FOR UPDATE SKIP LOCKED
);

-- name: QuotaSummary :many
SELECT key, metrics_from, issued, deferred, succeeded, refused, failed, body_bytes,
    latency_le_25, latency_le_100, latency_le_1000, cooldown_until > statement_timestamp() AS cooling_down
FROM harvest.quota_buckets WHERE key = ANY(sqlc.arg(keys)::text[])
    AND metrics_from = date_trunc('day', statement_timestamp(), 'UTC') ORDER BY key LIMIT 128;

-- name: QuotaPermitCount :one
SELECT count(*) FROM harvest.quota_permits WHERE bucket_keys && sqlc.arg(keys)::text[];
