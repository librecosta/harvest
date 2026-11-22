-- name: ExistingSteamEvidenceUpload :one
SELECT payload_sha256 = sqlc.arg(sha256)::bytea AND payload = sqlc.arg(body)::bytea AS identical
FROM harvest.steam_evidence_staging WHERE attempt_id = $1;

-- name: StageSteamEvidence :exec
INSERT INTO harvest.steam_evidence_staging (attempt_id, payload_sha256, payload, payload_bytes, observed_at)
VALUES ($1, $2, $3, octet_length($3::bytea), $4);

-- name: SteamEvidenceCandidates :many
WITH due AS MATERIALIZED (
    SELECT attempt_id FROM harvest.steam_evidence_staging
    WHERE next_check_at <= statement_timestamp()
    ORDER BY next_check_at, attempt_id LIMIT 4
)
SELECT a.id AS attempt_id, a.job_id, a.generation, j.namespace
FROM due JOIN harvest.attempts a ON a.id = due.attempt_id JOIN harvest.jobs j ON j.id = a.job_id;

-- name: SteamEvidenceStagedAt :one
SELECT observed_at, payload_bytes, capacity_limited FROM harvest.steam_evidence_staging WHERE attempt_id = $1 FOR UPDATE;

-- name: DelaySteamEvidence :exec
UPDATE harvest.steam_evidence_staging SET next_check_at = statement_timestamp() + interval '15 seconds'
WHERE attempt_id = $1;

-- name: DeleteStagedSteamEvidence :exec
DELETE FROM harvest.steam_evidence_staging WHERE attempt_id = $1;

-- name: ExpireStagedSteamEvidence :exec
DELETE FROM harvest.steam_evidence_staging WHERE attempt_id IN (
    SELECT attempt_id FROM harvest.steam_evidence_staging
    WHERE uploaded_at < statement_timestamp() - interval '24 hours'
    ORDER BY uploaded_at, attempt_id LIMIT 16 FOR UPDATE SKIP LOCKED
);

-- name: ConfirmSteamEvidence :one
UPDATE harvest.steam_evidence e
SET first_observed_at = least(e.first_observed_at, s.observed_at),
    first_job_id = CASE WHEN (s.observed_at, s.attempt_id) < (e.first_observed_at, e.first_attempt_id) THEN a.job_id ELSE e.first_job_id END,
    first_attempt_id = CASE WHEN (s.observed_at, s.attempt_id) < (e.first_observed_at, e.first_attempt_id) THEN s.attempt_id ELSE e.first_attempt_id END,
    last_confirmed_at = greatest(e.last_confirmed_at, s.observed_at),
    last_server_confirmed_at = greatest(e.last_server_confirmed_at, sqlc.arg(confirmed_at)::timestamptz),
    last_server_attempt_id = CASE WHEN (sqlc.arg(confirmed_at)::timestamptz, s.attempt_id) >
      (e.last_server_confirmed_at, e.last_server_attempt_id) THEN s.attempt_id ELSE e.last_server_attempt_id END
FROM harvest.steam_evidence_staging s JOIN harvest.attempts a ON a.id = s.attempt_id
WHERE s.attempt_id = sqlc.arg(attempt_id) AND e.payload_sha256 = s.payload_sha256
    AND e.namespace = sqlc.arg(namespace) AND e.target_id = sqlc.arg(target_id)
    AND e.capability = 'steam.details' AND e.locale = sqlc.arg(locale)
    AND e.store_country = sqlc.arg(store_country) AND e.egress_region = sqlc.arg(egress_region)
    AND e.payload = s.payload
RETURNING e.id;

-- name: ArchiveSteamEvidence :one
INSERT INTO harvest.steam_evidence (
    id, namespace, target_id, capability, locale, store_country, egress_region, payload_sha256,
    payload, payload_bytes, first_observed_at, last_confirmed_at, first_job_id, first_attempt_id, format_version,
    last_server_confirmed_at, last_server_attempt_id
)
SELECT sqlc.arg(id), sqlc.arg(namespace), sqlc.arg(target_id), 'steam.details',
    sqlc.arg(locale), sqlc.arg(store_country), sqlc.arg(egress_region), s.payload_sha256, s.payload,
    s.payload_bytes, s.observed_at, s.observed_at, sqlc.arg(job_id), s.attempt_id, 1, sqlc.arg(confirmed_at), s.attempt_id
FROM harvest.steam_evidence_staging s
WHERE s.attempt_id = sqlc.arg(attempt_id)
RETURNING id;

-- name: AttributeSteamEvidence :exec
INSERT INTO harvest.steam_evidence_attribution (attempt_id, job_id, evidence_id, observed_at, confirmed_at)
VALUES ($1, $2, $3, $4, $5);

-- name: LockSteamEvidenceMaintenance :one
SELECT * FROM harvest.steam_evidence_maintenance WHERE singleton FOR UPDATE;

-- name: LockSteamEvidenceTracking :exec
SELECT pg_advisory_xact_lock(170419, 319);

-- name: LockSteamEvidenceTrackingShared :exec
SELECT pg_advisory_xact_lock_shared(170419, 319);

-- name: SteamEvidenceScopeVersions :many
SELECT id, payload_bytes, last_server_confirmed_at, last_server_attempt_id
FROM harvest.steam_evidence
WHERE namespace = $1 AND target_id = $2 AND capability = 'steam.details'
    AND locale = $3 AND store_country = $4 AND egress_region = $5
ORDER BY last_server_confirmed_at DESC, last_server_attempt_id DESC LIMIT 5;

-- name: SteamEvidenceBudget :one
SELECT coalesce(sum(logical_bytes) FILTER (WHERE namespace = sqlc.arg(namespace)::text), 0)::bigint AS namespace_bytes,
    coalesce(sum(logical_bytes), 0)::bigint AS global_bytes,
    coalesce(max(logical_bytes), 0)::bigint AS largest_namespace_bytes
FROM harvest.steam_evidence_usage;

-- name: DeleteSteamEvidenceVersions :exec
DELETE FROM harvest.steam_evidence WHERE id = ANY(sqlc.arg(ids)::uuid[]);

-- name: MarkSteamEvidenceCapacityLimited :exec
UPDATE harvest.steam_evidence_staging SET capacity_limited = true,
    next_check_at = statement_timestamp() + interval '60 seconds'
WHERE attempt_id = $1;

-- name: CountSteamEvidenceCapacityRejection :exec
INSERT INTO harvest.steam_evidence_usage (namespace, capacity_limited_attempts) VALUES ($1, 1)
ON CONFLICT (namespace) DO UPDATE SET capacity_limited_attempts = steam_evidence_usage.capacity_limited_attempts + 1;

-- name: SteamEvidenceExpiryCandidates :many
SELECT e.id, e.namespace, e.target_id, e.locale, e.store_country, e.egress_region,
    e.last_server_confirmed_at, NOT EXISTS (
        SELECT 1 FROM harvest.steam_evidence newer
        WHERE newer.namespace = e.namespace AND newer.target_id = e.target_id AND newer.capability = e.capability
            AND newer.locale = e.locale AND newer.store_country = e.store_country AND newer.egress_region = e.egress_region
            AND (newer.last_server_confirmed_at, newer.last_server_attempt_id) > (e.last_server_confirmed_at, e.last_server_attempt_id)
    ) AS latest
FROM harvest.steam_evidence e
WHERE e.last_server_confirmed_at < sqlc.arg(cutoff)::timestamptz
    AND (e.last_server_confirmed_at, e.id) > (sqlc.arg(scan_at)::timestamptz, sqlc.arg(scan_id)::uuid)
ORDER BY e.last_server_confirmed_at, e.id LIMIT sqlc.arg(batch_size)::integer;

-- name: TrackedSteamEvidenceIDs :many
SELECT (c.value->>'id')::uuid AS id FROM jsonb_array_elements(sqlc.arg(candidates)::jsonb) c
WHERE EXISTS (
    SELECT 1 FROM harvest.schedules s JOIN harvest.targets t ON t.id = s.target_id AND t.namespace = s.namespace
    WHERE s.enabled AND s.capability = 'steam.details' AND s.request_schema = 1 AND s.execution_key = sqlc.arg(execution_key)::text
      AND s.target_id = (c.value->>'target_id')::uuid AND s.namespace = c.value->>'namespace'
      AND t.state = 'active' AND t.provider = 'steam' AND t.resource_type = 'app'
      AND s.scope->>'locale' = c.value->>'locale' AND upper(s.scope->>'region') = c.value->>'store_country'
      AND (c.value->'allowed_modes') ? coalesce(s.execution_policy->>'mode', '')
      AND CASE WHEN coalesce(jsonb_array_length(s.execution_policy->'regions'), 0) > 0
          THEN (s.execution_policy->'regions') ? (c.value->>'egress_region')
          ELSE coalesce(nullif(s.scope->>'egress_region', ''), c.value->>'inherited_region') = c.value->>'egress_region'
      END
);

-- name: AdvanceSteamEvidenceVersionCursor :exec
UPDATE harvest.steam_evidence_maintenance SET version_scan_at = $1, version_scan_id = $2 WHERE singleton;

-- name: SteamEvidenceAttributionExpiryCandidates :many
SELECT a.attempt_id, a.confirmed_at, (a.attempt_id = e.first_attempt_id OR a.attempt_id = e.last_server_attempt_id) AS pinned
FROM harvest.steam_evidence_attribution a JOIN harvest.steam_evidence e ON e.id = a.evidence_id
WHERE a.confirmed_at < sqlc.arg(cutoff)::timestamptz
    AND (a.confirmed_at, a.attempt_id) > (sqlc.arg(scan_at)::timestamptz, sqlc.arg(scan_id)::uuid)
ORDER BY a.confirmed_at, a.attempt_id LIMIT sqlc.arg(batch_size)::integer;

-- name: DeleteSteamEvidenceAttributions :exec
DELETE FROM harvest.steam_evidence_attribution WHERE attempt_id = ANY(sqlc.arg(ids)::uuid[]);

-- name: AdvanceSteamEvidenceAttributionCursor :exec
UPDATE harvest.steam_evidence_maintenance SET attribution_scan_at = $1, attribution_scan_id = $2 WHERE singleton;

-- name: ListSteamEvidence :many
WITH page AS MATERIALIZED (
    SELECT e.id, e.namespace, e.target_id, e.capability, e.locale, e.store_country, e.egress_region, e.payload_sha256,
        e.payload_bytes, e.first_observed_at, e.last_confirmed_at, e.last_server_confirmed_at, e.archived_at,
        e.first_job_id, e.first_attempt_id, e.format_version
    FROM harvest.steam_evidence e
    WHERE e.namespace = sqlc.arg(namespace)::text AND e.id > sqlc.arg(after_id)::uuid
        AND (sqlc.narg(evidence_id)::uuid IS NULL OR e.id = sqlc.narg(evidence_id)::uuid)
        AND (sqlc.narg(target_id)::uuid IS NULL OR e.target_id = sqlc.narg(target_id)::uuid)
        AND (sqlc.arg(app_id)::text = '' OR e.target_id = (
            SELECT t.id FROM harvest.targets t WHERE t.provider = 'steam' AND t.resource_type = 'app'
                AND t.namespace = sqlc.arg(namespace)::text AND t.external_key = sqlc.arg(app_id)::text
        ))
        AND (sqlc.arg(locale)::text = '' OR e.locale = sqlc.arg(locale)::text)
        AND (sqlc.arg(store_country)::text = '' OR e.store_country = sqlc.arg(store_country)::text)
        AND (sqlc.arg(egress_region)::text = '' OR e.egress_region = sqlc.arg(egress_region)::text)
    ORDER BY e.id LIMIT sqlc.arg(page_size)::integer
)
SELECT e.id, e.namespace, e.target_id, t.external_key AS app_id, e.capability, e.locale, e.store_country,
    e.egress_region, encode(e.payload_sha256, 'hex')::text AS sha256, e.payload_bytes,
    e.first_observed_at, e.last_confirmed_at AS last_observed_at, e.last_server_confirmed_at AS confirmed_at,
    e.archived_at, e.first_job_id, e.first_attempt_id, e.format_version
FROM page e JOIN harvest.targets t ON t.id = e.target_id AND t.namespace = e.namespace
ORDER BY e.id;

-- name: SteamEvidenceRaw :one
SELECT payload FROM harvest.steam_evidence WHERE id = $1
    AND namespace = ANY(sqlc.arg(namespaces)::text[]) AND capability = 'steam.details';

-- name: SteamEvidenceGlobalVisible :one
SELECT NOT EXISTS (
    SELECT 1 FROM harvest.steam_evidence_usage u WHERE NOT (u.namespace = ANY(sqlc.arg(namespaces)::text[]))
    UNION ALL
    SELECT 1 FROM harvest.steam_evidence_staging s
    JOIN harvest.attempts a ON a.id = s.attempt_id JOIN harvest.jobs j ON j.id = a.job_id
    WHERE NOT (j.namespace = ANY(sqlc.arg(namespaces)::text[]))
);

-- name: SteamEvidenceStorage :one
SELECT
    (SELECT coalesce(sum(u.logical_bytes), 0)::bigint FROM harvest.steam_evidence_usage u
        WHERE sqlc.arg(namespace)::text = '' OR u.namespace = sqlc.arg(namespace)::text) AS logical_bytes,
    (SELECT coalesce(sum(u.versions), 0)::bigint FROM harvest.steam_evidence_usage u
        WHERE sqlc.arg(namespace)::text = '' OR u.namespace = sqlc.arg(namespace)::text) AS versions,
    (SELECT coalesce(sum(u.capacity_limited_attempts), 0)::bigint FROM harvest.steam_evidence_usage u
        WHERE sqlc.arg(namespace)::text = '' OR u.namespace = sqlc.arg(namespace)::text) AS capacity_limited_attempts,
    (SELECT coalesce(sum(pg_column_size(e.payload)), 0)::bigint FROM harvest.steam_evidence e
        WHERE sqlc.arg(namespace)::text = '' OR e.namespace = sqlc.arg(namespace)::text) AS payload_column_bytes,
    (SELECT count(*) FROM harvest.steam_evidence_attribution a JOIN harvest.steam_evidence e ON e.id = a.evidence_id
        WHERE sqlc.arg(namespace)::text = '' OR e.namespace = sqlc.arg(namespace)::text) AS attributions,
    (SELECT count(*) FROM harvest.steam_evidence_staging s
        JOIN harvest.attempts a ON a.id = s.attempt_id JOIN harvest.jobs j ON j.id = a.job_id
        WHERE sqlc.arg(namespace)::text = '' OR j.namespace = sqlc.arg(namespace)::text) AS staging_rows,
    (SELECT coalesce(sum(s.payload_bytes), 0)::bigint FROM harvest.steam_evidence_staging s
        JOIN harvest.attempts a ON a.id = s.attempt_id JOIN harvest.jobs j ON j.id = a.job_id
        WHERE sqlc.arg(namespace)::text = '' OR j.namespace = sqlc.arg(namespace)::text) AS staging_bytes;
