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
SELECT observed_at FROM harvest.steam_evidence_staging WHERE attempt_id = $1 FOR UPDATE;

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
    last_confirmed_at = greatest(e.last_confirmed_at, s.observed_at)
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
    payload, payload_bytes, first_observed_at, last_confirmed_at, first_job_id, first_attempt_id, format_version
)
SELECT sqlc.arg(id), sqlc.arg(namespace), sqlc.arg(target_id), 'steam.details',
    sqlc.arg(locale), sqlc.arg(store_country), sqlc.arg(egress_region), s.payload_sha256, s.payload,
    s.payload_bytes, s.observed_at, s.observed_at, sqlc.arg(job_id), s.attempt_id, 1
FROM harvest.steam_evidence_staging s
WHERE s.attempt_id = sqlc.arg(attempt_id) AND (
    SELECT count(*) FROM harvest.steam_evidence e
    WHERE e.namespace = sqlc.arg(namespace) AND e.target_id = sqlc.arg(target_id)
        AND e.capability = 'steam.details' AND e.locale = sqlc.arg(locale)
        AND e.store_country = sqlc.arg(store_country) AND e.egress_region = sqlc.arg(egress_region)
) < 5
RETURNING id;

-- name: AttributeSteamEvidence :exec
INSERT INTO harvest.steam_evidence_attribution (attempt_id, job_id, evidence_id, observed_at, confirmed_at)
VALUES ($1, $2, $3, $4, $5);
