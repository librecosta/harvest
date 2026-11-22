-- +goose Up
CREATE TABLE harvest.steam_evidence_staging (
    attempt_id uuid PRIMARY KEY REFERENCES harvest.attempts ON DELETE CASCADE,
    payload_sha256 bytea NOT NULL CHECK (octet_length(payload_sha256) = 32),
    payload bytea NOT NULL,
    payload_bytes integer NOT NULL CHECK (payload_bytes BETWEEN 1 AND 524288 AND payload_bytes = octet_length(payload)),
    observed_at timestamptz NOT NULL,
    uploaded_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    next_check_at timestamptz NOT NULL DEFAULT statement_timestamp() + interval '2 seconds'
);
CREATE INDEX steam_evidence_staging_due_idx ON harvest.steam_evidence_staging (next_check_at, attempt_id);
CREATE INDEX steam_evidence_staging_age_idx ON harvest.steam_evidence_staging (uploaded_at, attempt_id);

CREATE TABLE harvest.steam_evidence (
    id uuid PRIMARY KEY,
    namespace text NOT NULL CHECK (length(namespace) BETWEEN 1 AND 64),
    target_id uuid NOT NULL REFERENCES harvest.targets,
    capability text NOT NULL CHECK (capability = 'steam.details'),
    locale text NOT NULL CHECK (locale IN ('en', 'zh-cn')),
    store_country text NOT NULL CHECK (store_country IN ('CN', 'US', 'HK')),
    egress_region text NOT NULL CHECK (egress_region ~ '^[a-z0-9][a-z0-9_-]{0,34}$'),
    payload_sha256 bytea NOT NULL CHECK (octet_length(payload_sha256) = 32),
    payload bytea NOT NULL,
    payload_bytes integer NOT NULL CHECK (payload_bytes BETWEEN 1 AND 524288 AND payload_bytes = octet_length(payload)),
    first_observed_at timestamptz NOT NULL,
    last_confirmed_at timestamptz NOT NULL CHECK (last_confirmed_at >= first_observed_at),
    first_job_id uuid NOT NULL,
    first_attempt_id uuid NOT NULL,
    format_version integer NOT NULL CHECK (format_version = 1),
    UNIQUE (namespace, target_id, capability, locale, store_country, egress_region, payload_sha256),
    UNIQUE (first_attempt_id)
);
CREATE INDEX steam_evidence_target_idx ON harvest.steam_evidence (target_id);

-- Job and Attempt lineage survives normal pruning of execution history.
CREATE TABLE harvest.steam_evidence_attribution (
    attempt_id uuid PRIMARY KEY,
    job_id uuid NOT NULL,
    evidence_id uuid NOT NULL REFERENCES harvest.steam_evidence,
    observed_at timestamptz NOT NULL,
    confirmed_at timestamptz NOT NULL
);
CREATE INDEX steam_evidence_attribution_version_idx ON harvest.steam_evidence_attribution (evidence_id, confirmed_at, attempt_id);

-- +goose Down
DROP TABLE harvest.steam_evidence_attribution;
DROP TABLE harvest.steam_evidence;
DROP TABLE harvest.steam_evidence_staging;
