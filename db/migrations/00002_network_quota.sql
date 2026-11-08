-- +goose Up
ALTER TABLE harvest.workers ADD COLUMN network_paths jsonb NOT NULL DEFAULT '[]';

CREATE TABLE harvest.quota_buckets (
    key text PRIMARY KEY,
    balance bigint NOT NULL CHECK (balance >= 0),
    refilled_at timestamptz NOT NULL,
    cooldown_until timestamptz NOT NULL DEFAULT 'epoch',
    metrics_from timestamptz NOT NULL DEFAULT 'epoch',
    issued bigint NOT NULL DEFAULT 0,
    deferred bigint NOT NULL DEFAULT 0,
    succeeded bigint NOT NULL DEFAULT 0,
    refused bigint NOT NULL DEFAULT 0,
    failed bigint NOT NULL DEFAULT 0,
    body_bytes bigint NOT NULL DEFAULT 0,
    latency_le_25 bigint NOT NULL DEFAULT 0,
    latency_le_100 bigint NOT NULL DEFAULT 0,
    latency_le_1000 bigint NOT NULL DEFAULT 0
);

CREATE TABLE harvest.quota_permits (
    id uuid PRIMARY KEY,
    attempt_id uuid NOT NULL REFERENCES harvest.attempts(id) ON DELETE CASCADE,
    operation_id uuid NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    bucket_keys text[] NOT NULL CHECK (cardinality(bucket_keys) = 4),
    account_key text NOT NULL,
    max_bytes bigint NOT NULL CHECK (max_bytes > 0),
    expires_at timestamptz NOT NULL,
    finished_at timestamptz,
    finish_hash bytea CHECK (octet_length(finish_hash) = 32),
    CHECK ((finished_at IS NULL) = (finish_hash IS NULL)),
    UNIQUE (attempt_id, operation_id)
);

CREATE INDEX quota_permits_expiry_idx ON harvest.quota_permits (expires_at, id);
CREATE INDEX quota_permits_active_buckets_idx ON harvest.quota_permits USING gin (bucket_keys)
    WHERE finished_at IS NULL;

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Harvest A2 downgrade requires backup and restore';
END $$;
-- +goose StatementEnd
