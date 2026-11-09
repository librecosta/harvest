-- +goose Up
CREATE TABLE harvest.operational_policies (
    name text PRIMARY KEY CHECK (name = 'network'),
    revision bigint NOT NULL CHECK (revision > 0),
    routing_revision bigint NOT NULL CHECK (routing_revision > 0 AND routing_revision <= revision),
    policy jsonb NOT NULL CHECK (jsonb_typeof(policy) = 'object' AND octet_length(policy::text) <= 131072),
    bindings jsonb NOT NULL CHECK (jsonb_typeof(bindings) = 'object' AND octet_length(bindings::text) <= 524288),
    checksum bytea NOT NULL CHECK (octet_length(checksum) = 32),
    updated_at timestamptz NOT NULL,
    updated_by text NOT NULL
);

ALTER TABLE harvest.attempts ADD COLUMN network_revision bigint NOT NULL DEFAULT 0 CHECK (network_revision >= 0);

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'Harvest operational policy requires backup/restore for rollback'; END $$;
-- +goose StatementEnd
