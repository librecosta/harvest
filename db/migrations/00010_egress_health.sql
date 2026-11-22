-- +goose Up
ALTER TABLE harvest.workers ADD COLUMN health_sequence bigint NOT NULL DEFAULT 0 CHECK (health_sequence >= 0);

CREATE TABLE harvest.worker_egress_health (
    node_id text NOT NULL REFERENCES harvest.worker_nodes (node_id),
    profile text NOT NULL,
    path_id text NOT NULL,
    kind text NOT NULL,
    region text NOT NULL,
    check_type text NOT NULL CHECK (check_type IN ('proxy_connection', 'transport', 'google_doh', 'steam')),
    minute_at timestamptz NOT NULL,
    last_at timestamptz NOT NULL,
    last_outcome text NOT NULL CHECK (last_outcome IN ('ok', 'transport_failure', 'upstream_failure')),
    successes bigint NOT NULL CHECK (successes >= 0),
    failures bigint NOT NULL CHECK (failures >= 0),
    transport_failures bigint NOT NULL CHECK (transport_failures BETWEEN 0 AND failures),
    latency_ms bigint NOT NULL CHECK (latency_ms >= 0),
    last_latency_ms integer NOT NULL CHECK (last_latency_ms BETWEEN 0 AND 5000),
    consecutive_failures bigint NOT NULL CHECK (consecutive_failures >= 0),
    recoveries bigint NOT NULL CHECK (recoveries >= 0),
    PRIMARY KEY (node_id, profile, path_id, kind, region, check_type, minute_at)
);
CREATE INDEX worker_egress_health_retention_idx ON harvest.worker_egress_health (minute_at);

CREATE TABLE harvest.egress_policy_actions (
    request_id uuid PRIMARY KEY,
    principal text NOT NULL,
    actor_id uuid NOT NULL,
    actor_name text NOT NULL,
    actor_role text NOT NULL CHECK (actor_role IN ('owner', 'operator')),
    namespace text NOT NULL,
    capability text NOT NULL,
    revision bigint NOT NULL CHECK (revision > 1),
    old_profile text NOT NULL,
    old_region text NOT NULL,
    profile text NOT NULL,
    region text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'egress health migration is forward only';
END $$;
-- +goose StatementEnd
