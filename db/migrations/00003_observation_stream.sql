-- +goose Up
CREATE TABLE harvest.change_heads (
    stream text PRIMARY KEY,
    epoch uuid NOT NULL,
    last_position bigint NOT NULL DEFAULT 0,
    min_position bigint NOT NULL DEFAULT 1,
    event_bytes bigint NOT NULL DEFAULT 0,
    total_event_bytes bigint NOT NULL DEFAULT 0,
    history_rows bigint NOT NULL DEFAULT 0,
    history_bytes bigint NOT NULL DEFAULT 0,
    last_pruned_at timestamptz NOT NULL DEFAULT 'epoch',
    history_scan_at timestamptz,
    history_scan_id uuid,
    CHECK (min_position > 0 AND min_position <= last_position + 1),
    CHECK (event_bytes >= 0 AND total_event_bytes >= event_bytes AND history_rows >= 0 AND history_bytes >= 0)
);

CREATE TABLE harvest.observations (
    id uuid PRIMARY KEY,
    stream text NOT NULL REFERENCES harvest.change_heads(stream),
    target_id uuid NOT NULL,
    capability text NOT NULL,
    output_schema integer NOT NULL,
    scope_key text NOT NULL,
    scope jsonb NOT NULL,
    state text NOT NULL CHECK (state IN ('present','confirmed_empty','unavailable','invalid','not_covered')),
    observed_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    data jsonb,
    content_hash bytea,
    source text NOT NULL,
    evidence text NOT NULL,
    sample boolean NOT NULL,
    job_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    stored_bytes integer NOT NULL CHECK (stored_bytes > 0),
    CHECK ((state IN ('present','confirmed_empty')) = (data IS NOT NULL AND content_hash IS NOT NULL))
);
CREATE INDEX observations_history ON harvest.observations (stream, received_at DESC, id DESC);
CREATE INDEX observations_entity_history ON harvest.observations (stream, target_id, capability, output_schema, scope_key, received_at DESC, id DESC);
CREATE INDEX observations_capability_history ON harvest.observations (stream, capability, received_at DESC, id DESC);

CREATE TABLE harvest.current_results (
    stream text NOT NULL,
    target_id uuid NOT NULL,
    capability text NOT NULL,
    output_schema integer NOT NULL,
    scope_key text NOT NULL,
    observation_id uuid NOT NULL REFERENCES harvest.observations(id),
    content_hash bytea NOT NULL,
    position bigint NOT NULL CHECK (position > 0),
    confirmed_at timestamptz NOT NULL,
    PRIMARY KEY (stream,target_id,capability,output_schema,scope_key)
);
CREATE UNIQUE INDEX current_observation ON harvest.current_results (observation_id);

CREATE TABLE harvest.change_events (
    stream text NOT NULL REFERENCES harvest.change_heads(stream),
    position bigint NOT NULL CHECK (position > 0),
    id uuid NOT NULL UNIQUE,
    action text NOT NULL CHECK (action IN ('upsert','withdraw','sample')),
    target_id uuid NOT NULL,
    capability text NOT NULL,
    output_schema integer NOT NULL,
    scope_key text NOT NULL,
    tombstone_scope jsonb,
    observation_id uuid REFERENCES harvest.observations(id),
    committed_at timestamptz NOT NULL,
    stored_bytes integer NOT NULL CHECK (stored_bytes > 0),
    PRIMARY KEY (stream,position),
    CHECK ((action = 'withdraw' AND observation_id IS NULL AND tombstone_scope IS NOT NULL)
        OR (action <> 'withdraw' AND observation_id IS NOT NULL AND tombstone_scope IS NULL))
);
CREATE INDEX change_observation ON harvest.change_events (observation_id) WHERE observation_id IS NOT NULL;

CREATE TABLE harvest.consumers (
    id uuid PRIMARY KEY,
    stream text NOT NULL REFERENCES harvest.change_heads(stream),
    name text NOT NULL,
    principal text NOT NULL,
    epoch uuid NOT NULL,
    policy bytea NOT NULL,
    ack_position bigint NOT NULL DEFAULT 0 CHECK (ack_position >= 0),
    last_activity timestamptz NOT NULL DEFAULT clock_timestamp(),
    reset_required boolean NOT NULL DEFAULT true,
    reset_reason text NOT NULL DEFAULT 'initial_snapshot',
    reset_id uuid,
    reset_base bigint NOT NULL DEFAULT 0,
    reset_bytes bigint NOT NULL DEFAULT 0,
    reset_deadline timestamptz,
    UNIQUE (stream,name),
    CHECK ((reset_id IS NULL) = (reset_deadline IS NULL)),
    CHECK (reset_required = (reset_reason <> ''))
);

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'Harvest observations and change cursors require backup/restore for rollback'; END $$;
-- +goose StatementEnd
