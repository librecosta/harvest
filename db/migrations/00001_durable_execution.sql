-- +goose Up
CREATE SCHEMA harvest;

CREATE TABLE harvest.targets (
    id uuid PRIMARY KEY,
    provider text NOT NULL,
    resource_type text NOT NULL,
    external_key text NOT NULL,
    namespace text NOT NULL,
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'archived')),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (provider, resource_type, external_key, namespace)
);
CREATE INDEX targets_namespace_idx ON harvest.targets (namespace, id);

CREATE TABLE harvest.schedules (
    id uuid PRIMARY KEY,
    target_id uuid NOT NULL REFERENCES harvest.targets,
    namespace text NOT NULL,
    capability text NOT NULL,
    request_schema integer NOT NULL CHECK (request_schema > 0),
    execution_key text NOT NULL,
    params jsonb NOT NULL CHECK (jsonb_typeof(params) = 'object' AND octet_length(params::text) <= 16384),
    scope jsonb NOT NULL CHECK (jsonb_typeof(scope) = 'object'),
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint) = 32),
    interval_seconds integer NOT NULL CHECK (interval_seconds BETWEEN 0 AND 2592000),
    cron text NOT NULL,
    CHECK ((interval_seconds > 0) <> (cron <> '')),
    anchor_at timestamptz NOT NULL,
    next_due_at timestamptz NOT NULL CHECK (next_due_at >= anchor_at),
    policy text NOT NULL CHECK (policy IN ('state_refresh', 'point_in_time')),
    enabled boolean NOT NULL,
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    missed_count integer NOT NULL DEFAULT 0 CHECK (missed_count BETWEEN 0 AND 1000000),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (fingerprint, interval_seconds, cron, anchor_at, policy)
);
CREATE INDEX schedules_due_idx ON harvest.schedules (next_due_at, id) WHERE enabled;
CREATE INDEX schedules_namespace_idx ON harvest.schedules (namespace, id);

CREATE TABLE harvest.jobs (
    id uuid PRIMARY KEY,
    target_id uuid NOT NULL REFERENCES harvest.targets,
    namespace text NOT NULL,
    capability text NOT NULL,
    request_schema integer NOT NULL CHECK (request_schema > 0),
    execution_key text NOT NULL,
    params jsonb NOT NULL CHECK (jsonb_typeof(params) = 'object' AND octet_length(params::text) <= 16384),
    scope jsonb NOT NULL CHECK (jsonb_typeof(scope) = 'object'),
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint) = 32),
    cause text NOT NULL CHECK (cause IN ('scheduled', 'manual', 'retry_requested')),
    schedule_id uuid REFERENCES harvest.schedules,
    schedule_revision bigint,
    scheduled_for timestamptz,
    origin_job_id uuid REFERENCES harvest.jobs ON DELETE SET NULL,
    CHECK ((schedule_id IS NULL) = (schedule_revision IS NULL)),
    CHECK ((cause = 'scheduled') = (scheduled_for IS NOT NULL)),
    CHECK (cause <> 'scheduled' OR schedule_id IS NOT NULL),
    UNIQUE (schedule_id, schedule_revision, scheduled_for),
    priority smallint NOT NULL CHECK (priority IN (0, 1)),
    available_at timestamptz NOT NULL,
    deadline_at timestamptz NOT NULL,
    state text NOT NULL DEFAULT 'queued' CHECK (state IN
        ('queued', 'running', 'retry_wait', 'cancel_requested', 'succeeded', 'partial', 'failed', 'cancelled')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 5 AND attempt_count <= max_attempts),
    generation bigint NOT NULL DEFAULT 0 CHECK (generation = attempt_count),
    last_code text NOT NULL DEFAULT '',
    accepted_attempt_id uuid,
    accepted_hash bytea CHECK (accepted_hash IS NULL OR octet_length(accepted_hash) = 32),
    CHECK ((accepted_attempt_id IS NULL) = (accepted_hash IS NULL)),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE UNIQUE INDEX jobs_active_manual_idx ON harvest.jobs (fingerprint, schedule_id) NULLS NOT DISTINCT
    WHERE cause = 'manual' AND state IN ('queued', 'running', 'retry_wait', 'cancel_requested');
CREATE INDEX jobs_due_idx ON harvest.jobs (execution_key, namespace, available_at, id)
    WHERE state IN ('queued', 'retry_wait');
CREATE INDEX jobs_deadline_idx ON harvest.jobs (deadline_at, id) WHERE state IN ('queued', 'retry_wait');
CREATE INDEX jobs_namespace_idx ON harvest.jobs (namespace, id);
CREATE INDEX jobs_history_idx ON harvest.jobs (updated_at, id)
    WHERE state IN ('succeeded', 'partial', 'failed', 'cancelled');

CREATE TABLE harvest.workers (
    id uuid PRIMARY KEY,
    principal text NOT NULL,
    offers text[] NOT NULL CHECK (cardinality(offers) BETWEEN 1 AND 32),
    egress_profiles text[] NOT NULL,
    capacity integer NOT NULL CHECK (capacity BETWEEN 1 AND 16),
    last_seen timestamptz NOT NULL DEFAULT clock_timestamp(),
    draining boolean NOT NULL DEFAULT false
);
CREATE INDEX workers_seen_idx ON harvest.workers (last_seen, id);

CREATE TABLE harvest.attempts (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES harvest.jobs ON DELETE CASCADE,
    worker_id uuid NOT NULL REFERENCES harvest.workers,
    generation bigint NOT NULL CHECK (generation > 0),
    claimed_at timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL,
    lease_until timestamptz NOT NULL,
    ended_at timestamptz,
    state text NOT NULL DEFAULT 'running' CHECK (state IN ('running', 'succeeded', 'partial', 'failed', 'expired', 'cancelled')),
    error_code text NOT NULL DEFAULT '',
    result_hash bytea CHECK (result_hash IS NULL OR octet_length(result_hash) = 32),
    receipt_state text,
    CHECK ((result_hash IS NULL) = (receipt_state IS NULL)),
    CHECK ((state = 'running') = (ended_at IS NULL)),
    UNIQUE (job_id, generation)
);
CREATE UNIQUE INDEX attempts_running_job_idx ON harvest.attempts (job_id) WHERE state = 'running';
CREATE INDEX attempts_lease_idx ON harvest.attempts (lease_until, id) WHERE state = 'running';
CREATE INDEX attempts_worker_idx ON harvest.attempts (worker_id, lease_until);

-- Several callers' idempotency keys may resolve to the same coalesced manual Job.
CREATE TABLE harvest.submissions (
    principal text NOT NULL,
    key text NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    job_id uuid REFERENCES harvest.jobs ON DELETE CASCADE,
    PRIMARY KEY (principal, key)
);
CREATE INDEX submissions_job_idx ON harvest.submissions (job_id);

-- +goose Down
-- A1 creates durable execution history. Operators must use a reviewed backup/restore
-- procedure when rolling back the schema.
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'Harvest execution history requires backup/restore for rollback'; END $$;
-- +goose StatementEnd
