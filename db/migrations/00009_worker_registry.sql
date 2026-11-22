-- +goose Up
CREATE TABLE harvest.worker_nodes (
    node_id text PRIMARY KEY CHECK (node_id ~ '^[a-z0-9][a-z0-9_-]{0,63}$'),
    principal text NOT NULL,
    namespaces text[] NOT NULL,
    capabilities text[] NOT NULL,
    current_worker_id uuid NOT NULL UNIQUE,
    desired_state text NOT NULL DEFAULT 'active' CHECK (desired_state IN ('active', 'paused', 'draining')),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0)
);

ALTER TABLE harvest.workers ADD COLUMN node_id text REFERENCES harvest.worker_nodes (node_id);
ALTER TABLE harvest.worker_nodes ADD CONSTRAINT worker_nodes_current_worker_fk
    FOREIGN KEY (current_worker_id) REFERENCES harvest.workers (id) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE harvest.worker_node_actions (
    request_id uuid PRIMARY KEY,
    node_id text NOT NULL REFERENCES harvest.worker_nodes (node_id),
    principal text NOT NULL,
    actor_id uuid,
    actor_name text NOT NULL,
    actor_role text NOT NULL,
    previous_state text NOT NULL CHECK (previous_state IN ('active', 'paused', 'draining')),
    desired_state text NOT NULL CHECK (desired_state IN ('active', 'paused', 'draining')),
    revision bigint NOT NULL CHECK (revision > 1),
    decided_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (node_id, revision)
);

-- +goose Down
-- The registry contains operator decisions and cannot be removed safely.
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'worker registry migration is forward only';
END $$;
-- +goose StatementEnd
