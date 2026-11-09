-- +goose Up
ALTER TABLE harvest.quota_permits
    ADD COLUMN rdap_stage smallint NOT NULL DEFAULT 0 CHECK (rdap_stage BETWEEN 0 AND 2),
    ADD COLUMN rdap_endpoint text NOT NULL DEFAULT '' CHECK (length(rdap_endpoint) <= 800),
    ADD CONSTRAINT rdap_endpoint_stage CHECK ((rdap_stage = 2) = (rdap_endpoint <> ''));
CREATE UNIQUE INDEX quota_permit_rdap_stage ON harvest.quota_permits (attempt_id, rdap_stage) WHERE rdap_stage <> 0;
CREATE INDEX jobs_robots_recent ON harvest.jobs (namespace, target_id, updated_at DESC)
    WHERE capability = 'web.robots' AND state NOT IN ('succeeded', 'queued');

-- +goose Down
DROP INDEX harvest.jobs_robots_recent;
DROP INDEX harvest.quota_permit_rdap_stage;
ALTER TABLE harvest.quota_permits
    DROP CONSTRAINT rdap_endpoint_stage,
    DROP COLUMN rdap_endpoint,
    DROP COLUMN rdap_stage;
