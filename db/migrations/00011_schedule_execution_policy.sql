-- +goose Up
ALTER TABLE harvest.schedules
    ADD COLUMN execution_policy jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(execution_policy) = 'object' AND octet_length(execution_policy::text) <= 512);
ALTER TABLE harvest.jobs
    ADD COLUMN execution_policy jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(execution_policy) = 'object' AND octet_length(execution_policy::text) <= 512);
ALTER TABLE harvest.attempts ADD COLUMN egress_region text;
ALTER TABLE harvest.observations ADD COLUMN execution_egress_region text;
DROP INDEX harvest.jobs_active_manual_idx;
CREATE UNIQUE INDEX jobs_active_manual_idx ON harvest.jobs (fingerprint, schedule_id, schedule_revision) NULLS NOT DISTINCT
    WHERE cause = 'manual' AND state IN ('queued', 'running', 'retry_wait', 'cancel_requested');

-- +goose Down
DROP INDEX harvest.jobs_active_manual_idx;
CREATE UNIQUE INDEX jobs_active_manual_idx ON harvest.jobs (fingerprint, schedule_id) NULLS NOT DISTINCT
    WHERE cause = 'manual' AND state IN ('queued', 'running', 'retry_wait', 'cancel_requested');
ALTER TABLE harvest.observations DROP COLUMN execution_egress_region;
ALTER TABLE harvest.attempts DROP COLUMN egress_region;
ALTER TABLE harvest.jobs DROP COLUMN execution_policy;
ALTER TABLE harvest.schedules DROP COLUMN execution_policy;
