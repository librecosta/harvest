-- +goose Up
CREATE INDEX jobs_schedule_history_idx ON harvest.jobs (schedule_id,id DESC) WHERE schedule_id IS NOT NULL;
CREATE INDEX jobs_schedule_failure_idx ON harvest.jobs (schedule_id,id DESC) WHERE schedule_id IS NOT NULL AND state IN ('failed','partial');

-- +goose Down
DROP INDEX harvest.jobs_schedule_failure_idx,harvest.jobs_schedule_history_idx;
