-- +goose Up
ALTER TABLE harvest.attempts DROP CONSTRAINT attempts_result_observation_ids_check,
 ADD CONSTRAINT attempts_result_observation_ids_check CHECK (cardinality(result_observation_ids) <= 4);

-- +goose Down
-- Existing v3 receipts prevent lossless rollback.
ALTER TABLE harvest.attempts DROP CONSTRAINT attempts_result_observation_ids_check,
 ADD CONSTRAINT attempts_result_observation_ids_check CHECK (cardinality(result_observation_ids) <= 3);
