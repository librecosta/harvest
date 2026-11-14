-- +goose Up
ALTER TABLE harvest.attempts ADD COLUMN result_observation_ids uuid[] NOT NULL DEFAULT '{}'
 CHECK (cardinality(result_observation_ids) <= 3);
