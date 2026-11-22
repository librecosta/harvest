-- +goose Up
ALTER TABLE harvest.steam_evidence
    ADD COLUMN last_server_confirmed_at timestamptz,
    ADD COLUMN last_server_attempt_id uuid,
    ADD COLUMN archived_at timestamptz;

-- The receipt clocks are authoritative. A v12 archive had no archive timestamp.
UPDATE harvest.steam_evidence e
SET last_server_confirmed_at = a.confirmed_at, last_server_attempt_id = a.attempt_id
FROM (
    SELECT DISTINCT ON (evidence_id) evidence_id, confirmed_at, attempt_id
    FROM harvest.steam_evidence_attribution
    ORDER BY evidence_id, confirmed_at DESC, attempt_id DESC
) a
WHERE a.evidence_id = e.id;
ALTER TABLE harvest.steam_evidence
    ALTER COLUMN last_server_confirmed_at SET NOT NULL,
    ALTER COLUMN last_server_attempt_id SET NOT NULL,
    ALTER COLUMN archived_at SET DEFAULT clock_timestamp();

ALTER TABLE harvest.steam_evidence_attribution DROP CONSTRAINT steam_evidence_attribution_evidence_id_fkey;
ALTER TABLE harvest.steam_evidence_attribution ADD CONSTRAINT steam_evidence_attribution_evidence_id_fkey
    FOREIGN KEY (evidence_id) REFERENCES harvest.steam_evidence ON DELETE CASCADE;

ALTER TABLE harvest.steam_evidence_staging ADD COLUMN capacity_limited boolean NOT NULL DEFAULT false;

CREATE INDEX steam_evidence_scope_order_idx ON harvest.steam_evidence
    (namespace, target_id, capability, locale, store_country, egress_region, last_server_confirmed_at DESC, last_server_attempt_id DESC);
CREATE INDEX steam_evidence_expiry_idx ON harvest.steam_evidence (last_server_confirmed_at, id);
CREATE INDEX steam_evidence_namespace_page_idx ON harvest.steam_evidence (namespace, id);
CREATE INDEX steam_evidence_target_page_idx ON harvest.steam_evidence (target_id, id);
CREATE INDEX steam_evidence_attribution_expiry_idx ON harvest.steam_evidence_attribution (confirmed_at, attempt_id);
CREATE INDEX steam_evidence_tracking_schedule_idx ON harvest.schedules (target_id, namespace, execution_key)
    WHERE enabled AND capability = 'steam.details';

CREATE TABLE harvest.steam_evidence_maintenance (
    singleton boolean PRIMARY KEY CHECK (singleton),
    version_scan_at timestamptz NOT NULL DEFAULT 'epoch',
    version_scan_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    attribution_scan_at timestamptz NOT NULL DEFAULT 'epoch',
    attribution_scan_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000'
);
INSERT INTO harvest.steam_evidence_maintenance (singleton) VALUES (true);

CREATE TABLE harvest.steam_evidence_usage (
    namespace text PRIMARY KEY,
    logical_bytes bigint NOT NULL DEFAULT 0 CHECK (logical_bytes >= 0),
    versions bigint NOT NULL DEFAULT 0 CHECK (versions >= 0),
    capacity_limited_attempts bigint NOT NULL DEFAULT 0 CHECK (capacity_limited_attempts >= 0)
);
INSERT INTO harvest.steam_evidence_usage (namespace, logical_bytes, versions)
SELECT namespace, sum(payload_bytes), count(*) FROM harvest.steam_evidence GROUP BY namespace;

-- +goose StatementBegin
CREATE FUNCTION harvest.account_steam_evidence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO harvest.steam_evidence_usage (namespace, logical_bytes, versions)
        VALUES (NEW.namespace, NEW.payload_bytes, 1)
        ON CONFLICT (namespace) DO UPDATE SET
            logical_bytes = steam_evidence_usage.logical_bytes + EXCLUDED.logical_bytes,
            versions = steam_evidence_usage.versions + 1;
    ELSE
        UPDATE harvest.steam_evidence_usage
        SET logical_bytes = logical_bytes - OLD.payload_bytes, versions = versions - 1
        WHERE namespace = OLD.namespace;
        DELETE FROM harvest.steam_evidence_usage
        WHERE namespace = OLD.namespace AND versions = 0 AND capacity_limited_attempts = 0;
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER steam_evidence_accounting AFTER INSERT OR DELETE ON harvest.steam_evidence
FOR EACH ROW EXECUTE FUNCTION harvest.account_steam_evidence();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'Steam evidence retention requires backup/restore for rollback'; END $$;
-- +goose StatementEnd
