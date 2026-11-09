-- +goose Up
ALTER TABLE harvest.quota_permits
    ADD COLUMN web_hop smallint CHECK (web_hop BETWEEN 0 AND 3),
    ADD COLUMN previous_permit uuid UNIQUE,
    ADD COLUMN redirect_digest text NOT NULL DEFAULT ''
        CHECK (redirect_digest = '' OR redirect_digest ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT web_redirect_hop CHECK (
        (previous_permit IS NOT NULL) = coalesce(web_hop > 0, false)
        AND (redirect_digest = '' OR web_hop IS NOT NULL)
    );

-- +goose Down
ALTER TABLE harvest.quota_permits
    DROP CONSTRAINT web_redirect_hop,
    DROP COLUMN redirect_digest,
    DROP COLUMN previous_permit,
    DROP COLUMN web_hop;
