-- +goose Up
ALTER TABLE harvest.steam_evidence DROP CONSTRAINT steam_evidence_capability_check,
    DROP CONSTRAINT steam_evidence_store_country_check,
    ADD CONSTRAINT steam_evidence_market_check CHECK (
        (capability = 'steam.details' AND store_country IN ('CN', 'US', 'HK'))
        OR (capability = 'steam.events' AND store_country = '')
    );

-- +goose Down
ALTER TABLE harvest.steam_evidence DROP CONSTRAINT steam_evidence_market_check,
    ADD CONSTRAINT steam_evidence_capability_check CHECK (capability = 'steam.details'),
    ADD CONSTRAINT steam_evidence_store_country_check CHECK (store_country IN ('CN', 'US', 'HK'));
