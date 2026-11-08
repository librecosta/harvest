package database

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

const schemaVersion = 1

func CheckSchema(ctx context.Context, pool *pgxpool.Pool) error {
	// Goose owns this table, so it is intentionally absent from sqlc's DDL.
	var version int64
	var applied, present bool
	err := pool.QueryRow(ctx, `SELECT version_id, is_applied,
		pg_catalog.to_regclass('harvest.targets') IS NOT NULL AND pg_catalog.to_regclass('harvest.schedules') IS NOT NULL
		AND pg_catalog.to_regclass('harvest.jobs') IS NOT NULL AND pg_catalog.to_regclass('harvest.attempts') IS NOT NULL
		AND pg_catalog.to_regclass('harvest.workers') IS NOT NULL AND pg_catalog.to_regclass('harvest.submissions') IS NOT NULL
		FROM public.goose_db_version ORDER BY id DESC LIMIT 1`).
		Scan(&version, &applied, &present)
	if err != nil || version != schemaVersion || !applied || !present {
		return errors.New("Harvest schema is unavailable or incompatible; check explicit Goose migration status")
	}
	return nil
}
