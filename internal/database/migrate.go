package database

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/librecosta/harvest/db"
	"github.com/librecosta/harvest/internal/config"
)

func Migrate(ctx context.Context, cfg config.Database, action string, out io.Writer) error {
	pool, err := open(ctx, cfg, action == "status")
	if err != nil {
		return err
	}
	defer pool.Close()

	sqlDB := stdlib.OpenDBFromPool(pool)
	defer sqlDB.Close()
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return errors.New("cannot initialize migration lock")
	}
	const versionTable = "public.goose_db_version"
	provider, err := goose.NewProvider(goose.DialectPostgres, sqlDB, db.Migrations,
		goose.WithTableName(versionTable),
		goose.WithSessionLocker(locker), goose.WithSlog(slog.New(slog.DiscardHandler)))
	if errors.Is(err, goose.ErrNoMigrations) {
		_, err = fmt.Fprintln(out, "No Harvest migrations.")
		return err
	}
	if err != nil {
		return errors.New("cannot load embedded Goose migrations")
	}

	if action == "up" {
		applied, err := provider.Up(ctx)
		if err != nil {
			return fmt.Errorf("Goose up failed after applying %d migrations", len(applied))
		}
		_, err = fmt.Fprintf(out, "Applied %d Harvest migrations.\n", len(applied))
		return err
	}
	// Goose Status initializes metadata on an unmigrated database. Report
	// the bundled sources directly until an explicit up creates that table.
	var initialized bool
	err = pool.QueryRow(ctx, "SELECT pg_catalog.to_regclass($1) IS NOT NULL", versionTable).Scan(&initialized)
	if err != nil {
		return errors.New("cannot inspect Goose migration metadata")
	}
	if !initialized {
		for _, source := range provider.ListSources() {
			if _, err := fmt.Fprintf(out, "%d pending\n", source.Version); err != nil {
				return err
			}
		}
		return nil
	}
	statuses, err := provider.Status(ctx)
	if err != nil {
		return errors.New("cannot read Goose migration status")
	}
	for _, status := range statuses {
		if _, err := fmt.Fprintf(out, "%d %s\n", status.Source.Version, status.State); err != nil {
			return err
		}
	}
	return nil
}
