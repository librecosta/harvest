package database

import (
	"io/fs"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"

	"github.com/librecosta/harvest/db"
)

func TestSchemaVersionMatchesEmbeddedMigrations(t *testing.T) {
	migrations, err := fs.ReadDir(db.Migrations, ".")
	if err != nil {
		t.Fatal(err)
	}

	var latest int64
	for _, migration := range migrations {
		if migration.IsDir() || !strings.HasSuffix(migration.Name(), ".sql") {
			continue
		}
		version, err := goose.NumericComponent(migration.Name())
		if err != nil {
			t.Fatal(err)
		}
		latest = max(latest, version)
	}

	if latest != schemaVersion {
		t.Fatalf("startup expects schema version %d, latest embedded migration is %d", schemaVersion, latest)
	}
}
