package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrivateComposeDatabaseTransport(t *testing.T) {
	const fixture = `http:
  listen: 0.0.0.0:8080
database:
  host: librecosta-postgres
  port: 5432
  name: harvest
  user: harvest_runtime
  password: synthetic
  tls_mode: MODE
  max_connections: 4
`
	file := filepath.Join(t.TempDir(), "server.yaml")
	load := func(raw string) (Config, error) {
		t.Helper()
		if err := os.WriteFile(file, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		return Load(file)
	}
	for _, mode := range []string{"disable", "verify-full"} {
		cfg, err := load(strings.Replace(fixture, "MODE", mode, 1))
		if err != nil || cfg.Database.Host != "librecosta-postgres" || cfg.Database.Port != 5432 || cfg.Database.TLSMode != mode {
			t.Fatal("configured PostgreSQL transport rejected or changed", err)
		}
	}
	valid := strings.Replace(fixture, "MODE", "disable", 1)
	for _, bad := range []string{
		strings.Replace(valid, "5432", "65536", 1),
		strings.Replace(valid, "password: synthetic", "password: ''", 1),
		strings.Replace(valid, "tls_mode: disable", "tls_mode: prefer", 1),
	} {
		if _, err := load(bad); err == nil {
			t.Fatal("accepted invalid database configuration")
		}
	}
}
