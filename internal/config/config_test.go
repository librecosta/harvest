package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validConfig = `database:
  host: "localhost"
  name: "harvest"
  user: "harvest"
  password: "fixture-secret:@"
`

func TestExplicitYAML(t *testing.T) {
	t.Setenv("HARVEST_HTTP_LISTEN", "0.0.0.0:9876")
	t.Setenv("HARVEST_DATABASE_PASSWORD", "wrong")
	path := filepath.Join(t.TempDir(), "explicit.yml")
	if err := os.WriteFile(path, []byte(validConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.HTTP.Listen != "127.0.0.1:8080" || got.Database.Password != "fixture-secret:@" ||
		got.Database.TLSMode != "verify-full" || got.Database.Port != 5432 ||
		got.Database.MaxConns != 4 {
		t.Fatal("explicit config and defaults were not preserved")
	}
	for _, badPath := range []string{"", path + ".json", path + ".yaml"} {
		if _, err := Load(badPath); err == nil {
			t.Errorf("accepted invalid config path %q", badPath)
		}
	}
}

func TestRejectInvalidYAML(t *testing.T) {
	for name, content := range map[string]string{
		"unknown top-level":   validConfig + "worker: {}\n",
		"unknown nested":      validConfig + "  fixture_secret: forbidden\n",
		"unknown null":        validConfig + "  ignored: null\n",
		"duplicate":           validConfig + "  user: duplicate\n",
		"multiple documents":  validConfig + "---\n{}\n",
		"alias":               validConfig + "  other: *unset\n",
		"case folding":        validConfig + "HTTP: {}\n",
		"dotted key":          validConfig + "http.listen: '0.0.0.0:8080'\n",
		"wrong port type":     validConfig + "  port: '5432'\n",
		"wrong password type": strings.Replace(validConfig, `"fixture-secret:@"`, "1234", 1),
		"wrong map type":      validConfig + "http: wrong\n",
		"wrong listen":        validConfig + "http: {listen: ':8080'}\n",
		"ambient tls":         validConfig + "  tls_mode: prefer\n",
		"multiple hosts":      strings.Replace(validConfig, "localhost", "localhost,other", 1),
		"missing password":    strings.Split(validConfig, "  password:")[0],
		"malformed secret":    validConfig + "  [fixture-secret:@\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); err == nil || strings.Contains(err.Error(), "fixture-secret") {
				t.Fatal("expected a sanitized configuration rejection")
			}
		})
	}
}

func TestProbeAddress(t *testing.T) {
	for listen, want := range map[string]string{
		"127.0.0.1:8080": "127.0.0.1:8080",
		"0.0.0.0:8080":   "127.0.0.1:8080",
		"[::]:8080":      "[::1]:8080",
		"[::1]:8080":     "[::1]:8080",
	} {
		if got := (HTTP{Listen: listen}).ProbeAddress(); got != want {
			t.Errorf("probe address for %s = %s, want %s", listen, got, want)
		}
	}
}
