package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRuntimeAndPolicyInputsAreSeparate(t *testing.T) {
	for _, suffix := range []string{"", ".proxy"} {
		runtime := filepath.Join("..", "..", "config"+suffix+".example.yaml")
		policy := filepath.Join("..", "..", "config.policy"+suffix+".example.yaml")
		if _, err := Load(runtime); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadPolicy(policy); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(policy); err == nil {
			t.Fatal("server accepted policy initialization input")
		}
		if _, err := LoadPolicy(runtime); err == nil {
			t.Fatal("policy command accepted runtime input")
		}
	}
	path := filepath.Join(t.TempDir(), "runtime.yaml")
	for _, extra := range []string{"network:\n  global_bucket: unused", "d4: {}", "development: {}", "steam_catalog_api_key: unused", "database_migration_url: unused"} {
		if err := os.WriteFile(path, []byte(validConfig+extra+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err == nil {
			t.Fatal("runtime accepted non-runtime configuration")
		}
	}
}
