package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkerYAMLSteamVersions(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("..", "..", "config.worker.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	detailsV2 := `  - {capability: "steam.details", request_schema: 1, output_schema: 2, implementation: "steam_v2"}`
	body := strings.Replace(string(source), "  # - {capability:", "  - {capability:", 1)
	path := filepath.Join(t.TempDir(), "worker.yaml")
	for _, input := range []struct {
		body  string
		valid bool
	}{
		{string(source), true},
		{body, true},
		{strings.Replace(body, detailsV2, detailsV2+"\n"+detailsV2, 1), false},
	} {
		if err := os.WriteFile(path, []byte(input.body), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := LoadWorker(path)
		if (err == nil) != input.valid {
			t.Fatalf("unexpected Offer YAML validation: %v", err)
		}
	}
}
