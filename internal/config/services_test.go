package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const workerYAML = `control_url: "http://127.0.0.1:8080"
token: "worker_test_only_0123456789abcdefgh"
concurrency: 1
offers:
  - capability: "test.run"
    request_schema: 1
    output_schema: 1
    implementation: "test_v1"
`

func TestWorkerYAMLIsIndependentAndStrict(t *testing.T) {
	for _, test := range []struct {
		name, yaml string
		valid      bool
	}{
		{"worker", workerYAML, true},
		{"server fields", workerYAML + "database: {password: secret}\n", false},
		{"unknown offer field", workerYAML + "    url: https://example.com\n", false},
		{"plain public URL", strings.Replace(workerYAML, "127.0.0.1", "8.8.8.8", 1), false},
		{"URL credentials", strings.Replace(workerYAML, "127.0.0.1", "user:secret@127.0.0.1", 1), false},
		{"duplicate key", workerYAML + "concurrency: 2\n", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "worker.yaml")
			if err := os.WriteFile(path, []byte(test.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadWorker(path)
			if (err == nil) != test.valid {
				t.Fatalf("unexpected validation result: %v", err)
			}
			if test.valid && (len(cfg.Offers) != 1 || cfg.Offers[0].Implementation != "test_v1") {
				t.Fatal("worker offer was not decoded")
			}
		})
	}
}
