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

const workerEgress = `egress:
  profiles:
    - name: regional
      mode: require_proxy
      endpoints: [exit]
  endpoints:
    - id: exit
      url: "socks5://fixture:testsecret@127.0.0.1:1080"
      enabled: true
      region: cn
      concurrency: 2
      health_target: "127.0.0.1:1080"
      cooldown_seconds: 10
      bytes_per_minute: 131072
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
		{"configured proxy", workerYAML + workerEgress, true},
		{"remote DNS scheme", workerYAML + strings.Replace(workerEgress, "socks5://", "socks5h://", 1), false},
		{"out of range port", workerYAML + strings.Replace(workerEgress, ":1080", ":70000", 1), false},
		{"credentials without password", workerYAML + strings.Replace(workerEgress, "fixture:testsecret", "fixture", 1), false},
		{"wrong health target", workerYAML + strings.Replace(workerEgress, "health_target: \"127.0.0.1:1080\"", "health_target: \"127.0.0.1:5555\"", 1), false},
		{"invalid fallback", workerYAML + strings.Replace(workerEgress, "mode: require_proxy", "mode: require_proxy\n      allow_direct: true", 1), false},
		{"unknown endpoint", workerYAML + strings.Replace(workerEgress, "[exit]", "[missing]", 1), false},
		{"fixture policy injection", workerYAML + workerEgress + "  fixture: true\n", false},
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
