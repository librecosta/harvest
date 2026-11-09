//go:build integration

package integration

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestA0(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	scratch := t.TempDir()
	project := "harvest-a0-" + strings.ToLower(rand.Text())
	containerYAML := filepath.Join(scratch, "docker.yaml")
	writeConfig(t, containerYAML, "0.0.0.0:8080", "postgres", "5432", password)
	dbPort, err := localPort()
	if err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(),
		"HARVEST_TEST_DB_PORT="+dbPort,
		"HARVEST_IMAGE="+project,
		"HARVEST_CONFIG="+containerYAML,
		"HARVEST_HTTP_PORT=0",
	)
	composeArgs := []string{"compose", "-p", project, "-f", "compose.yaml", "-f", "integration/compose.yaml"}
	docker := func(args ...string) string {
		t.Helper()
		return run(t, root, env, "docker", args...)
	}
	compose := func(args ...string) string {
		t.Helper()
		return docker(append(append([]string(nil), composeArgs...), args...)...)
	}
	t.Cleanup(func() {
		for _, args := range [][]string{
			append(append([]string(nil), composeArgs...), "down", "--volumes"),
			{"image", "rm", project},
		} {
			output, err := commandRun(root, env, "docker", args...)
			if err != nil {
				t.Errorf("fixture cleanup %v: %v\n%s", args, err, output)
			}
		}
	})
	docker("build", "-t", project, ".")
	compose("up", "-d", "--wait", "--wait-timeout", "45", "postgres")
	dbQuery := func(sql string) string {
		t.Helper()
		return compose("exec", "-T", "postgres", "psql", "-U", "harvest_test", "-d", "harvest_a0_test", "-Atc", sql)
	}

	bin := filepath.Join(scratch, "harvest")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	run(t, root, nil, "go", "build", "-o", bin, "./cmd/harvest")
	hostEnv := append(os.Environ(),
		"PGSERVICE=missing-service",
		"PGSERVICEFILE=missing-service-file",
		"PGPORT=1",
		"PGPASSWORD=ambient-password",
		"PGOPTIONS=-c application_name=ambient-override",
		"PGSSLMODE=verify-full",
		"HTTP_PROXY=http://127.0.0.1:1",
		"NO_PROXY=",
	)
	native := func(args ...string) string {
		t.Helper()
		return run(t, scratch, hostEnv, bin, args...)
	}
	configPath := filepath.Join(scratch, "native.yaml")

	t.Log("Upgrade from the A0 schema and preserve startup rollback")
	blocker, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	address := blocker.Addr().String()
	writeConfig(t, configPath, address, "127.0.0.1", dbPort, password)
	expectFailure(t, scratch, hostEnv, bin, "schema is unavailable or incompatible", "serve", "--local", "--config", configPath)
	if got := native("migrate", "status", "--config", configPath); got != "1 pending\n2 pending\n3 pending\n4 pending\n5 pending\n6 pending" {
		t.Fatalf("A0 upgrade status: %s", got)
	}
	if got := dbQuery("SELECT count(*) FROM pg_namespace WHERE nspname = 'harvest' UNION ALL SELECT count(*) FROM pg_tables WHERE schemaname = 'public'"); got != "0\n0" {
		t.Fatalf("serve or status applied DDL: %q", got)
	}
	if got := native("migrate", "up", "--config", configPath); got != "Applied 6 Harvest migrations." {
		t.Fatalf("A0 upgrade: %s", got)
	}
	if got := native("migrate", "status", "--config", configPath); got != "1 applied\n2 applied\n3 applied\n4 applied\n5 applied\n6 applied" {
		t.Fatalf("migration status: %s", got)
	}
	if got := native("migrate", "up", "--config", configPath); got != "Applied 0 Harvest migrations." {
		t.Fatalf("repeated up: %s", got)
	}
	assertSchema := func() {
		t.Helper()
		got := dbQuery("SELECT tablename FROM pg_tables WHERE schemaname = 'harvest' ORDER BY tablename")
		if got != "attempts\nchange_events\nchange_heads\nconsumers\ncurrent_results\njobs\nobservations\noperational_policies\nquota_buckets\nquota_permits\nschedules\nsubmissions\ntargets\nworkers" {
			t.Fatalf("unexpected schema: %q", got)
		}
	}
	assertSchema()
	expectFailure(t, scratch, hostEnv, bin, "cannot bind HTTP listener", "serve", "--local", "--config", configPath)
	if got := dbQuery("SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('harvest','ambient-override')"); got != "0" {
		t.Fatalf("failed startup left PostgreSQL sessions: %s", got)
	}
	if got := native("stop"); !strings.Contains(got, "No local Harvest run") {
		t.Fatalf("failed startup left ownership state: %s", got)
	}
	if conn, err := net.DialTimeout("tcp4", address, time.Second); err != nil {
		t.Fatal("the unrelated listener did not survive failed startup and stop")
	} else {
		conn.Close()
	}
	blocker.Close()
	writeConfig(t, configPath, address, "127.0.0.1", dbPort, "wrong-private-password")
	expectFailure(t, scratch, hostEnv, bin, "cannot connect to PostgreSQL 18", "serve", "--local", "--config", configPath)
	writeConfig(t, configPath, address, "127.0.0.1", dbPort, password)

	t.Log("Native runtime and production Compose both become ready")
	process := exec.Command(bin, "serve", "--local", "--config", configPath)
	process.Dir, process.Env = scratch, hostEnv
	process.Stdout, process.Stderr = os.Stdout, os.Stderr
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	defer process.Process.Kill()
	compose("up", "-d", "--wait", "--wait-timeout", "45", "harvest")
	dockerAddress := compose("port", "harvest", "8080")
	waitProbe(t, address, "/readyz", http.StatusOK, "ok")
	waitProbe(t, dockerAddress, "/readyz", http.StatusOK, "ok")
	native("healthcheck", "--config", configPath)
	id := compose("ps", "-q", "harvest")
	if got := docker("inspect", "--format", "{{.Config.User}} {{.HostConfig.ReadonlyRootfs}} {{(index .Mounts 0).RW}}", id); got != "65532:65532 true false" {
		t.Fatalf("unexpected container runtime settings: %s", got)
	}
	if got := dbQuery("SELECT count(*) >= 2 FROM pg_stat_activity WHERE application_name = 'harvest'"); got != "t" {
		t.Fatalf("expected both YAML-configured runtimes to use the disposable database: %s", got)
	}
	assertSchema()

	t.Log("PostgreSQL loss affects readiness only and both runtimes recover")
	compose("stop", "postgres")
	for _, addr := range []string{address, dockerAddress} {
		waitProbe(t, addr, "/readyz", http.StatusServiceUnavailable, "unavailable")
		waitProbe(t, addr, "/livez", http.StatusOK, "ok")
	}
	expectFailure(t, scratch, hostEnv, bin, "Harvest is not ready", "healthcheck", "--config", configPath)
	compose("up", "-d", "--wait", "--wait-timeout", "45", "postgres")
	for _, addr := range []string{address, dockerAddress} {
		waitProbe(t, addr, "/readyz", http.StatusOK, "ok")
	}
	native("healthcheck", "--config", configPath)
	assertSchema()

	t.Log("Checkout stop and Docker SIGTERM drain with a clean exit")
	otherCheckout := t.TempDir()
	if got := run(t, otherCheckout, hostEnv, bin, "stop"); !strings.Contains(got, "No local Harvest run") {
		t.Fatalf("stop from another checkout: %s", got)
	}
	native("healthcheck", "--config", configPath)
	native("stop")
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("native stop: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("native process did not stop")
	}
	native("stop")
	compose("stop", "harvest")
	if got := docker("inspect", "--format", "{{.State.ExitCode}} {{.State.OOMKilled}}", id); got != "0 false" {
		t.Fatalf("container did not exit cleanly after SIGTERM: %s", got)
	}
	logs := compose("logs", "--no-log-prefix", "harvest")
	if !strings.Contains(logs, "HTTP draining") || !strings.Contains(logs, `"drained":true`) {
		t.Fatalf("missing Fiber shutdown hooks: %s", logs)
	}
	if got := dbQuery("SELECT count(*) FROM pg_stat_activity WHERE application_name = 'harvest'"); got != "0" {
		t.Fatalf("shutdown left PostgreSQL sessions: %s", got)
	}
}

const password = "harvest_test_disposable"

func writeConfig(t *testing.T, path, listen, host, port, credential string) {
	t.Helper()
	data := fmt.Sprintf(`http:
  listen: %q
database:
  host: %q
  port: %s
  name: "harvest_a0_test"
  user: "harvest_test"
  password: %q
  tls_mode: "disable"
`, listen, host, port, credential)
	// The container's nonroot user must be able to read this disposable fixture.
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

func localPort() (string, error) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	return port, ln.Close()
}

func waitProbe(t *testing.T, address, path string, code int, status string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://" + address + path)
		if err == nil {
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err == nil && resp.StatusCode == code && resp.Header.Get("Cache-Control") == "no-store" &&
				string(body) == fmt.Sprintf(`{"status":%q}`, status) {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s%s did not return %d (%s)", address, path, code, status)
}

func commandRun(dir string, env []string, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir, cmd.Env = dir, env
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(strings.ReplaceAll(string(out), "\r\n", "\n")), err
}

func run(t *testing.T, dir string, env []string, name string, args ...string) string {
	t.Helper()
	out, err := commandRun(dir, env, name, args...)
	if err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, out)
	}
	return out
}

func expectFailure(t *testing.T, dir string, env []string, bin, reason string, args ...string) {
	t.Helper()
	out, err := commandRun(dir, env, bin, args...)
	if err == nil || !strings.Contains(out, reason) || strings.Contains(out, "wrong-private-password") ||
		strings.Contains(out, password) {
		t.Fatalf("expected sanitized failure for %v, exit error = %v, reason matched = %t", args, err, strings.Contains(out, reason))
	}
}
