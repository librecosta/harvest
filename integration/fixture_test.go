//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/librecosta/harvest/internal/command"
	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/database"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

var fixtureOffer = provider.Offer{Capability: "fixture.run", RequestSchema: 1, OutputSchema: 1, Implementation: "fixture_v1"}

type fixtureParams struct {
	Label string `json:"label"`
	Delay int32  `json:"delay_ms"`
	Mode  string `json:"mode"`
}

type fixtureOutput struct {
	Label string `json:"label"`
	Count int32  `json:"count"`
}

func fixtureRegistry() *provider.Registry {
	manifest := provider.Manifest{
		Name: "fixture",
		Resources: []provider.Resource{{Name: "key", Normalize: func(key string) (string, error) {
			key = strings.ToLower(key)
			if !provider.Identifier(key) {
				return "", errors.New("invalid fixture key")
			}
			return key, nil
		}}},
		Capabilities: []provider.Capability{{
			Offer: fixtureOffer, Resource: "key", MinInterval: time.Second, Timeout: 25 * time.Second, Partial: true,
			NormalizeParams: func(value jsontext.Value) (jsontext.Value, error) {
				p, err := provider.Object[fixtureParams](value)
				if err != nil || !provider.Identifier(p.Label) || p.Delay < 0 || p.Delay > 20000 {
					return nil, errors.New("invalid fixture params")
				}
				switch p.Mode {
				case "success", "partial", "retry", "fatal":
				default:
					return nil, errors.New("unknown fixture outcome")
				}
				return json.Marshal(p)
			},
			NormalizeOutput: provider.Normalize[fixtureOutput],
			Execute: func(ctx context.Context, task provider.Task, _ egress.Client) provider.Result {
				p, err := provider.Object[fixtureParams](task.Params)
				if err != nil {
					return provider.Result{State: "failed", Code: "invalid_payload"}
				}
				timer := time.NewTimer(time.Duration(p.Delay) * time.Millisecond)
				defer timer.Stop()
				select {
				case <-ctx.Done():
					return provider.Result{State: "failed", Code: "timeout"}
				case <-timer.C:
				}
				output, _ := json.Marshal(fixtureOutput{p.Label, 1})
				switch p.Mode {
				case "partial":
					return provider.Result{State: "partial", Code: "incomplete", Data: output}
				case "retry":
					return provider.Result{State: "failed", Code: "upstream_unavailable"}
				case "fatal":
					return provider.Result{State: "failed", Code: "policy_denied"}
				default:
					return provider.Result{State: "succeeded", Data: output}
				}
			},
		}},
	}
	if os.Getenv("HARVEST_TEST_OBSERVATIONS") == "true" {
		manifest.Capabilities = append(manifest.Capabilities, a3Capability(false), a3Capability(true))
	}
	if origins := os.Getenv("HARVEST_TEST_ORIGINS"); origins != "" {
		manifest.Capabilities = append(manifest.Capabilities, fixtureNetwork(strings.Split(origins, ",")))
	}
	builtins := provider.Builtins()
	if os.Getenv("HARVEST_TEST_WEB") != "" {
		for i, builtin := range builtins {
			if builtin.Name == "web" {
				builtins[i] = provider.WebManifest(webFixtureNetwork())
			}
		}
	}
	r, err := provider.New(append(builtins, manifest)...)
	if err != nil {
		panic(err)
	}
	return r
}

func TestMain(m *testing.M) {
	if len(os.Args) > 2 && os.Args[1] == "fixture" {
		logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
		if os.Getenv("HARVEST_TEST_ORIGINS") != "" || os.Getenv("HARVEST_TEST_WEB") != "" {
			go sampleProcessHeap()
		}
		if err := command.Run(os.Args[2:], fixtureRegistry(), logger); err != nil {
			logger.Error("command failed", "reason", err.Error())
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type testPlane struct {
	t            *testing.T
	scratch      string
	bin          string
	config       string
	address      string
	pool         *pgxpool.Pool
	client       *http.Client
	tokens       map[string]string
	compose      func(...string) string
	origins      []string
	observations bool
	webFixture   string
}

func startFixture(t *testing.T) *testPlane {
	t.Helper()
	p := newFixture(t)
	if got := p.cli("migrate", "up", "--config", p.config); got != "Applied 5 Harvest migrations." {
		t.Fatalf("fresh PG18 migration: %q", got)
	}
	return p
}

func newFixture(t *testing.T) *testPlane {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	scratch := t.TempDir()
	project := "harvest-a1-" + strings.ToLower(rand.Text())
	args := []string{"compose", "-p", project, "-f", "integration/compose.yaml"}
	disposablePort, err := localPort()
	if err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(), "HARVEST_TEST_DB_PORT="+disposablePort)
	compose := func(rest ...string) string {
		t.Helper()
		return run(t, root, env, "docker", append(append([]string{}, args...), rest...)...)
	}
	t.Cleanup(func() {
		out, err := commandDocker(root, env, append(args, "down", "--volumes")...)
		if err != nil {
			t.Errorf("disposable project cleanup: %v %s", err, out)
		}
	})
	compose("up", "-d", "--wait", "--wait-timeout", "45", "postgres")
	_, dbPort, err := net.SplitHostPort(compose("port", "postgres", "5432"))
	if err != nil {
		t.Fatal(err)
	}
	port, err := localPort()
	if err != nil {
		t.Fatal(err)
	}
	p := &testPlane{
		t: t, scratch: scratch, bin: bin, address: "127.0.0.1:" + port, compose: compose,
		config: filepath.Join(scratch, "server.yaml"), tokens: make(map[string]string),
		client: &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{}},
	}
	t.Cleanup(p.client.CloseIdleConnections)
	writeConfig(t, p.config, p.address, "127.0.0.1", dbPort, password)
	f, err := os.OpenFile(p.config, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, err = fmt.Fprintln(f, "principals:")
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range []struct{ name, role, namespaces string }{
		{"operator", "operator", "public, private, scheduler, embedded"},
		{"public_reader", "reader", "public"},
		{"worker_a", "worker", "public"},
		{"worker_b", "worker", "public"},
		{"private_worker", "worker", "private"},
		{"embedded_worker", "worker", "embedded"},
	} {
		token := rand.Text() + rand.Text()
		p.tokens[policy.name] = token
		_, err := fmt.Fprintf(f, "  - name: %q\n    role: %q\n    token: %q\n    namespaces: [%s]\n",
			policy.name, policy.role, token, policy.namespaces)
		if err != nil {
			t.Fatal(err)
		}
		if policy.role == "worker" {
			if _, err := fmt.Fprintln(f, "    capabilities: [fixture.run]\n    max_slots: 1\n    egress_profiles: [test_direct]"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := fmt.Fprintln(f, "local_worker:"); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(p.workerYAML("embedded_worker")), "\n") {
		if _, err := fmt.Fprintln(f, "  "+line); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(p.config)
	if err != nil {
		t.Fatal(err)
	}
	p.pool, err = database.Open(t.Context(), cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.pool.Close)
	return p
}

func commandDocker(dir string, env []string, args ...string) (string, error) {
	return commandRun(dir, env, "docker", args...)
}

func (p *testPlane) cli(args ...string) string {
	p.t.Helper()
	env := append(os.Environ(), "HARVEST_TEST_ORIGINS="+strings.Join(p.origins, ","), "HARVEST_TEST_WEB="+p.webFixture)
	return run(p.t, p.scratch, env, p.bin, append([]string{"fixture"}, args...)...)
}

func (p *testPlane) workerYAML(name string) string {
	return fmt.Sprintf(`control_url: %q
token: %q
concurrency: 1
egress_profiles: [test_direct]
offers:
  - capability: fixture.run
    request_schema: 1
    output_schema: 1
    implementation: fixture_v1
`, "http://"+p.address, p.tokens[name])
}

type child struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
	log  bytes.Buffer
}

func (p *testPlane) start(args ...string) *child {
	p.t.Helper()
	c := &child{cmd: exec.Command(p.bin, append([]string{"fixture"}, args...)...), done: make(chan struct{})}
	c.cmd.Dir = p.scratch
	c.cmd.Env = append(os.Environ(), "GOMAXPROCS=4", "HARVEST_TEST_ORIGINS="+strings.Join(p.origins, ","),
		fmt.Sprintf("HARVEST_TEST_OBSERVATIONS=%t", p.observations), "HARVEST_TEST_WEB="+p.webFixture,
		"HTTP_PROXY=http://127.0.0.1:1", "HTTPS_PROXY=http://127.0.0.1:1", "ALL_PROXY=socks5://127.0.0.1:1", "NO_PROXY=*")
	c.cmd.Stderr = &c.log
	if err := c.cmd.Start(); err != nil {
		p.t.Fatal(err)
	}
	go func() { c.err = c.cmd.Wait(); close(c.done) }()
	p.t.Cleanup(func() {
		_ = c.cmd.Process.Kill()
		<-c.done
		for _, token := range p.tokens {
			if strings.Contains(c.log.String(), token) {
				p.t.Error("fixture credential appeared in subprocess logs")
			}
		}
	})
	return c
}

func (p *testPlane) serve() *child {
	p.t.Helper()
	c := p.start("serve", "--local", "--config", p.config)
	waitProbe(p.t, p.address, "/readyz", 200, "ok")
	return c
}

func (p *testPlane) startWorker(name string) *child {
	p.t.Helper()
	path := filepath.Join(p.scratch, name+".yaml")
	if err := os.WriteFile(path, []byte(p.workerYAML(name)), 0o600); err != nil {
		p.t.Fatal(err)
	}
	return p.start("worker", "--config", path)
}

func (c *child) kill(t *testing.T) {
	t.Helper()
	if err := c.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-c.done
	if c.err == nil {
		t.Fatal("expected a crashed process")
	}
}

func (c *child) waitClean(t *testing.T) {
	t.Helper()
	select {
	case <-c.done:
		if c.err != nil {
			t.Fatalf("subprocess failed: %v\n%s", c.err, c.log.String())
		}
	case <-time.After(18 * time.Second):
		t.Fatal("subprocess did not drain")
	}
}

func (p *testPlane) request(method, path, who, key string, in any) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(p.t.Context(), 3*time.Second)
	defer cancel()
	var body []byte
	if in != nil {
		var err error
		body, err = json.Marshal(in)
		if err != nil {
			return 0, nil, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://"+p.address+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if who != "" {
		req.Header.Set("Authorization", "Bearer "+p.tokens[who])
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return resp.StatusCode, data, err
}

func (p *testPlane) call(method, path, who, key string, in, out any, want int) {
	p.t.Helper()
	status, data, err := p.request(method, path, who, key, in)
	if err != nil || status != want {
		p.t.Fatalf("%s %s: got %d %s, %v, want %d", method, path, status, data, err, want)
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			p.t.Fatalf("decode %s: %v", path, err)
		}
	}
}

func (p *testPlane) register(providerName, resource, key, namespace string) uuid.UUID {
	p.t.Helper()
	var out struct {
		ID uuid.UUID `json:"id"`
	}
	p.call("POST", "/api/v1/targets", "operator", "", execution.TargetInput{
		Provider: providerName, ResourceType: resource, ExternalKey: key, Namespace: namespace,
	}, &out, 201)
	if !execution.UUIDv7(out.ID) {
		p.t.Fatalf("target ID is not UUIDv7: %s", out.ID)
	}
	return out.ID
}

func eventually(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}
