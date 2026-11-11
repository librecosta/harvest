package execution

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/provider"
)

func TestDeploymentExamples(t *testing.T) {
	registry, err := provider.New(provider.Builtins()...)
	if err != nil {
		t.Fatal(err)
	}
	for _, example := range []struct{ name, suffix string }{{"direct", ""}, {"proxy", ".proxy"}} {
		t.Run(example.name, func(t *testing.T) {
			root := filepath.Join("..", "..")
			server, err := config.Load(filepath.Join(root, "config"+example.suffix+".example.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			worker, err := config.LoadWorker(filepath.Join(root, "config.worker"+example.suffix+".example.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			engine := New(nil, registry, server.Principals, server.Retention)
			if err := engine.validatePolicy(server.Network); err != nil {
				t.Fatal(err)
			}
			i := slices.IndexFunc(server.Principals, func(p config.Principal) bool { return p.Role == "worker" && p.Token == worker.Token })
			if i < 0 {
				t.Fatal("worker has no server principal")
			}
			p := server.Principals[i]
			if err := engine.ValidateLocalWorker(p, worker); err != nil {
				t.Fatal(err)
			}
			engine.network = server.Network
			if err := engine.WorkerReadiness(p); err != nil {
				t.Fatal(err)
			}
			for _, offer := range worker.Offers {
				if !slices.ContainsFunc(server.Network.Sources, func(ruleSource egress.SourceConfig) bool {
					return ruleSource.Capability == offer.Capability && slices.Contains(p.EgressProfiles, ruleSource.Profile)
				}) {
					t.Fatalf("no matching source policy for %s", offer.Capability)
				}
			}
		})
	}
}
