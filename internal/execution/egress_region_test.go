package execution

import (
	"testing"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/egress"
)

func TestEgressPlanNeverFallsBackAcrossRegions(t *testing.T) {
	route := egress.RouteConfig{Profile: "regional", Mode: egress.PreferProxy, AllowDirect: true, Paths: []egress.PathConfig{
		{ID: "direct", Kind: "direct", Region: "cn"},
		{ID: "edge", Kind: "socks5", Region: "intl"},
	}}
	worker := config.Principal{EgressProfiles: []string{route.Profile}}
	engine := Engine{network: egress.NetworkConfig{Routes: []egress.RouteConfig{route}}}
	paths := []egress.Advertisement{
		{Profile: route.Profile, Mode: route.Mode, ID: "direct", Kind: "direct", Region: "cn", State: "healthy"},
		{Profile: route.Profile, Mode: route.Mode, ID: "edge", Kind: "socks5", Region: "intl", State: "cooldown"},
	}
	rule := egress.SourceConfig{Profile: route.Profile}
	if plan, ok := engine.plan(worker, rule, "", paths); plan != nil || ok {
		t.Fatal("ambiguous policy admitted an unscoped task")
	}
	rule.EgressRegion = "intl"
	if plan, ok := engine.plan(worker, rule, "cn", paths); plan != nil || ok {
		t.Fatal("a task overrode the policy region")
	}
	if plan, ok := engine.plan(worker, rule, "", paths); ok || plan == nil || len(plan.Paths) != 1 || plan.Paths[0].Region != "intl" {
		t.Fatalf("proxy cooldown admitted direct on a different region: %+v", plan)
	}
	paths[1].State = "degraded"
	if plan, ok := engine.plan(worker, rule, "intl", paths); !ok || len(plan.Paths) != 1 || plan.Paths[0].Kind != "socks5" {
		t.Fatalf("recovered proxy not selectable: %+v", plan)
	}
}
