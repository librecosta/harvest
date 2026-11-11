package execution

import (
	"context"
	json "encoding/json/v2"
	"slices"
	"strings"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/provider"
)

func (e *Engine) route(profile string) egress.RouteConfig {
	for _, r := range e.network.Routes {
		if r.Profile == profile {
			return r
		}
	}
	return egress.RouteConfig{}
}

func (e *Engine) source(capability, namespace string) egress.SourceConfig {
	for _, rule := range e.network.Sources {
		if rule.Capability == capability && rule.Namespace == namespace {
			return rule
		}
	}
	return egress.SourceConfig{}
}

func routePath(r egress.RouteConfig, a egress.Advertisement) (egress.PathConfig, bool) {
	if r.Profile == "" || r.Profile != a.Profile || r.Mode != a.Mode {
		return egress.PathConfig{}, false
	}
	for _, path := range r.Paths {
		if path.Path() == a.Path() {
			return path, true
		}
	}
	return egress.PathConfig{}, false
}

func (e *Engine) validatePaths(p config.Principal, capacity int32, paths []egress.Advertisement) ([]byte, error) {
	if len(paths) > egress.MaxPaths {
		return nil, ErrInvalid
	}
	paths = append([]egress.Advertisement{}, paths...)
	slices.SortFunc(paths, func(a, b egress.Advertisement) int {
		if a.Profile != b.Profile {
			return strings.Compare(a.Profile, b.Profile)
		}
		return strings.Compare(a.ID, b.ID)
	})
	for i, a := range paths {
		if !slices.Contains(p.EgressProfiles, a.Profile) || a.Capacity < 1 || a.Capacity > capacity ||
			(i > 0 && paths[i-1].Profile == a.Profile && paths[i-1].ID == a.ID) {
			return nil, ErrForbidden
		}
		// A granted local path may be advertised ahead of a policy change.
		// A matching central route is still required for claim and permit.
		if !provider.Identifier(a.ID) || !egress.Mode(a.Mode) || (a.Region != "" && (len(a.Region) > 16 || !provider.Identifier(a.Region))) ||
			(a.Kind == egress.Direct && (a.ID != egress.Direct || a.Mode == egress.RequireProxy)) ||
			(a.Kind != egress.Direct && (!egress.ProxyKind(a.Kind) || a.ID == egress.Direct || a.Mode == egress.Direct)) {
			return nil, ErrForbidden
		}
		switch a.State {
		case "healthy", "degraded", "cooldown", "disabled":
		default:
			return nil, ErrInvalid
		}
	}
	return json.Marshal(paths)
}

func decodePaths(data []byte) ([]egress.Advertisement, error) {
	var out []egress.Advertisement
	err := json.Unmarshal(data, &out, json.RejectUnknownMembers(true))
	return out, err
}

func samePaths(left, right []byte) bool {
	a, ae := decodePaths(left)
	b, be := decodePaths(right)
	return ae == nil && be == nil && slices.EqualFunc(a, b, func(l, r egress.Advertisement) bool {
		l.State, r.State = "", ""
		return l == r
	})
}

func (e *Engine) plan(p config.Principal, rule egress.SourceConfig, region string, paths []egress.Advertisement) (*egress.Plan, bool) {
	if !slices.Contains(p.EgressProfiles, rule.Profile) {
		return nil, false
	}
	route := e.route(rule.Profile)
	plan := &egress.Plan{Profile: route.Profile, Mode: route.Mode, AllowDirect: route.AllowDirect}
	healthy := false
	for _, a := range paths {
		if _, ok := routePath(route, a); !ok || (region != "" && a.Region != region) {
			continue
		}
		plan.Paths = append(plan.Paths, a)
		healthy = healthy || a.State == "healthy" || a.State == "degraded"
	}
	return plan, healthy
}

func routeKey(offer, namespace, region string) string { return offer + "|" + namespace + "|" + region }

func (e *Engine) dispatchOffers(ctx context.Context, db *q.Queries, p config.Principal, w q.HarvestWorker) ([]string, []string, error) {
	paths, err := decodePaths(w.NetworkPaths)
	if err != nil {
		return nil, nil, err
	}
	var local, network []string
	var accounts []string
	if len(paths) != 0 && !e.policyUncertain {
		for _, rule := range e.network.Sources {
			if canNamespace(p, rule.Namespace) && canCapability(p, rule.Capability) {
				accounts = append(accounts, rule.AccountBucket)
			}
		}
	}
	var blocked []q.UnavailableQuotaAccountsRow
	if len(accounts) != 0 {
		blocked, err = e.unavailableAccounts(ctx, db, accounts)
		if err != nil {
			return nil, nil, err
		}
	}
	for _, name := range p.Capabilities {
		cap, ok := e.Registry.Capability(name)
		if !ok || cap.Execute == nil || !slices.Contains(w.Offers, cap.Key()) {
			continue
		}
		if cap.Network == nil {
			local = append(local, cap.Key())
			continue
		}
		if e.policyUncertain {
			continue
		}
		for _, rule := range e.network.Sources {
			if rule.Capability != name || !canNamespace(p, rule.Namespace) || slices.ContainsFunc(blocked, func(row q.UnavailableQuotaAccountsRow) bool { return row.Key == rule.AccountBucket }) {
				continue
			}
			plan, ready := e.plan(p, rule, "", paths)
			if !ready {
				continue
			}
			network = append(network, routeKey(cap.Key(), rule.Namespace, ""))
			for _, a := range plan.Paths {
				key := routeKey(cap.Key(), rule.Namespace, a.Region)
				if a.Region != "" && (a.State == "healthy" || a.State == "degraded") && !slices.Contains(network, key) {
					network = append(network, key)
				}
			}
		}
	}
	return local, network, nil
}

func (e *Engine) unavailableAccounts(ctx context.Context, db *q.Queries, keys []string) ([]q.UnavailableQuotaAccountsRow, error) {
	slices.Sort(keys)
	keys = slices.Compact(keys)
	limits := make([]int64, len(keys))
	for i, key := range keys {
		if index := slices.IndexFunc(e.network.Quotas, func(cfg egress.QuotaConfig) bool { return cfg.ID == key }); index >= 0 {
			limits[i] = e.network.Quotas[index].DailyLimit
		}
	}
	return db.UnavailableQuotaAccounts(ctx, q.UnavailableQuotaAccountsParams{Keys: keys, DailyLimits: limits})
}

func (e *Engine) networkWaiting(ctx context.Context, job q.HarvestJob, cap provider.Capability) (string, error) {
	if e.policyUncertain {
		return "egress_policy_unavailable", nil
	}
	rule := e.source(cap.Capability, job.Namespace)
	if rule.Profile == "" {
		return "egress_policy_unavailable", nil
	}
	blocked, err := e.unavailableAccounts(ctx, e.queries, []string{rule.AccountBucket})
	if err != nil {
		return "", err
	}
	if len(blocked) != 0 {
		if blocked[0].DailyExhausted {
			return "daily_quota_exhausted", nil
		}
		return "upstream_cooldown", nil
	}
	var scope provider.Scope
	if err := json.Unmarshal(job.Scope, &scope); err != nil {
		return "", err
	}
	workers, err := e.queries.NetworkWorkers(ctx, q.NetworkWorkersParams{
		Principals: e.workerPrincipals(job.Namespace, []string{job.Capability}), ExecutionKey: job.ExecutionKey,
	})
	if err != nil {
		return "", err
	}
	for _, w := range workers {
		paths, err := decodePaths(w.NetworkPaths)
		if err != nil {
			return "", err
		}
		for _, p := range e.workerPolicies {
			if p.Name != w.Principal {
				continue
			}
			if _, ok := e.plan(p, rule, scope.EgressRegion, paths); ok {
				return "awaiting_claim", nil
			}
		}
	}
	return "no_healthy_egress", nil
}
