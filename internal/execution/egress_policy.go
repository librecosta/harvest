package execution

import (
	"context"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
	"uuid"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/provider"
)

type EgressPolicyEdit struct {
	ExpectedRevision int64     `json:"expected_revision"`
	Namespace        string    `json:"-"`
	Capability       string    `json:"capability"`
	Profile          string    `json:"profile"`
	Mode             string    `json:"mode"`
	Region           string    `json:"region"`
	RequestID        uuid.UUID `json:"request_id"`
	ActorID          uuid.UUID `json:"actor_id"`
	ActorName        string    `json:"actor_name"`
	ActorRole        string    `json:"actor_role"`
}

type EgressPolicyView struct {
	PolicyStatus
	CanEdit   bool               `json:"can_edit"`
	Truncated bool               `json:"truncated"`
	Rules     []EgressPolicyRule `json:"rules"`
}

type EgressPolicyRule struct {
	Capability string          `json:"capability"`
	Profile    string          `json:"profile"`
	Mode       string          `json:"mode"`
	Region     string          `json:"region"`
	Reason     string          `json:"reason"`
	Options    []EgressProfile `json:"options"`
	Workers    []EgressMatch   `json:"workers"`
}

type EgressProfile struct {
	Profile     string   `json:"profile"`
	Mode        string   `json:"mode"`
	Regions     []string `json:"regions"`
	AllowDirect bool     `json:"allow_direct"`
}

type EgressMatch struct {
	NodeID    string                 `json:"node_id,omitempty"`
	RuntimeID *uuid.UUID             `json:"runtime_id,omitempty"`
	Shared    bool                   `json:"shared"`
	Reason    string                 `json:"reason"`
	Paths     []egress.Advertisement `json:"paths"`
}

func (e *Engine) EditEgressPolicy(ctx context.Context, p config.Principal, namespace string, edit EgressPolicyEdit) (PolicyStatus, error) {
	if namespace == "" || !provider.Identifier(namespace) {
		return PolicyStatus{}, ErrInvalid
	}
	if p.Role != "operator" || !canNamespace(p, namespace) || !canCapability(p, edit.Capability) {
		return PolicyStatus{}, ErrForbidden
	}
	if edit.RequestID == uuid.Nil() || edit.ActorID == uuid.Nil() ||
		(edit.ActorRole != "owner" && edit.ActorRole != "operator") || strings.TrimSpace(edit.ActorName) == "" ||
		utf8.RuneCountInString(edit.ActorName) > 80 || strings.ContainsFunc(edit.ActorName, func(r rune) bool {
		return unicode.IsControl(r) || unicode.Is(unicode.Cf, r)
	}) || !provider.Identifier(edit.Profile) || !egress.Mode(edit.Mode) ||
		(edit.Region != "" && (len(edit.Region) > 16 || !provider.Identifier(edit.Region))) {
		return PolicyStatus{}, ErrInvalid
	}
	edit.Namespace = namespace
	view, err := e.replaceNetworkPolicy(ctx, p, PolicyUpdate{ExpectedRevision: edit.ExpectedRevision}, &edit)
	return view.PolicyStatus, err
}

func (e *Engine) EgressPolicy(ctx context.Context, p config.Principal, namespace string) (EgressPolicyView, error) {
	if !provider.Identifier(namespace) {
		return EgressPolicyView{}, ErrInvalid
	}
	if !canNamespace(p, namespace) {
		return EgressPolicyView{}, ErrForbidden
	}
	e.admission.RLock()
	defer e.admission.RUnlock()
	if e.policyUncertain {
		return EgressPolicyView{}, ErrPolicyUnavailable
	}
	out := EgressPolicyView{PolicyStatus: policyStatus(e.policy), CanEdit: p.Role == "operator", Rules: []EgressPolicyRule{}}
	var sources []egress.SourceConfig
	for _, source := range e.network.Sources {
		if source.Namespace == namespace && canCapability(p, source.Capability) {
			sources = append(sources, source)
		}
	}
	if len(sources) == 0 {
		return out, nil
	}
	workerGrants := make(map[string]config.Principal)
	principals, accounts := make([]string, 0), make([]string, 0, len(sources))
	for _, worker := range e.workerPolicies {
		if canNamespace(worker, namespace) {
			principals = append(principals, worker.Name)
			workerGrants[worker.Name] = worker
		}
	}
	for _, source := range sources {
		accounts = append(accounts, source.AccountBucket)
	}
	workers, err := e.queries.PolicyWorkerPaths(ctx, principals)
	if err != nil {
		return EgressPolicyView{}, err
	}
	blocked, err := e.unavailableAccounts(ctx, e.queries, accounts)
	if err != nil {
		return EgressPolicyView{}, err
	}
	if len(workers) > 200 {
		workers = workers[:200]
		out.Truncated = true
	}
	for _, source := range sources {
		capability, ok := e.Registry.Capability(source.Capability)
		if !ok || capability.Network == nil {
			continue
		}
		route := e.route(source.Profile)
		rule := EgressPolicyRule{Capability: source.Capability, Profile: source.Profile, Mode: route.Mode,
			Region: source.EgressRegion, Options: e.egressProfiles(namespace, capability), Workers: []EgressMatch{}}
		_, regionReady := resolveEgressRegion(source, route, "")
		for _, worker := range workers {
			grant := workerGrants[worker.Principal]
			if !slices.Contains(grant.Capabilities, source.Capability) || !slices.Contains(worker.Offers, capability.Key()) {
				continue
			}
			match, err := e.egressMatch(source, route, grant, worker)
			if err != nil {
				return EgressPolicyView{}, err
			}
			rule.Workers = append(rule.Workers, match)
			if len(rule.Workers) == 25 {
				out.Truncated = true
				break
			}
		}
		switch {
		case slices.ContainsFunc(blocked, func(row q.UnavailableQuotaAccountsRow) bool {
			return row.Key == source.AccountBucket && row.DailyExhausted
		}):
			rule.Reason = "daily_quota_exhausted"
		case slices.ContainsFunc(blocked, func(row q.UnavailableQuotaAccountsRow) bool { return row.Key == source.AccountBucket }):
			rule.Reason = "upstream_cooldown"
		case !regionReady:
			rule.Reason = "egress_region_required"
		case len(rule.Workers) == 0:
			rule.Reason = "no_matching_worker"
		case !slices.ContainsFunc(rule.Workers, func(match EgressMatch) bool { return match.Reason == "ready" }):
			rule.Reason = "no_healthy_egress"
		default:
			rule.Reason = "ready"
		}
		out.Rules = append(out.Rules, rule)
	}
	return out, nil
}

func (e *Engine) egressProfiles(namespace string, cap provider.Capability) []EgressProfile {
	options := make([]EgressProfile, 0)
	for _, route := range e.network.Routes {
		if (!cap.Network.AllowDirect && (route.Mode == egress.Direct || route.AllowDirect)) ||
			!slices.ContainsFunc(e.workerPolicies, func(worker config.Principal) bool {
				return canNamespace(worker, namespace) && slices.Contains(worker.Capabilities, cap.Capability) &&
					slices.Contains(worker.EgressProfiles, route.Profile)
			}) {
			continue
		}
		option := EgressProfile{Profile: route.Profile, Mode: route.Mode, AllowDirect: route.AllowDirect, Regions: []string{}}
		for _, path := range route.Paths {
			if path.Region != "" && !slices.Contains(option.Regions, path.Region) {
				option.Regions = append(option.Regions, path.Region)
			}
		}
		slices.Sort(option.Regions)
		options = append(options, option)
	}
	return options
}

func (e *Engine) egressMatch(source egress.SourceConfig, route egress.RouteConfig, p config.Principal, w q.PolicyWorkerPathsRow) (EgressMatch, error) {
	match := EgressMatch{Shared: len(p.Namespaces) != 1, Paths: []egress.Advertisement{}}
	if !match.Shared {
		if w.NodeID != nil {
			match.NodeID = *w.NodeID
		}
		match.RuntimeID = &w.ID
	}
	paths, err := decodePaths(w.NetworkPaths)
	if err != nil {
		return EgressMatch{}, err
	}
	if slices.Contains(p.EgressProfiles, source.Profile) && !match.Shared {
		for _, path := range paths {
			if _, ok := routePath(route, path); ok && (source.EgressRegion == "" || path.Region == source.EgressRegion) {
				match.Paths = append(match.Paths, path)
			}
		}
	}
	plan, ready := e.plan(p, source, "", paths)
	switch {
	case !w.Online:
		match.Reason = "worker_offline"
	case w.Blocked != nil && *w.Blocked:
		match.Reason = "worker_paused"
	case plan == nil || len(plan.Paths) == 0:
		match.Reason = "no_matching_path"
	case !ready:
		match.Reason = "path_cooldown"
	case w.ActiveTasks >= int64(w.Capacity):
		match.Reason = "worker_full"
	case !slices.ContainsFunc(plan.Paths, func(path egress.Advertisement) bool {
		return path.State == "healthy" && path.InFlight < path.Capacity || path.State == "degraded" && path.InFlight == 0
	}):
		match.Reason = "endpoint_full"
	default:
		match.Reason = "ready"
	}
	return match, nil
}
