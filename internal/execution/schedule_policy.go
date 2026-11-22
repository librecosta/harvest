package execution

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"slices"
	"strings"
	"uuid"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/provider"
)

const directFirst = "direct_first"

var (
	ErrScheduleRegion   = errors.New("schedule_egress_region_not_allowed")
	ErrScheduleMode     = errors.New("schedule_egress_mode_not_allowed")
	ErrScheduleRetry    = errors.New("schedule_retry_not_allowed")
	ErrScheduleInterval = errors.New("schedule_interval_not_allowed")
)

type ExecutionPolicy struct {
	Regions     []string `json:"regions,omitempty"`
	Mode        string   `json:"mode,omitempty"`
	MaxAttempts int32    `json:"max_attempts,omitempty"`
	Backoff     string   `json:"backoff,omitempty"`
}

func executionPolicy(data []byte) ExecutionPolicy {
	var policy ExecutionPolicy
	// Values are written only by the typed schedule and Job commands.
	_ = json.Unmarshal(data, &policy)
	return policy
}

func allowedMode(route egress.RouteConfig, allowDirect bool, mode string) bool {
	switch mode {
	case "":
		return route.Profile != ""
	case directFirst:
		return allowDirect && (route.Mode == egress.Direct || route.Mode == egress.PreferProxy && route.AllowDirect)
	case egress.PreferProxy:
		return route.Mode == egress.PreferProxy
	case egress.RequireProxy:
		return route.Mode == egress.PreferProxy || route.Mode == egress.RequireProxy
	default:
		return false
	}
}

func allowedTaskPath(rule egress.SourceConfig, path egress.PathConfig, mode string, allowDirect bool) bool {
	return (rule.EgressRegion == "" || path.Region == rule.EgressRegion) &&
		(path.Kind != egress.Direct || allowDirect && mode != egress.RequireProxy)
}

func (e *Engine) validateExecutionPolicy(cap provider.Capability, namespace, cadence string, policy ExecutionPolicy) error {
	if policy.MaxAttempts < 0 || policy.MaxAttempts > maxAttempts ||
		(policy.Backoff != "" && policy.Backoff != "fixed" && policy.Backoff != "exponential") ||
		((cadence == "point_in_time" || cap.PointInTimeOnly) && (policy.MaxAttempts != 0 || policy.Backoff != "")) {
		return ErrScheduleRetry
	}
	if policy.Mode == "" && len(policy.Regions) == 0 {
		return nil
	}
	if e.policyUncertain {
		return ErrPolicyUnavailable
	}
	if cap.Network == nil {
		return ErrScheduleMode
	}
	rule := e.source(cap.Capability, namespace)
	route := e.route(rule.Profile)
	if !allowedMode(route, cap.Network.AllowDirect, policy.Mode) {
		return ErrScheduleMode
	}
	if len(policy.Regions) > 8 {
		return ErrScheduleRegion
	}
	for i, region := range policy.Regions {
		if !provider.Identifier(region) || len(region) > 16 || slices.Contains(policy.Regions[:i], region) ||
			!slices.ContainsFunc(route.Paths, func(path egress.PathConfig) bool {
				return path.Region == region && allowedTaskPath(rule, path, policy.Mode, cap.Network.AllowDirect)
			}) {
			return ErrScheduleRegion
		}
	}
	return nil
}

func taskAllowsRegion(rule egress.SourceConfig, route egress.RouteConfig, requested, region string, policy ExecutionPolicy) bool {
	if rule.EgressRegion != "" && region != rule.EgressRegion {
		return false
	}
	if len(policy.Regions) != 0 {
		return slices.Contains(policy.Regions, region)
	}
	resolved, allowed := resolveEgressRegion(rule, route, requested)
	return allowed && region == resolved
}

// All paths in a claim belong to one permitted region. Permits use the region
// stored on the attempt even if a different endpoint becomes healthy later.
func (e *Engine) taskNetwork(p config.Principal, cap provider.Capability, rule egress.SourceConfig, requested string,
	policy ExecutionPolicy, paths []egress.Advertisement, pinned *string) (*egress.Plan, string, bool) {
	route := e.route(rule.Profile)
	if !slices.Contains(p.EgressProfiles, rule.Profile) || !allowedMode(route, cap.Network.AllowDirect, policy.Mode) {
		return nil, "", false
	}
	regions := policy.Regions
	if len(regions) == 0 {
		region, allowed := resolveEgressRegion(rule, route, requested)
		if !allowed {
			return nil, "", false
		}
		regions = []string{region}
	}
	if pinned != nil {
		regions = []string{*pinned}
	}
	for _, region := range regions {
		if !taskAllowsRegion(rule, route, requested, region, policy) {
			continue
		}
		plan := &egress.Plan{Profile: route.Profile, Mode: route.Mode,
			AllowDirect: route.AllowDirect && policy.Mode != egress.RequireProxy, DirectFirst: policy.Mode == directFirst}
		ready := false
		for _, advertised := range paths {
			path, match := routePath(route, advertised)
			if !match || path.Region != region || !allowedTaskPath(rule, path, policy.Mode, cap.Network.AllowDirect) {
				continue
			}
			plan.Paths = append(plan.Paths, advertised)
			ready = ready || pathReady(advertised)
		}
		if ready || pinned != nil {
			return plan, region, ready
		}
	}
	return nil, "", false
}

func pathReady(a egress.Advertisement) bool {
	return a.State == "healthy" || a.State == "degraded"
}

type SchedulePolicyPreview struct {
	Revision        int64            `json:"revision"`
	IntervalSeconds int32            `json:"interval_seconds"`
	ExecutionPolicy *ExecutionPolicy `json:"execution_policy,omitempty"`
}

type SchedulePolicyView struct {
	Revision        int64         `json:"revision"`
	Profile         string        `json:"profile,omitempty"`
	DefaultMode     string        `json:"default_mode,omitempty"`
	EffectiveMode   string        `json:"effective_mode,omitempty"`
	Regions         []string      `json:"regions"`
	AllowedRegions  []string      `json:"allowed_regions"`
	AllowedModes    []string      `json:"allowed_modes"`
	MinimumInterval int32         `json:"minimum_interval_seconds"`
	MaxInterval     int32         `json:"maximum_interval_seconds"`
	MaxAttempts     int32         `json:"max_attempts"`
	Backoff         string        `json:"backoff"`
	PointInTime     bool          `json:"point_in_time"`
	Workers         []EgressMatch `json:"workers"`
	Reason          string        `json:"reason,omitempty"`
}

func (e *Engine) ScheduleExecutionPolicy(ctx context.Context, principal config.Principal, id uuid.UUID, preview *SchedulePolicyPreview) (SchedulePolicyView, error) {
	e.admission.RLock()
	defer e.admission.RUnlock()
	s, err := e.Schedule(ctx, principal, id)
	if err != nil {
		return SchedulePolicyView{}, err
	}
	cap, ok := e.Registry.Capability(s.Capability)
	if !ok || cap.Execute == nil {
		return SchedulePolicyView{}, ErrUnsupported
	}
	policy := executionPolicy(s.ExecutionPolicy)
	if preview != nil {
		if preview.Revision != s.Revision {
			return SchedulePolicyView{}, ErrConflict
		}
		if preview.ExecutionPolicy == nil || preview.IntervalSeconds < 1 {
			return SchedulePolicyView{}, ErrInvalid
		}
		r := recurrence(s)
		r.IntervalSeconds = preview.IntervalSeconds
		if _, err := parseCadence(r, cap.MinInterval); err != nil {
			return SchedulePolicyView{}, ErrScheduleInterval
		}
		policy = *preview.ExecutionPolicy
		if err := e.validateExecutionPolicy(cap, s.Namespace, s.Policy, policy); err != nil {
			return SchedulePolicyView{}, err
		}
	}
	out := SchedulePolicyView{
		Revision: s.Revision, MinimumInterval: int32(cap.MinInterval.Seconds()), MaxInterval: 30 * 24 * 3600,
		Regions: []string{}, AllowedRegions: []string{}, AllowedModes: []string{}, Workers: []EgressMatch{},
		PointInTime: s.Policy == "point_in_time", MaxAttempts: int32(maxAttempts), Backoff: "exponential",
	}
	if out.PointInTime {
		out.MaxAttempts, out.Backoff = 1, ""
	} else {
		if policy.MaxAttempts != 0 {
			out.MaxAttempts = policy.MaxAttempts
		}
		if policy.Backoff != "" {
			out.Backoff = policy.Backoff
		}
	}
	if cap.Network == nil {
		return out, nil
	}
	rule := e.source(s.Capability, s.Namespace)
	route := e.route(rule.Profile)
	out.Profile, out.DefaultMode, out.EffectiveMode = route.Profile, route.Mode, route.Mode
	if policy.Mode != "" {
		out.EffectiveMode = policy.Mode
	}
	if e.policyUncertain || route.Profile == "" {
		out.Reason = "egress_policy_unavailable"
		return out, nil
	}
	for _, mode := range []string{directFirst, egress.PreferProxy, egress.RequireProxy} {
		if allowedMode(route, cap.Network.AllowDirect, mode) {
			out.AllowedModes = append(out.AllowedModes, mode)
		}
	}
	var scope provider.Scope
	if err := json.Unmarshal(s.Scope, &scope); err != nil {
		return SchedulePolicyView{}, err
	}
	for _, path := range route.Paths {
		if !allowedTaskPath(rule, path, "", cap.Network.AllowDirect) {
			continue
		}
		if path.Region != "" && !slices.Contains(out.AllowedRegions, path.Region) {
			out.AllowedRegions = append(out.AllowedRegions, path.Region)
		}
		if taskAllowsRegion(rule, route, scope.EgressRegion, path.Region, policy) &&
			allowedTaskPath(rule, path, policy.Mode, cap.Network.AllowDirect) && !slices.Contains(out.Regions, path.Region) {
			out.Regions = append(out.Regions, path.Region)
		}
	}
	slices.Sort(out.AllowedRegions)
	slices.Sort(out.Regions)
	if !allowedMode(route, cap.Network.AllowDirect, policy.Mode) {
		out.Reason = "schedule_egress_mode_not_allowed"
		return out, nil
	}
	if len(out.Regions) == 0 {
		out.Reason = "egress_region_conflict"
		return out, nil
	}
	workers, err := e.queries.NetworkWorkers(ctx, q.NetworkWorkersParams{
		Principals: e.workerPrincipals(s.Namespace, []string{s.Capability}), ExecutionKey: s.ExecutionKey,
	})
	if err != nil {
		return SchedulePolicyView{}, err
	}
	if len(workers) == 0 {
		out.Reason = "no_matching_worker"
		return out, nil
	}
	out.Reason = "no_healthy_egress"
	for _, worker := range workers {
		paths, err := decodePaths(worker.NetworkPaths)
		if err != nil {
			return SchedulePolicyView{}, err
		}
		for _, p := range e.workerPolicies {
			if p.Name != worker.Principal {
				continue
			}
			plan, _, ready := e.taskNetwork(p, cap, rule, scope.EgressRegion, policy, paths, nil)
			if !ready {
				break
			}
			out.Reason = ""
			match := EgressMatch{Shared: len(p.Namespaces) != 1, Paths: []egress.Advertisement{}}
			if !match.Shared {
				match.RuntimeID = &worker.ID
				if worker.NodeID != nil {
					match.NodeID = *worker.NodeID
				}
				match.Paths = plan.Paths
			}
			out.Workers = append(out.Workers, match)
			break
		}
		if len(out.Workers) >= 20 {
			break
		}
	}
	blocked, err := e.unavailableAccounts(ctx, e.queries, []string{rule.AccountBucket})
	if err != nil {
		return SchedulePolicyView{}, err
	}
	if len(blocked) != 0 {
		out.Reason = "upstream_cooldown"
		if blocked[0].DailyExhausted {
			out.Reason = "daily_quota_exhausted"
		}
	}
	return out, nil
}

func networkOffer(offer, namespace, region, mode string) string {
	return strings.Join([]string{offer, namespace, region, mode}, "|")
}
