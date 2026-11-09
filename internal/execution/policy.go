package execution

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/egress"
)

const MaxPolicyBody = 64 << 10

var (
	ErrPolicyConflict    = errors.New("policy_revision_conflict")
	ErrPolicyMissing     = errors.New("policy_not_configured")
	ErrPolicyInvalid     = errors.New("invalid_network_policy")
	ErrPolicyBinding     = errors.New("policy_quota_binding_conflict")
	ErrPolicyChanged     = errors.New("network_policy_changed")
	ErrPolicyUnavailable = errors.New("network_policy_unavailable")
)

type PolicyStatus struct {
	Status          string `json:"status"`
	Revision        int64  `json:"revision"`
	RoutingRevision int64  `json:"routing_revision"`
}

type PolicyView struct {
	PolicyStatus
	Policy    *egress.NetworkConfig `json:"policy,omitempty"`
	Checksum  string                `json:"checksum,omitempty"`
	UpdatedAt time.Time             `json:"updated_at,omitzero"`
	UpdatedBy string                `json:"updated_by,omitempty"`
}

type PolicyUpdate struct {
	ExpectedRevision int64                 `json:"expected_revision"`
	Policy           *egress.NetworkConfig `json:"policy"`
}

func policyStatus(row q.HarvestOperationalPolicy) PolicyStatus {
	if row.Revision == 0 {
		return PolicyStatus{Status: "not_configured"}
	}
	return PolicyStatus{"active", row.Revision, row.RoutingRevision}
}

func (e *Engine) policyView() PolicyView {
	row := e.policy
	out := PolicyView{PolicyStatus: policyStatus(row)}
	if row.Revision > 0 {
		network := canonicalNetwork(e.network)
		out.Policy, out.Checksum, out.UpdatedAt, out.UpdatedBy = &network, hex.EncodeToString(row.Checksum), row.UpdatedAt.UTC(), row.UpdatedBy
	}
	return out
}

func coversNetwork(p config.Principal, c egress.NetworkConfig) bool {
	return p.Role == "operator" && !slices.ContainsFunc(c.Sources, func(s egress.SourceConfig) bool {
		return !canNamespace(p, s.Namespace) || !canCapability(p, s.Capability)
	})
}

func (e *Engine) NetworkPolicy(p config.Principal) (PolicyView, error) {
	e.admission.RLock()
	defer e.admission.RUnlock()
	if !coversNetwork(p, e.network) {
		return PolicyView{}, ErrForbidden
	}
	if e.policyUncertain {
		return PolicyView{}, ErrPolicyUnavailable
	}
	return e.policyView(), nil
}

func (e *Engine) validatePolicy(c egress.NetworkConfig) error {
	if c.Routes == nil || c.Sources == nil || c.Quotas == nil {
		return ErrPolicyInvalid
	}
	if err := config.ValidateNetwork(c); err != nil {
		return err
	}
	for _, r := range c.Routes {
		if !slices.ContainsFunc(e.workerPolicies, func(p config.Principal) bool { return slices.Contains(p.EgressProfiles, r.Profile) }) {
			return errors.New("network route has no configured Worker profile grant")
		}
	}
	for _, rule := range c.Sources {
		cap, ok := e.Registry.Capability(rule.Capability)
		if !ok || cap.Execute == nil || cap.Network == nil {
			return errors.New("network source requires a compiled HTTP capability")
		}
		if !slices.ContainsFunc(e.workerPolicies, func(p config.Principal) bool {
			return canNamespace(p, rule.Namespace) && canCapability(p, rule.Capability) && slices.Contains(p.EgressProfiles, rule.Profile)
		}) {
			return errors.New("network source lacks a matching Worker grant")
		}
		r := c.Routes[slices.IndexFunc(c.Routes, func(r egress.RouteConfig) bool { return r.Profile == rule.Profile })]
		if (r.Mode == egress.Direct || r.AllowDirect) && !cap.Network.AllowDirect {
			return errors.New("network route exceeds compiled direct allowance")
		}
		if cap.Network.RequireRegion && !slices.ContainsFunc(r.Paths, func(p egress.PathConfig) bool { return p.Region != "" }) {
			return errors.New("network route lacks a required egress region")
		}
	}
	return nil
}

func canonicalNetwork(c egress.NetworkConfig) egress.NetworkConfig {
	c.Routes = slices.Clone(c.Routes)
	for i := range c.Routes {
		c.Routes[i].Paths = slices.Clone(c.Routes[i].Paths)
		slices.SortFunc(c.Routes[i].Paths, func(a, b egress.PathConfig) int { return strings.Compare(a.ID, b.ID) })
	}
	slices.SortFunc(c.Routes, func(a, b egress.RouteConfig) int { return strings.Compare(a.Profile, b.Profile) })
	c.Sources = slices.Clone(c.Sources)
	slices.SortFunc(c.Sources, func(a, b egress.SourceConfig) int {
		return strings.Compare(a.Namespace+"/"+a.Capability, b.Namespace+"/"+b.Capability)
	})
	c.Quotas = slices.Clone(c.Quotas)
	slices.SortFunc(c.Quotas, func(a, b egress.QuotaConfig) int { return strings.Compare(a.ID, b.ID) })
	return c
}

func policyBytes(c egress.NetworkConfig) ([]byte, []byte, error) {
	data, err := json.Marshal(c, json.Deterministic(true))
	sum := sha256.Sum256(data)
	return data, sum[:], err
}

// Keep logical quota identities across removals, even after accounting rows
// age out. The central policy never receives the actual endpoint address.
func extendQuotaBindings(c egress.NetworkConfig, prior map[string]string) (map[string]string, error) {
	bindings := make(map[string]string, len(prior))
	maps.Copy(bindings, prior)
	bind := func(key, value string) bool {
		if old, ok := bindings[key]; ok {
			return old == value
		}
		bindings[key] = value
		return true
	}
	quota := func(identity, key, role string) bool {
		return bind(identity, key) && bind("bucket/"+key, role)
	}
	if c.GlobalBucket != "" && !quota("global", c.GlobalBucket, "global") {
		return nil, ErrPolicyBinding
	}
	for _, s := range c.Sources {
		key := "source/" + s.Namespace + "/" + s.Capability
		if !quota(key+"/provider", s.ProviderBucket, "provider") || !quota(key+"/account", s.AccountBucket, "account") {
			return nil, ErrPolicyBinding
		}
	}
	for _, r := range c.Routes {
		for _, p := range r.Paths {
			key := "path/" + p.ID
			if p.Kind == egress.Direct {
				key += "/" + p.Region
			}
			if !bind(key+"/identity", p.Kind+"/"+p.Region) || !quota(key+"/quota", p.Bucket, "egress") {
				return nil, ErrPolicyBinding
			}
		}
	}
	if len(bindings) > 2048 {
		return nil, ErrPolicyBinding
	}
	return bindings, nil
}

func (e *Engine) LoadNetworkPolicy(ctx context.Context) error {
	e.admission.Lock()
	defer e.admission.Unlock()
	return e.loadNetworkPolicy(ctx)
}

func (e *Engine) loadNetworkPolicy(ctx context.Context) error {
	row, err := e.queries.NetworkPolicy(ctx)
	if errors.Is(err, pgx.ErrNoRows) && e.policy.Revision == 0 {
		return nil
	}
	if err != nil {
		return errors.New("cannot load operational network policy")
	}
	var network egress.NetworkConfig
	var bindings map[string]string
	if json.Unmarshal(row.Policy, &network, json.RejectUnknownMembers(true)) != nil ||
		json.Unmarshal(row.Bindings, &bindings, json.RejectUnknownMembers(true)) != nil || e.validatePolicy(network) != nil {
		return errors.New("stored operational network policy is incompatible")
	}
	network = canonicalNetwork(network)
	_, sum, err := policyBytes(network)
	expected, bindingErr := extendQuotaBindings(network, bindings)
	if err != nil || !bytes.Equal(sum, row.Checksum) || bindingErr != nil || !maps.Equal(expected, bindings) {
		return errors.New("stored operational network policy checksum or bindings are invalid")
	}
	e.policy, e.network, e.quotaBindings = row, network, bindings
	e.policyUncertain = false
	return nil
}

func (e *Engine) InitNetworkPolicy(ctx context.Context, network egress.NetworkConfig) error {
	if len(network.Sources) == 0 {
		return errors.New("policy init requires a complete network block")
	}
	if err := e.validatePolicy(network); err != nil {
		return err
	}
	network = canonicalNetwork(network)
	bindings, err := extendQuotaBindings(network, nil)
	if err != nil {
		return err
	}
	data, sum, err := policyBytes(network)
	if err != nil {
		return err
	}
	ledger, err := json.Marshal(bindings, json.Deterministic(true))
	if err != nil {
		return err
	}
	_, err = tx(ctx, e, func(db *q.Queries) (struct{}, error) {
		var zero struct{}
		n, err := db.InitNetworkPolicy(ctx, q.InitNetworkPolicyParams{Policy: data, Bindings: ledger, Checksum: sum})
		if err != nil {
			return zero, err
		}
		if n == 0 {
			return zero, errors.New("operational network policy already initialized")
		}
		return zero, db.EnsureQuotaBuckets(ctx, quotaKeys(network))
	})
	return err
}

func (e *Engine) ReplaceNetworkPolicy(ctx context.Context, p config.Principal, in PolicyUpdate) (PolicyView, error) {
	if in.ExpectedRevision < 1 || in.Policy == nil {
		return PolicyView{}, ErrInvalid
	}
	e.admission.Lock()
	defer e.admission.Unlock()
	if e.draining.Load() {
		return PolicyView{}, ErrDraining
	}
	if !coversNetwork(p, e.network) || !coversNetwork(p, *in.Policy) {
		return PolicyView{}, ErrForbidden
	}
	if e.policyUncertain {
		if err := e.loadNetworkPolicy(ctx); err != nil {
			return PolicyView{}, ErrPolicyUnavailable
		}
		if !coversNetwork(p, e.network) {
			return PolicyView{}, ErrForbidden
		}
	}
	if e.policy.Revision == 0 {
		return PolicyView{}, ErrPolicyMissing
	}
	next := canonicalNetwork(*in.Policy)
	var bindings map[string]string
	row, err := tx(ctx, e, func(db *q.Queries) (q.HarvestOperationalPolicy, error) {
		old, err := db.LockNetworkPolicy(ctx)
		if err != nil {
			return old, err
		}
		if old.Revision != e.policy.Revision || !bytes.Equal(old.Checksum, e.policy.Checksum) {
			return old, errors.New("operational policy changed outside the active control plane")
		}
		if in.ExpectedRevision != old.Revision {
			return old, ErrPolicyConflict
		}
		if err := e.validatePolicy(next); err != nil {
			return old, ErrPolicyInvalid
		}
		bindings, err = extendQuotaBindings(next, e.quotaBindings)
		if err != nil {
			return old, err
		}
		data, sum, err := policyBytes(next)
		if err != nil || bytes.Equal(sum, old.Checksum) {
			return old, err
		}
		ledger, err := json.Marshal(bindings, json.Deterministic(true))
		if err != nil {
			return old, err
		}
		if err := reconcileQuotas(ctx, db, e.network, next); err != nil {
			return old, err
		}
		routing := old.RoutingRevision
		if !sameRouting(e.network, next) {
			routing = old.Revision + 1
		}
		return db.ReplaceNetworkPolicy(ctx, q.ReplaceNetworkPolicyParams{
			RoutingRevision: routing, Policy: data, Bindings: ledger, Checksum: sum, UpdatedBy: p.Name,
		})
	})
	if err != nil {
		if row.Revision <= e.policy.Revision {
			return PolicyView{}, err
		}
		// The UPDATE returned but COMMIT did not. Resolve the outcome with
		// an independent deadline before allowing another network admission.
		e.policyUncertain = true
		check, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if e.loadNetworkPolicy(check) != nil {
			return PolicyView{}, ErrPolicyUnavailable
		}
		if e.policy.Revision == row.Revision && bytes.Equal(e.policy.Checksum, row.Checksum) {
			return e.policyView(), nil
		}
		return PolicyView{}, err
	}
	if row.Revision != e.policy.Revision {
		e.policy, e.network, e.quotaBindings = row, next, bindings
	}
	return e.policyView(), nil
}

func sameRouting(a, b egress.NetworkConfig) bool {
	return a.GlobalBucket == b.GlobalBucket && slices.Equal(a.Sources, b.Sources) &&
		slices.EqualFunc(a.Routes, b.Routes, func(l, r egress.RouteConfig) bool {
			return l.Profile == r.Profile && l.Mode == r.Mode && l.AllowDirect == r.AllowDirect && slices.Equal(l.Paths, r.Paths)
		})
}
