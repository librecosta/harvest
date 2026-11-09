package execution

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"strings"
	"uuid"

	"github.com/jackc/pgx/v5"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/provider"
)

func (e *Engine) RegisterTarget(ctx context.Context, p config.Principal, in TargetInput) (q.HarvestTarget, error) {
	if !canNamespace(p, in.Namespace) {
		return q.HarvestTarget{}, ErrForbidden
	}
	key, err := e.Registry.Normalize(in.Provider, in.ResourceType, in.ExternalKey)
	if err != nil {
		return q.HarvestTarget{}, ErrInvalid
	}
	return e.queries.UpsertTarget(ctx, q.UpsertTargetParams{
		ID: uuid.NewV7(), Provider: in.Provider, ResourceType: in.ResourceType, ExternalKey: key, Namespace: in.Namespace,
	})
}

func (e *Engine) Target(ctx context.Context, p config.Principal, id uuid.UUID) (q.HarvestTarget, error) {
	return found(e.queries.Target(ctx, q.TargetParams{ID: id, Namespaces: p.Namespaces}))
}

func (e *Engine) Targets(ctx context.Context, p config.Principal, page Page) ([]q.HarvestTarget, error) {
	if !canNamespace(p, page.Namespace) {
		return nil, ErrForbidden
	}
	return e.queries.ListTargets(ctx, q.ListTargetsParams{Namespace: page.Namespace, AfterID: page.After, PageSize: page.Size})
}

func (e *Engine) EditTarget(ctx context.Context, p config.Principal, id uuid.UUID, revision int64, state string) (q.HarvestTarget, error) {
	if revision < 1 || (state != "active" && state != "archived") {
		return q.HarvestTarget{}, ErrInvalid
	}
	if _, err := e.Target(ctx, p, id); err != nil {
		return q.HarvestTarget{}, err
	}
	value, err := e.queries.UpdateTarget(ctx, q.UpdateTargetParams{ID: id, State: state, ExpectedRevision: revision, Namespaces: p.Namespaces})
	if errors.Is(err, pgx.ErrNoRows) {
		return value, ErrConflict
	}
	return value, err
}

type work struct {
	target      q.HarvestTarget
	capability  provider.Capability
	params      []byte
	scope       []byte
	fingerprint []byte
}

func (e *Engine) resolve(ctx context.Context, db *q.Queries, p config.Principal, in Request) (work, error) {
	if !UUIDv7(in.TargetID) || !in.Scope.Valid() {
		return work{}, ErrInvalid
	}
	target, err := found(db.LockTarget(ctx, q.LockTargetParams{ID: in.TargetID, Namespaces: p.Namespaces}))
	if err != nil {
		return work{}, err
	}
	if !canCapability(p, in.Capability) {
		return work{}, ErrForbidden
	}
	cap, ok := e.Registry.Capability(in.Capability)
	if !ok || cap.RequestSchema != in.RequestSchema || cap.Resource != target.ResourceType ||
		!strings.HasPrefix(cap.Capability, target.Provider+".") {
		return work{}, ErrInvalid
	}
	if cap.Network != nil && (cap.Network.RequireRegion && in.Scope.EgressRegion == "" ||
		cap.Network.WebMethod != "" && !cap.Network.WebTarget(target.ExternalKey)) {
		return work{}, ErrInvalid
	}
	params, err := cap.NormalizeParams(in.Params)
	if err != nil {
		return work{}, ErrInvalid
	}
	if cap.Execute == nil {
		return work{}, ErrUnsupported
	}
	in.Params = params
	scope, err := json.Marshal(in.Scope)
	if err != nil {
		return work{}, err
	}
	fingerprint, err := hashValue(struct {
		Request
		Namespace string
		Version   string
	}{in, target.Namespace, cap.Key()})
	return work{target, cap, params, scope, fingerprint}, err
}

func (e *Engine) resolveStored(ctx context.Context, db *q.Queries, namespaces []string, id uuid.UUID,
	capability, key string, params, scope, fingerprint []byte) (work, error) {
	cap, ok := e.Registry.Capability(capability)
	if !ok || cap.Execute == nil || cap.Key() != key {
		return work{}, ErrUnsupported
	}
	target, err := found(db.Target(ctx, q.TargetParams{ID: id, Namespaces: namespaces}))
	if err != nil {
		return work{}, err
	}
	return work{target, cap, params, scope, fingerprint}, nil
}

func (w work) task() (provider.Task, error) {
	var scope provider.Scope
	if err := json.Unmarshal(w.scope, &scope, json.RejectUnknownMembers(true)); err != nil {
		return provider.Task{}, err
	}
	return provider.Task{
		TargetID: w.target.ID, Provider: w.target.Provider, Resource: w.target.ResourceType, ExternalKey: w.target.ExternalKey,
		Namespace: w.target.Namespace, Offer: w.capability.Offer, Params: jsontext.Value(w.params), Scope: scope,
	}, nil
}
