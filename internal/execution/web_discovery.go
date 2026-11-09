package execution

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"sync"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"

	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/provider"
)

type rdapCache struct {
	mu          sync.Mutex
	services    egress.Bootstrap
	validUntil  time.Time
	leader      uuid.UUID
	leaderUntil time.Time
	retryAfter  time.Time
}

func (c *rdapCache) authorize(stage int16, domain string) (string, error) {
	now := time.Now()
	fresh := now.Before(c.validUntil)
	if stage == egress.RDAPLookup {
		if !fresh {
			if now.Before(c.retryAfter) {
				return "", egress.ErrBootstrapUnavailable
			}
			return "", egress.ErrBootstrapRequired
		}
		endpoint := c.services.Endpoint(domain)
		if endpoint == "" {
			return "", egress.ErrRegistryMissing
		}
		if _, err := egress.RegistryEndpoint(endpoint, domain); err != nil {
			return "", egress.ErrRegistryMissing
		}
		return endpoint, nil
	}
	if fresh {
		return "", egress.ErrBootstrapReady
	}
	if now.Before(c.retryAfter) {
		return "", egress.ErrBootstrapUnavailable
	}
	return "", nil
}

func assetPath(ctx context.Context, db *q.Queries, t provider.Task) (string, []string, error) {
	params, err := provider.Object[provider.AssetParams](t.Params)
	if err != nil {
		return "", nil, ErrInvalid
	}
	if params.ObservationID == nil {
		return egress.FaviconPath, nil, nil
	}
	_, key, err := canonicalScope(provider.SegmentScope{Namespace: t.Namespace, Scope: t.Scope, Dimensions: map[string]string{"section": "html"}})
	if err != nil {
		return "", nil, ErrInvalid
	}
	rows, err := db.ObservationsByID(ctx, []uuid.UUID{*params.ObservationID})
	if err != nil {
		return "", nil, err
	}
	if len(rows) != 1 {
		return "", nil, egress.ErrCandidate
	}
	o := rows[0]
	if o.TargetID != t.TargetID || o.Stream != t.Namespace || o.Capability != "web.http" || o.OutputSchema != 1 ||
		o.ScopeKey != key || o.State != "present" || o.Source != "web" {
		return "", nil, egress.ErrCandidate
	}
	path, sizes := provider.ApprovedAssetPath(t.ExternalKey, params, o.Data)
	if path == "" {
		return "", nil, egress.ErrCandidate
	}
	return path, sizes, nil
}

func authorizeRobots(ctx context.Context, db *q.Queries, job q.HarvestJob, scope provider.Scope, uri string) error {
	_, key, err := canonicalScope(provider.SegmentScope{Namespace: job.Namespace, Scope: scope, Dimensions: map[string]string{"section": "robots"}})
	if err != nil {
		return ErrInvalid
	}
	data, err := db.CurrentRobotsPolicy(ctx, q.CurrentRobotsPolicyParams{Stream: job.Namespace, TargetID: job.TargetID,
		ScopeKey: key, JobScope: job.Scope})
	if errors.Is(err, pgx.ErrNoRows) {
		return egress.ErrRobotsUnknown
	}
	if err != nil {
		return err
	}
	allowed, err := provider.AllowsRobots(data, uri)
	if err != nil {
		return egress.ErrRobotsUnknown
	}
	if !allowed {
		return egress.ErrRobotsBlocked
	}
	return nil
}

func approveFirstWebPath(ctx context.Context, db *q.Queries, job q.HarvestJob, target q.HarvestTarget, source *egress.Source, scope provider.Scope) (string, error) {
	if !source.Candidate {
		return source.InitialPath(), nil
	}
	path, _, err := assetPath(ctx, db, provider.Task{TargetID: target.ID, ExternalKey: target.ExternalKey, Namespace: job.Namespace, Scope: scope, Params: job.Params})
	return path, err
}

func webJobScope(job q.HarvestJob) (provider.Scope, error) {
	var scope provider.Scope
	err := json.Unmarshal(job.Scope, &scope)
	return scope, err
}
