// Package worker executes only capabilities compiled into its own registry.
// Its only control-plane dependency is the authenticated HTTP protocol.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/config"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

func Validate(cfg config.Worker, registry *provider.Registry) error {
	for _, offer := range cfg.Offers {
		c, ok := registry.Capability(offer.Capability)
		if !ok || c.Execute == nil || c.Offer != offer {
			return errors.New("worker offer is not compiled and executable in this binary")
		}
		if c.Network != nil && len(cfg.Egress.Profiles) == 0 {
			return errors.New("network worker offer requires a configured egress profile")
		}
	}
	return nil
}

func Run(ctx context.Context, cfg config.Worker, registry *provider.Registry, logger *slog.Logger) error {
	if err := Validate(cfg, registry); err != nil {
		return err
	}
	pool := egress.NewPool(cfg.Egress)
	defer pool.Close()
	pool.Probe(ctx)
	api := newClient(cfg)
	defer api.http.CloseIdleConnections()
	reg := execution.Registration{ID: uuid.NewV7(), Offers: cfg.Offers, Capacity: cfg.Concurrency, EgressProfiles: cfg.EgressProfiles, Paths: pool.Advertise(cfg.Concurrency)}
	for {
		_, err := api.request(ctx, http.MethodPost, "/workers/register", reg, nil)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			break
		}
		if !transient(err) {
			return err
		}
		wait(ctx, 2*time.Second)
	}
	logger.Info("worker registered", "worker_id", reg.ID.String())
	base := "/workers/" + reg.ID.String()
	life, stopWork := context.WithCancel(context.WithoutCancel(ctx))
	defer stopWork()
	var paused atomic.Bool
	var loops sync.WaitGroup
	loops.Go(func() {
		ticker := time.NewTicker(execution.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pool.Probe(ctx)
				paths := pool.Advertise(cfg.Concurrency)
				var out execution.Heartbeat
				_, err := api.request(ctx, http.MethodPost, base+"/heartbeat", execution.Heartbeat{Paths: paths}, &out)
				paused.Store(err != nil || out.Draining)
				if responseCode(err, "not_found") {
					_, _ = api.request(ctx, http.MethodPost, "/workers/register", reg, nil)
				}
			}
		}
	})
	for range cfg.Concurrency {
		loops.Go(func() {
			for ctx.Err() == nil {
				if !paused.Load() {
					var claim execution.Claim
					started := time.Now()
					ok, err := api.request(ctx, http.MethodPost, base+"/claim", nil, &claim)
					if err == nil && ok {
						execute(life, api, registry, pool, claim, started)
					}
				}
				wait(ctx, time.Second+time.Duration(rand.IntN(250))*time.Millisecond)
			}
		})
	}
	<-ctx.Done()
	pool.Drain()
	done := make(chan struct{})
	go func() { loops.Wait(); close(done) }()
	drain, cancel := context.WithTimeout(life, execution.PreDrain)
	defer cancel()
	// Keep renewals/completions alive while telling the server to stop admission.
	hb, cancelHB := context.WithTimeout(drain, time.Second)
	var status execution.Heartbeat
	_, _ = api.request(hb, http.MethodPost, base+"/heartbeat", execution.Heartbeat{Draining: true}, &status)
	cancelHB()
	select {
	case <-done:
	case <-drain.Done():
		stopWork()
		<-done
	}
	logger.Info("worker stopped", "worker_id", reg.ID.String())
	return nil
}

func wait(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

const safetyMargin = 100 * time.Millisecond

func execute(ctx context.Context, api client, registry *provider.Registry, pool *egress.Pool, claim execution.Claim, started time.Time) {
	cap, ok := registry.Capability(claim.Task.Offer.Capability)
	if !ok || cap.Execute == nil || cap.Offer != claim.Task.Offer || claim.LeaseMS < 1 {
		return
	}
	deadline := started.Add(time.Duration(claim.LeaseMS)*time.Millisecond - safetyMargin)
	if time.Until(deadline) <= 0 {
		return
	}
	workCtx, cancelWork := context.WithTimeout(ctx, cap.Timeout)
	defer cancelWork()
	var network egress.Client
	var policyChanged atomic.Bool
	if cap.Network != nil {
		if claim.Network == nil {
			return
		}
		session := pool.Session(workCtx, *claim.Network, *cap.Network, claim.Task.Namespace, cap.Capability, permitGate{api, claim, &policyChanged},
			egress.WebDestination{ID: claim.Task.TargetID, Origin: claim.Task.ExternalKey})
		defer session.Close()
		network = session.Client()
	}
	result := make(chan provider.Result, 1)
	go func() {
		key, err := registry.Normalize(claim.Task.Provider, claim.Task.Resource, claim.Task.ExternalKey)
		params, paramErr := cap.NormalizeParams(claim.Task.Params)
		if err != nil || paramErr != nil || key != claim.Task.ExternalKey || !claim.Task.Scope.Valid() {
			result <- provider.Result{State: "failed", Code: "invalid_payload"}
			return
		}
		claim.Task.Params = params
		result <- cap.Execute(workCtx, claim.Task, network)
	}()
	base := "/attempts/" + claim.AttemptID.String()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	renew := time.NewTicker(execution.RenewInterval)
	defer renew.Stop()
	var completed <-chan provider.Result = result
	workDone := workCtx.Done()
	var pending *provider.Result
	var retry <-chan time.Time
	for {
		if pending != nil && retry == nil {
			call, cancel := context.WithDeadline(ctx, minTime(deadline, time.Now().Add(time.Second)))
			var receipt execution.Receipt
			_, err := api.request(call, http.MethodPost, base+"/complete", execution.Completion{
				Fence: claim.Fence, OutputSchema: cap.OutputSchema, Result: *pending,
			}, &receipt)
			cancel()
			if err == nil {
				return
			}
			if responseCode(err, "cancel_requested") && pending.State != "cancelled" {
				cancelWork()
				completed, workDone = nil, nil
				pending = &provider.Result{State: "cancelled", Code: "cancelled"}
				continue
			}
			if !transient(err) {
				return
			}
			retry = time.After(400 * time.Millisecond)
		}
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			return
		case outcome := <-completed:
			cancelWork()
			completed, workDone = nil, nil
			if policyChanged.Load() {
				outcome = provider.Result{State: "failed", Code: "network_policy_changed"}
			}
			pending = &outcome
		case <-workDone:
			completed, workDone = nil, nil
			code := "timeout"
			if policyChanged.Load() {
				code = "network_policy_changed"
			}
			pending = &provider.Result{State: "failed", Code: code}
		case <-retry:
			retry = nil
		case <-renew.C:
			sent := time.Now()
			call, cancel := context.WithDeadline(ctx, minTime(deadline, sent.Add(time.Second)))
			var response execution.Renewal
			_, err := api.request(call, http.MethodPost, base+"/renew", claim.Fence, &response)
			cancel()
			if responseCode(err, "cancel_requested") {
				cancelWork()
				completed, workDone, retry = nil, nil, nil
				pending = &provider.Result{State: "cancelled", Code: "cancelled"}
				continue
			}
			if err != nil {
				// An ambiguous completion may already have committed. Retry its
				// receipt even if renew now reports a closed Attempt.
				if !transient(err) && pending == nil {
					return
				}
				continue
			}
			deadline = sent.Add(time.Duration(response.LeaseMS)*time.Millisecond - safetyMargin)
			timer.Reset(time.Until(deadline))
		}
	}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
