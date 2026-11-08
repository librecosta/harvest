package egress

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"
)

type endpoint struct {
	cfg       EndpointConfig
	url       *url.URL
	state     string
	nextProbe time.Time
	probing   bool
	inflight  int32
	window    time.Time
	used      int64
}

type transportKey struct {
	endpoint, profile, namespace, capability string
	fixture                                  bool
}

type cachedTransport struct {
	transport *http.Transport
	used      time.Time
}

type Pool struct {
	mu         sync.Mutex
	profiles   []ProfileConfig
	endpoints  map[string]*endpoint
	transports map[transportKey]cachedTransport
	draining   bool
	closeOnce  sync.Once
}

func NewPool(cfg Config) *Pool {
	p := &Pool{profiles: cfg.Profiles, endpoints: make(map[string]*endpoint), transports: make(map[transportKey]cachedTransport)}
	for _, c := range cfg.Endpoints {
		u, _ := url.Parse(c.URL) // Validated at configuration load.
		state := "disabled"
		if c.Enabled {
			state = "degraded"
		}
		p.endpoints[c.ID] = &endpoint{cfg: c, url: u, state: state}
	}
	return p
}

func (p *Pool) Drain() {
	p.mu.Lock()
	p.draining = true
	p.mu.Unlock()
}

func (p *Pool) Close() {
	p.closeOnce.Do(func() {
		p.Drain()
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, t := range p.transports {
			t.transport.CloseIdleConnections()
		}
		clear(p.transports)
	})
}

func (p *Pool) Advertise(slots int32) []Advertisement {
	p.mu.Lock()
	defer p.mu.Unlock()
	paths := make([]Advertisement, 0)
	for _, profile := range p.profiles {
		if profile.Mode == Direct || profile.AllowDirect {
			paths = append(paths, Advertisement{profile.Name, profile.Mode, Direct, Direct, profile.DirectRegion, "healthy", slots})
		}
		for _, id := range profile.Endpoints {
			e := p.endpoints[id]
			paths = append(paths, Advertisement{profile.Name, profile.Mode, id, e.url.Scheme, e.cfg.Region, e.state, min(slots, e.cfg.Concurrency)})
		}
	}
	return paths
}

func (p *Pool) Probe(ctx context.Context) {
	p.mu.Lock()
	var probes []*endpoint
	now := time.Now()
	for _, e := range p.endpoints {
		if e.cfg.Enabled && !e.probing && !now.Before(e.nextProbe) {
			e.probing = true
			probes = append(probes, e)
		}
	}
	for key, t := range p.transports {
		if now.Sub(t.used) > 2*time.Minute {
			t.transport.CloseIdleConnections()
			delete(p.transports, key)
		}
	}
	p.mu.Unlock()
	for _, e := range probes {
		call, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		conn, err := dialProxy(call, &net.Dialer{}, e.url, nil)
		cancel()
		if err == nil {
			conn.Close()
		}
		p.mu.Lock()
		e.probing = false
		now := time.Now()
		// A newer passive failure wins over an older active probe.
		if !now.Before(e.nextProbe) {
			switch {
			case err != nil:
				e.cooldown(now)
			case e.state == "healthy":
				e.nextProbe = now.Add(30 * time.Second)
			default:
				e.state = "degraded" // one half-open, quota-gated source request
				e.nextProbe = now.Add(30 * time.Second)
			}
		}
		p.mu.Unlock()
	}
}

func (e *endpoint) cooldown(now time.Time) {
	e.state = "cooldown"
	e.nextProbe = now.Add(time.Duration(e.cfg.CooldownSeconds) * time.Second)
}

type reservation struct {
	path      Advertisement
	transport *http.Transport
	endpoint  *endpoint
	epoch     time.Time
	maxBytes  int64
}

func (p *Pool) reserve(plan Plan, source Source, namespace, capability, pinned string, excluded []string, wireBudget int64) (reservation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.draining {
		return reservation{}, ErrUnavailable
	}
	i := slices.IndexFunc(p.profiles, func(c ProfileConfig) bool { return c.Name == plan.Profile })
	if i < 0 {
		return reservation{}, ErrPolicy
	}
	profile := p.profiles[i]
	if profile.Mode != plan.Mode {
		return reservation{}, ErrPolicy
	}
	now := time.Now()
	var selected *endpoint
	var path Advertisement
	var direct *Advertisement
	busy := false
	for _, candidate := range plan.Paths {
		if slices.Contains(excluded, candidate.ID) || (pinned != "" && candidate.ID != pinned) {
			continue
		}
		if candidate.Profile != profile.Name || (candidate.State != "healthy" && candidate.State != "degraded") {
			continue
		}
		if candidate.Kind == Direct {
			if candidate.ID == Direct && candidate.Region == profile.DirectRegion && source.AllowDirect &&
				(plan.Mode == Direct || (plan.Mode == PreferProxy && plan.AllowDirect && profile.AllowDirect)) {
				d := candidate
				direct = &d
			}
			continue
		}
		if plan.Mode == Direct || !slices.Contains(profile.Endpoints, candidate.ID) {
			continue
		}
		e := p.endpoints[candidate.ID]
		if e.url.Scheme != candidate.Kind || e.cfg.Region != candidate.Region ||
			(e.state != "healthy" && e.state != "degraded") {
			continue
		}
		if now.Sub(e.window) >= time.Minute {
			e.window, e.used = now, 0
		}
		limit := e.cfg.Concurrency
		if e.state == "degraded" {
			limit = 1
		}
		if e.inflight >= limit || e.used+wireBudget > e.cfg.BytesPerMinute {
			busy = true
			continue
		}
		if selected == nil || e.inflight < selected.inflight ||
			(e.inflight == selected.inflight && candidate.ID < path.ID) {
			selected, path = e, candidate
		}
	}
	if selected == nil {
		if busy {
			return reservation{}, ErrDeferred
		}
		if direct == nil {
			return reservation{}, ErrUnavailable
		}
		path = *direct
	}
	key := transportKey{path.ID, plan.Profile, namespace, capability, source.Fixture}
	entry, ok := p.transports[key]
	if !ok {
		if len(p.transports) >= 256 {
			return reservation{}, ErrUnavailable
		}
		var target *url.URL
		if selected != nil {
			target = selected.url
		}
		entry.transport = routeTransport(target, source.Fixture)
	}
	entry.used = now
	p.transports[key] = entry
	r := reservation{path: path, transport: entry.transport, endpoint: selected, maxBytes: wireBudget}
	if selected != nil {
		selected.inflight++
		selected.used += r.maxBytes
		r.epoch = selected.window
	}
	return r, nil
}

func (p *Pool) release(r reservation, n int64, networkErr error, reached bool) {
	e := r.endpoint
	if e == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	e.inflight--
	if r.epoch == e.window {
		e.used -= r.maxBytes - n
	}
	switch {
	case errors.Is(networkErr, errConnect) || errors.Is(networkErr, errProxyRefused):
		e.cooldown(time.Now())
	case reached && e.state == "degraded":
		e.state = "healthy"
		e.nextProbe = time.Now().Add(30 * time.Second)
	}
}
