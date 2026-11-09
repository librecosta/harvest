package config

import (
	"errors"
	"net/url"
	"strings"

	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/provider"
)

func region(s string) bool {
	return s == "" || (len(s) <= 16 && provider.Identifier(s))
}

func validateEgress(c egress.Config) error {
	if len(c.Profiles) > 8 || len(c.Endpoints) > 16 {
		return errors.New("too many worker egress profiles or endpoints")
	}
	endpoints := make(map[string]bool)
	for _, endpoint := range c.Endpoints {
		u, err := url.Parse(endpoint.URL)
		if err != nil || !egress.ProxyKind(u.Scheme) || !egress.ValidHostPort(u) || u.Port() == "" ||
			u.Opaque != "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery ||
			strings.Contains(endpoint.URL, "#") ||
			!provider.Identifier(endpoint.ID) || endpoint.ID == egress.Direct || endpoints[endpoint.ID] ||
			!region(endpoint.Region) || endpoint.Concurrency < 1 || endpoint.Concurrency > 16 ||
			endpoint.CooldownSeconds < 2 || endpoint.CooldownSeconds > 300 ||
			endpoint.BytesPerMinute < 32<<10 || endpoint.BytesPerMinute > 1<<30 ||
			(endpoint.HealthTarget != "" && endpoint.HealthTarget != u.Host) {
			return errors.New("invalid or duplicate worker egress endpoint")
		}
		if u.User != nil {
			password, ok := u.User.Password()
			user := u.User.Username()
			if !ok || len(user) == 0 || len(user) > 128 || len(password) == 0 || len(password) > 128 ||
				strings.ContainsAny(user+password, "\r\n\x00") || strings.ContainsRune(user, ':') {
				return errors.New("invalid worker proxy credentials")
			}
		}
		endpoints[endpoint.ID] = true
	}
	names := make(map[string]bool)
	paths := 0
	for _, profile := range c.Profiles {
		if !provider.Identifier(profile.Name) || names[profile.Name] || !egress.Mode(profile.Mode) ||
			!region(profile.DirectRegion) || !identifiers(profile.Endpoints) ||
			(profile.Mode != egress.PreferProxy && profile.AllowDirect) ||
			(profile.Mode == egress.Direct && len(profile.Endpoints) != 0) ||
			(profile.Mode != egress.Direct && len(profile.Endpoints) == 0) ||
			(profile.Mode != egress.Direct && !profile.AllowDirect && profile.DirectRegion != "") {
			return errors.New("invalid or duplicate worker egress profile")
		}
		names[profile.Name] = true
		paths += len(profile.Endpoints)
		if profile.Mode == egress.Direct || profile.AllowDirect {
			paths++
		}
		for _, id := range profile.Endpoints {
			if !endpoints[id] {
				return errors.New("worker profile references an unknown endpoint")
			}
		}
	}
	if paths > egress.MaxPaths {
		return errors.New("too many worker egress paths")
	}
	return nil
}

// ValidateNetwork checks the nonsecret policy document shape and references.
func ValidateNetwork(c egress.NetworkConfig) error {
	if len(c.Routes) == 0 && len(c.Sources) == 0 && len(c.Quotas) == 0 && c.GlobalBucket == "" {
		return nil
	}
	if len(c.Routes) < 1 || len(c.Routes) > 16 || len(c.Sources) < 1 || len(c.Sources) > 64 ||
		len(c.Quotas) < 4 || len(c.Quotas) > 128 {
		return errors.New("invalid server network policy size")
	}
	quotas := make(map[string]bool)
	for _, q := range c.Quotas {
		if !provider.Identifier(q.ID) || quotas[q.ID] || q.Burst < 1 || q.Burst > 64 ||
			q.PerMinute < 1 || q.PerMinute > 1200 || q.Concurrency < 1 || q.Concurrency > 64 {
			return errors.New("invalid or duplicate server quota")
		}
		quotas[q.ID] = true
	}
	if !quotas[c.GlobalBucket] {
		return errors.New("unknown global quota")
	}
	routes := make(map[string]egress.RouteConfig)
	for _, r := range c.Routes {
		if !provider.Identifier(r.Profile) || routes[r.Profile].Profile != "" || !egress.Mode(r.Mode) ||
			len(r.Paths) < 1 || len(r.Paths) > 16 || (r.AllowDirect && r.Mode != egress.PreferProxy) {
			return errors.New("invalid or duplicate server egress route")
		}
		seen := make(map[string]bool)
		proxies := 0
		for _, path := range r.Paths {
			if !provider.Identifier(path.ID) || seen[path.ID] || !region(path.Region) || !quotas[path.Bucket] ||
				(path.Kind == egress.Direct && (path.ID != egress.Direct || (r.Mode != egress.Direct && !r.AllowDirect))) ||
				(path.Kind != egress.Direct && (!egress.ProxyKind(path.Kind) || path.ID == egress.Direct || r.Mode == egress.Direct)) {
				return errors.New("invalid or duplicate server egress path")
			}
			if path.Kind != egress.Direct {
				proxies++
			}
			seen[path.ID] = true
		}
		if (r.Mode != egress.Direct && proxies == 0) || (r.AllowDirect && !seen[egress.Direct]) {
			return errors.New("server egress route lacks a required path")
		}
		routes[r.Profile] = r
	}
	sources := make(map[string]bool)
	for _, s := range c.Sources {
		key := s.Capability + "/" + s.Namespace
		r := routes[s.Profile]
		if !validCapability(s.Capability) || !provider.Identifier(s.Namespace) || sources[key] ||
			r.Profile == "" || !quotas[s.ProviderBucket] || !quotas[s.AccountBucket] {
			return errors.New("invalid or duplicate server source policy")
		}
		sources[key] = true
		for _, path := range r.Paths {
			if !identifiers([]string{c.GlobalBucket, s.ProviderBucket, s.AccountBucket, path.Bucket}) {
				return errors.New("a source must use distinct global, provider, account and egress quota buckets")
			}
		}
	}
	return nil
}
