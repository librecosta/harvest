package config

import (
	"errors"
	"net/netip"
	"net/url"
	"slices"
	"strings"

	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/provider"
)

type Principal struct {
	Name           string   `mapstructure:"name"`
	Token          string   `mapstructure:"token"`
	Role           string   `mapstructure:"role"`
	Namespaces     []string `mapstructure:"namespaces"`
	Capabilities   []string `mapstructure:"capabilities"`
	EgressProfiles []string `mapstructure:"egress_profiles"`
	MaxSlots       int32    `mapstructure:"max_slots"`
}

type Worker struct {
	ControlURL     string           `mapstructure:"control_url"`
	Token          string           `mapstructure:"token"`
	Offers         []provider.Offer `mapstructure:"offers"`
	EgressProfiles []string         `mapstructure:"egress_profiles"`
	Concurrency    int32            `mapstructure:"concurrency"`
	Egress         egress.Config    `mapstructure:"egress"`
}

func LoadWorker(path string) (Worker, error) {
	var cfg Worker
	if err := decode(path, &cfg, nil); err != nil {
		return cfg, err
	}
	return cfg, validateWorker(cfg)
}

func validateWorker(w Worker) error {
	u, err := url.Parse(w.ControlURL)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" ||
		u.ForceQuery || strings.Contains(w.ControlURL, "#") || (u.Path != "" && u.Path != "/") {
		return errors.New("control_url must be an origin without credentials, query or fragment")
	}
	if u.Scheme != "https" && (u.Scheme != "http" || !privateHost(u.Hostname())) {
		return errors.New("worker control_url requires HTTPS or a private IP for HTTP")
	}
	if !validToken(w.Token) || w.Concurrency < 1 || w.Concurrency > 16 ||
		len(w.Offers) < 1 || len(w.Offers) > 32 || !identifiers(w.EgressProfiles) {
		return errors.New("invalid worker token, concurrency, offers or egress profiles")
	}
	seen := make(map[string]bool)
	for _, offer := range w.Offers {
		if !validCapability(offer.Capability) || offer.RequestSchema < 1 || offer.OutputSchema < 1 ||
			!provider.Identifier(offer.Implementation) || seen[offer.Capability] {
			return errors.New("invalid or duplicate worker offer")
		}
		seen[offer.Capability] = true
	}
	return validateEgress(w.Egress)
}

func privateHost(host string) bool {
	ip, err := netip.ParseAddr(host)
	if err != nil || ip.Zone() != "" {
		return false
	}
	ip = ip.Unmap()
	return ip.IsLoopback() || ip.IsPrivate() ||
		netip.MustParsePrefix("100.64.0.0/10").Contains(ip)
}

func validatePrincipals(cfg Config) error {
	if len(cfg.Principals) > 64 {
		return errors.New("at most 64 service principals may be configured")
	}
	names, tokens := make(map[string]bool), make(map[string]bool)
	for _, p := range cfg.Principals {
		if !provider.Identifier(p.Name) || !validToken(p.Token) || names[p.Name] || tokens[p.Token] ||
			len(p.Namespaces) == 0 || !identifiers(p.Namespaces) || !identifiers(p.EgressProfiles) {
			return errors.New("invalid or duplicate service principal, token or scope")
		}
		names[p.Name], tokens[p.Token] = true, true
		if len(p.Capabilities) > 32 {
			return errors.New("too many capability grants")
		}
		for i, c := range p.Capabilities {
			if !validCapability(c) || slices.Contains(p.Capabilities[:i], c) {
				return errors.New("invalid or duplicate capability grant")
			}
		}
		switch p.Role {
		case "reader", "writer", "operator":
			if p.MaxSlots != 0 || len(p.EgressProfiles) != 0 {
				return errors.New("consumer principals cannot grant worker slots or egress profiles")
			}
		case "worker":
			if p.MaxSlots < 1 || p.MaxSlots > 16 || len(p.Capabilities) == 0 {
				return errors.New("worker principal must grant capabilities and 1 to 16 slots")
			}
		default:
			return errors.New("invalid service principal role")
		}
	}
	if cfg.LocalWorker != nil {
		if err := validateWorker(*cfg.LocalWorker); err != nil {
			return err
		}
		u, _ := url.Parse(cfg.LocalWorker.ControlURL)
		if u.Scheme != "http" || u.Host != cfg.HTTP.ProbeAddress() {
			return errors.New("local_worker control_url must point to this server's local HTTP listener")
		}
		for _, p := range cfg.Principals {
			if p.Role == "worker" && p.Token == cfg.LocalWorker.Token {
				return nil
			}
		}
		return errors.New("local_worker requires a configured worker principal")
	}
	return nil
}

func identifiers(values []string) bool {
	if len(values) > 32 {
		return false
	}
	for i, v := range values {
		if !provider.Identifier(v) || slices.Contains(values[:i], v) {
			return false
		}
	}
	return true
}

func validCapability(value string) bool {
	name, operation, ok := strings.Cut(value, ".")
	return ok && provider.Identifier(name) && provider.Identifier(operation)
}

func validToken(value string) bool {
	return len(value) >= 32 && len(value) <= 128 && strings.IndexFunc(value, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_')
	}) < 0
}
