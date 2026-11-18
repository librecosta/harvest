package config

import (
	"errors"
	"github.com/librecosta/harvest/internal/egress"
)

// PolicyInput is a one-time maintenance command input, never server configuration.
type PolicyInput struct {
	Database   Database             `mapstructure:"database"`
	Principals []Principal          `mapstructure:"principals"`
	Network    egress.NetworkConfig `mapstructure:"network"`
}

func LoadPolicy(path string) (PolicyInput, error) {
	var cfg PolicyInput
	if err := decode(path, &cfg, map[string]any{
		"database.port": 5432, "database.tls_mode": "verify-full", "database.max_connections": 4,
	}); err != nil {
		return PolicyInput{}, err
	}
	runtime := Config{HTTP: HTTP{Listen: "127.0.0.1:8080"}, Database: cfg.Database, Principals: cfg.Principals}
	if err := runtime.validate(); err != nil {
		return PolicyInput{}, err
	}
	if err := validatePrincipals(runtime); err != nil {
		return PolicyInput{}, err
	}
	if err := ValidateNetwork(cfg.Network); err != nil {
		return PolicyInput{}, err
	}
	if len(cfg.Network.Routes) == 0 {
		return PolicyInput{}, errors.New("policy initialization requires a nonempty network policy")
	}
	return cfg, nil
}
