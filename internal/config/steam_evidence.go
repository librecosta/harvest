package config

import "errors"

type SteamEvidence struct {
	MaxVersions    int32 `mapstructure:"max_versions_per_scope" json:"max_versions_per_scope"`
	HistoryDays    int   `mapstructure:"history_days" json:"history_days"`
	NamespaceBytes int64 `mapstructure:"namespace_budget_bytes" json:"namespace_budget_bytes"`
	GlobalBytes    int64 `mapstructure:"global_budget_bytes" json:"global_budget_bytes"`
	PruneBatch     int32 `mapstructure:"prune_batch_size" json:"prune_batch_size"`
}

func DefaultSteamEvidence() SteamEvidence {
	return SteamEvidence{MaxVersions: 5, HistoryDays: 90, NamespaceBytes: 256 << 20, GlobalBytes: 1 << 30, PruneBatch: 32}
}

func (c SteamEvidence) validate() error {
	if c.MaxVersions < 1 || c.MaxVersions > 5 || c.HistoryDays < 1 || c.HistoryDays > 365 ||
		c.NamespaceBytes < 512<<10 || c.GlobalBytes < c.NamespaceBytes || c.GlobalBytes > 64<<30 ||
		c.PruneBatch < 1 || c.PruneBatch > 128 {
		return errors.New("invalid steam_evidence version, age, byte budget or prune batch")
	}
	return nil
}
