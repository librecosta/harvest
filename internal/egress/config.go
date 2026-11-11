package egress

type Config struct {
	Profiles  []ProfileConfig  `mapstructure:"profiles"`
	Endpoints []EndpointConfig `mapstructure:"endpoints"`
}

type ProfileConfig struct {
	Name         string   `mapstructure:"name"`
	Mode         string   `mapstructure:"mode"`
	AllowDirect  bool     `mapstructure:"allow_direct"`
	DirectRegion string   `mapstructure:"direct_region"`
	Endpoints    []string `mapstructure:"endpoints"`
}

type EndpointConfig struct {
	ID              string `mapstructure:"id"`
	URL             string `mapstructure:"url"`
	Enabled         bool   `mapstructure:"enabled"`
	Region          string `mapstructure:"region"`
	Concurrency     int32  `mapstructure:"concurrency"`
	HealthTarget    string `mapstructure:"health_target"`
	CooldownSeconds int32  `mapstructure:"cooldown_seconds"`
	BytesPerMinute  int64  `mapstructure:"bytes_per_minute"`
}

type NetworkConfig struct {
	GlobalBucket string         `mapstructure:"global_bucket" json:"global_bucket"`
	Routes       []RouteConfig  `mapstructure:"routes" json:"routes"`
	Sources      []SourceConfig `mapstructure:"sources" json:"sources"`
	Quotas       []QuotaConfig  `mapstructure:"quotas" json:"quotas"`
}

type RouteConfig struct {
	Profile     string       `mapstructure:"profile" json:"profile"`
	Mode        string       `mapstructure:"mode" json:"mode"`
	AllowDirect bool         `mapstructure:"allow_direct" json:"allow_direct"`
	Paths       []PathConfig `mapstructure:"paths" json:"paths"`
}

type PathConfig struct {
	ID     string `mapstructure:"id" json:"id"`
	Kind   string `mapstructure:"kind" json:"kind"`
	Region string `mapstructure:"region" json:"region"`
	Bucket string `mapstructure:"bucket" json:"bucket"`
}

func (p PathConfig) Path() Path { return Path{p.ID, p.Kind, p.Region} }

type SourceConfig struct {
	Capability     string `mapstructure:"capability" json:"capability"`
	Namespace      string `mapstructure:"namespace" json:"namespace"`
	Profile        string `mapstructure:"profile" json:"profile"`
	ProviderBucket string `mapstructure:"provider_bucket" json:"provider_bucket"`
	AccountBucket  string `mapstructure:"account_bucket" json:"account_bucket"`
}

type QuotaConfig struct {
	ID          string `mapstructure:"id" json:"id"`
	Burst       int64  `mapstructure:"burst" json:"burst"`
	PerMinute   int64  `mapstructure:"per_minute" json:"per_minute"`
	Concurrency int64  `mapstructure:"concurrency" json:"concurrency"`
	DailyLimit  int64  `mapstructure:"daily_limit" json:"daily_limit,omitempty"`
}
