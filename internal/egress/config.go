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
	GlobalBucket string         `mapstructure:"global_bucket"`
	Routes       []RouteConfig  `mapstructure:"routes"`
	Sources      []SourceConfig `mapstructure:"sources"`
	Quotas       []QuotaConfig  `mapstructure:"quotas"`
}

type RouteConfig struct {
	Profile     string       `mapstructure:"profile"`
	Mode        string       `mapstructure:"mode"`
	AllowDirect bool         `mapstructure:"allow_direct"`
	Paths       []PathConfig `mapstructure:"paths"`
}

type PathConfig struct {
	ID     string `mapstructure:"id"`
	Kind   string `mapstructure:"kind"`
	Region string `mapstructure:"region"`
	Bucket string `mapstructure:"bucket"`
}

func (p PathConfig) Path() Path { return Path{p.ID, p.Kind, p.Region} }

type SourceConfig struct {
	Capability     string `mapstructure:"capability"`
	Namespace      string `mapstructure:"namespace"`
	Profile        string `mapstructure:"profile"`
	ProviderBucket string `mapstructure:"provider_bucket"`
	AccountBucket  string `mapstructure:"account_bucket"`
}

type QuotaConfig struct {
	ID          string `mapstructure:"id"`
	Burst       int64  `mapstructure:"burst"`
	PerMinute   int64  `mapstructure:"per_minute"`
	Concurrency int64  `mapstructure:"concurrency"`
}
