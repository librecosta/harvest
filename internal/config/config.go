package config

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
	"go.yaml.in/yaml/v3"

	"github.com/librecosta/harvest/internal/egress"
)

const (
	ProbeTimeout    = time.Second
	ShutdownTimeout = 10 * time.Second
)

type Config struct {
	HTTP        HTTP                 `mapstructure:"http"`
	Database    Database             `mapstructure:"database"`
	Principals  []Principal          `mapstructure:"principals"`
	LocalWorker *Worker              `mapstructure:"local_worker"`
	Network     egress.NetworkConfig `mapstructure:"network"`
	Retention   Retention            `mapstructure:"retention"`
}

type HTTP struct {
	Listen string `mapstructure:"listen"`
}

type Database struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Name     string `mapstructure:"name"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	TLSMode  string `mapstructure:"tls_mode"`
	MaxConns int    `mapstructure:"max_connections"`
}

func Load(path string) (Config, error) {
	var cfg Config
	r := DefaultRetention()
	err := decode(path, &cfg, map[string]any{
		"http.listen":              "127.0.0.1:8080",
		"database.port":            5432,
		"database.tls_mode":        "verify-full",
		"database.max_connections": 4,
		"retention.event_days":     r.EventDays,
		"retention.history_days":   r.HistoryDays,
		"retention.sample_days":    r.SampleDays,
		"retention.event_rows":     r.EventRows,
		"retention.event_bytes":    r.EventBytes,
		"retention.history_rows":   r.HistoryRows,
		"retention.history_bytes":  r.HistoryBytes,
	})
	if err != nil {
		return cfg, err
	}
	if err := cfg.validate(); err != nil {
		return cfg, err
	}
	if err := ValidateNetwork(cfg.Network); err != nil {
		return cfg, err
	}
	if err := cfg.Retention.validate(); err != nil {
		return cfg, err
	}
	return cfg, validatePrincipals(cfg)
}

func decode(path string, dst any, defaults map[string]any) error {
	switch filepath.Ext(path) {
	case ".yaml", ".yml":
	default:
		return errors.New("--config must name a .yaml or .yml file")
	}
	data, err := readFile(path)
	if err != nil {
		return err
	}
	// Reject constructs Viper can normalize or drop before checking unknown keys.
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var doc, extra yaml.Node
	if decoder.Decode(&doc) != nil || len(doc.Content) != 1 ||
		doc.Content[0].Kind != yaml.MappingNode || !plainYAML(doc.Content[0]) ||
		!errors.Is(decoder.Decode(&extra), io.EOF) {
		return errors.New("config must be one YAML mapping with unique lowercase keys and explicit values")
	}
	v := viper.New()
	v.SetConfigType("yaml")
	for key, value := range defaults {
		v.SetDefault(key, value)
	}
	if v.ReadConfig(bytes.NewReader(data)) != nil {
		return errors.New("invalid YAML configuration")
	}
	if v.UnmarshalExact(dst, func(dc *mapstructure.DecoderConfig) {
		dc.WeaklyTypedInput = false
		dc.DecodeHook = nil
	}) != nil {
		return errors.New("configuration contains unknown fields or invalid field types")
	}
	return nil
}

func readFile(path string) ([]byte, error) {
	const maxSize = 64 << 10
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("cannot read explicit YAML configuration file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot open explicit YAML configuration file")
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxSize+1))
	if err != nil || len(data) > maxSize {
		return nil, errors.New("configuration must be readable and at most 64 KiB")
	}
	return data, nil
}

func plainYAML(n *yaml.Node) bool {
	switch n.Kind {
	case yaml.MappingNode:
		if len(n.Content) == 0 {
			return false
		}
		seen := make(map[string]bool, len(n.Content)/2)
		for i := 0; i < len(n.Content); i += 2 {
			key := n.Content[i]
			if key.Kind != yaml.ScalarNode || key.Tag != "!!str" ||
				key.Value == "" || seen[key.Value] || !plainYAML(n.Content[i+1]) {
				return false
			}
			for _, char := range key.Value {
				if (char < 'a' || char > 'z') && char != '_' {
					return false
				}
			}
			seen[key.Value] = true
		}
		return true
	case yaml.SequenceNode:
		for _, child := range n.Content {
			if !plainYAML(child) {
				return false
			}
		}
		return true
	case yaml.ScalarNode:
		return n.Tag == "!!str" || n.Tag == "!!int" || n.Tag == "!!bool"
	default:
		return false
	}
}

func (cfg Config) validate() error {
	host, portText, err := net.SplitHostPort(cfg.HTTP.Listen)
	ip, ipErr := netip.ParseAddr(host)
	port, portErr := strconv.ParseUint(portText, 10, 16)
	if err != nil || ipErr != nil || ip.Zone() != "" || portErr != nil || port == 0 {
		return errors.New("http.listen must be an IP address and a port from 1 to 65535")
	}

	db := cfg.Database
	if db.Host == "" || strings.ContainsAny(db.Host, " /\\,[]%?#@\r\n\t\x00") ||
		(strings.Contains(db.Host, ":") && net.ParseIP(db.Host) == nil) ||
		db.Port < 1 || db.Port > 65535 {
		return errors.New("database.host and database.port must identify one TCP endpoint")
	}
	switch strings.ToLower(strings.TrimSpace(db.Name)) {
	case "", "postgres", "template0", "template1":
		return errors.New("database.name must identify a dedicated Harvest database")
	}
	if strings.TrimSpace(db.User) == "" || db.Password == "" ||
		strings.ContainsRune(db.Name+db.User+db.Password, 0) {
		return errors.New("database.user and database.password must be explicit nonempty strings")
	}
	if db.TLSMode != "verify-full" && db.TLSMode != "disable" {
		return errors.New("database.tls_mode must be verify-full or disable")
	}
	if db.MaxConns < 2 || db.MaxConns > 32 {
		return errors.New("database.max_connections must be between 2 and 32")
	}
	return nil
}

func (cfg HTTP) ProbeAddress() string {
	host, port, _ := net.SplitHostPort(cfg.Listen)
	switch host {
	case "0.0.0.0":
		host = "127.0.0.1"
	default:
		if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
			host = "::1"
		}
	}
	return net.JoinHostPort(host, port)
}
