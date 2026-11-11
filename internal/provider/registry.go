// Package provider defines the compiled capability boundary used by the
// control plane and workers. Registry entries do not own the database.
package provider

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/egress"
)

const MaxValueSize = 12 << 10

var identifier = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

func Identifier(s string) bool { return identifier.MatchString(s) }

type Offer struct {
	Capability     string `json:"capability" mapstructure:"capability"`
	RequestSchema  int32  `json:"request_schema" mapstructure:"request_schema"`
	OutputSchema   int32  `json:"output_schema" mapstructure:"output_schema"`
	Implementation string `json:"implementation" mapstructure:"implementation"`
}

func (o Offer) Key() string {
	return fmt.Sprintf("%s:%d:%d:%s", o.Capability, o.RequestSchema, o.OutputSchema, o.Implementation)
}

type Scope struct {
	Locale       string `json:"locale,omitempty"`
	Region       string `json:"region,omitempty"`
	EgressRegion string `json:"egress_region,omitempty"`
}

func (s Scope) Valid() bool {
	for _, value := range []string{s.Locale, s.Region, s.EgressRegion} {
		if len(value) > 35 || strings.IndexFunc(value, func(r rune) bool {
			return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_')
		}) >= 0 {
			return false
		}
	}
	return true
}

type Task struct {
	WebPath     string         `json:"web_path,omitempty"`
	AssetSizes  []string       `json:"asset_sizes,omitempty"`
	TargetID    uuid.UUID      `json:"target_id"`
	Provider    string         `json:"provider"`
	Resource    string         `json:"resource_type"`
	ExternalKey string         `json:"external_key"`
	Namespace   string         `json:"namespace"`
	Offer       Offer          `json:"offer"`
	Params      jsontext.Value `json:"params"`
	Scope       Scope          `json:"scope"`
}

type Result struct {
	State    string         `json:"state"`
	Code     string         `json:"code,omitempty"`
	Data     jsontext.Value `json:"data,omitempty"`
	Segments []Segment      `json:"segments,omitempty"`
}

type Resource struct {
	Name      string
	Normalize func(string) (string, error)
}

type Capability struct {
	Offer
	Resource        string
	MinInterval     time.Duration
	Timeout         time.Duration
	Partial         bool
	PointInTimeOnly bool
	NormalizeParams func(jsontext.Value) (jsontext.Value, error)
	NormalizeOutput func(jsontext.Value) (jsontext.Value, error)
	Observation     *ObservationPolicy
	Network         *egress.Source
	Execute         func(context.Context, Task, egress.Client) Result
}

type Manifest struct {
	Name         string
	Resources    []Resource
	Capabilities []Capability
}

type Description struct {
	Provider     string   `json:"provider"`
	Resources    []string `json:"resources"`
	Capabilities []Info   `json:"capabilities"`
}

type Info struct {
	Offer
	Resource           string `json:"resource_type"`
	Status             string `json:"status"`
	MinIntervalSeconds int32  `json:"min_interval_seconds"`
}

type Registry struct {
	manifests map[string]Manifest
	caps      map[string]Capability
}

func New(manifests ...Manifest) (*Registry, error) {
	r := &Registry{manifests: make(map[string]Manifest), caps: make(map[string]Capability)}
	for _, m := range manifests {
		if !Identifier(m.Name) || r.manifests[m.Name].Name != "" || len(m.Resources) == 0 {
			return nil, errors.New("invalid provider registration")
		}
		resources := make(map[string]bool)
		for _, res := range m.Resources {
			if !Identifier(res.Name) || res.Normalize == nil || resources[res.Name] {
				return nil, errors.New("invalid resource registration")
			}
			resources[res.Name] = true
		}
		for _, c := range m.Capabilities {
			name, operation, ok := strings.Cut(c.Capability, ".")
			if !ok || name != m.Name || !Identifier(operation) || !resources[c.Resource] ||
				c.RequestSchema < 1 || c.OutputSchema < 1 || !Identifier(c.Implementation) ||
				c.MinInterval < time.Second || c.Timeout < time.Second || c.Timeout > 2*time.Minute ||
				c.NormalizeParams == nil || (c.Observation == nil && c.NormalizeOutput == nil) || r.caps[c.Capability].Capability != "" ||
				(c.Network != nil && !c.Network.Valid()) || (c.Observation != nil && !c.Observation.valid()) {
				return nil, errors.New("invalid capability registration")
			}
			r.caps[c.Capability] = c
		}
		r.manifests[m.Name] = Manifest{m.Name, slices.Clone(m.Resources), slices.Clone(m.Capabilities)}
	}
	return r, nil
}

func (r *Registry) Normalize(provider, resource, key string) (string, error) {
	if len(key) > 1024 {
		return "", errors.New("external key exceeds size limit")
	}
	for _, res := range r.manifests[provider].Resources {
		if res.Name == resource {
			return res.Normalize(key)
		}
	}
	return "", errors.New("unknown provider or resource")
}

func (r *Registry) Capability(name string) (Capability, bool) {
	c, ok := r.caps[name]
	return c, ok
}

func (r *Registry) ExecutableKeys() []string {
	var keys []string
	for _, c := range r.caps {
		if c.Execute != nil {
			keys = append(keys, c.Key())
		}
	}
	return keys
}

func (r *Registry) Describe() []Description {
	var result []Description
	for _, m := range r.manifests {
		d := Description{Provider: m.Name, Resources: []string{}, Capabilities: []Info{}}
		for _, res := range m.Resources {
			d.Resources = append(d.Resources, res.Name)
		}
		for _, c := range m.Capabilities {
			status := "not_implemented"
			if c.Execute != nil {
				status = "executable"
			}
			d.Capabilities = append(d.Capabilities, Info{c.Offer, c.Resource, status, int32(c.MinInterval / time.Second)})
		}
		result = append(result, d)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Provider < result[j].Provider })
	return result
}

// Typed normalizers can share the strict wire decoder without accepting
// arbitrary objects or reflection-based request rules.
func Object[T any](value jsontext.Value) (T, error) {
	var dst T
	data := bytes.TrimSpace(value)
	if len(data) < 2 || data[0] != '{' || len(data) > MaxValueSize ||
		json.Unmarshal(data, &dst, json.RejectUnknownMembers(true)) != nil {
		return dst, errors.New("invalid typed JSON object")
	}
	return dst, nil
}

func Normalize[T any](value jsontext.Value) (jsontext.Value, error) {
	dst, err := Object[T](value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(dst)
}
