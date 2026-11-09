package provider

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"slices"
	"time"
)

const (
	MaxSegmentData    = 512 << 10
	MaxSegments       = 16
	MaxCompletionBody = 2 << 20
)

type SegmentScope struct {
	Namespace string `json:"namespace"`
	Scope
	Dimensions map[string]string `json:"dimensions,omitempty"`
}

type Segment struct {
	Scope        SegmentScope   `json:"scope"`
	OutputSchema int32          `json:"output_schema"`
	ObservedAt   time.Time      `json:"observed_at"`
	State        string         `json:"state"`
	Data         jsontext.Value `json:"data,omitzero"`
	Source       string         `json:"source"`
	Evidence     string         `json:"evidence,omitempty"`
	Withdraw     bool           `json:"withdraw,omitempty"`
}

// ObservationPolicy is part of a compiled capability version. Scopes derives
// the exact expected semantic coverage from the immutable Task.
type ObservationPolicy struct {
	Scopes        func(Task) []SegmentScope
	NormalizeData func(state string, data jsontext.Value) (jsontext.Value, error)
	// NormalizeScopedData returns the persisted data and its semantic projection.
	NormalizeScopedData     func(Task, SegmentScope, string, jsontext.Value) (data, semantic jsontext.Value, err error)
	NormalizeDiagnosticData func(string, jsontext.Value) (jsontext.Value, error)
	DiagnosticEvidence      []string
	Source                  string
	EmptyEvidence           string
	WithdrawEvidence        string
	MaxDataBytes            int
	Sample                  bool
}

func (p *ObservationPolicy) valid() bool {
	return p.Scopes != nil && (p.NormalizeData != nil || p.NormalizeScopedData != nil) && Identifier(p.Source) && p.MaxDataBytes > 0 &&
		!slices.ContainsFunc(p.DiagnosticEvidence, func(code string) bool { return !Identifier(code) }) && p.MaxDataBytes <= MaxSegmentData && (p.EmptyEvidence == "" || Identifier(p.EmptyEvidence)) &&
		(p.WithdrawEvidence == "" || !p.Sample && Identifier(p.WithdrawEvidence) && p.WithdrawEvidence != p.EmptyEvidence)
}

// ObservationObject decodes the typed value strictly. The capability
// policy additionally constrains the encoded size and semantic value.
func ObservationObject[T any](data jsontext.Value) (T, error) {
	var value T
	if len(data) == 0 || len(data) > MaxSegmentData || data[0] != '{' {
		return value, errors.New("invalid typed observation object")
	}
	err := json.Unmarshal(data, &value, json.RejectUnknownMembers(true))
	return value, err
}
