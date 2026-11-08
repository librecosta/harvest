package execution

import (
	"encoding/json/jsontext"
	"errors"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/provider"
)

var (
	ErrInvalid     = errors.New("invalid_request")
	ErrForbidden   = errors.New("forbidden")
	ErrNotFound    = errors.New("not_found")
	ErrConflict    = errors.New("conflict")
	ErrUnsupported = errors.New("capability_unavailable")
	ErrDraining    = errors.New("draining")
	ErrFence       = errors.New("lease_lost")
	ErrCancelled   = errors.New("cancel_requested")
	ErrTerminal    = errors.New("already_terminal")
)

const (
	LeaseTTL          = 12 * time.Second
	RenewInterval     = 3 * time.Second
	HeartbeatInterval = 5 * time.Second
	WorkerTTL         = 30 * time.Second
	PreDrain          = 5 * time.Second
	MaxBodySize       = 16 << 10
)

type Request struct {
	TargetID      uuid.UUID      `json:"target_id"`
	Capability    string         `json:"capability"`
	RequestSchema int32          `json:"request_schema"`
	Params        jsontext.Value `json:"params"`
	Scope         provider.Scope `json:"scope"`
}

type TargetInput struct {
	Provider     string `json:"provider"`
	ResourceType string `json:"resource_type"`
	ExternalKey  string `json:"external_key"`
	Namespace    string `json:"namespace"`
}

type Recurrence struct {
	IntervalSeconds int32     `json:"interval_seconds,omitempty"`
	Cron            string    `json:"cron,omitempty"`
	AnchorAt        time.Time `json:"anchor_at"`
	Policy          string    `json:"policy"`
}

type ScheduleInput struct {
	Request    Request    `json:"request"`
	Recurrence Recurrence `json:"recurrence"`
	Enabled    *bool      `json:"enabled,omitempty"`
}

type SchedulePatch struct {
	Revision   int64       `json:"revision"`
	Request    *Request    `json:"request,omitempty"`
	Recurrence *Recurrence `json:"recurrence,omitempty"`
	Enabled    *bool       `json:"enabled,omitempty"`
}

type Submit struct {
	Request    *Request   `json:"request,omitempty"`
	ScheduleID *uuid.UUID `json:"schedule_id,omitempty"`
}

type Registration struct {
	ID             uuid.UUID        `json:"id"`
	Offers         []provider.Offer `json:"offers"`
	EgressProfiles []string         `json:"egress_profiles,omitempty"`
	Capacity       int32            `json:"capacity"`
}

type Heartbeat struct {
	Draining bool `json:"draining"`
}

type Claim struct {
	Fence
	AttemptID  uuid.UUID     `json:"attempt_id"`
	LeaseUntil time.Time     `json:"lease_until"`
	LeaseMS    int64         `json:"lease_ms"`
	Task       provider.Task `json:"task"`
}

type Fence struct {
	JobID      uuid.UUID `json:"job_id"`
	WorkerID   uuid.UUID `json:"worker_id"`
	Generation int64     `json:"generation"`
}

type Renewal struct {
	LeaseUntil time.Time `json:"lease_until"`
	LeaseMS    int64     `json:"lease_ms"`
}

type Completion struct {
	Fence
	OutputSchema int32           `json:"output_schema"`
	Result       provider.Result `json:"result"`
}

type Receipt struct {
	AttemptID  uuid.UUID `json:"attempt_id"`
	JobID      uuid.UUID `json:"job_id"`
	State      string    `json:"state"`
	AcceptedAt time.Time `json:"accepted_at"`
}

type Page struct {
	Namespace string
	After     uuid.UUID
	Size      int32
}
