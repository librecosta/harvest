package execution

import (
	"context"
	json "encoding/json/v2"
	"slices"
	"time"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/egress"
)

const MaxHeartbeatBody = 96 << 10

type WorkerHealthBatch struct {
	Sequence int64                 `json:"sequence"`
	Reports  []egress.HealthReport `json:"reports"`
}

func storeWorkerHealth(ctx context.Context, db *q.Queries, w q.HarvestWorker, data []byte, b *WorkerHealthBatch) error {
	if b == nil {
		return nil
	}
	if w.NodeID == nil || b.Sequence < 1 || len(b.Reports) == 0 || len(b.Reports) > 4*egress.MaxPaths {
		return ErrInvalid
	}
	if b.Sequence <= w.HealthSequence {
		return nil
	}
	paths, err := decodePaths(data)
	if err != nil {
		return err
	}
	seen := make(map[[3]string]bool, len(b.Reports))
	for _, row := range b.Reports {
		key := [3]string{row.Profile, row.PathID, row.CheckType}
		total := row.Successes + row.Failures
		if seen[key] || !slices.ContainsFunc(paths, func(p egress.Advertisement) bool {
			return p.Profile == row.Profile && p.ID == row.PathID && p.Kind == row.Kind && p.Region == row.Region
		}) || !validHealthReport(row, total) {
			return ErrInvalid
		}
		seen[key] = true
	}
	encoded, err := json.Marshal(b.Reports)
	if err != nil {
		return err
	}
	if err := db.RecordWorkerHealth(ctx, q.RecordWorkerHealthParams{NodeID: *w.NodeID, Reports: encoded}); err != nil {
		return err
	}
	_, err = db.AcceptWorkerHealth(ctx, q.AcceptWorkerHealthParams{WorkerID: w.ID, Sequence: b.Sequence})
	return err
}

func validHealthReport(r egress.HealthReport, total int64) bool {
	if r.Successes < 0 || r.Successes > 1_000_000 || r.Failures < 0 || r.Failures > 1_000_000 || total < 1 || total > 1_000_000 ||
		r.TransportFailures < 0 || r.TransportFailures > r.Failures || r.TrailingFailures < 0 || r.TrailingFailures > r.Failures ||
		r.Recoveries < 0 || r.Recoveries > r.Successes || r.LastLatencyMS < 0 || r.LastLatencyMS > 5000 ||
		r.LatencyMS < int64(r.LastLatencyMS) || r.LatencyMS > total*5000 {
		return false
	}
	if r.LastOutcome == "ok" {
		if r.Successes == 0 || r.TrailingFailures != 0 {
			return false
		}
	} else if (r.LastOutcome != "transport_failure" && r.LastOutcome != "upstream_failure") || r.TrailingFailures == 0 ||
		(r.LastOutcome == "transport_failure" && r.TransportFailures == 0) ||
		(r.LastOutcome == "upstream_failure" && r.TransportFailures == r.Failures) ||
		(r.Successes == 0 && r.TrailingFailures != r.Failures) {
		return false
	}
	switch r.CheckType {
	case "proxy_connection":
		return r.Kind != egress.Direct && r.Failures == r.TransportFailures
	case "transport":
		return r.Recoveries == 0 && r.Failures == r.TransportFailures
	case "google_doh", "steam":
		return r.Recoveries == 0
	default:
		return false
	}
}

type WorkerHealthView struct {
	WindowHours int                 `json:"window_hours"`
	Checks      []WorkerHealthCheck `json:"checks"`
}

type WorkerHealthCheck struct {
	Profile             string    `json:"profile"`
	PathID              string    `json:"path_id"`
	Kind                string    `json:"kind"`
	Region              string    `json:"region"`
	CheckType           string    `json:"check_type"`
	LastOutcome         string    `json:"last_outcome"`
	LastAt              time.Time `json:"last_at"`
	LastLatencyMS       int32     `json:"last_latency_ms"`
	ConsecutiveFailures int64     `json:"consecutive_failures"`
	Successes           int64     `json:"successes"`
	Failures            int64     `json:"failures"`
	TransportFailures   int64     `json:"transport_failures"`
	LatencyMS           int64     `json:"latency_ms"`
	Recoveries          int64     `json:"recoveries"`
}

func (e *Engine) GetWorkerHealth(ctx context.Context, p config.Principal, namespace, id string) (WorkerHealthView, error) {
	if !config.ValidWorkerNodeID(id) {
		return WorkerHealthView{}, ErrInvalid
	}
	nodes, err := e.workerNodeRows(ctx, p, namespace, id, "", 1)
	if err != nil {
		return WorkerHealthView{}, err
	}
	if len(nodes.Items) == 0 {
		return WorkerHealthView{}, ErrNotFound
	}
	rows, err := e.queries.WorkerHealth(ctx, id)
	if err != nil {
		return WorkerHealthView{}, err
	}
	out := WorkerHealthView{WindowHours: 24, Checks: make([]WorkerHealthCheck, 0, len(rows))}
	for _, row := range rows {
		if slices.ContainsFunc(nodes.Items[0].Paths, func(p egress.Advertisement) bool {
			return p.Profile == row.Profile && p.ID == row.PathID && p.Kind == row.Kind && p.Region == row.Region
		}) {
			out.Checks = append(out.Checks, WorkerHealthCheck{
				Profile: row.Profile, PathID: row.PathID, Kind: row.Kind, Region: row.Region, CheckType: row.CheckType,
				LastOutcome: row.LastOutcome, LastAt: row.LastAt.UTC(), LastLatencyMS: row.LastLatencyMs,
				ConsecutiveFailures: row.ConsecutiveFailures, Successes: row.Successes, Failures: row.Failures,
				TransportFailures: row.TransportFailures, LatencyMS: row.LatencyMs, Recoveries: row.Recoveries,
			})
		}
	}
	return out, nil
}
