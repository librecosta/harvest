//go:build integration

package integration

import (
	"context"
	"crypto/rand"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/librecosta/harvest/internal/execution"
)

const consumerDB = "harvest_d_projection"
const consumerUser = "harvest_d_reader"

var errProjectionExpired = errors.New("Harvest replay expired")

type consumerRequest func(string, string, any) (int, []byte, error)

// The adapter has a consumer-only DB connection and an authenticated HTTP
// function. The fixture's privileged connections are kept outside the adapter.
type dConsumer struct {
	db     *pgxpool.Pool
	call   consumerRequest
	stream string
	id     uuid.UUID
}

type projectionState struct {
	Generation uuid.UUID
	Cursor     string
	Reset      *uuid.UUID
}

type targetIdentity struct {
	Provider     string
	ResourceType string
	ExternalKey  string
}

func newConsumerDB(t *testing.T, p *testPlane) *pgxpool.Config {
	t.Helper()
	password := rand.Text()
	a3Exec(t, p, "CREATE ROLE "+consumerUser+" LOGIN PASSWORD '"+password+"'")
	a3Exec(t, p, "CREATE DATABASE "+consumerDB+" OWNER "+consumerUser)
	a3Exec(t, p, "REVOKE CONNECT ON DATABASE "+pgx.Identifier{p.pool.Config().ConnConfig.Database}.Sanitize()+" FROM PUBLIC")
	cfg := p.pool.Config()
	cfg.ConnConfig.User, cfg.ConnConfig.Password, cfg.ConnConfig.Database = consumerUser, password, consumerDB
	cfg.MaxConns = 2
	forbidden := cfg.ConnConfig.Copy()
	forbidden.Database = p.pool.Config().ConnConfig.Database
	if other, err := pgx.ConnectConfig(t.Context(), forbidden); err == nil {
		other.Close(t.Context())
		t.Fatal("consumer role could connect to Harvest database")
	}
	return cfg
}

func openDConsumer(t *testing.T, cfg *pgxpool.Config, call consumerRequest, stream string, id uuid.UUID) dConsumer {
	t.Helper()
	db, err := pgxpool.NewWithConfig(t.Context(), cfg.Copy())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	_, err = db.Exec(t.Context(), `
CREATE TABLE IF NOT EXISTS consumer_cursor (
    stream text PRIMARY KEY, generation uuid NOT NULL, cursor text NOT NULL, reset uuid
);
CREATE TABLE IF NOT EXISTS projection (
    stream text NOT NULL, generation uuid NOT NULL, target_id uuid NOT NULL, capability text NOT NULL,
    output_schema integer NOT NULL, scope_key text NOT NULL, position bigint NOT NULL,
    provider text NOT NULL, resource_type text NOT NULL, external_key text NOT NULL,
    scope jsonb NOT NULL, value jsonb,
    PRIMARY KEY (stream, generation, target_id, capability, output_schema, scope_key)
)`)
	if err != nil {
		t.Fatal(err)
	}
	return dConsumer{db, call, stream, id}
}

func (c dConsumer) api(method, path string, in, out any) error {
	status, data, err := c.call(method, path, in)
	if err != nil {
		return err
	}
	if status == http.StatusGone {
		return errProjectionExpired
	}
	if status != http.StatusOK {
		return fmt.Errorf("consumer HTTP %d for %s", status, strings.Split(path, "?")[0])
	}
	return json.Unmarshal(data, out)
}

func (c dConsumer) control(suffix string) string {
	return "/api/v1/consumers/" + c.id.String() + suffix
}

func (c dConsumer) query() url.Values {
	return url.Values{"stream": {c.stream}, "consumer": {c.id.String()}, "limit": {"2"}}
}

func (c dConsumer) state(ctx context.Context, tx pgx.Tx, lock bool) (s projectionState, err error) {
	query := "SELECT generation,cursor,reset FROM consumer_cursor WHERE stream=$1"
	if lock {
		query += " FOR UPDATE"
	}
	var db interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	} = c.db
	if tx != nil {
		db = tx
	}
	err = db.QueryRow(ctx, query, c.stream).Scan(&s.Generation, &s.Cursor, &s.Reset)
	return
}

func (c dConsumer) ack(ctx context.Context) error {
	s, err := c.state(ctx, nil, false)
	if err != nil {
		return err
	}
	var result execution.ConsumerView
	err = c.api("POST", c.control("/ack"), execution.ConsumerAck{Stream: c.stream, Cursor: s.Cursor, Reset: s.Reset}, &result)
	if err != nil {
		return err
	}
	if result.ACK != s.Cursor || result.ResetRequired {
		return errors.New("unexpected Harvest ACK")
	}
	if s.Reset != nil {
		_, err = c.db.Exec(ctx, "UPDATE consumer_cursor SET reset=NULL WHERE stream=$1 AND generation=$2 AND cursor=$3", c.stream, s.Generation, s.Cursor)
	}
	return err
}

type projectionRecord struct {
	execution.ResultIdentity
	position int64
	scope    jsontext.Value
	value    *execution.ObservationView
	target   targetIdentity
}

func (c dConsumer) identity(id uuid.UUID, cache map[uuid.UUID]targetIdentity) (targetIdentity, error) {
	if target, ok := cache[id]; ok {
		return target, nil
	}
	var target struct {
		Provider     string `json:"provider"`
		ResourceType string `json:"resource_type"`
		ExternalKey  string `json:"external_key"`
		Namespace    string `json:"namespace"`
	}
	if err := c.api("GET", "/api/v1/targets/"+id.String(), nil, &target); err != nil {
		return targetIdentity{}, err
	}
	if target.Namespace != c.stream || target.Provider == "" || target.ResourceType == "" || target.ExternalKey == "" {
		return targetIdentity{}, errors.New("invalid target identity")
	}
	out := targetIdentity{target.Provider, target.ResourceType, target.ExternalKey}
	cache[id] = out
	return out, nil
}

func (c dConsumer) put(ctx context.Context, tx pgx.Tx, generation uuid.UUID, rows []projectionRecord) error {
	for _, row := range rows {
		var value []byte
		var err error
		if row.value != nil {
			value, err = json.Marshal(row.value)
			if err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, `INSERT INTO projection VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
ON CONFLICT (stream,generation,target_id,capability,output_schema,scope_key) DO UPDATE
SET position=EXCLUDED.position, scope=EXCLUDED.scope, value=EXCLUDED.value
WHERE projection.position<EXCLUDED.position`,
			c.stream, generation, row.TargetID, row.Capability, row.OutputSchema, row.ScopeKey, row.position,
			row.target.Provider, row.target.ResourceType, row.target.ExternalKey, []byte(row.scope), value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c dConsumer) records(events []execution.ChangeView, cache map[uuid.UUID]targetIdentity) ([]projectionRecord, error) {
	rows := make([]projectionRecord, 0, len(events))
	for _, e := range events {
		scope := e.TombstoneScope
		switch e.Action {
		case "withdraw":
			if len(scope) == 0 || e.Observation != nil {
				return nil, errors.New("invalid withdrawal")
			}
		case "sample", "upsert":
			if e.Observation == nil || e.Observation.ResultIdentity != e.ResultIdentity {
				return nil, errors.New("invalid change observation")
			}
			scope = e.Observation.Scope
		default:
			return nil, errors.New("unknown change action")
		}
		target, err := c.identity(e.TargetID, cache)
		if err != nil {
			return nil, err
		}
		rows = append(rows, projectionRecord{e.ResultIdentity, e.Position, scope, e.Observation, target})
	}
	return rows, nil
}

func (c dConsumer) changes(after, through string, reset *uuid.UUID) (execution.ChangesView, error) {
	query := c.query()
	query.Set("after", after)
	if reset != nil {
		query.Set("reset", reset.String())
		query.Set("through", through)
	}
	var page execution.ChangesView
	err := c.api("GET", "/api/v1/changes?"+query.Encode(), nil, &page)
	if err == nil && (page.Next == "" || page.Next == after && (page.HasMore || len(page.Items) > 0)) {
		err = errors.New("change cursor did not advance")
	}
	return page, err
}

// Reset stages each bounded page in the consumer DB. Publication and the
// durable reset receipt are one transaction. A later call ACKs that receipt.
func (c dConsumer) reset(ctx context.Context, betweenPages func()) error {
	var reset execution.ResetView
	if err := c.api("POST", c.control("/reset"), map[string]string{"stream": c.stream}, &reset); err != nil {
		return err
	}
	// An interrupted staging generation is disposable. Preserve the active one.
	if _, err := c.db.Exec(ctx, `DELETE FROM projection p WHERE stream=$1 AND NOT EXISTS (
SELECT 1 FROM consumer_cursor c WHERE c.stream=p.stream AND c.generation=p.generation)`, c.stream); err != nil {
		return err
	}
	stage := func(rows []projectionRecord) error {
		return pgx.BeginFunc(ctx, c.db, func(tx pgx.Tx) error { return c.put(ctx, tx, reset.Reset, rows) })
	}
	cache := make(map[uuid.UUID]targetIdentity)
	query := c.query()
	query.Set("reset", reset.Reset.String())
	var through string
	for pages := 0; ; pages++ {
		if pages == 256 {
			return errors.New("fixture snapshot page budget exhausted")
		}
		var page execution.CurrentView
		if err := c.api("GET", "/api/v1/snapshots/current?"+query.Encode(), nil, &page); err != nil {
			return err
		}
		rows := make([]projectionRecord, 0, len(page.Items))
		for _, item := range page.Items {
			target, err := c.identity(item.TargetID, cache)
			if err != nil {
				return err
			}
			rows = append(rows, projectionRecord{item.ResultIdentity, item.Position, item.Scope, &item.ObservationView, target})
		}
		if err := stage(rows); err != nil {
			return err
		}
		if pages == 0 && betweenPages != nil {
			if page.After == "" {
				return errors.New("fixture expected more than one snapshot page")
			}
			betweenPages()
		}
		if page.After == "" {
			through = page.Head
			break
		}
		query.Set("after", page.After)
	}
	cursor := reset.Baseline
	for pages := 0; cursor != through; pages++ {
		if pages == 256 {
			return errors.New("fixture replay page budget exhausted")
		}
		changes, err := c.changes(cursor, through, &reset.Reset)
		if err != nil {
			return err
		}
		rows, err := c.records(changes.Items, cache)
		if err != nil {
			return err
		}
		if err := stage(rows); err != nil {
			return err
		}
		cursor = changes.Next
	}
	return pgx.BeginFunc(ctx, c.db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO consumer_cursor VALUES ($1,$2,$3,$2)
ON CONFLICT (stream) DO UPDATE SET generation=EXCLUDED.generation,cursor=EXCLUDED.cursor,reset=EXCLUDED.reset`,
			c.stream, reset.Reset, cursor); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "DELETE FROM projection WHERE stream=$1 AND generation<>$2", c.stream, reset.Reset)
		return err
	})
}

func (c dConsumer) apply(ctx context.Context, from string, page execution.ChangesView) error {
	rows, err := c.records(page.Items, make(map[uuid.UUID]targetIdentity))
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, c.db, func(tx pgx.Tx) error {
		s, err := c.state(ctx, tx, true)
		if err != nil || s.Cursor == page.Next {
			return err
		}
		if s.Cursor != from || s.Reset != nil {
			return errors.New("local cursor changed before publication")
		}
		if err := c.put(ctx, tx, s.Generation, rows); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "UPDATE consumer_cursor SET cursor=$2 WHERE stream=$1", c.stream, page.Next)
		return err
	})
}

func (c dConsumer) drain(ctx context.Context) error {
	if err := c.ack(ctx); err != nil {
		return err
	}
	s, err := c.state(ctx, nil, false)
	if err != nil {
		return err
	}
	for pages := 0; pages < 256; pages++ {
		page, err := c.changes(s.Cursor, "", nil)
		if err != nil {
			return err
		}
		if err := c.apply(ctx, s.Cursor, page); err != nil {
			return err
		}
		s.Cursor = page.Next
		if !page.HasMore {
			return c.ack(ctx)
		}
	}
	return errors.New("fixture incremental page budget exhausted")
}
