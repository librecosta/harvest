//go:build integration

package integration

import (
	"strings"
	"testing"
	"uuid"
)

func measureA3Plans(t *testing.T, p *testPlane) {
	t.Helper()
	const count = 8192
	ids, targets, events := make([]uuid.UUID, count), make([]uuid.UUID, count), make([]uuid.UUID, count)
	for i := range count {
		ids[i], targets[i], events[i] = uuid.NewV7(), uuid.NewV7(), uuid.NewV7()
	}
	// Synthetic cardinality fixture only. Execution acceptance goes through
	// the real HTTP/Worker path in TestA3.
	tx, err := p.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context())
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := tx.Exec(t.Context(), sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO harvest.change_heads (stream,epoch,last_position,event_bytes,total_event_bytes,history_rows,history_bytes)
		VALUES ('plan_a3',$1,$2,$2::bigint*1100,$2::bigint*1100,$2::bigint,$2::bigint*900)`, uuid.NewV7(), count)
	exec(`INSERT INTO harvest.observations (id,stream,target_id,capability,output_schema,scope_key,scope,state,
		observed_at,received_at,data,content_hash,source,evidence,sample,job_id,attempt_id,stored_bytes)
		SELECT id,'plan_a3',target,'fixture.detail',1,repeat('a',64),'{"namespace":"plan_a3"}','present',
		now(),now()-n*interval '1 second','{"value":0}',decode(repeat('00',32),'hex'),'fixture','',false,$3,$4,900
		FROM unnest($1::uuid[],$2::uuid[]) WITH ORDINALITY AS seed(id,target,n)`, ids, targets, uuid.NewV7(), uuid.NewV7())
	exec(`INSERT INTO harvest.current_results (stream,target_id,capability,output_schema,scope_key,observation_id,content_hash,position,confirmed_at)
		SELECT 'plan_a3',target,'fixture.detail',1,repeat('a',64),id,decode(repeat('00',32),'hex'),n,now()
		FROM unnest($1::uuid[],$2::uuid[]) WITH ORDINALITY AS seed(id,target,n)`, ids, targets)
	exec(`INSERT INTO harvest.change_events (stream,position,id,action,target_id,capability,output_schema,scope_key,observation_id,committed_at,stored_bytes)
		SELECT 'plan_a3',n,event,'upsert',target,'fixture.detail',1,repeat('a',64),id,now(),1100
		FROM unnest($1::uuid[],$2::uuid[],$3::uuid[]) WITH ORDINALITY AS seed(id,target,event,n)`, ids, targets, events)
	exec("ANALYZE harvest.observations,harvest.current_results,harvest.change_events")
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, plan := range []struct {
		name, sql, index string
		args             []any
	}{
		{"current", `SELECT c.target_id,c.capability,c.output_schema,c.scope_key,c.observation_id,c.position,c.confirmed_at,o.stored_bytes
			FROM harvest.current_results c JOIN harvest.observations o ON o.id=c.observation_id
			WHERE c.stream='plan_a3' AND (c.target_id,c.capability,c.output_schema,c.scope_key)>($1,'fixture.detail',1,repeat('a',64))
			ORDER BY c.target_id,c.capability,c.output_schema,c.scope_key LIMIT 51`, "current_results_pkey", []any{targets[count/2]}},
		{"history", `SELECT id,received_at,stored_bytes FROM harvest.observations WHERE stream='plan_a3'
			AND (received_at,id) < (now(),'ffffffff-ffff-ffff-ffff-ffffffffffff')
			AND received_at >= 'epoch' AND received_at <= now() ORDER BY received_at DESC,id DESC LIMIT 51`, "observations_history", nil},
		{"changes", `SELECT * FROM harvest.change_events WHERE stream='plan_a3' AND position>4096 AND position<=8192
			ORDER BY position LIMIT 51`, "change_events_pkey", nil},
	} {
		rows, err := p.pool.Query(t.Context(), "EXPLAIN (ANALYZE,BUFFERS) "+plan.sql, plan.args...)
		if err != nil {
			t.Fatal(err)
		}
		var lines []string
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			lines = append(lines, line)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if result := strings.Join(lines, "\n"); !strings.Contains(result, plan.index) || !strings.Contains(result, "Buffers:") {
			t.Fatalf("%s plan lost bounded index scan:\n%s", plan.name, result)
		} else {
			t.Logf("8,192-row %s plan:\n%s", plan.name, result)
		}
	}
}
