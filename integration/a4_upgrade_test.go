//go:build integration

package integration

import (
	"testing"
	"uuid"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/librecosta/harvest/db"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/execution"
)

func upgradeA3(t *testing.T, p *testPlane) (uuid.UUID, execution.PermitFinish) {
	t.Helper()
	conn := stdlib.OpenDBFromPool(p.pool)
	defer conn.Close()
	m, err := goose.NewProvider(goose.DialectPostgres, conn, db.Migrations, goose.WithTableName("public.goose_db_version"))
	if err != nil {
		t.Fatal(err)
	}
	if v, err := m.UpTo(t.Context(), 3); err != nil || len(v) != 3 {
		t.Fatalf("seed A3: %v %v", v, err)
	}
	target, job, worker, attempt, permit, observation := uuid.NewV7(), uuid.NewV7(), uuid.NewV7(), uuid.NewV7(), uuid.NewV7(), uuid.NewV7()
	hash := make([]byte, 32)
	a3Exec(t, p, "INSERT INTO harvest.targets (id,provider,resource_type,external_key,namespace) VALUES ($1,'fixture','key','a4-upgrade','scheduler')", target)
	a3Exec(t, p, `INSERT INTO harvest.jobs
		(id,target_id,namespace,capability,request_schema,execution_key,params,scope,fingerprint,cause,priority,available_at,deadline_at,state,attempt_count,generation,max_attempts)
		VALUES ($1,$2,'scheduler','fixture.run',1,'fixture.run:1:1:fixture_v1','{}','{}',$3,'manual',1,now(),now()+interval '1 day','running',1,1,2)`, job, target, hash)
	a3Exec(t, p, "INSERT INTO harvest.workers (id,principal,offers,egress_profiles,capacity) VALUES ($1,'upgrade',ARRAY['fixture.run:1:1:fixture_v1'],ARRAY[]::text[],1)", worker)
	a3Exec(t, p, "INSERT INTO harvest.attempts (id,job_id,worker_id,generation,claimed_at,heartbeat_at,lease_until) VALUES ($1,$2,$3,1,now(),now(),now()+interval '1 day')", attempt, job, worker)
	a3Exec(t, p, `INSERT INTO harvest.quota_buckets (key,balance,refilled_at,cooldown_until,metrics_from,issued,refused)
		VALUES ('net403',12345,now(),now()+interval '1 day',date_trunc('day',now(),'UTC'),11,3)`)
	a3Exec(t, p, `INSERT INTO harvest.quota_permits (id,attempt_id,operation_id,request_hash,bucket_keys,account_key,max_bytes,expires_at)
		VALUES ($1,$2,$3,$4,ARRAY['global_fixture','fixture_provider','net403','edge_a'],'net403',4096,now()+interval '1 day')`, permit, attempt, uuid.NewV7(), hash)
	a3Exec(t, p, `INSERT INTO harvest.change_heads (stream,epoch,last_position,event_bytes,total_event_bytes,history_rows,history_bytes)
		VALUES ('scheduler',$1,1,1024,1024,1,768)`, uuid.NewV7())
	a3Exec(t, p, `INSERT INTO harvest.observations (id,stream,target_id,capability,output_schema,scope_key,scope,state,
		observed_at,received_at,data,content_hash,source,evidence,sample,job_id,attempt_id,stored_bytes)
		VALUES ($1,'scheduler',$2,'fixture.detail',1,repeat('0',64),'{"namespace":"scheduler"}','present',
		now(),now(),'{"value":0}',$3,'fixture','',false,$4,$5,768)`, observation, target, hash, job, attempt)
	a3Exec(t, p, `INSERT INTO harvest.current_results (stream,target_id,capability,output_schema,scope_key,observation_id,content_hash,position,confirmed_at)
		VALUES ('scheduler',$1,'fixture.detail',1,repeat('0',64),$2,$3,1,now())`, target, observation, hash)
	a3Exec(t, p, `INSERT INTO harvest.change_events (stream,position,id,action,target_id,capability,output_schema,scope_key,observation_id,committed_at,stored_bytes)
		VALUES ('scheduler',1,$1,'upsert',$2,'fixture.detail',1,repeat('0',64),$3,now(),1024)`, uuid.NewV7(), target, observation)
	expectFailure(t, p.scratch, nil, p.bin, "schema is unavailable or incompatible", "fixture", "serve", "--local", "--config", p.config)
	if got := p.cli("migrate", "up", "--config", p.config); got != "Applied 2 Harvest migrations." {
		t.Fatal(got)
	}
	if n := a3Count(t, p, `SELECT count(*) FROM harvest.targets t JOIN harvest.jobs j ON j.target_id=t.id
		JOIN harvest.attempts a ON a.job_id=j.id JOIN harvest.quota_permits p ON p.attempt_id=a.id
		JOIN harvest.quota_buckets b ON b.key=p.account_key JOIN harvest.observations o ON o.attempt_id=a.id
		JOIN harvest.current_results c ON c.observation_id=o.id JOIN harvest.change_events e ON e.observation_id=o.id
		JOIN harvest.change_heads h ON h.stream=e.stream
		WHERE t.id=$1 AND a.network_revision=0 AND p.finished_at IS NULL AND p.expires_at>now()
		AND b.balance=12345 AND b.issued=11 AND b.refused=3 AND b.cooldown_until>now() AND h.last_position=1 AND c.position=1`, target); n != 1 {
		t.Fatal("A3 state changed during migration")
	}
	return permit, execution.PermitFinish{Fence: execution.Fence{JobID: job, WorkerID: worker, Generation: 1}, AttemptID: attempt, Usage: egress.Usage{Outcome: "transport"}}
}
