//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func testA3Commits(t *testing.T, p *testPlane) {
	for _, rollback := range []bool{false, true} {
		t.Run(fmt.Sprintf("rollback=%t", rollback), func(t *testing.T) {
			child := *p
			child.t = t
			p := &child
			wA, wB, wPrivate := p.a3Register("worker_a"), p.a3Register("worker_b"), p.a3Register("private_worker")
			a := p.register("fixture", "key", fmt.Sprintf("gate-a-%t", rollback), "public")
			b := p.register("fixture", "key", fmt.Sprintf("gate-b-%t", rollback), "public")
			private := p.register("fixture", "key", fmt.Sprintf("gate-private-%t", rollback), "private")
			params := observationParams{Mode: "present", Value: 11}
			first, payloadA := p.a3Claim("worker_a", wA, a, sampleOffer, params)
			second, payloadB := p.a3Claim("worker_b", wB, b, sampleOffer, params)
			third, payloadPrivate := p.a3Claim("private_worker", wPrivate, private, sampleOffer, params)
			head := a3Position(t, a3ReadCurrent(t, p, "public").Head)
			const lock = int64(811403)
			conn, err := p.pool.Acquire(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Release()
			_, err = conn.Exec(t.Context(), "SELECT pg_advisory_lock($1)", lock)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", lock)
			failure := ""
			if rollback {
				failure = "RAISE EXCEPTION 'forced integration rollback';"
			}
			a3Exec(t, p, fmt.Sprintf(`CREATE FUNCTION harvest.a3_gate() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN IF NEW.job_id='%s' AND NEW.state='succeeded' THEN
				PERFORM pg_advisory_xact_lock(%d::bigint); %s END IF; RETURN NEW; END $$`, first.JobID, lock, failure))
			a3Exec(t, p, `CREATE TRIGGER a3_gate BEFORE UPDATE ON harvest.attempts FOR EACH ROW EXECUTE FUNCTION harvest.a3_gate()`)
			type response struct {
				status int
				data   []byte
				err    error
			}
			doneA, doneB := make(chan response, 1), make(chan response, 1)
			go func() {
				status, data, err := p.request("POST", completePath(first), "worker_a", "", payloadA)
				doneA <- response{status, data, err}
			}()
			eventually(t, time.Second, "first completion waiting after event write", func() bool {
				return a3Count(t, p, `SELECT count(*) FROM pg_stat_activity
					WHERE $1 = ANY(pg_blocking_pids(pid)) AND wait_event='advisory'`, int32(conn.Conn().PgConn().PID())) == 1
			})
			go func() {
				status, data, err := p.request("POST", completePath(second), "worker_b", "", payloadB)
				doneB <- response{status, data, err}
			}()
			eventually(t, time.Second, "second completion waiting on change head", func() bool {
				return a3Count(t, p, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock'
					AND query LIKE '%LockChangeHead%' AND cardinality(pg_blocking_pids(pid))>0`) == 1
			})
			p.call("POST", completePath(third), "private_worker", "", payloadPrivate, nil, 200)
			if got := a3Position(t, a3ReadCurrent(t, p, "public").Head); got != head {
				t.Fatalf("read saw uncommitted head %d (previous %d)", got, head)
			}
			_, err = conn.Exec(t.Context(), "SELECT pg_advisory_unlock($1)", lock)
			if err != nil {
				t.Fatal(err)
			}
			resultA, resultB := <-doneA, <-doneB
			wantA := 200
			if rollback {
				wantA = 503
			}
			if resultA.status != wantA || resultB.status != 200 || resultA.err != nil || resultB.err != nil {
				t.Fatalf("completion A=%+v B=%+v", resultA, resultB)
			}
			a3Exec(t, p, "DROP TRIGGER a3_gate ON harvest.attempts")
			a3Exec(t, p, "DROP FUNCTION harvest.a3_gate()")
			wantB := head + 2
			if rollback {
				wantB--
				if a3Count(t, p, "SELECT count(*) FROM harvest.observations WHERE target_id=$1", a) != 0 ||
					p.job(first.JobID).Attempts[0].State != "running" {
					t.Fatal("failed transaction exposed result or durable receipt")
				}
			} else if got := a3Count(t, p, "SELECT position FROM harvest.change_events WHERE target_id=$1", a); got != head+1 {
				t.Fatalf("first position=%d", got)
			}
			if got := a3Count(t, p, "SELECT position FROM harvest.change_events WHERE target_id=$1", b); got != wantB {
				t.Fatalf("second position=%d want %d", got, wantB)
			}
			if rollback {
				p.call("POST", completePath(first), "worker_a", "", payloadA, nil, 200)
				if got := a3Count(t, p, "SELECT position FROM harvest.change_events WHERE target_id=$1", a); got != wantB+1 {
					t.Fatal("rollback left a gap")
				}
			}
		})
	}
}
