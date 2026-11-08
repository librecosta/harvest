//go:build integration

package integration

import (
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/execution"
	"github.com/librecosta/harvest/internal/provider"
)

type networkSubmit func(namespace, label, mode, region string) testJob
type networkState func(testJob, string) jobDetail

func assertWireQuota(t *testing.T, p *testPlane, submit networkSubmit, state networkState) {
	t.Helper()
	const principal = "net_wire_worker"
	reg := execution.Registration{ID: uuid.NewV7(), Offers: []provider.Offer{networkOffer}, Capacity: 1, Paths: []egress.Advertisement{
		{Profile: "local", Mode: "direct", ID: "direct", Kind: "direct", Region: "cn", State: "healthy", Capacity: 1},
	}}
	p.call("POST", "/internal/v1/workers/register", principal, "", reg, nil, 204)
	job := submit("net_wire", "wire_job", "ok", "cn")
	var claim execution.Claim
	p.call("POST", "/internal/v1/workers/"+reg.ID.String()+"/claim", principal, "", nil, &claim, 200)
	if claim.JobID != job.ID || claim.Network == nil {
		t.Fatal("wrong wire quota assignment")
	}
	path := "/internal/v1/attempts/" + claim.AttemptID.String() + "/permit"
	in := execution.PermitRequest{Fence: claim.Fence, Intent: egress.Intent{
		OperationID: uuid.NewV7(), Profile: "local", PathID: "direct", Origin: 0, Method: "GET", MaxBytes: 1024,
	}}
	p.call("POST", path, "operator", "", in, nil, 403)
	p.call("POST", path, "net_worker_a", "", in, nil, 404)
	unauthorized := in
	unauthorized.Profile = "socks_edge"
	p.call("POST", path, principal, "", unauthorized, nil, 403)
	unauthorized = in
	unauthorized.Origin = 2
	p.call("POST", path, principal, "", unauthorized, nil, 403)
	var a, duplicate egress.Grant
	p.call("POST", path, principal, "", in, &a, 200)
	p.call("POST", path, principal, "", in, &duplicate, 200)
	if !execution.UUIDv7(a.ID) || duplicate.ID != a.ID || !duplicate.ExpiresAt.Equal(a.ExpiresAt) {
		t.Fatal("permit replay changed receipt")
	}
	assertPermits(t, p, job, 1)
	changed := in
	changed.Origin = 2
	p.call("POST", path, principal, "", changed, nil, 409)
	in.OperationID = uuid.NewV7()
	var blocked egress.Grant
	p.call("POST", path, principal, "", in, &blocked, 200)
	if blocked.RetryMS < 1 || blocked.ID != uuid.Nil() {
		t.Fatal("unspent rate/concurrency bound bypassed")
	}
	var issued, total int
	err := p.pool.QueryRow(t.Context(), "SELECT issued, balance FROM harvest.quota_buckets WHERE key='net_wire'").Scan(&issued, &total)
	if err != nil || issued != 1 || total != 0 {
		t.Fatalf("replay or deferral debited twice: %d %d %v", issued, total, err)
	}

	// Advance only the disposable fixture's permit expiry. An uncertain
	// network request still consumes its rate token.
	if _, err := p.pool.Exec(t.Context(), "UPDATE harvest.quota_permits SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", a.ID); err != nil {
		t.Fatal(err)
	}
	p.call("POST", path, principal, "", in, &blocked, 200)
	if blocked.RetryMS < 1 || blocked.ID != uuid.Nil() {
		t.Fatal("expired permit refunded rate tokens")
	}
	if _, err := p.pool.Exec(t.Context(), "UPDATE harvest.quota_buckets SET refilled_at=clock_timestamp()-interval '1 minute' WHERE key='net_wire'"); err != nil {
		t.Fatal(err)
	}
	var b egress.Grant
	p.call("POST", path, principal, "", in, &b, 200)
	if !execution.UUIDv7(b.ID) || b.ID == a.ID {
		t.Fatal("expired permit still holds concurrency")
	}
	finish := execution.PermitFinish{Fence: claim.Fence, AttemptID: claim.AttemptID, Usage: egress.Usage{Outcome: "success", Bytes: 4}}
	complete := "/internal/v1/permits/" + b.ID.String() + "/finish"
	p.call("POST", complete, principal, "", finish, nil, 204)
	p.call("POST", complete, principal, "", finish, nil, 204)
	finish.Bytes++
	p.call("POST", complete, principal, "", finish, nil, 409)
	cancelJob(p, job)
	in.OperationID = uuid.NewV7()
	p.call("POST", path, principal, "", in, nil, 409)
	ack := execution.Completion{Fence: claim.Fence, OutputSchema: 1, Result: provider.Result{State: "cancelled", Code: "cancelled"}}
	p.call("POST", "/internal/v1/attempts/"+claim.AttemptID.String()+"/complete", principal, "", ack, nil, 200)
	state(job, "cancelled")
	p.call("POST", path, principal, "", in, nil, 409)

	// Pruning is bounded and leaves recent operation replays intact.
	if _, err := p.pool.Exec(t.Context(), "UPDATE harvest.quota_permits SET expires_at=clock_timestamp()-interval '7 minutes' WHERE id=$1", a.ID); err != nil {
		t.Fatal(err)
	}
	eventually(t, 12*time.Second, "expired permit pruned", func() bool {
		var n int
		if err := p.pool.QueryRow(t.Context(), "SELECT count(*) FROM harvest.quota_permits WHERE id=$1", a.ID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 0
	})
	assertPermits(t, p, job, 1)

	stale := submit("net_wire", "stale_permit", "ok", "cn")
	p.call("POST", "/internal/v1/workers/"+reg.ID.String()+"/claim", principal, "", nil, &claim, 200)
	if claim.JobID != stale.ID {
		t.Fatal("wrong stale fixture claim")
	}
	if _, err := p.pool.Exec(t.Context(), "UPDATE harvest.attempts SET lease_until=clock_timestamp()-interval '1 ms' WHERE id=$1", claim.AttemptID); err != nil {
		t.Fatal(err)
	}
	in.Fence = claim.Fence
	status, data, err := p.request("POST", "/internal/v1/attempts/"+claim.AttemptID.String()+"/permit", principal, "", in)
	if err != nil || status != 409 || !strings.Contains(string(data), "lease_lost") {
		t.Fatalf("stale lease authorized: %d %v", status, err)
	}
	cancelJob(p, stale)
}
