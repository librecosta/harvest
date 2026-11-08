package worker

import (
	"context"
	"net/http"
	"time"
	"uuid"

	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/execution"
)

type permitGate struct {
	client client
	claim  execution.Claim
}

func (g permitGate) Acquire(ctx context.Context, in egress.Intent) (egress.Grant, error) {
	ctx, cancel := context.WithTimeout(ctx, 1200*time.Millisecond)
	defer cancel()
	path := "/attempts/" + g.claim.AttemptID.String() + "/permit"
	body := execution.PermitRequest{Fence: g.claim.Fence, Intent: in}
	var out egress.Grant
	for range 2 {
		call, stop := context.WithTimeout(ctx, 550*time.Millisecond)
		ok, err := g.client.request(call, http.MethodPost, path, body, &out)
		stop()
		if err == nil && ok && (out.RetryMS > 0 || (execution.UUIDv7(out.ID) && out.LeaseMS > 0)) {
			return out, nil
		}
		if responseCode(err, "network_budget_exhausted") {
			return egress.Grant{}, egress.ErrBudget
		}
		if responseCode(err, "forbidden") {
			return egress.Grant{}, egress.ErrPolicy
		}
		if err == nil || !transient(err) || ctx.Err() != nil {
			break
		}
		wait(ctx, 100*time.Millisecond)
	}
	return egress.Grant{}, egress.ErrUnavailable
}

func (g permitGate) Finish(ctx context.Context, id uuid.UUID, usage egress.Usage) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	path := "/permits/" + id.String() + "/finish"
	body := execution.PermitFinish{Fence: g.claim.Fence, AttemptID: g.claim.AttemptID, Usage: usage}
	for range 2 {
		call, stop := context.WithTimeout(ctx, 450*time.Millisecond)
		_, err := g.client.request(call, http.MethodPost, path, body, nil)
		stop()
		if err == nil || !transient(err) || ctx.Err() != nil {
			return
		}
		wait(ctx, 100*time.Millisecond)
	}
}
