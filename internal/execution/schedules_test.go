package execution

import (
	"testing"
	"time"

	q "github.com/librecosta/harvest/internal/database/queries"
)

func TestAnchoredCadenceAndCronCoverage(t *testing.T) {
	anchor := time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		rule    Recurrence
		elapsed time.Duration
		gap     time.Duration
		missed  int32
	}{
		{Recurrence{IntervalSeconds: 60, AnchorAt: anchor, Policy: "state_refresh"}, 185 * time.Second, 5 * time.Second, 3},
		{Recurrence{Cron: "0 2 * * *", AnchorAt: anchor, Policy: "point_in_time"}, 30*24*time.Hour + 4*time.Minute, 4 * time.Minute, 30},
		{Recurrence{Cron: "* * * * *", AnchorAt: anchor, Policy: "state_refresh"}, 180*24*time.Hour + 4*time.Second, 4 * time.Second, cronCountLimit},
	} {
		c, err := parseCadence(test.rule, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		now := anchor.Add(test.elapsed)
		last, next, missed := c.slots(c.first(anchor), now)
		if last != now.Add(-test.gap) || !next.After(now) || missed != test.missed {
			t.Fatalf("got %v %v %d for %+v", last, next, missed, test.rule)
		}
	}
	for _, rule := range []Recurrence{
		{Cron: "CRON_TZ=Asia/Shanghai * * * * *", AnchorAt: anchor, Policy: "state_refresh"},
		{Cron: "@every 5s", AnchorAt: anchor, Policy: "state_refresh"},
		{Cron: "0 0 31 2 *", AnchorAt: anchor, Policy: "state_refresh"},
		{IntervalSeconds: 1, AnchorAt: anchor, Policy: "state_refresh"},
	} {
		if _, err := parseCadence(rule, time.Minute); err == nil {
			t.Errorf("accepted %+v", rule)
		}
	}
}

func TestRetryUsesBoundedDatabaseTime(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	j := q.HarvestJob{AttemptCount: 2, MaxAttempts: 3, DeadlineAt: now.Add(time.Minute)}
	if state, next := retryState(j, now, "failed", true); state != "retry_wait" || next != now.Add(2*time.Second) {
		t.Fatalf("retry result: %s %v", state, next)
	}
	j.DeadlineAt = now.Add(time.Second)
	if state, _ := retryState(j, now, "failed", true); state != "failed" {
		t.Fatal("scheduled retry past the deadline")
	}
	j.AttemptCount, j.DeadlineAt = 3, now.Add(time.Minute)
	if state, _ := retryState(j, now, "partial", true); state != "partial" {
		t.Fatal("retry exceeded its attempt budget")
	}
}
