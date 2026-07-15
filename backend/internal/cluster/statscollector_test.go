package cluster

import (
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

// gastrolog-mliwrd: the cluster ingest/route series sums counters across
// TTL-live peer broadcasts. A peer whose stats expired and later resumed
// rejoined the sum as a one-tick jump that read as traffic — the UI showed
// 5m averages of 138K/s from a 40K/s source, which is arithmetically
// impossible for a true average. The summed window must re-anchor on any
// contributor-set change, exactly like the counter-reset branch.
func TestSummedWindowReanchorsOnMembershipChange(t *testing.T) {
	t.Parallel()
	s := &rateSeries{}
	now := time.Unix(1_700_000_000, 0)
	tick := func(sum int64, membership string) *gastrologv1.ThroughputRate {
		now = now.Add(5 * time.Second)
		s.observe(now, sum, membership, true)
		return s.emit()
	}

	// Steady state: 3 contributors, true rate 40K/s.
	s.observe(now, 0, "self,a,b", true) // seed
	var sum int64
	for range 24 { // 2 minutes of ticks
		sum += 200_000 // 40K/s * 5s
		tick(sum, "self,a,b")
	}

	// Peer b's stats expire: its 10M-record contribution leaves the sum.
	sum -= 10_000_000
	r := tick(sum, "self,a")
	if r.InstantPerSec != 0 {
		t.Fatalf("expiry tick instant = %v, want 0 (re-anchor, no sample)", r.InstantPerSec)
	}

	// One quiet tick with the smaller set.
	sum += 200_000
	tick(sum, "self,a")

	// Peer b resumes broadcasting: its cumulative counter rejoins the sum.
	sum += 10_000_000 + 200_000
	r = tick(sum, "self,a,b")
	if r.InstantPerSec != 0 {
		t.Fatalf("reappearance tick instant = %v/s, want 0 — the jump must not read as traffic", r.InstantPerSec)
	}
	for i, ew := range []float64{r.Avg_1MPerSec, r.Avg_5MPerSec, r.Avg_15MPerSec} {
		if ew > 45_000 {
			t.Fatalf("EWMA[%d] = %.0f/s after reappearance — exceeds the 40K/s source max (gastrolog-mliwrd regression)", i, ew)
		}
	}
}
