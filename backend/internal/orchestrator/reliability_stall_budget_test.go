package orchestrator_test

import (
	"testing"
	"time"
)

// The stall budget decides when a convergence wait is allowed to fail. Counting
// seconds gets this wrong in the one situation that matters: under full-suite
// load the sweeps that would produce progress are themselves delayed, so a
// wall-clock budget buys fewer of them exactly when it needs to buy more, and a
// starved harness is reported as a wedged cluster.
//
// Counting the cluster's own sweeps removes that coupling. These tests pin the
// three properties that makes true, because a clean run proves nothing here —
// this suite has been closed on clean runs before and reopened.

// sweepSource is a stand-in for the harness's per-node schedulers: a set of
// (node, job) pairs whose LastRun can be advanced on demand.
type sweepSource struct {
	pairs map[string]time.Time
}

func newSweepSource(pairs ...string) *sweepSource {
	s := &sweepSource{pairs: map[string]time.Time{}}
	base := time.Unix(1000, 0)
	for _, p := range pairs {
		s.pairs[p] = base
	}
	return s
}

// advanceAll sweeps every pair once. A sweep is only counted
// if the watcher OBSERVES the advance, so callers must poll between sweeps —
// exactly as the wait loop does, where polling is faster than the one-second
// sweep cadence.
func (s *sweepSource) advanceAll() {
	for k, v := range s.pairs {
		s.pairs[k] = v.Add(time.Second)
	}
}

// counterOver drives the REAL sweepCounter against the stand-in, so these
// tests exercise the counting rule that ships rather than a copy of it.
func counterOver(s *sweepSource) *sweepCounter {
	return newSweepCounter(func() map[string]time.Time {
		out := make(map[string]time.Time, len(s.pairs))
		for k, v := range s.pairs {
			out[k] = v
		}
		return out
	})
}

// A budget of N sweeps PER JOB must mean that regardless of how many jobs the
// cluster runs. A raw sweep total does not: a four-node harness at the test
// profile's one-second cadence observes tens per second, so a flat
// budget expires in about one second — which is how the first attempt at this
// fix made every pipeline test fail inside two seconds.
func TestStallBudgetScalesWithJobCount(t *testing.T) {
	t.Parallel()
	const perJob = 20

	for _, jobs := range [][]string{
		{"n1:retention"},
		{"n1:retention", "n1:catchup", "n2:retention", "n2:catchup"},
		{"n1:a", "n1:b", "n1:c", "n2:a", "n2:b", "n2:c", "n3:a", "n3:b", "n3:c", "n4:a", "n4:b", "n4:c"},
	} {
		src := newSweepSource(jobs...)
		c := counterOver(src)
		anchor := c.observe()

		// One sweep per job short of the budget must NOT be a stall.
		for range perJob - 1 {
			src.advanceAll()
			c.observe()
		}
		if idle, budget := c.observe()-anchor, perJob*c.pairs(); idle >= budget {
			t.Errorf("%d jobs: %d sweeps/job tripped a %d-per-job budget (idle=%d budget=%d)",
				len(jobs), perJob-1, perJob, idle, budget)
		}
		// The full budget must.
		src.advanceAll()
		c.observe()
		if idle, budget := c.observe()-anchor, perJob*c.pairs(); idle < budget {
			t.Errorf("%d jobs: %d sweeps/job did not reach a %d-per-job budget (idle=%d budget=%d)",
				len(jobs), perJob, perJob, idle, budget)
		}
	}
}

// The property the whole change exists for: a cluster that is not running burns
// no budget. Wall-clock does the opposite — it drains fastest precisely when the
// machine is too loaded to make progress.
func TestStallBudgetIsNotConsumedByAStarvedCluster(t *testing.T) {
	t.Parallel()
	src := newSweepSource("n1:retention", "n2:retention", "n3:retention")
	c := counterOver(src)
	anchor := c.observe()

	// Wall-clock passes; the cluster runs no sweeps.
	for range 100 {
		if idle := c.observe() - anchor; idle != 0 {
			t.Fatalf("a starved cluster consumed %d sweeps of budget", idle)
		}
	}
	if idle, budget := c.observe()-anchor, 20*c.pairs(); idle >= budget {
		t.Fatal("a starved cluster reached the stall budget; it would be reported as wedged")
	}
}

// And the converse, so the budget is not simply unreachable: a cluster that
// keeps sweeping without the metric moving is a real wedge and must still fail.
func TestStallBudgetIsReachedByATickingClusterMakingNoProgress(t *testing.T) {
	t.Parallel()
	src := newSweepSource("n1:retention", "n2:retention")
	c := counterOver(src)
	anchor := c.observe()

	for range 20 {
		src.advanceAll()
		c.observe()
	}
	idle, budget := c.observe()-anchor, 20*c.pairs()
	if idle < budget {
		t.Fatalf("a sweeping cluster with no progress did not reach the budget (idle=%d budget=%d) — "+
			"a real wedge would hang until the hard backstop", idle, budget)
	}
}

// A job registering after the wait starts must not be miscounted as a sweep: it
// has no previous LastRun to have advanced from.
func TestStallBudgetIgnoresNewlyRegisteredJobs(t *testing.T) {
	t.Parallel()
	src := newSweepSource("n1:retention")
	c := counterOver(src)
	anchor := c.observe()

	src.pairs["n1:archival"] = time.Unix(2000, 0)
	if idle := c.observe() - anchor; idle != 0 {
		t.Errorf("registering a job counted as %d sweeps; only an ADVANCE is a sweep", idle)
	}
}
