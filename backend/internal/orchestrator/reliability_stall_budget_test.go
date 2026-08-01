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
// Counting the cluster's own ticks removes that coupling. These tests pin the
// three properties that makes true, because a clean run proves nothing here —
// this suite has been closed on clean runs before and reopened.

// tickSource is a stand-in for the harness's per-node schedulers: a set of
// (node, job) pairs whose LastRun can be advanced on demand.
type tickSource struct {
	pairs map[string]time.Time
}

func newTickSource(pairs ...string) *tickSource {
	s := &tickSource{pairs: map[string]time.Time{}}
	base := time.Unix(1000, 0)
	for _, p := range pairs {
		s.pairs[p] = base
	}
	return s
}

// advanceAll ticks every pair once, which is one round. A tick is only counted
// if the watcher OBSERVES the advance, so callers must poll between rounds —
// exactly as the wait loop does, where polling is faster than the one-second
// sweep cadence.
func (s *tickSource) advanceAll() {
	for k, v := range s.pairs {
		s.pairs[k] = v.Add(time.Second)
	}
}

// watcherOver drives the REAL clusterTickWatcher against the stand-in, so these
// tests exercise the counting rule that ships rather than a copy of it.
func watcherOver(s *tickSource) *clusterTickWatcher {
	return newTickWatcher(func() map[string]time.Time {
		out := make(map[string]time.Time, len(s.pairs))
		for k, v := range s.pairs {
			out[k] = v
		}
		return out
	})
}

// A budget of N rounds must mean N rounds regardless of how many jobs the
// cluster runs. A raw tick total does not: a four-node harness at the test
// profile's one-second cadence emits tens of ticks per second, so a fixed tick
// budget expires in about one second — which is how the first attempt at this
// fix made every pipeline test fail inside two seconds.
func TestStallBudgetScalesWithJobCount(t *testing.T) {
	t.Parallel()
	const rounds = 20

	for _, jobs := range [][]string{
		{"n1:retention"},
		{"n1:retention", "n1:catchup", "n2:retention", "n2:catchup"},
		{"n1:a", "n1:b", "n1:c", "n2:a", "n2:b", "n2:c", "n3:a", "n3:b", "n3:c", "n4:a", "n4:b", "n4:c"},
	} {
		src := newTickSource(jobs...)
		c := watcherOver(src)
		anchor := c.observe()

		// Exactly one round short of the budget must NOT be a stall.
		for range rounds - 1 {
			src.advanceAll()
			c.observe()
		}
		if idle, budget := c.observe()-anchor, rounds*c.pairs(); idle >= budget {
			t.Errorf("%d jobs: %d rounds tripped a %d-round budget (idle=%d budget=%d)",
				len(jobs), rounds-1, rounds, idle, budget)
		}
		// The full budget must.
		src.advanceAll()
		c.observe()
		if idle, budget := c.observe()-anchor, rounds*c.pairs(); idle < budget {
			t.Errorf("%d jobs: %d rounds did not reach a %d-round budget (idle=%d budget=%d)",
				len(jobs), rounds, rounds, idle, budget)
		}
	}
}

// The property the whole change exists for: a cluster that is not running burns
// no budget. Wall-clock does the opposite — it drains fastest precisely when the
// machine is too loaded to make progress.
func TestStallBudgetIsNotConsumedByAStarvedCluster(t *testing.T) {
	t.Parallel()
	src := newTickSource("n1:retention", "n2:retention", "n3:retention")
	c := watcherOver(src)
	anchor := c.observe()

	// Wall-clock passes; the cluster does not tick.
	for range 100 {
		if idle := c.observe() - anchor; idle != 0 {
			t.Fatalf("a starved cluster consumed %d ticks of budget", idle)
		}
	}
	if idle, budget := c.observe()-anchor, 20*c.pairs(); idle >= budget {
		t.Fatal("a starved cluster reached the stall budget; it would be reported as wedged")
	}
}

// And the converse, so the budget is not simply unreachable: a cluster that
// keeps ticking without the metric moving is a real wedge and must still fail.
func TestStallBudgetIsReachedByATickingClusterMakingNoProgress(t *testing.T) {
	t.Parallel()
	src := newTickSource("n1:retention", "n2:retention")
	c := watcherOver(src)
	anchor := c.observe()

	for range 20 {
		src.advanceAll()
		c.observe()
	}
	idle, budget := c.observe()-anchor, 20*c.pairs()
	if idle < budget {
		t.Fatalf("a ticking cluster with no progress did not reach the budget (idle=%d budget=%d) — "+
			"a real wedge would hang until the hard backstop", idle, budget)
	}
}

// A job registering after the wait starts must not be miscounted as a tick: it
// has no previous LastRun to have advanced from.
func TestStallBudgetIgnoresNewlyRegisteredJobs(t *testing.T) {
	t.Parallel()
	src := newTickSource("n1:retention")
	c := watcherOver(src)
	anchor := c.observe()

	src.pairs["n1:archival"] = time.Unix(2000, 0)
	if idle := c.observe() - anchor; idle != 0 {
		t.Errorf("registering a job counted as %d ticks; only an ADVANCE is a tick", idle)
	}
}
