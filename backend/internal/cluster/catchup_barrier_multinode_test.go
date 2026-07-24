package cluster_test

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/glid"
)

// TestFourNodeFollowerCatchupBarrier is the multi-node regression test for the
// event-driven startup FSM catch-up (gastrolog-1go57). On a 4-node cluster a
// follower that has joined an established cluster must, after committing a
// catch-up barrier (store.Barrier — the primitive WaitForFSMCatchup uses),
// have its local FSM caught up to every entry committed cluster-wide before
// the barrier returned. The barrier is forwarded to the leader and released
// by the follower's FSM apply notification, never by polling: the read-back
// below uses NO retry and NO sleep.
func TestFourNodeFollowerCatchupBarrier(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping multi-node cluster test in short mode")
	}

	nodes := fourNodeCluster(t)
	leader := waitStableLeader(t, nodes, 5*time.Second)

	// Commit fresh state on the leader after the cluster is formed, so a
	// follower is not guaranteed to have replicated it yet. The catch-up
	// barrier must drive each follower's FSM past this write.
	id := glid.New()
	putReplProbe(t, leader, id, "catchup-"+id.String(), "on leader")

	for _, follower := range followersOf(nodes) {
		if err := follower.store.Barrier(context.Background()); err != nil {
			t.Fatalf("catch-up barrier on follower %s: %v", follower.srv.Addr(), err)
		}
		got := readBackNow(t, follower, id, "after catch-up barrier")
		if got.MaxAge == nil || *got.MaxAge != dummyMaxAge {
			t.Errorf("follower read wrong policy after catch-up barrier: %+v", got)
		}
	}
}
