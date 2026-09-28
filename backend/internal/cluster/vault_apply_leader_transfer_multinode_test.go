package cluster_test

// A forwarded vault-ctl apply must survive a leadership transfer. A
// follower's view of the leader trails the transfer by a heartbeat or two,
// so a forward issued in that window resolves the node that just stepped
// down; the forwarder has to re-resolve and reach the successor rather than
// surface the refusal. This test hunts that window deliberately — kick a
// transfer, catch a follower still naming the old leader after it stepped
// down, forward through exactly that follower — instead of running enough
// iterations to meet the window by luck.

import (
	"errors"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

func TestFourNodeVaultCtlForwardSurvivesLeadershipTransfer(t *testing.T) {
	if testing.Short() {
		t.Skip("drives a four-node vault-ctl group through repeated leadership transfers")
	}
	nodes := fourNodeCluster(t)
	vaultID := glid.New()
	gid, groups := setupVaultCtlGroup(t, nodes, vaultID)

	const attempts = 25
	caught := 0
	for range attempts {
		old := waitVaultCtlLeader(t, groups, 10*time.Second)

		// Kick the transfer without waiting for it to finish, then hunt for
		// a follower whose view still names the old leader AFTER the old
		// leader has stepped down.
		fut := old.raft.LeadershipTransfer()

		var stale *vaultCtlTestGroup
		hunt := time.Now().Add(2 * time.Second)
		for time.Now().Before(hunt) {
			if old.raft.State() == hraft.Leader {
				continue // old still leads; window not open yet
			}
			for _, g := range groups {
				if g == old {
					continue
				}
				if _, id := g.raft.LeaderWithID(); string(id) == old.node.id {
					stale = g
					break
				}
			}
			if stale != nil {
				break
			}
		}
		_ = fut.Error()
		if stale == nil {
			continue // window not caught this round
		}
		caught++

		cid := chunk.NewChunkID()
		now := time.Now()
		fwd := cluster.NewVaultCtlChunkApplyForwarder(
			stale.raft, gid, vaultID, stale.fsm.ApplyWait(),
			stale.node.srv.PeerConns(), cluster.ReplicationTimeout)
		// The forwarder absorbs the transfer inside its own budget; an
		// ErrNoRaftLeader here means that budget ended mid-election, which a
		// caller may retry. Any other error during a healthy transfer means
		// the forwarder gave up on a cluster that has a leader.
		retryBudget := time.Now().Add(10 * time.Second)
		for {
			err := fwd.Apply(vaultctlfsm.MarshalCreateChunk(cid, now, now, now))
			if err == nil {
				break
			}
			if errors.Is(err, cluster.ErrNoRaftLeader) && time.Now().Before(retryBudget) {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			t.Fatalf("forward through follower with stale view (window caught on round %d): %v", caught, err)
		}
	}
	t.Logf("windows caught: %d of %d transfers", caught, attempts)
	if caught == 0 {
		t.Skip("stale-view window never observed; premise unmet")
	}
}
