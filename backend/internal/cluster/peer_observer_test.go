package cluster_test

import (
	"context"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

// TestRegisterPeerObserver_FiresOnRemoval spins up a 2-node cluster, removes
// the follower, and verifies the peer-observer channel receives a
// PeerObservation with Removed=true for that node. This is the integration
// proof that the observer wiring works against a real Raft.
func TestRegisterPeerObserver_FiresOnRemoval(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping multi-node cluster test in short mode")
	}

	node1 := newTestNode(t, "node-1", true)
	defer node1.close()
	waitLeader(t, node1.raft, 5*time.Second)

	node2 := newTestNode(t, "node-2", false)
	defer node2.close()

	// Register the peer observer on the leader BEFORE adding the follower,
	// so we also verify Added events don't corrupt state (we only look for
	// Removed).
	ch := make(chan hraft.Observation, 16)
	node1.srv.RegisterPeerObserver(ch)

	addVoter(t, node1.srv.Addr(), "node-2", node2.srv.Addr())

	// Wait for 2-voter config.
	deadline := time.After(5 * time.Second)
	for {
		cfg := node1.raft.GetConfiguration()
		if cfg.Error() == nil && len(cfg.Configuration().Servers) == 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for 2-node configuration")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Remove node-2.
	if err := node1.raft.RemoveServer(hraft.ServerID("node-2"), 0, 5*time.Second).Error(); err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}

	// Expect a PeerObservation with Removed=true for node-2 within a few
	// seconds. Drain other observations (adds) along the way.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for node-2 removal observation")
		case obs := <-ch:
			po, ok := obs.Data.(hraft.PeerObservation)
			if !ok {
				continue
			}
			if po.Removed && string(po.Peer.ID) == "node-2" {
				return // success
			}
		}
	}
}
