package cluster

import (
	"io"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

// createTierRaft builds an in-process Raft instance for use in forwarder
// tests. The name is historical (kept to avoid churn in existing tests);
// in the current architecture this just builds a generic Raft group, not
// a per-tier one. New tests should use this directly rather than inventing
// parallel helpers.
func createTierRaft(t *testing.T, nodeID string, fsm hraft.FSM, bootstrap bool, members []hraft.Server) *hraft.Raft {
	t.Helper()
	_, trans := hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(nodeID), 500*time.Millisecond)

	cfg := hraft.DefaultConfig()
	cfg.LocalID = hraft.ServerID(nodeID)
	cfg.HeartbeatTimeout = 300 * time.Millisecond
	cfg.ElectionTimeout = 300 * time.Millisecond
	cfg.LeaderLeaseTimeout = 150 * time.Millisecond
	cfg.LogOutput = io.Discard

	store := hraft.NewInmemStore()
	snap := hraft.NewInmemSnapshotStore()

	r, err := hraft.NewRaft(cfg, fsm, store, store, snap, trans)
	if err != nil {
		t.Fatalf("NewRaft %s: %v", nodeID, err)
	}
	t.Cleanup(func() { _ = r.Shutdown().Error() })

	if bootstrap {
		boot := members
		if len(boot) == 0 {
			boot = []hraft.Server{{ID: hraft.ServerID(nodeID), Address: trans.LocalAddr()}}
		}
		if err := r.BootstrapCluster(hraft.Configuration{Servers: boot}).Error(); err != nil {
			t.Fatalf("BootstrapCluster %s: %v", nodeID, err)
		}
	}
	return r
}

// waitTierLeader blocks until r observes itself (or any member) as leader,
// up to timeout.
func waitTierLeader(t *testing.T, r *hraft.Raft, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r.State() == hraft.Leader {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no leader within %s", timeout)
}
