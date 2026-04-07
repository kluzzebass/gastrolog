package cluster

import (
	"io"
	"testing"
	"time"

	tierfsm "gastrolog/internal/tier/raftfsm"

	hraft "github.com/hashicorp/raft"
)

// --- Single-node happy: local leader applies successfully ---

func TestTierApplyForwarder_LocalLeader(t *testing.T) {
	t.Parallel()

	fsm := tierfsm.New()
	r := createTierRaft(t, "leader", fsm, true, nil)
	waitTierLeader(t, r, 5*time.Second)

	forwarder := NewTierApplyForwarder(r, "tier-local", nil, ReplicationTimeout)

	chunkID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	cmd := tierfsm.MarshalCreateChunk(chunkID, time.Now(), time.Now(), time.Now())
	if err := forwarder.Apply(cmd); err != nil {
		t.Fatalf("Apply on leader: %v", err)
	}

	if fsm.Get(chunkID) == nil {
		t.Error("expected chunk in FSM after local apply")
	}
	if !fsm.Ready() {
		t.Error("expected FSM Ready()=true after apply")
	}
}

// --- Single-node happy: multiple commands apply in order ---

func TestTierApplyForwarder_MultipleApplies(t *testing.T) {
	t.Parallel()

	fsm := tierfsm.New()
	r := createTierRaft(t, "leader", fsm, true, nil)
	waitTierLeader(t, r, 5*time.Second)

	forwarder := NewTierApplyForwarder(r, "tier-multi", nil, ReplicationTimeout)

	// Create, then seal.
	id := [16]byte{0xAA}
	now := time.Now()
	if err := forwarder.Apply(tierfsm.MarshalCreateChunk(id, now, now, now)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := forwarder.Apply(tierfsm.MarshalSealChunk(id, now, 100, 5000, now, now)); err != nil {
		t.Fatalf("seal: %v", err)
	}

	entry := fsm.Get(id)
	if entry == nil {
		t.Fatal("chunk not in FSM")
	}
	if !entry.Sealed {
		t.Error("expected sealed")
	}
	if entry.RecordCount != 100 {
		t.Errorf("expected 100 records, got %d", entry.RecordCount)
	}
}

// --- Single-node unhappy: no leader, not bootstrapped ---

func TestTierApplyForwarder_NoLeader(t *testing.T) {
	t.Parallel()

	fsm := tierfsm.New()
	r := createTierRaft(t, "lonely", fsm, false, nil)

	// No PeerConns — can't forward either.
	forwarder := NewTierApplyForwarder(r, "tier-none", nil, 2*time.Second)

	cmd := tierfsm.MarshalCreateChunk([16]byte{1}, time.Now(), time.Now(), time.Now())
	err := forwarder.Apply(cmd)
	if err == nil {
		t.Fatal("expected error when no leader and can't forward")
	}
}

// --- Single-node unhappy: leader shutdown mid-apply ---

func TestTierApplyForwarder_LeaderShutdown(t *testing.T) {
	t.Parallel()

	fsm := tierfsm.New()
	r := createTierRaft(t, "doomed", fsm, true, nil)
	waitTierLeader(t, r, 5*time.Second)

	forwarder := NewTierApplyForwarder(r, "tier-shutdown", nil, ReplicationTimeout)

	// Shut down the Raft instance.
	r.Shutdown()

	cmd := tierfsm.MarshalCreateChunk([16]byte{0xDD}, time.Now(), time.Now(), time.Now())
	err := forwarder.Apply(cmd)
	if err == nil {
		t.Fatal("expected error after leader shutdown")
	}
}

// --- helpers ---

func createTierRaft(t *testing.T, nodeID string, fsm hraft.FSM, bootstrap bool, members []hraft.Server) *hraft.Raft {
	t.Helper()

	conf := hraft.DefaultConfig()
	conf.LocalID = hraft.ServerID(nodeID)
	conf.HeartbeatTimeout = 500 * time.Millisecond
	conf.ElectionTimeout = 500 * time.Millisecond
	conf.LeaderLeaseTimeout = 250 * time.Millisecond
	conf.LogOutput = io.Discard

	store := hraft.NewInmemStore()
	snap := hraft.NewInmemSnapshotStore()
	_, trans := hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(nodeID), 1*time.Second)

	r, err := hraft.NewRaft(conf, fsm, store, store, snap, trans)
	if err != nil {
		t.Fatalf("create raft: %v", err)
	}
	t.Cleanup(func() { r.Shutdown() })

	if bootstrap {
		servers := []hraft.Server{{ID: hraft.ServerID(nodeID), Address: hraft.ServerAddress(nodeID)}}
		if len(members) > 0 {
			servers = members
		}
		r.BootstrapCluster(hraft.Configuration{Servers: servers})
	}

	return r
}

func waitTierLeader(t *testing.T, r *hraft.Raft, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r.State() == hraft.Leader {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for tier Raft leader")
}
