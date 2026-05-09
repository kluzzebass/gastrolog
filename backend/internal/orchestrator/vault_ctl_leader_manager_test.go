package orchestrator

import (
	"gastrolog/internal/glid"
	"io"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/raftgroup"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// makeSingleNodeVaultGroup builds a single-node Raft group using in-memory
// transport + storage. Returns the group, the FSM, and a cleanup func.
func makeSingleNodeVaultGroup(t *testing.T, nodeID string) (*raftgroup.Group, *vaultctlfsm.FSM, func()) {
	t.Helper()

	fsm := vaultctlfsm.New()
	_, trans := hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(nodeID), 1*time.Second)

	conf := hraft.DefaultConfig()
	conf.LocalID = hraft.ServerID(nodeID)
	conf.HeartbeatTimeout = 200 * time.Millisecond
	conf.ElectionTimeout = 200 * time.Millisecond
	conf.LeaderLeaseTimeout = 100 * time.Millisecond
	conf.LogOutput = io.Discard

	store := hraft.NewInmemStore()
	snap := hraft.NewInmemSnapshotStore()

	r, err := hraft.NewRaft(conf, fsm, store, store, snap, trans)
	if err != nil {
		t.Fatalf("create raft: %v", err)
	}

	// Bootstrap as a single-node cluster.
	bootCfg := hraft.Configuration{
		Servers: []hraft.Server{
			{ID: hraft.ServerID(nodeID), Address: hraft.ServerAddress(nodeID)},
		},
	}
	if err := r.BootstrapCluster(bootCfg).Error(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	// Wait for leadership.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if r.State() == hraft.Leader {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if r.State() != hraft.Leader {
		_ = r.Shutdown().Error()
		t.Fatal("did not become leader within 3s")
	}

	g := &raftgroup.Group{Raft: r, FSM: fsm}

	cleanup := func() {
		_ = r.Shutdown().Error()
	}
	return g, fsm, cleanup
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestVaultCtlLeaderManager_StartStopIdempotent(t *testing.T) {
	t.Parallel()

	g, _, cleanup := makeSingleNodeVaultGroup(t, "node-1")
	defer cleanup()

	mgr := newVaultCtlLeaderManager(discardLogger())
	defer mgr.StopAll()

	instID := glid.New()

	// Start twice — second is a no-op.
	mgr.Start(instID, g)
	mgr.Start(instID, g)

	// Stop and start again — should re-register cleanly.
	mgr.Stop(instID)
	mgr.Start(instID, g)
	mgr.Stop(instID)

	// Stopping a inst with no loop should be safe.
	mgr.Stop(instID)
}

// TestVaultCtlLeaderManager_ReconcileAddsMissingMember verifies that the leader
// epoch's reconcile pass calls AddVoter when the desired member list contains
// a node that's not in the current Raft configuration.
//
// We use a single-node group + a synthetic peer address. AddVoter writes the
// configuration change locally even though the synthetic peer is unreachable
// (the change is committed via the local node's quorum-of-one). We verify the
// new member appears in GetConfiguration.
func TestVaultCtlLeaderManager_ReconcileAddsMissingMember(t *testing.T) {
	t.Parallel()

	g, _, cleanup := makeSingleNodeVaultGroup(t, "leader-add")
	defer cleanup()

	mgr := newVaultCtlLeaderManager(discardLogger())
	defer mgr.StopAll()

	instID := glid.New()

	// Desired set = current (just the leader) + a synthetic second member.
	mgr.SetDesiredMembers(instID, []hraft.Server{
		{ID: "leader-add", Address: "leader-add"},
		{ID: "synthetic-peer", Address: "synthetic-addr"},
	})

	mgr.Start(instID, g)

	// Wait for the reconcile pass to add the synthetic peer.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		future := g.Raft.GetConfiguration()
		if err := future.Error(); err == nil {
			for _, srv := range future.Configuration().Servers {
				if string(srv.ID) == "synthetic-peer" {
					return // success
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("synthetic peer was not added to Raft configuration within 5s")
}

// TestVaultCtlLeaderManager_ReconcileRemovesExtras verifies that the leader
// epoch's reconcile pass calls RemoveServer when a member is in the current
// configuration but not in the desired set. We need a real 2-voter cluster
// (so the configuration change can commit) plus a synthetic 3rd doomed
// voter that we want removed.
func TestVaultCtlLeaderManager_ReconcileRemovesExtras(t *testing.T) {
	t.Parallel()

	// Build a 2-real-node cluster: alive-1 and alive-2.
	groups, cleanup := makeTwoNodeVaultGroup(t, "alive-1", "alive-2")
	defer cleanup()
	leader := groups[0]

	// Add a synthetic 3rd voter (doomed). With 2 alive + 1 dead, the
	// majority is 2 (the two alive nodes), so the AddVoter commits even
	// though doomed never acks. After the add, removing doomed also
	// commits via the same 2-of-3 majority.
	if err := leader.Raft.AddVoter("doomed", "doomed-addr", 0, 5*time.Second).Error(); err != nil {
		t.Fatalf("seed AddVoter: %v", err)
	}

	mgr := newVaultCtlLeaderManager(discardLogger())
	defer mgr.StopAll()

	instID := glid.New()

	// Desired set = just the two alive nodes. doomed should be removed.
	mgr.SetDesiredMembers(instID, []hraft.Server{
		{ID: "alive-1", Address: "alive-1"},
		{ID: "alive-2", Address: "alive-2"},
	})

	mgr.Start(instID, leader)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		future := leader.Raft.GetConfiguration()
		if err := future.Error(); err == nil {
			found := false
			for _, srv := range future.Configuration().Servers {
				if string(srv.ID) == "doomed" {
					found = true
					break
				}
			}
			if !found {
				return // success
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("doomed peer was not removed from Raft configuration within 5s")
}

// makeTwoNodeVaultGroup builds a 2-node inst Raft cluster using in-memory
// transport. Returns the groups (group[0] is the leader after election)
// and a cleanup func.
func makeTwoNodeVaultGroup(t *testing.T, id1, id2 string) ([]*raftgroup.Group, func()) {
	t.Helper()

	ids := []string{id1, id2}
	fsms := make([]*vaultctlfsm.FSM, 2)
	rafts := make([]*hraft.Raft, 2)
	transports := make([]*hraft.InmemTransport, 2)

	members := []hraft.Server{
		{ID: hraft.ServerID(id1), Address: hraft.ServerAddress(id1)},
		{ID: hraft.ServerID(id2), Address: hraft.ServerAddress(id2)},
	}

	for i, nid := range ids {
		_, trans := hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(nid), 1*time.Second)
		transports[i] = trans
	}
	transports[0].Connect(hraft.ServerAddress(id2), transports[1])
	transports[1].Connect(hraft.ServerAddress(id1), transports[0])

	for i, nid := range ids {
		fsms[i] = vaultctlfsm.New()

		conf := hraft.DefaultConfig()
		conf.LocalID = hraft.ServerID(nid)
		conf.HeartbeatTimeout = 200 * time.Millisecond
		conf.ElectionTimeout = 200 * time.Millisecond
		conf.LeaderLeaseTimeout = 100 * time.Millisecond
		conf.LogOutput = io.Discard

		store := hraft.NewInmemStore()
		snap := hraft.NewInmemSnapshotStore()

		r, err := hraft.NewRaft(conf, fsms[i], store, store, snap, transports[i])
		if err != nil {
			t.Fatalf("create raft %s: %v", nid, err)
		}

		// Both nodes seed symmetrically.
		r.BootstrapCluster(hraft.Configuration{Servers: members})
		rafts[i] = r
	}

	// Wait for a leader.
	deadline := time.Now().Add(5 * time.Second)
	leaderIdx := -1
	for time.Now().Before(deadline) && leaderIdx < 0 {
		for i, r := range rafts {
			if r.State() == hraft.Leader {
				leaderIdx = i
				break
			}
		}
		if leaderIdx < 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if leaderIdx < 0 {
		for _, r := range rafts {
			_ = r.Shutdown().Error()
		}
		t.Fatal("no leader elected within 5s")
	}

	groups := make([]*raftgroup.Group, 2)
	groups[0] = &raftgroup.Group{Raft: rafts[leaderIdx], FSM: fsms[leaderIdx]}
	follower := 1 - leaderIdx
	groups[1] = &raftgroup.Group{Raft: rafts[follower], FSM: fsms[follower]}

	cleanup := func() {
		for _, r := range rafts {
			_ = r.Shutdown().Error()
		}
	}
	return groups, cleanup
}

// TestVaultCtlLeaderManager_ReconcileNoOpWhenStable verifies that a reconcile
// pass against a configuration that already matches the desired set does
// not make any membership changes (idempotency).
func TestVaultCtlLeaderManager_ReconcileNoOpWhenStable(t *testing.T) {
	t.Parallel()

	g, _, cleanup := makeSingleNodeVaultGroup(t, "stable-node")
	defer cleanup()

	mgr := newVaultCtlLeaderManager(discardLogger())
	defer mgr.StopAll()

	instID := glid.New()

	// Desired = current = just the leader.
	mgr.SetDesiredMembers(instID, []hraft.Server{
		{ID: "stable-node", Address: "stable-node"},
	})

	// Snapshot configuration before reconcile.
	beforeIdx := g.Raft.GetConfiguration().Index()

	mgr.Start(instID, g)

	// Give the reconcile pass a moment to run, then verify the
	// configuration index hasn't changed (no membership writes).
	time.Sleep(500 * time.Millisecond)

	afterIdx := g.Raft.GetConfiguration().Index()
	if afterIdx != beforeIdx {
		t.Errorf("configuration index changed from %d to %d; reconcile should have been a no-op",
			beforeIdx, afterIdx)
	}
}

// TestVaultMembershipMap_RoundTrip exercises the basic Set/Get/Delete + copy
// semantics of the desired-members map.
func TestVaultMembershipMap_RoundTrip(t *testing.T) {
	t.Parallel()

	m := newVaultCtlMembershipMap()
	instID := glid.New()

	// Initial Get returns nil.
	if got := m.Get(instID); got != nil {
		t.Errorf("expected nil for unknown inst, got %v", got)
	}

	// Set + Get round-trip.
	original := []hraft.Server{
		{ID: "a", Address: "a-addr"},
		{ID: "b", Address: "b-addr"},
	}
	m.Set(instID, original)
	got := m.Get(instID)
	if len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" {
		t.Errorf("Get returned wrong slice: %v", got)
	}

	// Mutating the returned slice does not affect the stored copy.
	got[0].ID = "MUTATED"
	got2 := m.Get(instID)
	if got2[0].ID != "a" {
		t.Errorf("stored slice was mutated by caller; got %v", got2)
	}

	// Mutating the original input also doesn't affect the stored copy
	// (Set takes a defensive copy).
	original[1].ID = "ALSO-MUTATED"
	got3 := m.Get(instID)
	if got3[1].ID != "b" {
		t.Errorf("stored slice was mutated by Set caller; got %v", got3)
	}

	// Delete clears the entry.
	m.Delete(instID)
	if got := m.Get(instID); got != nil {
		t.Errorf("expected nil after Delete, got %v", got)
	}
}
