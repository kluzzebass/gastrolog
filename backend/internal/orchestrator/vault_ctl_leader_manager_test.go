package orchestrator

import (
	"fmt"
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

	vaultID := glid.New()

	// Start twice — second is a no-op.
	mgr.Start(vaultID, g)
	mgr.Start(vaultID, g)

	// Stop and start again — should re-register cleanly.
	mgr.Stop(vaultID)
	mgr.Start(vaultID, g)
	mgr.Stop(vaultID)

	// Stopping an instance with no loop should be safe.
	mgr.Stop(vaultID)
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

	vaultID := glid.New()

	// Desired set = current (just the leader) + a synthetic second member.
	mgr.SetDesiredMembers(vaultID, []hraft.Server{
		{ID: "leader-add", Address: "leader-add"},
		{ID: "synthetic-peer", Address: "synthetic-addr"},
	})

	mgr.Start(vaultID, g)

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

// gastrolog-4zy8a: when a known voter's address changes (e.g. a K8s pod gets
// a new IP after a rolling restart), the reconcile pass must rewrite the
// stored address via AddVoter. Without this, the vault-ctl Raft group keeps
// the old IP and the rebooted pod is unreachable forever.
//
// Uses a 2-real-node cluster (so the 2 reachable members form a 2-of-3
// majority for config changes) plus a synthetic 3rd voter "peer-rolled"
// whose address we want to update from oldAddr to newAddr. The cluster's
// 2 alive nodes commit the config change without needing the synthetic
// peer to acknowledge.
func TestVaultCtlLeaderManager_ReconcileRefreshesStaleAddress(t *testing.T) {
	t.Parallel()

	groups, cleanup := makeTwoNodeVaultGroup(t, "alive-1", "alive-2")
	defer cleanup()
	leader := groups[0]

	const oldAddr = "10.0.0.5:4566"
	if err := leader.Raft.AddVoter("peer-rolled", oldAddr, 0, 5*time.Second).Error(); err != nil {
		t.Fatalf("seed AddVoter with old addr: %v", err)
	}

	mgr := newVaultCtlLeaderManager(discardLogger())
	defer mgr.StopAll()

	vaultID := glid.New()

	// SetDesiredMembers with peer-rolled at the NEW address.
	const newAddr = "10.0.0.42:4566"
	mgr.SetDesiredMembers(vaultID, []hraft.Server{
		{ID: "alive-1", Address: "alive-1"},
		{ID: "alive-2", Address: "alive-2"},
		{ID: "peer-rolled", Address: newAddr},
	})

	mgr.Start(vaultID, leader)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		future := leader.Raft.GetConfiguration()
		if err := future.Error(); err == nil {
			for _, srv := range future.Configuration().Servers {
				if string(srv.ID) == "peer-rolled" && string(srv.Address) == newAddr {
					return // success
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("peer-rolled address was not updated to %s within 5s", newAddr)
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

	vaultID := glid.New()

	// Desired set = just the two alive nodes. doomed should be removed.
	mgr.SetDesiredMembers(vaultID, []hraft.Server{
		{ID: "alive-1", Address: "alive-1"},
		{ID: "alive-2", Address: "alive-2"},
	})

	mgr.Start(vaultID, leader)

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

// makeTwoNodeVaultGroup builds a 2-node instance Raft cluster using in-memory
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

	vaultID := glid.New()

	// Desired = current = just the leader.
	mgr.SetDesiredMembers(vaultID, []hraft.Server{
		{ID: "stable-node", Address: "stable-node"},
	})

	// Snapshot configuration before reconcile.
	beforeIdx := g.Raft.GetConfiguration().Index()

	mgr.Start(vaultID, g)

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
	vaultID := glid.New()

	// Initial Get returns nil.
	if got := m.Get(vaultID); got != nil {
		t.Errorf("expected nil for unknown vaultInst, got %v", got)
	}

	// Set + Get round-trip.
	original := []hraft.Server{
		{ID: "a", Address: "a-addr"},
		{ID: "b", Address: "b-addr"},
	}
	m.Set(vaultID, original)
	got := m.Get(vaultID)
	if len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" {
		t.Errorf("Get returned wrong slice: %v", got)
	}

	// Mutating the returned slice does not affect the stored copy.
	got[0].ID = "MUTATED"
	got2 := m.Get(vaultID)
	if got2[0].ID != "a" {
		t.Errorf("stored slice was mutated by caller; got %v", got2)
	}

	// Mutating the original input also doesn't affect the stored copy
	// (Set takes a defensive copy).
	original[1].ID = "ALSO-MUTATED"
	got3 := m.Get(vaultID)
	if got3[1].ID != "b" {
		t.Errorf("stored slice was mutated by Set caller; got %v", got3)
	}

	// Delete clears the entry.
	m.Delete(vaultID)
	if got := m.Get(vaultID); got != nil {
		t.Errorf("expected nil after Delete, got %v", got)
	}
}

// TestVaultCtlLeaderManager_SetDesiredMembersWakesEpoch is the
// gastrolog-5n6xz regression for the desiredChanged signal path. Before
// the fix the leader-epoch reconcile loop only woke on the 30 s
// safety-net ticker, so a SetDesiredMembers call that arrived mid-burst
// had to wait up to half a minute before its diff was applied. After
// the fix, SetDesiredMembers fires desiredChanged and the next reconcile
// pass runs within a select-iteration of the goroutine scheduler.
//
// Uses a 2-real-node cluster so the AddVoter for a third (synthetic)
// peer commits via the two live nodes — exercising the full reconcile
// path including the post-AddVoter signal re-fire.
func TestVaultCtlLeaderManager_SetDesiredMembersWakesEpoch(t *testing.T) {
	t.Parallel()

	groups, cleanup := makeTwoNodeVaultGroup(t, "wake-1", "wake-2")
	defer cleanup()
	leader := groups[0]

	mgr := newVaultCtlLeaderManager(discardLogger())
	defer mgr.StopAll()

	vaultID := glid.New()

	// Initial desired = current config (both reals, no diff). Start the
	// epoch and let the initial reconcile complete as a no-op.
	mgr.SetDesiredMembers(vaultID, []hraft.Server{
		{ID: "wake-1", Address: "wake-1"},
		{ID: "wake-2", Address: "wake-2"},
	})
	mgr.Start(vaultID, leader)
	time.Sleep(200 * time.Millisecond)

	// Now fire a real diff. With the wake-on-signal path, the synthetic
	// peer should land in the configuration within ~1 s. Without it,
	// convergence would wait for vaultMembershipReconcileInterval (30 s)
	// — well past this deadline.
	mgr.SetDesiredMembers(vaultID, []hraft.Server{
		{ID: "wake-1", Address: "wake-1"},
		{ID: "wake-2", Address: "wake-2"},
		{ID: "wake-synth", Address: "wake-synth"},
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		fut := leader.Raft.GetConfiguration()
		if err := fut.Error(); err == nil {
			for _, srv := range fut.Configuration().Servers {
				if srv.ID == "wake-synth" {
					return
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("synthetic peer did not converge within 2 s — wake signal not driving reconcile")
}

// TestVaultCtlLeaderManager_BurstYieldsAndResumes is the gastrolog-5n6xz
// regression for the cap-per-pass + signal-rewake path. We submit a
// desired set with more peers than vaultMembershipMaxPerPass and verify
// the burst converges fully via multiple short passes driven by the
// re-fired desiredChanged signal. A regression to the pre-fix "serialize
// the whole burst" or "wait 30 s for next tick" behavior would fail this
// test on either correctness (incomplete config) or timing (deadline).
//
// vaultMembershipCommitTimeout is shortened so the bail path is fast
// when individual commits stall on the unreachable synthetic peers
// past the second one (quorum maths: 2 reals + 1 synth = 3-of-3 commits;
// 2 reals + 2 synth needs 3-of-4 = one synth must ACK, which never
// happens). The bail itself feeds the re-fire path under test.
func TestVaultCtlLeaderManager_BurstYieldsAndResumes(t *testing.T) {
	t.Parallel()

	groups, cleanup := makeTwoNodeVaultGroup(t, "burst-1", "burst-2")
	defer cleanup()
	leader := groups[0]

	mgr := newVaultCtlLeaderManager(discardLogger())
	// Shorten the bail timeout so the test doesn't sit through a 15 s
	// stall when individual commits can't form quorum past the first
	// synthetic peer. Per-manager (not package-global), so parallel
	// tests don't see this override.
	mgr.commitTimeout = 200 * time.Millisecond
	defer mgr.StopAll()

	vaultID := glid.New()

	// Desired = 2 reals + 5 synthetic peers. Each reconcile pass commits
	// at most one new voter (next pass's AddVoter for a second synth
	// can't commit because the first synth never ACKs), so convergence
	// to "synth-0 in log" is the testable signal that yield+resume works.
	desired := []hraft.Server{
		{ID: "burst-1", Address: "burst-1"},
		{ID: "burst-2", Address: "burst-2"},
	}
	for i := range 5 {
		desired = append(desired, hraft.Server{
			ID:      hraft.ServerID(fmt.Sprintf("burst-synth-%d", i)),
			Address: hraft.ServerAddress(fmt.Sprintf("burst-synth-addr-%d", i)),
		})
	}
	mgr.SetDesiredMembers(vaultID, desired)
	mgr.Start(vaultID, leader)

	// At minimum, the first synthetic peer must converge well inside the
	// 30 s safety-net window — proving the re-fired signal drives the
	// next pass without waiting for the periodic tick after the bail.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		fut := leader.Raft.GetConfiguration()
		if err := fut.Error(); err == nil {
			for _, srv := range fut.Configuration().Servers {
				if srv.ID == "burst-synth-0" {
					return
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("first synthetic peer did not converge within 3 s after burst")
}
