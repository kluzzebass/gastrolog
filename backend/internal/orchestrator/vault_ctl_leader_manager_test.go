package orchestrator

import (
	"fmt"
	"gastrolog/internal/glid"
	"io"
	"log/slog"
	"sync"
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

// When a known voter's address changes (e.g. a K8s pod gets a new IP after a
// rolling restart), the reconcile pass must rewrite the stored address via
// AddVoter. Without this, the vault-ctl Raft group keeps the old IP and the
// rebooted pod is unreachable forever.
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

// TestVaultCtlLeaderManager_SetDesiredMembersWakesEpoch is the regression
// for the desiredChanged signal path. Before the fix the leader-epoch
// reconcile loop only woke on the 30 s safety-net ticker, so a
// SetDesiredMembers call that arrived mid-burst had to wait up to half a
// minute before its diff was applied. After the fix, SetDesiredMembers
// fires desiredChanged and the next reconcile pass runs within a
// select-iteration of the goroutine scheduler.
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
	// convergence would wait for vaultCtlMembershipReconcileSchedule (30 s)
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

// TestVaultCtlLeaderManager_BurstYieldsAndResumes is the regression for the
// cap-per-pass + signal-rewake path. We submit a desired set with more peers
// than vaultMembershipMaxPerPass and verify the burst converges fully via
// multiple short passes driven by the re-fired desiredChanged signal. A
// regression to the pre-fix "serialize the whole burst" or "wait 30 s for
// next tick" behavior would fail this test on either correctness (incomplete
// config) or timing (deadline).
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

// transferIfNeeded damping: a misaligned leader must be observed on two
// consecutive passes before a transfer is commanded, so a single organic
// election settles in one term instead of cascading through immediate
// re-alignment transfers.
func TestTransferDampingConfirmsOnSecondIdenticalSighting(t *testing.T) {
	t.Parallel()
	m := &vaultCtlLeaderManager{}
	vault := glid.New()

	if m.confirmMisalignment(vault, "node-B", "node-A") {
		t.Fatal("first sighting must not confirm")
	}
	if !m.confirmMisalignment(vault, "node-B", "node-A") {
		t.Fatal("second identical sighting must confirm")
	}
}

func TestTransferDampingResetsWhenLeaderChangesBetweenPasses(t *testing.T) {
	t.Parallel()
	m := &vaultCtlLeaderManager{}
	vault := glid.New()

	_ = m.confirmMisalignment(vault, "node-B", "node-A")
	// An election happened between passes: different wrong leader now.
	// That's a NEW misalignment — it must start its own two-pass count.
	if m.confirmMisalignment(vault, "node-C", "node-A") {
		t.Fatal("changed current leader must reset the observation, not confirm")
	}
	if !m.confirmMisalignment(vault, "node-C", "node-A") {
		t.Fatal("repeat of the new sighting must confirm")
	}
}

func TestTransferDampingResetsWhenDesiredChangesBetweenPasses(t *testing.T) {
	t.Parallel()
	m := &vaultCtlLeaderManager{}
	vault := glid.New()

	_ = m.confirmMisalignment(vault, "node-B", "node-A")
	// Placement leader changed between passes: different target.
	if m.confirmMisalignment(vault, "node-B", "node-D") {
		t.Fatal("changed desired leader must reset the observation, not confirm")
	}
}

func TestTransferDampingClearResets(t *testing.T) {
	t.Parallel()
	m := &vaultCtlLeaderManager{}
	vault := glid.New()

	_ = m.confirmMisalignment(vault, "node-B", "node-A")
	m.clearMisalignment(vault) // group aligned (or transfer commanded) in between
	if m.confirmMisalignment(vault, "node-B", "node-A") {
		t.Fatal("sighting after clear must start a fresh two-pass count")
	}
}

func TestTransferDampingIsPerVault(t *testing.T) {
	t.Parallel()
	m := &vaultCtlLeaderManager{}
	a, b := glid.New(), glid.New()

	_ = m.confirmMisalignment(a, "node-B", "node-A")
	if m.confirmMisalignment(b, "node-B", "node-A") {
		t.Fatal("vault B's first sighting must not be confirmed by vault A's history")
	}
	if !m.confirmMisalignment(a, "node-B", "node-A") {
		t.Fatal("vault A's second sighting must confirm independently")
	}
}

// configHasServer reports whether the given server ID is present in the
// group's current Raft configuration. Returns (present, ok) — ok is false
// if the configuration could not be read this poll.
func configHasServer(g *raftgroup.Group, id hraft.ServerID) (present, ok bool) {
	fut := g.Raft.GetConfiguration()
	if err := fut.Error(); err != nil {
		return false, false
	}
	for _, srv := range fut.Configuration().Servers {
		if srv.ID == id {
			return true, true
		}
	}
	return false, true
}

// TestVaultCtlLeaderManager_ConcurrentDesiredChangeDuringReconcileNotLost is
// the regression for the desiredChanged lost-wake race that the 30 s
// membership-reconcile safety tick used to paper over.
//
// desiredChanged is a close-and-recreate signal. The old runLeaderEpoch loop
// captured the wake channel at the top of each iteration and reconciled
// inside the select case, so a Notify that fired WHILE reconcile was running
// closed a channel nobody was waiting on and was replaced — the wake was lost
// and the change waited for the periodic tick. The fix captures the wake
// channel BEFORE each pass reads the desired set and waits on it AFTER acting,
// so a racing Notify is always observed on the next iteration.
//
// The reconcileHook injects exactly that race: the initial pass reads the
// desired set (reals only — a no-op pass), and while it runs the hook flips
// the desired set to reals+synth and fires the Notify. There is NO
// scheduler/cron in this harness, so the ONLY thing that can drive the
// follow-up AddNonvoter of synth is the wake captured before the initial
// pass. If that wake were lost (the pre-fix behavior), synth would never be
// added and the test would time out. Because synth is never part of the
// initial configuration, its eventual presence is an unambiguous witness
// that the racing wake was delivered.
func TestVaultCtlLeaderManager_ConcurrentDesiredChangeDuringReconcileNotLost(t *testing.T) {
	t.Parallel()

	groups, cleanup := makeTwoNodeVaultGroup(t, "lostwake-1", "lostwake-2")
	defer cleanup()
	leader := groups[0]

	mgr := newVaultCtlLeaderManager(discardLogger())
	defer mgr.StopAll()

	vaultID := glid.New()

	reals := []hraft.Server{
		{ID: "lostwake-1", Address: "lostwake-1"},
		{ID: "lostwake-2", Address: "lostwake-2"},
	}
	withSynth := append(append([]hraft.Server{}, reals...),
		hraft.Server{ID: "lostwake-synth", Address: "lostwake-synth"})

	// Initial desired = reals only: the first reconcile pass is a no-op.
	mgr.SetDesiredMembers(vaultID, reals)

	// One-shot hook: while the initial (no-op) pass runs — after it has read
	// the desired set — flip the desired set to include synth and fire the
	// Notify, racing the in-flight pass. Only a wake captured BEFORE that
	// pass can carry this change into the next reconcile.
	var once sync.Once
	mgr.reconcileHook = func() {
		once.Do(func() {
			mgr.SetDesiredMembers(vaultID, withSynth)
		})
	}

	mgr.Start(vaultID, leader)

	// synth was never in the initial config, so its presence proves the
	// racing wake drove a follow-up pass. A lost wake leaves synth absent
	// forever (no periodic tick exists in this harness).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if present, ok := configHasServer(leader, "lostwake-synth"); ok && present {
			return // success — the concurrent wake was not lost
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("synth was never added within 5s — the concurrent desired change was lost (lost-wake regression)")
}

// TestVaultCtlLeaderManager_MembershipConvergesWhileLeaderElsewhere covers the
// cluster-first case: a membership change (config dispatch fans SetDesiredMembers
// to every node's manager) converges event-driven even though only one node is
// the vault-ctl Raft leader. The follower node's manager receives the same call
// but its leader epoch never runs (LeaderLoop only fires OnLead on the leader),
// so it must be a harmless no-op while the leader's epoch does the AddVoter.
//
// No scheduler/cron is wired here, so convergence proves the event path alone
// carries a scale-out across nodes.
func TestVaultCtlLeaderManager_MembershipConvergesWhileLeaderElsewhere(t *testing.T) {
	t.Parallel()

	groups, cleanup := makeTwoNodeVaultGroup(t, "elsewhere-1", "elsewhere-2")
	defer cleanup()
	leaderGroup := groups[0]
	followerGroup := groups[1]

	mgrLeader := newVaultCtlLeaderManager(discardLogger())
	defer mgrLeader.StopAll()
	mgrFollower := newVaultCtlLeaderManager(discardLogger())
	defer mgrFollower.StopAll()

	vaultID := glid.New()

	// Both managers start their local group's leader loop. Only the leader
	// group's OnLead actually runs a reconcile epoch.
	mgrLeader.Start(vaultID, leaderGroup)
	mgrFollower.Start(vaultID, followerGroup)

	// Config dispatch fans the new desired set to every node's manager.
	desired := []hraft.Server{
		{ID: "elsewhere-1", Address: "elsewhere-1"},
		{ID: "elsewhere-2", Address: "elsewhere-2"},
		{ID: "elsewhere-synth", Address: "elsewhere-synth"},
	}
	mgrLeader.SetDesiredMembers(vaultID, desired)
	mgrFollower.SetDesiredMembers(vaultID, desired)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if present, ok := configHasServer(leaderGroup, "elsewhere-synth"); ok && present {
			return // converged via the event path, no tick
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("synthetic peer did not converge event-driven within 5s (follower manager should be a no-op, leader converges)")
}

// TestVaultCtlLeaderManager_RapidDesiredChurnConvergesToFinal exercises the
// coalescing property of the desiredChanged capture semantics under a rapid
// burst of SetDesiredMembers calls: only the LAST desired set matters, and the
// group must converge to it without any periodic tick even though most of the
// intermediate Notifies land while a reconcile pass is already running.
//
// The interleaved calls toggle synth in and out; the FINAL call adds it. If
// the final wake (or a decisive intermediate one) were lost while a reconcile
// pass was already running, the group could settle on a stale reals-only set.
// synth is never part of the initial config, so its terminal presence —
// matching the last SetDesiredMembers — proves the last wake won.
func TestVaultCtlLeaderManager_RapidDesiredChurnConvergesToFinal(t *testing.T) {
	t.Parallel()

	groups, cleanup := makeTwoNodeVaultGroup(t, "churn-1", "churn-2")
	defer cleanup()
	leader := groups[0]

	mgr := newVaultCtlLeaderManager(discardLogger())
	defer mgr.StopAll()

	vaultID := glid.New()

	reals := []hraft.Server{
		{ID: "churn-1", Address: "churn-1"},
		{ID: "churn-2", Address: "churn-2"},
	}
	withSynth := append(append([]hraft.Server{}, reals...),
		hraft.Server{ID: "churn-synth", Address: "churn-synth"})

	mgr.SetDesiredMembers(vaultID, reals)
	mgr.Start(vaultID, leader)

	// Rapid churn: remove, add, remove, add, ... Most of these Notifies land
	// while a reconcile pass is already running, exercising the wake-capture
	// coalescing under load.
	for i := range 12 {
		if i%2 == 0 {
			mgr.SetDesiredMembers(vaultID, reals)
		} else {
			mgr.SetDesiredMembers(vaultID, withSynth)
		}
	}
	// Final desired state includes synth.
	mgr.SetDesiredMembers(vaultID, withSynth)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if present, ok := configHasServer(leader, "churn-synth"); ok && present {
			return // converged to the final desired set
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("did not converge to final desired set (synth) within 5s — a wake in the burst was lost")
}
