package orchestrator

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// These tests pin the event-driven placement reconcile that replaced the 15s
// orchestrator placement sweep (gastrolog-29xpy). Every case drives
// ReconcileVaultPlacement / ReconcilePlacements directly — the entry points the
// FSM config dispatcher calls — with NO scheduler registered, so a green run
// proves the work happens on the config event, not on a tick.

// mnLoader is a mutable SystemLoader so a test can change the placement config
// between reconcile calls (the way successive CmdSetVaultPlacements applies do).
type mnLoader struct{ sys *system.System }

func (l *mnLoader) Load(context.Context) (*system.System, error) { return l.sys, nil }

func mkSys(vaultID glid.GLID, placements []system.VaultPlacement, nscs []system.NodeStorageConfig) *system.System {
	return &system.System{
		Runtime: system.Runtime{
			NodeStorageConfigs: nscs,
			// Placements live on the runtime map, their owner; VaultConfig no
			// longer mirrors them (gastrolog-617qns).
			VaultPlacements: map[glid.GLID][]system.VaultPlacement{vaultID: placements},
		},
		Config: system.Config{Vaults: []system.VaultConfig{{ID: vaultID, Name: "v"}}},
	}
}

// TestReconcileVaultPlacement_RefreshesFollowerTargets closes the exact gap the
// sweep used to paper over: a leader instance whose follower set changed while
// it kept its role once carried stale FollowerTargets until the next 15s tick
// (the event path only refreshed targets at instance BUILD). Now the placement
// event itself refreshes them.
func TestReconcileVaultPlacement_RefreshesFollowerTargets(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	sL, sA, sB := glid.New(), glid.New(), glid.New()
	nscs := []system.NodeStorageConfig{
		{NodeID: "L", FileStorages: []system.FileStorage{{ID: sL}}},
		{NodeID: "A", FileStorages: []system.FileStorage{{ID: sA}}},
		{NodeID: "B", FileStorages: []system.FileStorage{{ID: sB}}},
	}
	oldPlacements := []system.VaultPlacement{{StorageID: sL.String(), Leader: true}, {StorageID: sA.String()}}
	newPlacements := []system.VaultPlacement{{StorageID: sL.String(), Leader: true}, {StorageID: sB.String()}}

	inst := &VaultInstance{VaultID: vaultID, IsFollower: false,
		FollowerTargets: system.FollowerTargets(oldPlacements, nscs)}

	o := newTestOrch(t, Config{LocalNodeID: "L"})
	o.setSystemLoader(&mnLoader{sys: mkSys(vaultID, newPlacements, nscs)})
	o.vaults[vaultID] = &Vault{ID: vaultID, Instance: inst}

	o.ReconcileVaultPlacement(context.Background(), vaultID)

	wantTargets := system.FollowerTargets(newPlacements, nscs)
	if !replicationTargetsEqual(inst.FollowerTargets, wantTargets) {
		t.Fatalf("FollowerTargets not refreshed on placement event: got %v, want %v",
			replicationTargetNodes(inst.FollowerTargets), replicationTargetNodes(wantTargets))
	}
	// Sanity: the new target set is genuinely different from the old one.
	if replicationTargetsEqual(inst.FollowerTargets, system.FollowerTargets(oldPlacements, nscs)) {
		t.Fatal("test did not actually change the follower set")
	}
}

// TestReconcileVaultPlacement_RacyNoLeaderDoesNotStrand reproduces the race the
// sweep was built to heal — "a raced role update during placement flapping left
// every node believing it was a follower, and with no further config changes the
// vault sat leaderless for hours" (rotationsweep.go). The guarded event-driven
// reconcile must (1) refuse to flip roles on a placement that resolves to no
// leader, (2) report the raw leaderless condition so the catalog's DelayOn
// annunciates it on read WITHOUT any periodic re-raise, and (3) clear the moment
// a real leader resolves — all on config events.
func TestReconcileVaultPlacement_RacyNoLeaderDoesNotStrand(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	sL, sF := glid.New(), glid.New()
	nscs := []system.NodeStorageConfig{
		{NodeID: "L", FileStorages: []system.FileStorage{{ID: sL}}},
		{NodeID: "F", FileStorages: []system.FileStorage{{ID: sF}}},
	}

	// Deterministic clock so the DelayOn window advances without wall time.
	var now = time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	coll := alert.NewWithClock(clock)

	// Local node F is currently a healthy follower pointing at leader L.
	inst := &VaultInstance{VaultID: vaultID, IsFollower: true, LeaderNodeID: "L"}

	o := newTestOrch(t, Config{LocalNodeID: "F", Alerts: coll})
	o.vaults[vaultID] = &Vault{ID: vaultID, Instance: inst}

	// Mid-flap: placements resolve to NO leader (only a follower entry lands).
	noLeader := []system.VaultPlacement{{StorageID: sF.String()}}
	o.setSystemLoader(&mnLoader{sys: mkSys(vaultID, noLeader, nscs)})
	o.ReconcileVaultPlacement(context.Background(), vaultID)

	// Role MUST be untouched on unresolvable placement — this is the whole race.
	if !inst.IsFollower || inst.LeaderNodeID != "L" {
		t.Fatalf("role flipped on no-leader placement: IsFollower=%v LeaderNodeID=%q",
			inst.IsFollower, inst.LeaderNodeID)
	}
	// Inside the DelayOn window: raw condition raised but not yet annunciated.
	if coll.Count() != 0 {
		t.Fatalf("leaderless annunciated inside delay-on window: %d", coll.Count())
	}

	// Sustained: the single event-driven raise self-annunciates once the window
	// elapses — proving no periodic sweep re-raise is needed.
	typ, ok := alert.TypeByID("vault-leaderless")
	if !ok || typ.DelayOn <= 0 {
		t.Fatal("vault-leaderless must carry a catalog DelayOn")
	}
	now = now.Add(typ.DelayOn + time.Second)
	if coll.Count() != 1 {
		t.Fatalf("sustained leaderless did not annunciate after delay-on: %d", coll.Count())
	}

	// A real leader resolves on the next placement event: clear + heal role.
	resolved := []system.VaultPlacement{{StorageID: sL.String(), Leader: true}, {StorageID: sF.String()}}
	o.setSystemLoader(&mnLoader{sys: mkSys(vaultID, resolved, nscs)})
	o.ReconcileVaultPlacement(context.Background(), vaultID)

	if coll.Count() != 0 {
		t.Fatalf("leaderless not cleared after leader resolved: %d", coll.Count())
	}
	if !inst.IsFollower || inst.LeaderNodeID != "L" {
		t.Fatalf("follower role/pointer wrong after resolve: IsFollower=%v LeaderNodeID=%q",
			inst.IsFollower, inst.LeaderNodeID)
	}
}

// TestReconcileVaultPlacement_PropagatesToAllNodes is the multi-node case: a
// single placement config, replicated to every node, must drive each node's
// local instance to the correct role and (on the leader) the correct
// FollowerTargets when each node independently reconciles from that config —
// exactly what the FSM dispatch fan-out does when it delivers
// NotifyVaultPlacementsSet to all four nodes. Every instance starts in the
// WRONG "everyone's a follower" state (the stranded-leaderless shape) to prove
// the event-driven reconcile heals it on every node without a sweep.
func TestReconcileVaultPlacement_PropagatesToAllNodes(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	sL, sF1, sF2, sF3 := glid.New(), glid.New(), glid.New(), glid.New()
	nscs := []system.NodeStorageConfig{
		{NodeID: "L", FileStorages: []system.FileStorage{{ID: sL}}},
		{NodeID: "F1", FileStorages: []system.FileStorage{{ID: sF1}}},
		{NodeID: "F2", FileStorages: []system.FileStorage{{ID: sF2}}},
		{NodeID: "F3", FileStorages: []system.FileStorage{{ID: sF3}}},
	}
	placements := []system.VaultPlacement{
		{StorageID: sL.String(), Leader: true},
		{StorageID: sF1.String()},
		{StorageID: sF2.String()},
		{StorageID: sF3.String()},
	}
	sys := mkSys(vaultID, placements, nscs)

	insts := make(map[string]*VaultInstance, 4)
	for _, nodeID := range []string{"L", "F1", "F2", "F3"} {
		// Start every node in the wrong role: convinced it is a follower of a
		// stale leader — the exact hung state the sweep used to unstick.
		inst := &VaultInstance{VaultID: vaultID, IsFollower: true, LeaderNodeID: "stale"}
		insts[nodeID] = inst
		o := newTestOrch(t, Config{LocalNodeID: nodeID})
		o.setSystemLoader(&mnLoader{sys: sys})
		o.vaults[vaultID] = &Vault{ID: vaultID, Instance: inst}
		o.ReconcileVaultPlacement(context.Background(), vaultID)
	}

	if insts["L"].IsFollower || insts["L"].LeaderNodeID != "" {
		t.Fatalf("leader node not healed: IsFollower=%v LeaderNodeID=%q",
			insts["L"].IsFollower, insts["L"].LeaderNodeID)
	}
	wantTargets := system.FollowerTargets(placements, nscs)
	if !replicationTargetsEqual(insts["L"].FollowerTargets, wantTargets) {
		t.Fatalf("leader FollowerTargets wrong: got %v want %v",
			replicationTargetNodes(insts["L"].FollowerTargets), replicationTargetNodes(wantTargets))
	}
	for _, nodeID := range []string{"F1", "F2", "F3"} {
		if !insts[nodeID].IsFollower || insts[nodeID].LeaderNodeID != "L" {
			t.Fatalf("follower %s wrong: IsFollower=%v LeaderNodeID=%q",
				nodeID, insts[nodeID].IsFollower, insts[nodeID].LeaderNodeID)
		}
	}
}

// ---- gastrolog-235dm7: stable-leader rebalance wakes the ack reconcile ----
//
// gastrolog-3fu9t made the reconciler's leader-only categories converge on the
// lead-gained edge (ReconcileMembershipCatchup ← onVaultCtlLeadGained). That
// covers every membership move that comes WITH a leadership change. It does not
// cover a rebalance under a STABLE leader: placements move a follower from one
// node to another, the leader keeps its role and its vault-ctl Raft leadership,
// and no lead-gained edge fires — so the leader's pendingDeletes kept naming
// the departed node in ExpectedFrom until the 20s backstop tick noticed. The
// reassignment IS the event; these tests pin that it now wakes the reconcile.
//
// Every case neuters the vault-catchup-sweep schedule first
// (neuterCatchupSweep) so a green run cannot be the backstop tick's doing.

// neuterCatchupSweep removes the periodic reconcile backstop from the
// orchestrator's scheduler so a test can only pass on the event path. Fails the
// test if the job is still registered afterwards — the guard is worthless if
// the job name drifts.
func neuterCatchupSweep(t *testing.T, o *Orchestrator) {
	t.Helper()
	o.scheduler.RemoveJob(vaultCatchupSweepJobName)
	if o.scheduler.HasJob(vaultCatchupSweepJobName) {
		t.Fatalf("%s still scheduled: the test cannot prove event-driven convergence", vaultCatchupSweepJobName)
	}
}

// prunedRecorder captures ApplyRaftPruneNode proposals. The wake runs on the
// orchestrator's auxWg goroutine, so tests read it only after auxWg.Wait().
type prunedRecorder struct {
	mu    sync.Mutex
	nodes []string
}

func (p *prunedRecorder) apply(nodeID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nodes = append(p.nodes, nodeID)
	return nil
}

func (p *prunedRecorder) list() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.nodes...)
}

// pendingDeleteFSM returns a vault-ctl FSM holding exactly one pendingDelete
// whose ExpectedFrom is the given node set — the receipt-protocol state that
// goes stale when placement drops one of those nodes.
func pendingDeleteFSM(expectedFrom ...string) *vaultctlfsm.FSM {
	fsm := vaultctlfsm.New()
	now := time.Now()
	chunkID := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(chunkID, now, now, now)})
	_ = fsm.Apply(&hraft.Log{
		Data: vaultctlfsm.MarshalRequestDelete(chunkID, now, "retention-ttl", expectedFrom),
	})
	return fsm
}

// instanceWithPendingAcks builds a vault instance for nodeID with the given
// FollowerTargets, a wired lifecycle reconciler, and an FSM carrying one
// pendingDelete over expectedFrom. Callers flip IsFollower/LeaderNodeID for
// follower nodes.
func instanceWithPendingAcks(vaultID glid.GLID, nodeID string, targets []system.ReplicationTarget, rec *prunedRecorder, expectedFrom ...string) *VaultInstance {
	inst := &VaultInstance{
		VaultID:         vaultID,
		IsFollower:      false,
		FollowerTargets: targets,
		RaftApplyFacet:  RaftApplyFacet{ApplyRaftPruneNode: rec.apply},
	}
	inst.Reconciler = NewVaultLifecycleReconciler(nil, vaultID, inst, nodeID, slog.Default())
	inst.Reconciler.fsm = pendingDeleteFSM(expectedFrom...)
	return inst
}

// TestReconcileVaultPlacement_StableLeaderRebalancePrunesStalePendingAcks is
// the issue's exact shape: placements move the follower from A to B while
// leader L keeps its role (no leadership change, no RemoveServer). L's
// pendingDelete still expects an ack from A, which will never come. The
// FollowerTargets reassignment must wake the ack reconcile and prune A.
func TestReconcileVaultPlacement_StableLeaderRebalancePrunesStalePendingAcks(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	sL, sA, sB := glid.New(), glid.New(), glid.New()
	nscs := []system.NodeStorageConfig{
		{NodeID: "L", FileStorages: []system.FileStorage{{ID: sL}}},
		{NodeID: "A", FileStorages: []system.FileStorage{{ID: sA}}},
		{NodeID: "B", FileStorages: []system.FileStorage{{ID: sB}}},
	}
	oldPlacements := []system.VaultPlacement{{StorageID: sL.String(), Leader: true}, {StorageID: sA.String()}}
	newPlacements := []system.VaultPlacement{{StorageID: sL.String(), Leader: true}, {StorageID: sB.String()}}

	pruned := &prunedRecorder{}
	inst := instanceWithPendingAcks(vaultID, "L",
		system.FollowerTargets(oldPlacements, nscs), pruned, "L", "A")

	o := newTestOrch(t, Config{LocalNodeID: "L"})
	neuterCatchupSweep(t, o)
	o.setSystemLoader(&mnLoader{sys: mkSys(vaultID, newPlacements, nscs)})
	o.vaults[vaultID] = &Vault{ID: vaultID, Instance: inst}

	// The placement event, and nothing else.
	o.ReconcileVaultPlacement(context.Background(), vaultID)
	o.auxWg.Wait()

	if got := pruned.list(); len(got) != 1 || got[0] != "A" {
		t.Fatalf("stable-leader rebalance pruned %v, want [A] — the departed follower's stale ExpectedFrom must be pruned on the placement event, not on the backstop tick", got)
	}
}

// TestReconcilePlacements_StableLeaderRebalancePrunesStalePendingAcks covers
// the other dispatcher entry point: a node-storage-config change remaps
// storage→node, moving FollowerTargets with no placement edit at all
// (NotifyNodeStorageConfigSet → ReconcilePlacements). Same stable leader, same
// stuck ack, same requirement.
func TestReconcilePlacements_StableLeaderRebalancePrunesStalePendingAcks(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	sL, sF := glid.New(), glid.New()
	placements := []system.VaultPlacement{{StorageID: sL.String(), Leader: true}, {StorageID: sF.String()}}
	// The follower storage sF migrates from node A to node B. Placements are
	// untouched; only the NSC changed.
	oldNSCs := []system.NodeStorageConfig{
		{NodeID: "L", FileStorages: []system.FileStorage{{ID: sL}}},
		{NodeID: "A", FileStorages: []system.FileStorage{{ID: sF}}},
	}
	newNSCs := []system.NodeStorageConfig{
		{NodeID: "L", FileStorages: []system.FileStorage{{ID: sL}}},
		{NodeID: "B", FileStorages: []system.FileStorage{{ID: sF}}},
	}

	pruned := &prunedRecorder{}
	inst := instanceWithPendingAcks(vaultID, "L",
		system.FollowerTargets(placements, oldNSCs), pruned, "L", "A")

	o := newTestOrch(t, Config{LocalNodeID: "L"})
	neuterCatchupSweep(t, o)
	o.setSystemLoader(&mnLoader{sys: mkSys(vaultID, placements, newNSCs)})
	o.vaults[vaultID] = &Vault{ID: vaultID, Instance: inst}

	o.ReconcilePlacements(context.Background())
	o.auxWg.Wait()

	if got := pruned.list(); len(got) != 1 || got[0] != "A" {
		t.Fatalf("NSC-driven rebalance pruned %v, want [A]", got)
	}
}

// TestReconcileVaultPlacement_UnchangedPlacementDoesNotWakeCatchup pins the
// gate: the wake fires on a MOVE, not on every config republish. An unrelated
// vault edit re-fires NotifyVaultPut with identical placements; that must not
// spray Raft proposals across every vault on the node. The stale ExpectedFrom
// here ("ghost", never in placement) is deliberately left to the periodic
// backstop — this documents what stays on the tick.
func TestReconcileVaultPlacement_UnchangedPlacementDoesNotWakeCatchup(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	sL, sF := glid.New(), glid.New()
	nscs := []system.NodeStorageConfig{
		{NodeID: "L", FileStorages: []system.FileStorage{{ID: sL}}},
		{NodeID: "F", FileStorages: []system.FileStorage{{ID: sF}}},
	}
	placements := []system.VaultPlacement{{StorageID: sL.String(), Leader: true}, {StorageID: sF.String()}}

	pruned := &prunedRecorder{}
	inst := instanceWithPendingAcks(vaultID, "L",
		system.FollowerTargets(placements, nscs), pruned, "L", "F", "ghost")

	o := newTestOrch(t, Config{LocalNodeID: "L"})
	neuterCatchupSweep(t, o)
	o.setSystemLoader(&mnLoader{sys: mkSys(vaultID, placements, nscs)})
	o.vaults[vaultID] = &Vault{ID: vaultID, Instance: inst}

	// Republish the SAME placements: nothing moved.
	o.ReconcileVaultPlacement(context.Background(), vaultID)
	o.auxWg.Wait()

	if got := pruned.list(); len(got) != 0 {
		t.Fatalf("no-op placement republish proposed %v; the wake must be gated on an actual membership move", got)
	}
	// The reconcile itself still works when driven directly — proving the
	// stale state is real and only the WAKE was gated.
	inst.Reconciler.SweepStalePendingDeleteAcks()
	if got := pruned.list(); len(got) != 1 || got[0] != "ghost" {
		t.Fatalf("backstop sweep pruned %v, want [ghost]", got)
	}
}

// TestReconcileVaultPlacement_StableLeaderRebalanceMultiNode is the cluster
// case. One placement config change (follower F3 → F4) is replicated to every
// node, and each node independently reconciles its own instance from it. Only
// the leader may propose the prune: the ack reconcile is placement-leader-only,
// so followers reconciling the same event must stay silent. This is the
// invariant that keeps a rebalance from producing N duplicate CmdPruneNode
// proposals in an N-node cluster.
func TestReconcileVaultPlacement_StableLeaderRebalanceMultiNode(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	sL, sF1, sF2, sF3, sF4 := glid.New(), glid.New(), glid.New(), glid.New(), glid.New()
	nscs := []system.NodeStorageConfig{
		{NodeID: "L", FileStorages: []system.FileStorage{{ID: sL}}},
		{NodeID: "F1", FileStorages: []system.FileStorage{{ID: sF1}}},
		{NodeID: "F2", FileStorages: []system.FileStorage{{ID: sF2}}},
		{NodeID: "F3", FileStorages: []system.FileStorage{{ID: sF3}}},
		{NodeID: "F4", FileStorages: []system.FileStorage{{ID: sF4}}},
	}
	oldPlacements := []system.VaultPlacement{
		{StorageID: sL.String(), Leader: true},
		{StorageID: sF1.String()}, {StorageID: sF2.String()}, {StorageID: sF3.String()},
	}
	// Rebalance: F3 leaves the placement set, F4 joins. Leader L is untouched.
	newPlacements := []system.VaultPlacement{
		{StorageID: sL.String(), Leader: true},
		{StorageID: sF1.String()}, {StorageID: sF2.String()}, {StorageID: sF4.String()},
	}
	sys := mkSys(vaultID, newPlacements, nscs)

	expectedFrom := []string{"L", "F1", "F2", "F3"}
	pruned := map[string]*prunedRecorder{}
	for _, nodeID := range []string{"L", "F1", "F2", "F3", "F4"} {
		rec := &prunedRecorder{}
		pruned[nodeID] = rec

		var inst *VaultInstance
		if nodeID == "L" {
			inst = instanceWithPendingAcks(vaultID, nodeID,
				system.FollowerTargets(oldPlacements, nscs), rec, expectedFrom...)
		} else {
			inst = instanceWithPendingAcks(vaultID, nodeID, nil, rec, expectedFrom...)
			inst.IsFollower = true
			inst.LeaderNodeID = "L"
		}

		o := newTestOrch(t, Config{LocalNodeID: nodeID})
		neuterCatchupSweep(t, o)
		o.setSystemLoader(&mnLoader{sys: sys})
		o.vaults[vaultID] = &Vault{ID: vaultID, Instance: inst}

		o.ReconcileVaultPlacement(context.Background(), vaultID)
		o.auxWg.Wait()
	}

	if got := pruned["L"].list(); len(got) != 1 || got[0] != "F3" {
		t.Fatalf("leader pruned %v, want [F3]", got)
	}
	for _, nodeID := range []string{"F1", "F2", "F3", "F4"} {
		if got := pruned[nodeID].list(); len(got) != 0 {
			t.Fatalf("follower %s proposed %v; only the placement leader may prune stale ExpectedFrom", nodeID, got)
		}
	}
}
