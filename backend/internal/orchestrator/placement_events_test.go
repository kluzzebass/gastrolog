package orchestrator

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
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
		Runtime: system.Runtime{NodeStorageConfigs: nscs},
		Config:  system.Config{Vaults: []system.VaultConfig{{ID: vaultID, Name: "v", Placements: placements}}},
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
	o.sysLoader = &mnLoader{sys: mkSys(vaultID, newPlacements, nscs)}
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
	o.sysLoader = &mnLoader{sys: mkSys(vaultID, noLeader, nscs)}
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
	o.sysLoader = &mnLoader{sys: mkSys(vaultID, resolved, nscs)}
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
		o.sysLoader = &mnLoader{sys: sys}
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
