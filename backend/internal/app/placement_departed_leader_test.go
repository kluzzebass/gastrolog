package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// A node that has been REMOVED is categorically different from one that is
// merely silent: its NodeConfig is gone from the FSM, which is a durable,
// operator-initiated, replicated fact rather than a liveness inference. The
// placement guards exist to protect a node that still exists, and none of them
// should pin a placement to one that does not.
//
// Before gastrolog-68y1vn a departed LEADER read as NodeStateUnknown (the zero
// value of the nodeStates map), fell into the Unknown/Live case, failed the
// heartbeat check, and had its placement retained forever by the two-clock
// guard's "node just went quiet" branch. Nothing converged it, and followers
// were never extended to compensate because the placement target already looked
// met. Follower departures re-placed correctly the whole time — the leader was
// the case that stranded.
//
// These tests drive the real removal path (nodeRemover.remove, which deletes
// the NodeConfig) and then reconcile, so they exercise departure the way the
// cluster does rather than by hand-editing state.

// placementAfterReconcile removes nodeName through the real remover, reconciles
// with the named nodes alive, and returns the vault's holder set by node ID.
func placementAfterReconcile(t *testing.T, c *rfTestCluster, removeName string, aliveNames []string, vaultID glid.GLID) (map[string]bool, *alert.Collector) {
	t.Helper()
	ctx := context.Background()
	logger, _ := testLogger()
	var mu sync.Mutex
	var removed []string
	remover := newTestRemover(c.store, logger, &removed, &mu)

	if err := remover.remove(ctx, c.ids[removeName].String(),
		cluster.RemoveNodeOptions{Policy: cluster.RemovalPolicyOperator}); err != nil {
		t.Fatalf("remove %s: %v", removeName, err)
	}

	peers := cluster.NewPeerState(time.Minute, 0)
	now := time.Now()
	for _, n := range aliveNames {
		peers.Update(c.ids[n].String(), nil, now)
	}
	alerts := alert.New()
	pm := &placementManager{
		cfgStore:    c.store,
		peerState:   peers,
		alerts:      alerts,
		localNodeID: c.ids[aliveNames[0]].String(),
		logger:      logger,
		triggerCh:   make(chan struct{}, 1),
	}
	pm.reconcile(ctx)

	placements, err := c.store.GetVaultPlacements(ctx, vaultID)
	if err != nil {
		t.Fatalf("GetVaultPlacements: %v", err)
	}
	nscs, err := c.store.ListNodeStorageConfigs(ctx)
	if err != nil {
		t.Fatalf("ListNodeStorageConfigs: %v", err)
	}
	holders := map[string]bool{}
	for _, nid := range system.PlacementNodeIDs(placements, nscs) {
		holders[nid] = true
	}
	return holders, alerts
}

// leaderAfter returns the vault's current leader node ID.
func leaderAfter(t *testing.T, c *rfTestCluster, vaultID glid.GLID) string {
	t.Helper()
	ctx := context.Background()
	placements, _ := c.store.GetVaultPlacements(ctx, vaultID)
	nscs, _ := c.store.ListNodeStorageConfigs(ctx)
	return system.LeaderNodeID(placements, nscs)
}

// The memory-vault case is the one that survives eligibility: memory vaults
// report EVERY node eligible ("any node can serve memory vaults"), including a
// node that has left, so passing the state guard is not on its own enough to
// re-place one.
func TestPlacement_DepartedLeader_MemoryVault_IsReplaced(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	for _, n := range []string{"a", "b", "c", "d"} {
		c.node(n, system.NodeStateLive)
	}
	// "a" is the leader (memVault makes the first node leader).
	vaultID := c.memVault("triple", 3, "a", "b", "c")

	holders, alerts := placementAfterReconcile(t, c, "a", []string{"b", "c", "d"}, vaultID)

	if holders[c.ids["a"].String()] {
		t.Errorf("departed leader still holds a placement: %v", holders)
	}
	if leader := leaderAfter(t, c, vaultID); leader == c.ids["a"].String() {
		t.Errorf("leader placement still pinned to the departed node %s", leader)
	} else if leader == "" {
		t.Errorf("vault left with no leader at all after the leader departed")
	}
	if len(holders) != 3 {
		t.Errorf("RF=3 not restored after the leader departed: holders %v", holders)
	}
	if hasAlert(alerts, "vault-soft-offline-leader:") {
		t.Errorf("soft-offline-leader alarm raised for a DEPARTED node: that alarm means " +
			"'still a member, currently unreachable', and re-placement should have cleared it")
	}
}

// The file-vault case fails eligibility on its own, because DeleteNode sweeps
// the node's storage config. It is covered too so the outcome does not depend
// on which mechanism happens to catch the departure.
func TestPlacement_DepartedLeader_FileVault_IsReplaced(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	for _, n := range []string{"a", "b", "c", "d"} {
		c.node(n, system.NodeStateLive)
	}
	const class = 1
	sa := c.storage("a", class)
	sb := c.storage("b", class)
	sc := c.storage("c", class)
	c.storage("d", class) // spare with the right class
	vaultID := c.fileVault("filed", 3, class, sa, sb, sc)

	holders, _ := placementAfterReconcile(t, c, "a", []string{"b", "c", "d"}, vaultID)

	if holders[c.ids["a"].String()] {
		t.Errorf("departed leader still holds a placement: %v", holders)
	}
	if leader := leaderAfter(t, c, vaultID); leader == c.ids["a"].String() || leader == "" {
		t.Errorf("leader not re-placed off the departed node: got %q", leader)
	}
	if len(holders) != 3 {
		t.Errorf("RF=3 not restored after the leader departed: holders %v", holders)
	}
}

// The other half of the distinction, and the property that must NOT regress: a
// leader that is merely unreachable keeps its placement. That is the soft-offline
// guard (gastrolog-slc6l) and the two-clock guard (gastrolog-2d35dc) doing their
// job — a transiently-absent node must not have its chunks orphaned — and it is
// also the no-auto-remove stance: only an operator removes a node.
//
// Same shape as the departed tests, minus the removal, so the two sit side by
// side and the difference between them is exactly "does the NodeConfig exist".
func TestPlacement_UnreachableLeader_KeepsPlacement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := newRFTestCluster(t)
	for _, n := range []string{"a", "b", "c", "d"} {
		c.node(n, system.NodeStateLive)
	}
	vaultID := c.memVault("triple", 3, "a", "b", "c")
	leaderBefore := leaderAfter(t, c, vaultID)

	// "a" is still a cluster member; it just stopped heartbeating.
	logger, _ := testLogger()
	peers := cluster.NewPeerState(time.Minute, 0)
	now := time.Now()
	for _, n := range []string{"b", "c", "d"} {
		peers.Update(c.ids[n].String(), nil, now)
	}
	alerts := alert.New()
	pm := &placementManager{
		cfgStore:    c.store,
		peerState:   peers,
		alerts:      alerts,
		localNodeID: c.ids["b"].String(),
		logger:      logger,
		triggerCh:   make(chan struct{}, 1),
	}
	pm.reconcile(ctx)

	if got := leaderAfter(t, c, vaultID); got != leaderBefore {
		t.Errorf("leader rotated off a merely-unreachable node: was %q, now %q — "+
			"the two-clock guard must hold placement until the node lifecycle state changes",
			leaderBefore, got)
	}
	if !hasAlert(alerts, "vault-soft-offline-leader:") {
		t.Errorf("expected the soft-offline-leader alarm for an unreachable leader, got none")
	}
}
