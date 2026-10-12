package app

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// Placement against node lifecycle state and heartbeat liveness. Heartbeat
// liveness is momentary — a pod restart drops a node out of the alive set for
// seconds — so it must never cost a node a placement it already holds. The
// lifecycle state decides two things: whether a member counts toward the
// replication factor, and whether a node may take a placement it does not
// already hold.

// reconcileWith runs one placement pass over c with exactly the named nodes
// heartbeat-alive. localName is the reconciling node and is always alive.
func reconcileWith(t *testing.T, c *rfTestCluster, localName string, aliveNames ...string) *alert.Collector {
	t.Helper()
	logger, _ := testLogger()
	peers := cluster.NewPeerState(time.Minute, 0)
	now := time.Now()
	for _, n := range aliveNames {
		if n != localName {
			peers.Update(c.ids[n].String(), nil, now)
		}
	}
	alerts := alert.New()
	pm := &placementManager{
		cfgStore:    c.store,
		peerState:   peers,
		alerts:      alerts,
		localNodeID: c.ids[localName].String(),
		logger:      logger,
		triggerCh:   make(chan struct{}, 1),
	}
	pm.reconcile(context.Background())
	return alerts
}

// holderNames returns the vault's placement holders by node name, leader first.
func holderNames(t *testing.T, c *rfTestCluster, vaultID glid.GLID) (leader string, followers []string) {
	t.Helper()
	ctx := context.Background()
	placements, err := c.store.GetVaultPlacements(ctx, vaultID)
	if err != nil {
		t.Fatalf("GetVaultPlacements: %v", err)
	}
	nscs, err := c.store.ListNodeStorageConfigs(ctx)
	if err != nil {
		t.Fatalf("ListNodeStorageConfigs: %v", err)
	}
	names := make(map[string]string, len(c.ids))
	for name, id := range c.ids {
		names[id.String()] = name
	}
	nameOf := func(id string) string {
		if n, ok := names[id]; ok {
			return n
		}
		return id
	}
	if l := system.LeaderNodeID(placements, nscs); l != "" {
		leader = nameOf(l)
	}
	for _, f := range system.FollowerNodeIDs(placements, nscs) {
		followers = append(followers, nameOf(f))
	}
	slices.Sort(followers)
	return leader, followers
}

var nonLiveStates = []system.NodeState{
	system.NodeStateUnreachable,
	system.NodeStateMaintenance,
}

// Every lifecycle state crossed with heartbeat liveness, for a follower that
// already holds a placement. The follower is always retained; it counts
// toward RF — so no backfill — exactly when its node is a working replica.
func TestSelectFollowers_ExistingFollowerByStateAndLiveness(t *testing.T) {
	t.Parallel()
	// Whether a Live spare is backfilled: only when the existing follower
	// is not a working replica.
	backfills := map[system.NodeState]bool{
		system.NodeStateLive:        false,
		system.NodeStateUnreachable: true,
		system.NodeStateMaintenance: true,
	}
	for state, wantBackfill := range backfills {
		for _, alive := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/alive=%v", state, alive), func(t *testing.T) {
				t.Parallel()
				c := newRFTestCluster(t)
				c.node("a", system.NodeStateLive)
				c.node("b", state)
				c.node("c", system.NodeStateLive)
				vaultID := c.memVault("pair", 2, "a", "b")

				aliveNames := []string{"a", "c"}
				if alive {
					aliveNames = append(aliveNames, "b")
				}
				alerts := reconcileWith(t, c, "a", aliveNames...)

				leader, followers := holderNames(t, c, vaultID)
				if leader != "a" {
					t.Fatalf("leader moved: got %q", leader)
				}
				if !slices.Contains(followers, "b") {
					t.Fatalf("follower on a %s node (alive=%v) lost its placement: followers %v", state, alive, followers)
				}
				if gotBackfill := slices.Contains(followers, "c"); gotBackfill != wantBackfill {
					t.Fatalf("backfill onto c = %v, want %v (followers %v)", gotBackfill, wantBackfill, followers)
				}
				if hasAlert(alerts, "vault-underreplicated:") {
					t.Errorf("RF is met (directly or by backfill); under-replicated alarm must not be raised")
				}
			})
		}
	}
}

// A follower whose node has left the cluster — no NodeConfig and no
// heartbeat — is dropped and replaced; a follower whose NodeConfig has not
// landed yet but which is heartbeating is kept and counted.
func TestSelectFollowers_DepartedVersusConfigPending(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("departed", func(t *testing.T) {
		t.Parallel()
		c := newRFTestCluster(t)
		c.node("a", system.NodeStateLive)
		gone := c.node("b", system.NodeStateLive)
		c.node("c", system.NodeStateLive)
		vaultID := c.memVault("pair", 2, "a", "b")
		if err := c.store.DeleteNode(ctx, gone); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}

		reconcileWith(t, c, "a", "a", "c")

		if _, followers := holderNames(t, c, vaultID); !slices.Equal(followers, []string{"c"}) {
			t.Fatalf("departed follower must be replaced by c, got %v", followers)
		}
	})

	t.Run("config pending", func(t *testing.T) {
		t.Parallel()
		c := newRFTestCluster(t)
		c.node("a", system.NodeStateLive)
		c.node("c", system.NodeStateLive)
		pending := glid.New()
		c.ids["b"] = pending // heartbeating, NodeConfig not written yet
		vaultID := c.memVault("pair", 2, "a", "b")

		reconcileWith(t, c, "a", "a", "b", "c")

		if _, followers := holderNames(t, c, vaultID); !slices.Equal(followers, []string{"b"}) {
			t.Fatalf("follower without a NodeConfig yet must be kept and counted, got %v", followers)
		}
	})
}

// A follower pod restarting is out of the alive set for a moment. The
// placement must come through untouched — no drop, no backfill — or the node
// removes its vault instance and another node rebuilds the replica.
func TestPlacement_FollowerRestart_KeepsPlacement(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	all := []string{"a", "b", "c", "d"}
	for _, n := range all {
		c.node(n, system.NodeStateLive)
	}
	const class = 1
	sa := c.storage("a", class)
	sb := c.storage("b", class)
	c.storage("c", class)
	c.storage("d", class)
	fileID := c.fileVault("files", 2, class, sa, sb)
	memID := c.memVault("mem", 2, "a", "b")

	for _, alive := range [][]string{all, {"a", "c", "d"}, all} {
		alerts := reconcileWith(t, c, "a", alive...)
		for _, id := range []glid.GLID{fileID, memID} {
			leader, followers := holderNames(t, c, id)
			if leader != "a" || !slices.Equal(followers, []string{"b"}) {
				t.Fatalf("alive=%v: placement changed during a follower restart: leader %q followers %v", alive, leader, followers)
			}
		}
		if hasAlert(alerts, "vault-underreplicated:") {
			t.Fatalf("alive=%v: a restarting follower is not a missing replica", alive)
		}
	}
}

// A rolling restart across a 5-node cluster: each node in turn drops out of
// the alive set while a reconcile runs. No vault's placement changes at any
// step.
func TestPlacement_RollingRestart_NoPlacementChurn(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	all := []string{"a", "b", "c", "d", "e"}
	for _, n := range all {
		c.node(n, system.NodeStateLive)
	}
	vaults := map[glid.GLID][2]any{}
	record := func(id glid.GLID) {
		l, f := holderNames(t, c, id)
		vaults[id] = [2]any{l, f}
	}
	record(c.memVault("v1", 3, "a", "b", "c"))
	record(c.memVault("v2", 3, "c", "d", "e"))
	record(c.memVault("v3", 2, "e", "a"))

	for _, restarting := range all {
		local := "a"
		if restarting == "a" {
			local = "b"
		}
		var alive []string
		for _, n := range all {
			if n != restarting {
				alive = append(alive, n)
			}
		}
		reconcileWith(t, c, local, alive...)
		for id, want := range vaults {
			l, f := holderNames(t, c, id)
			if l != want[0] || !slices.Equal(f, want[1].([]string)) {
				t.Fatalf("restart of %s: vault placement changed from %v to leader %q followers %v", restarting, want, l, f)
			}
		}
	}
}

// A soft-offline follower is retained but does not count; a Live spare is
// backfilled. When the node returns to Live it counts again, and the surplus
// backfill is trimmed — the original member stays.
func TestPlacement_SoftOfflineFollower_BackfillThenTrimOnReturn(t *testing.T) {
	t.Parallel()
	for _, state := range []system.NodeState{system.NodeStateUnreachable, system.NodeStateMaintenance} {
		t.Run(state.String(), func(t *testing.T) {
			t.Parallel()
			c := newRFTestCluster(t)
			for _, n := range []string{"a", "b", "c", "d"} {
				c.node(n, system.NodeStateLive)
			}
			vaultID := c.memVault("triple", 3, "a", "b", "c")

			c.setState("b", state)
			reconcileWith(t, c, "a", "a", "c", "d")
			if _, f := holderNames(t, c, vaultID); !slices.Equal(f, []string{"b", "c", "d"}) {
				t.Fatalf("%s follower must be retained and d backfilled, got %v", state, f)
			}

			c.setState("b", system.NodeStateLive)
			reconcileWith(t, c, "a", "a", "b", "c", "d")
			if _, f := holderNames(t, c, vaultID); !slices.Equal(f, []string{"b", "c"}) {
				t.Fatalf("after b returns the backfill must trim back to RF, keeping b: got %v", f)
			}
		})
	}
}

// With no spare, a soft-offline follower is retained and the vault is
// reported under-replicated rather than silently counted.
func TestPlacement_SoftOfflineFollower_NoSpare_UnderReplicated(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateUnreachable)
	vaultID := c.memVault("pair", 2, "a", "b")

	alerts := reconcileWith(t, c, "a", "a")

	if _, f := holderNames(t, c, vaultID); !slices.Equal(f, []string{"b"}) {
		t.Fatalf("unreachable follower must be retained, got %v", f)
	}
	if !hasAlert(alerts, "vault-underreplicated:") {
		t.Fatal("an unreachable follower is not a working replica; expected the under-replicated alarm")
	}
}

// A heartbeating node in any non-Live lifecycle state is never chosen as a
// new follower.
func TestPlacement_NonLiveNode_NeverNewFollower(t *testing.T) {
	t.Parallel()
	for _, state := range nonLiveStates {
		t.Run(state.String(), func(t *testing.T) {
			t.Parallel()
			c := newRFTestCluster(t)
			c.node("a", system.NodeStateLive)
			c.node("x", state)
			c.node("y", system.NodeStateLive)
			c.node("z", system.NodeStateLive)
			vaultID := c.memVault("triple", 3, "a")

			alerts := reconcileWith(t, c, "a", "a", "x", "y", "z")

			if _, f := holderNames(t, c, vaultID); !slices.Equal(f, []string{"y", "z"}) {
				t.Fatalf("followers must be the Live nodes y,z only, got %v", f)
			}
			if hasAlert(alerts, "vault-underreplicated:") {
				t.Errorf("RF met by Live nodes; no under-replicated alarm expected")
			}

			// With only the non-Live node left as a spare, the vault stays
			// under-replicated rather than placing onto it.
			c2 := newRFTestCluster(t)
			c2.node("a", system.NodeStateLive)
			c2.node("x", state)
			v2 := c2.memVault("pair", 2, "a")
			alerts2 := reconcileWith(t, c2, "a", "a", "x")
			if _, f := holderNames(t, c2, v2); len(f) != 0 {
				t.Fatalf("%s node must not take a new follower placement, got %v", state, f)
			}
			if !hasAlert(alerts2, "vault-underreplicated:") {
				t.Errorf("expected the under-replicated alarm when the only spare is %s", state)
			}
		})
	}
}

// A heartbeating node in any non-Live lifecycle state is never chosen as a
// new leader.
func TestPlacement_NonLiveNode_NeverNewLeader(t *testing.T) {
	t.Parallel()
	for _, state := range nonLiveStates {
		t.Run(state.String(), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			c := newRFTestCluster(t)
			c.node("a", system.NodeStateLive) // reconciling node; no storage of the class
			c.node("x", state)
			const class = 7
			c.storage("x", class)
			vaultID := glid.New()
			if err := c.store.PutVault(ctx, system.VaultConfig{
				ID: vaultID, Name: "files", Type: system.VaultTypeFile, StorageClass: class,
			}); err != nil {
				t.Fatalf("PutVault: %v", err)
			}

			alerts := reconcileWith(t, c, "a", "a", "x")
			if l, _ := holderNames(t, c, vaultID); l != "" {
				t.Fatalf("%s node took a new leader placement: leader %q", state, l)
			}
			if !hasAlert(alerts, "vault-no-eligible-node:") {
				t.Errorf("expected the no-eligible-node alarm when the only storage is on a %s node", state)
			}

			// A Live node with the class appears: it is the one chosen.
			c.node("y", system.NodeStateLive)
			c.storage("y", class)
			reconcileWith(t, c, "a", "a", "x", "y")
			if l, _ := holderNames(t, c, vaultID); l != "y" {
				t.Fatalf("leader must land on the Live node y, got %q", l)
			}
		})
	}
}

// The RF-preservation gate and the placer answer the same question: after
// the target is removed, can the vault get back to RF? The gate's verdict
// must match what the placer actually does, for every lifecycle state the
// spare can be in — with the spare heartbeating, so liveness cannot mask a
// disagreement on state.
func TestRFGate_AgreesWithPlacer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	states := append([]system.NodeState{system.NodeStateLive}, nonLiveStates...)
	for _, state := range states {
		t.Run(state.String(), func(t *testing.T) {
			t.Parallel()
			c := newRFTestCluster(t)
			target := c.node("a", system.NodeStateLive)
			c.node("b", system.NodeStateLive)
			c.node("c", system.NodeStateLive)
			c.node("d", state)
			vaultID := c.memVault("triple", 3, "a", "b", "c")

			gateAllows := len(c.below("a")) == 0

			if err := c.store.DeleteNode(ctx, target); err != nil {
				t.Fatalf("DeleteNode: %v", err)
			}
			reconcileWith(t, c, "b", "b", "c", "d")
			leader, followers := holderNames(t, c, vaultID)
			holders := len(followers)
			if leader != "" {
				holders++
			}
			placerRestores := holders >= 3

			if gateAllows != placerRestores {
				t.Fatalf("spare in state %s: gate allows removal = %v, placer restores RF = %v (leader %q followers %v)",
					state, gateAllows, placerRestores, leader, followers)
			}
			if wantAllow := state == system.NodeStateLive; gateAllows != wantAllow {
				t.Fatalf("spare in state %s: gate allows removal = %v, want %v", state, gateAllows, wantAllow)
			}
		})
	}
}

func TestAcceptsNewPlacements(t *testing.T) {
	t.Parallel()
	want := map[system.NodeState]bool{
		system.NodeStateUnknown:     true,
		system.NodeStateLive:        true,
		system.NodeStateUnreachable: false,
		system.NodeStateMaintenance: false,
	}
	for state, w := range want {
		if got := acceptsNewPlacements(state); got != w {
			t.Errorf("acceptsNewPlacements(%s) = %v, want %v", state, got, w)
		}
	}
}
