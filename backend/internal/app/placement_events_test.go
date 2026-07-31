package app

import (
	"context"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
)

// The app-side placement tick (placementReconcileSchedule) is a safety net for
// the one input that has no FSM event — peer-heartbeat liveness expiry (TTL
// poll). Every OTHER placement input arrives as an FSM notification, and the
// dispatcher must wake the placement manager / orchestrator reconcile on it
// rather than leaving it to the periodic pass. Each subtest
// drives one notification with NO scheduler wired — the tick neutered — and
// asserts the event-driven delegation fired.
func TestPlacementEventDelegation(t *testing.T) {
	t.Parallel()

	newWithTrigger := func(mo *mockOrch, store system.Store) (*configDispatcher, *int) {
		d := newTestDispatcher(mo, store, &captureHandler{})
		var triggers int
		d.placementTrigger = func() { triggers++ }
		return d, &triggers
	}

	t.Run("ingester_put_wakes_placement", func(t *testing.T) {
		t.Parallel()
		mo := &mockOrch{}
		d, triggers := newWithTrigger(mo, &stubCfgStore{})
		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: glid.New()})
		if *triggers != 1 {
			t.Fatalf("ingester put: want 1 placement trigger (singleton assignment), got %d", *triggers)
		}
	})

	t.Run("ingester_delete_wakes_placement", func(t *testing.T) {
		t.Parallel()
		mo := &mockOrch{}
		d, triggers := newWithTrigger(mo, &stubCfgStore{})
		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterDeleted, ID: glid.New()})
		if *triggers != 1 {
			t.Fatalf("ingester delete: want 1 placement trigger, got %d", *triggers)
		}
	})

	t.Run("node_config_put_wakes_placement", func(t *testing.T) {
		t.Parallel()
		mo := &mockOrch{}
		d, triggers := newWithTrigger(mo, &stubCfgStore{})
		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyNodeConfigPut, ID: glid.New()})
		if *triggers != 1 {
			t.Fatalf("node config put: want 1 placement trigger (membership eligibility), got %d", *triggers)
		}
	})

	t.Run("node_config_delete_wakes_placement", func(t *testing.T) {
		t.Parallel()
		mo := &mockOrch{}
		d, triggers := newWithTrigger(mo, &stubCfgStore{})
		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyNodeConfigDeleted, ID: glid.New()})
		if *triggers != 1 {
			t.Fatalf("node config delete: want 1 placement trigger, got %d", *triggers)
		}
	})

	t.Run("nsc_set_wakes_placement_and_orchestrator_reconcile", func(t *testing.T) {
		t.Parallel()
		mo := &mockOrch{}
		d, triggers := newWithTrigger(mo, &stubCfgStore{})
		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyNodeStorageConfigSet, ID: glid.New()})
		if *triggers != 1 {
			t.Fatalf("NSC set: want 1 placement trigger, got %d", *triggers)
		}
		// NSC remaps storage→node, which shifts FollowerTargets/role without a
		// placement edit, so the orchestrator must reconcile every local
		// instance directly.
		if mo.reconcilePlacementsCalls != 1 {
			t.Fatalf("NSC set: want 1 orchestrator ReconcilePlacements, got %d", mo.reconcilePlacementsCalls)
		}
	})

	t.Run("placements_set_reconciles_that_vault", func(t *testing.T) {
		t.Parallel()
		vaultID := glid.New()
		mo := &mockOrch{}
		d, _ := newWithTrigger(mo, &stubCfgStore{
			vault:      &system.VaultConfig{ID: vaultID, Enabled: true, Type: system.VaultTypeMemory},
			placements: map[glid.GLID][]system.VaultPlacement{vaultID: {{StorageID: system.SyntheticStorageID("local"), Leader: true}}},
		})
		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPlacementsSet, ID: vaultID})
		if len(mo.reconcileVaultPlacementCalls) != 1 || mo.reconcileVaultPlacementCalls[0] != vaultID {
			t.Fatalf("placements set: want ReconcileVaultPlacement(%s), got %v", vaultID, mo.reconcileVaultPlacementCalls)
		}
	})

	t.Run("vault_delete_republishes_routing", func(t *testing.T) {
		t.Parallel()
		vaultID := glid.New()
		mo := &mockOrch{vaults: []glid.GLID{vaultID}}
		d, _ := newWithTrigger(mo, &stubCfgStore{})
		before := mo.reloadFiltersCalls
		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultDeleted, ID: vaultID})
		if mo.reloadFiltersCalls != before+1 {
			t.Fatalf("vault delete: want routing republish (ReloadFilters), got %d calls", mo.reloadFiltersCalls-before)
		}
	})

	t.Run("replay_reconciles_all_placements", func(t *testing.T) {
		t.Parallel()
		mo := &mockOrch{}
		d := newTestDispatcher(mo, &stubCfgStore{}, &captureHandler{})
		d.ReplayConfigFromStore(context.Background())
		if mo.reconcilePlacementsCalls != 1 {
			t.Fatalf("replay: want 1 orchestrator ReconcilePlacements (post-snapshot catch-up), got %d", mo.reconcilePlacementsCalls)
		}
	})
}
