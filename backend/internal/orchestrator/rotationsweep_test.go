package orchestrator

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/lifecycle"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// TestApplyRotationFromConfig_RefreshesRotationReceivingSnapshot is the sweep
// half of the gastrolog-2oav7 fix: a vault instance built when the cluster's
// NSCs were incomplete locks the rotation coordinator's c.receiving to a
// short list. The 15s rotationSweep tick now recomputes the snapshot from
// current placements + NSCs and pushes it through SetReceiving, so by the
// next BeginRotation the list reflects the full placement.
func TestApplyRotationFromConfig_RefreshesRotationReceivingSnapshot(t *testing.T) {
	t.Parallel()

	orch := newTestOrch(t, Config{LocalNodeID: "node-A", Phase: lifecycle.New()})

	vaultID := glid.New()
	storageIDs := []glid.GLID{glid.New(), glid.New(), glid.New()}
	placements := []system.VaultPlacement{
		{StorageID: storageIDs[0].String()},
		{StorageID: storageIDs[1].String()},
		{StorageID: storageIDs[2].String()},
	}
	nscs := []system.NodeStorageConfig{
		{NodeID: "node-A", FileStorages: []system.FileStorage{{ID: storageIDs[0]}}},
		{NodeID: "node-B", FileStorages: []system.FileStorage{{ID: storageIDs[1]}}},
		{NodeID: "node-C", FileStorages: []system.FileStorage{{ID: storageIDs[2]}}},
	}

	// Pre-fix repro: coordinator built with a stale (incomplete) receiving
	// snapshot — node-C is missing, mirroring an NSC that hadn't replicated
	// yet at instance build time. Wired into a VaultInstance via the
	// RotationCoordinator field that the sweep refreshes.
	fsm := vaultctlfsm.New()
	applier := &captureApplier{fsm: fsm}
	coord := newRotationCoordinator(vaultID, applier, fsm,
		func() time.Time { return time.Now() }, []string{"node-A", "node-B"})

	vaultInst := &VaultInstance{
		VaultID:             vaultID,
		Type:                "memory",
		Chunks:              nil, // applyFanOutConfig type-asserts to FanOutConfigSetter and returns when absent
		RotationCoordinator: coord,
	}

	sys := &system.System{
		Runtime: system.Runtime{NodeStorageConfigs: nscs},
	}
	cfg := &sys.Config
	vaultCfg := system.VaultConfig{
		ID:         vaultID,
		Name:       "fanout-refresh-test",
		Placements: placements,
		// RotationPolicyID nil — return after the receiving refresh,
		// avoiding the cron-job side effects further down the function.
	}

	orch.applyRotationFromConfig(sys, cfg, vaultCfg, vaultInst, map[string]bool{})

	// Trigger a rotation; the new chunk's payload should now stamp the
	// full 3-node Receiving set into FSM Holding.
	newID, err := coord.BeginRotation(chunk.ChunkID{})
	if err != nil {
		t.Fatalf("BeginRotation: %v", err)
	}
	p := fsm.Placement(newID)
	if p == nil {
		t.Fatal("new chunk has no placement entry")
	}
	if len(p.Receiving) != 3 {
		t.Errorf("Receiving = %v, want 3 nodes (sweep refresh didn't take effect)", p.Receiving)
	}
	want := map[string]bool{"node-A": true, "node-B": true, "node-C": true}
	for _, n := range p.Receiving {
		if !want[n] {
			t.Errorf("unexpected node %q in Receiving", n)
		}
		delete(want, n)
	}
	if len(want) > 0 {
		t.Errorf("missing nodes after sweep refresh: %v", want)
	}
}
