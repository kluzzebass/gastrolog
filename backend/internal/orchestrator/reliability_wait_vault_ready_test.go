package orchestrator_test

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/waittest"
)

// TestOrchRel_WaitVaultReadyWakesOnGroupFirstApply: a vault added at runtime
// on a four-node cluster gets a fresh vault-ctl group on every node, which
// needs an election before its FSM applies anything. A WaitVaultReady caller
// started on each node right after its AddVault — before the group can have
// applied — returns once that node's group FSM applies its first entry.
// The callers' contexts carry no deadline: only the readiness signal can
// release them.
func TestOrchRel_WaitVaultReadyWakesOnGroupFirstApply(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node reliability test")
	}
	t.Parallel()
	h := newOrchRelHarness(t, 4, withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords))
	ctx := context.Background()
	homeIdxs := []int{0, 1, 2, 3}
	v, vaultCfg := h.addRuntimeVault("B", homeIdxs)

	done := make(map[string]chan error, len(homeIdxs))
	for _, idx := range homeIdxs {
		n := h.nodes[h.nodeIDs[idx]]
		if err := n.orch.AddVault(ctx, vaultCfg, n.factories); err != nil {
			t.Fatalf("AddVault on %s: %v", n.label, err)
		}
		ch := make(chan error, 1)
		done[n.label] = ch
		go func() { ch <- n.orch.WaitVaultReady(t.Context(), v.id) }()
	}

	failsafe := time.After(waittest.Failsafe)
	for label, ch := range done {
		select {
		case err := <-ch:
			if err != nil {
				t.Errorf("%s: WaitVaultReady = %v, want nil", label, err)
			}
		case <-failsafe:
			t.Fatalf("%s: WaitVaultReady still blocked after the vault's group could elect and apply", label)
		}
	}
}
