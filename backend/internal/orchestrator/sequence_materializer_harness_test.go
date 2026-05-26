package orchestrator_test

import (
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// TestBurnedTailMaterializeAfterAsymmetricIngest exercises burned-tail recording
// through the seq-assign burn path on a real vault-ctl Raft group, then
// materializes across the gap on every replica.
func TestBurnedTailMaterializeAfterAsymmetricIngest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multinode burned-tail materialize scrutiny in short mode")
	}
	h := newOrchRelHarness(t, 4, withSequencedWritePath(3))
	h.setDefaultIngestRoute(t)

	nodeA := h.nodeIDs[0]
	now := time.Now().Truncate(time.Nanosecond)
	ingesterA := glid.New()

	var lastRec chunk.Record
	for i := range 7 {
		lastRec = gateSequencedRecord("burn-", ingesterA, now, uint32(i+1))
		if err := h.ingestOnNode(nodeA, lastRec); err != nil {
			t.Fatalf("ingest %d: %v", i+1, err)
		}
	}
	if err := h.nodes[nodeA].orch.BurnActiveSeqLeaseTailForTest(h.vaultID, 7); err != nil {
		t.Fatalf("burn seq lease tail: %v", err)
	}
	h.assertAllocatorBurnedTailCovers(t, 8, 10)

	seq := h.lastAssignedSeq(t, nodeA, lastRec.EventID)
	h.assertSpoolSlotOnAllReplicas(t, seq, lastRec)

	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 10, PrevBoundSeq: 0}
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		if _, err := n.orch.MaterializeFenceForTest(h.vaultID, fence); err != nil {
			t.Fatalf("node %s materialize: %v", n.label, err)
		}
		if got := n.orch.ConvergenceWatermark(h.vaultID); got != 10 {
			t.Fatalf("node %s C_r = %d, want 10", n.label, got)
		}
	}
	h.assertNoChunkAppendLanding(t)
}

func (h *orchRelHarness) assertAllocatorBurnedTailCovers(t *testing.T, wantStart, wantEnd uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ok, fail := h.burnedTailCoversAllNodes(wantStart, wantEnd); ok {
			return
		} else if fail != "" {
			_ = fail
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, fail := h.burnedTailCoversAllNodes(wantStart, wantEnd)
	t.Fatal(fail)
}

func (h *orchRelHarness) burnedTailCoversAllNodes(wantStart, wantEnd uint64) (bool, string) {
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		sub, err := n.orch.VaultCtlSubFSMForTest(h.vaultID)
		if err != nil {
			return false, fmt.Sprintf("node %s VaultCtlSubFSM: %v", n.label, err)
		}
		if sub == nil {
			return false, fmt.Sprintf("node %s: vault-ctl FSM missing", n.label)
		}
		alloc := sub.SeqAllocatorState()
		found := false
		for _, tail := range alloc.BurnedTails {
			if tail.Start <= wantStart && tail.End >= wantEnd {
				found = true
				break
			}
		}
		if !found {
			return false, fmt.Sprintf("node %s BurnedTails = %+v, want a tail covering %d-%d", n.label, alloc.BurnedTails, wantStart, wantEnd)
		}
	}
	return true, ""
}
