package orchestrator_test

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// TestSequencedMaterializeReconcileHealthyIngest is the P5 capstone happy path:
// asymmetric ingesters, spool parity, materialize, and C_r on every replica.
func TestSequencedMaterializeReconcileHealthyIngest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multinode materialize/reconcile capstone in short mode")
	}
	h := newOrchRelHarness(t, 4, withSequencedWritePath(3))
	h.setDefaultIngestRoute(t)

	nodeA := h.nodeIDs[0]
	ingesterA := glid.New()
	ingesterB := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	for i := range 8 {
		rec := gateSequencedRecord("a-", ingesterA, now, uint32(i+1))
		if err := h.ingestOnNode(nodeA, rec); err != nil {
			t.Fatalf("ingest a-%d: %v", i+1, err)
		}
		seq := h.lastAssignedSeq(t, nodeA, rec.EventID)
		h.assertSpoolSlotOnAllReplicas(t, seq, rec)
	}
	for i := range 2 {
		rec := gateSequencedRecord("b-", ingesterB, now, uint32(i+1))
		// Keep assign on nodeA so vault_seq stays contiguous in the active swath.
		if err := h.ingestOnNode(nodeA, rec); err != nil {
			t.Fatalf("ingest b-%d: %v", i+1, err)
		}
		seq := h.lastAssignedSeq(t, nodeA, rec.EventID)
		h.assertSpoolSlotOnAllReplicas(t, seq, rec)
	}

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
}

// TestSequencedSpoolSlotHealRecovery injects a missing replica slot and verifies
// peer spool pull restores convergence on the victim node.
func TestSequencedSpoolSlotHealRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multinode spool heal capstone in short mode")
	}
	h := newOrchRelHarness(t, 4, withSequencedWritePath(3))
	h.setDefaultIngestRoute(t)

	victimID := h.nodeIDs[3]
	ingesterNode := h.nodeIDs[0]
	ingester := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	h.nodes[victimID].orch.SetSpoolReplicaWriteFilterForTest(func(_ glid.GLID, _ chunk.Record) bool {
		return true
	})

	var lastRec chunk.Record
	for i := range 5 {
		lastRec = gateSequencedRecord("heal-", ingester, now, uint32(i+1))
		if err := h.ingestOnNode(ingesterNode, lastRec); err != nil {
			t.Fatalf("ingest %d: %v", i+1, err)
		}
	}
	h.waitForSpoolThroughNodes(t, 5, h.nodeIDs[0], h.nodeIDs[1], h.nodeIDs[2])
	if _, err := h.nodes[victimID].orch.ReadVaultSpoolSeq(h.vaultID, 3); err == nil {
		t.Fatalf("victim %s should miss spool seq 3", h.nodes[victimID].label)
	}
	for _, id := range h.nodeIDs {
		if id == victimID {
			continue
		}
		if _, err := h.nodes[id].orch.ReadVaultSpoolSeq(h.vaultID, 3); err != nil {
			t.Fatalf("node %s: spool seq 3 missing: %v", h.nodes[id].label, err)
		}
	}

	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 5, PrevBoundSeq: 0}
	for _, id := range h.nodeIDs {
		if id == victimID {
			continue
		}
		if _, err := h.nodes[id].orch.MaterializeFenceForTest(h.vaultID, fence); err != nil {
			t.Fatalf("node %s materialize: %v", h.nodes[id].label, err)
		}
	}

	victim := h.nodes[victimID].orch
	victim.SetReplicaWatermarksForTest(h.vaultID, fence.UpperBoundSeq, 0)
	if err := victim.ReconcileFenceForTest(h.vaultID, fence); err == nil {
		t.Fatal("expected reconcile error before heal")
	}
	h.waitForConvergence(t, victimID, fence.UpperBoundSeq)
}

// TestSequencedRestartPreservesReplicaWatermarks verifies durable M_r/C_r survive
// orchestrator restart and idempotent reconcile on a file-backed spool replica.
func TestSequencedRestartPreservesReplicaWatermarks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multinode restart capstone in short mode")
	}
	h := newOrchRelHarness(t, 4, withSequencedWritePath(3))
	h.setDefaultIngestRoute(t)

	nodeA := h.nodeIDs[0]
	ingester := glid.New()
	now := time.Now().Truncate(time.Nanosecond)
	for i := range 4 {
		rec := gateSequencedRecord("rst-", ingester, now, uint32(i+1))
		if err := h.ingestOnNode(nodeA, rec); err != nil {
			t.Fatalf("ingest %d: %v", i+1, err)
		}
	}
	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 4, PrevBoundSeq: 0}
	for _, id := range h.nodeIDs {
		if _, err := h.nodes[id].orch.MaterializeFenceForTest(h.vaultID, fence); err != nil {
			t.Fatalf("node %s materialize: %v", h.nodes[id].label, err)
		}
	}

	restartID := h.nodeIDs[2]
	if got := h.nodes[restartID].orch.ConvergenceWatermark(h.vaultID); got != 4 {
		t.Fatalf("pre-restart C_r = %d, want 4", got)
	}
	h.stopNode(restartID)
	h.startNode(restartID)
	if h.sequencedRF > 0 {
		h.wireCrossNodeReplication()
		h.wireInProcessVaultCtlApply()
		h.wireClusterRecordForwarding()
		h.wireSpoolSlotHeal()
	}
	h.waitForAllReady()

	deadline := time.Now().Add(orchHarnessReadyWait)
	for time.Now().Before(deadline) {
		if _, err := h.nodes[restartID].orch.ListLocalChunkMetas(h.vaultID); err != nil {
			time.Sleep(30 * time.Millisecond)
			continue
		}
		if got := h.nodes[restartID].orch.ConvergenceWatermark(h.vaultID); got == 4 {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("node %s: C_r not restored after restart", h.nodes[restartID].label)
}

// TestSequencedLeaderFailoverDuringMaterialize exercises vault-ctl leader
// transfer during the materialize window and verifies convergence still completes.
func TestSequencedLeaderFailoverDuringMaterialize(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multinode leader failover capstone in short mode")
	}
	h := newOrchRelHarness(t, 4, withSequencedWritePath(3))
	h.setDefaultIngestRoute(t)

	nodeA := h.nodeIDs[0]
	ingester := glid.New()
	now := time.Now().Truncate(time.Nanosecond)
	for i := range 6 {
		rec := gateSequencedRecord("fo-", ingester, now, uint32(i+1))
		if err := h.ingestOnNode(nodeA, rec); err != nil {
			t.Fatalf("ingest %d: %v", i+1, err)
		}
	}

	leader := h.waitForVaultCtlLeader()
	g := leader.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(h.vaultID))
	if g == nil {
		t.Fatal("vault-ctl group missing on leader")
	}
	if err := g.Raft.LeadershipTransfer().Error(); err != nil {
		t.Fatalf("LeadershipTransfer: %v", err)
	}

	deadline := time.Now().Add(orchHarnessLeaderWait)
	for time.Now().Before(deadline) {
		for _, id := range h.nodeIDs {
			n := h.nodes[id]
			grp := n.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(h.vaultID))
			if grp != nil && grp.Raft.State() == hraft.Leader && id != leader.id {
				goto transferred
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("vault-ctl leadership did not transfer within %s", orchHarnessLeaderWait)

transferred:
	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 6, PrevBoundSeq: 0}
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		if _, err := n.orch.MaterializeFenceForTest(h.vaultID, fence); err != nil {
			t.Fatalf("node %s materialize: %v", n.label, err)
		}
		if got := n.orch.ConvergenceWatermark(h.vaultID); got != 6 {
			t.Fatalf("node %s C_r = %d, want 6", n.label, got)
		}
	}
}

func (h *orchRelHarness) waitForConvergence(t *testing.T, nodeID string, want uint64) {
	t.Helper()
	deadline := time.Now().Add(orchHarnessConvWait)
	for time.Now().Before(deadline) {
		if got := h.nodes[nodeID].orch.ConvergenceWatermark(h.vaultID); got >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("node %s: C_r did not reach %d within %s", h.nodes[nodeID].label, want, orchHarnessConvWait)
}

func (h *orchRelHarness) waitForSpoolThroughAllReplicas(t *testing.T, through uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		allPresent := true
		for seq := uint64(1); seq <= through; seq++ {
			for _, id := range h.nodeIDs {
				if _, err := h.nodes[id].orch.ReadVaultSpoolSeq(h.vaultID, seq); err != nil {
					allPresent = false
					break
				}
			}
			if !allPresent {
				break
			}
		}
		if allPresent {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("spool seq 1..%d not present on all replicas within timeout", through)
}

func (h *orchRelHarness) waitForSpoolThroughNodes(t *testing.T, through uint64, nodeIDs ...string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		allPresent := true
		for seq := uint64(1); seq <= through; seq++ {
			for _, id := range nodeIDs {
				if _, err := h.nodes[id].orch.ReadVaultSpoolSeq(h.vaultID, seq); err != nil {
					allPresent = false
					break
				}
			}
			if !allPresent {
				break
			}
		}
		if allPresent {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("spool seq 1..%d not present on selected nodes within timeout", through)
}
