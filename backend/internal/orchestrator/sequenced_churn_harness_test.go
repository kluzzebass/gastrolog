package orchestrator_test

import (
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// TestSequencedPausedPeerIngestCompletes verifies sequenced ingest and spool
// fan-out remain healthy when one replica peer is paused.
func TestSequencedPausedPeerIngestCompletes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sequenced paused-peer churn in short mode")
	}
	h := newOrchRelHarness(t, 4, withSequencedWritePath(3))
	h.setDefaultIngestRoute(t)

	victim := h.nodeIDs[3]
	h.pausePeer(victim)
	t.Cleanup(func() { h.unpausePeer(victim) })

	nodeA := h.nodeIDs[0]
	ingester := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	for i := range 12 {
		rec := gateSequencedRecord("pause-", ingester, now, uint32(i+1))
		if err := h.ingestOnNode(nodeA, rec); err != nil {
			t.Fatalf("ingest %d: %v", i+1, err)
		}
		seq := h.lastAssignedSeq(t, nodeA, rec.EventID)
		live := []string{h.nodeIDs[0], h.nodeIDs[1], h.nodeIDs[2]}
		h.assertSpoolSlotOnVaultNodes(t, h.vaultID, live, seq, rec)
	}
	h.assertNoChunkAppendLanding(t)
}

// TestSequencedSlowPeerBurstIngestAbsorbs verifies burst ingest completes while
// one replica peer is artificially slowed.
func TestSequencedSlowPeerBurstIngestAbsorbs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sequenced slow-peer churn in short mode")
	}
	h := newOrchRelHarness(t, 4, withSequencedWritePath(3))
	h.setDefaultIngestRoute(t)

	victim := h.nodeIDs[2]
	h.slowPeer(victim, 150*time.Millisecond)
	t.Cleanup(func() { h.slowPeer(victim, 0) })

	nodeB := h.nodeIDs[1]
	ingester := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	const burst = 40
	for i := range burst {
		rec := gateSequencedRecord("slow-burst-", ingester, now, uint32(i+1))
		if err := h.ingestOnNode(nodeB, rec); err != nil {
			t.Fatalf("burst ingest %d: %v", i+1, err)
		}
	}
	h.waitForSpoolThroughAllReplicasExtended(t, burst, 30*time.Second)
	h.assertNoChunkAppendLanding(t)
}

// TestSequencedFollowerWipeReconcileHealCatchup wipes a follower's durable
// state, then verifies assigned-missing heal during fence reconcile restores
// spool slots from peers.
func TestSequencedFollowerWipeReconcileHealCatchup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sequenced follower-wipe churn in short mode")
	}
	h := newOrchRelHarness(t, 4, withSequencedWritePath(3))
	h.setDefaultIngestRoute(t)

	victimID := h.nodeIDs[3]
	ingesterNode := h.nodeIDs[0]
	ingester := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	for i := range 6 {
		rec := gateSequencedRecord("wipe-", ingester, now, uint32(i+1))
		if err := h.ingestOnNode(ingesterNode, rec); err != nil {
			t.Fatalf("ingest %d: %v", i+1, err)
		}
	}
	h.waitForSpoolThroughAllReplicas(t, 6)

	h.stopNode(victimID)
	h.wipeNode(victimID)
	h.startNode(victimID)
	if h.sequencedRF > 0 {
		h.wireCrossNodeReplication()
		h.wireInProcessVaultCtlApply()
		h.wireClusterRecordForwarding()
		h.wireSpoolSlotHeal()
	}
	h.waitForAllReady()

	fence := vaultctlfsm.FenceRecord{ID: 1, UpperBoundSeq: 6, PrevBoundSeq: 0}
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
