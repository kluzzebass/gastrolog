package orchestrator_test

import (
	"strconv"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// Reliability matrix at the orchestrator layer. Complements the vault-FSM-
// level matrix in backend/internal/vaultraft. That one is fast and narrow
// (only vault-ctl Raft); this one is slower but catches bugs at
// orchestrator wiring boundaries — readiness gating on real
// LocalVaultsReplicationReady, ApplyConfig correctness, file-tier
// chunk manager emitting CmdCreateChunk/CmdSealChunk through vault-ctl
// Raft to followers.
//
// The harness uses file-backed tiers (not memory) because only the
// file-tier ChunkManager wires SetAnnouncer — the pathway that propagates
// sealed-chunk metadata across the cluster. Memory-tier chunks stay
// local to the leader and would make replication scenarios vacuous.
//
// Scenarios landed:
//   - OrchRel_FreshCluster_VaultReady           (end-to-end readiness bug regression)
//   - OrchRel_SealedChunk_ReplicatesCrossNode   (append + seal → manifest replicates)
//   - OrchRel_Restart_SealedChunkSurvives       (WAL replay at orchestrator layer)

// Boots a 3-node cluster with real vault-ctl Raft; every node's
// orchestrator reports LocalVaultsReplicationReady=true within the
// harness's deadline. This is the real end-to-end regression test for
// gastrolog-5j6eu: on fresh init with no user commands, readiness must
// flip true because hraft's post-bootstrap LogConfiguration + post-
// election LogNoop advance r.AppliedIndex(), and the isFSMReady closure
// we wire in buildTierRaftCallbacks now keys on that.
//
// Goes through the full orchestrator.LocalVaultsReplicationReady →
// Vault.ReadinessErr → tier.IsFSMReady path used by search/ingest RPCs
// in production.
func TestOrchRel_FreshCluster_VaultReady(t *testing.T) {
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	for _, id := range h.nodeIDs {
		if !h.nodes[id].orch.LocalVaultsReplicationReady() {
			t.Errorf("%s: LocalVaultsReplicationReady=false after harness boot", h.nodes[id].label)
		}
	}
}

// Append records on the leader, force a seal, then confirm the sealed
// chunk's metadata shows up in every node's ListAllChunkMetas within
// the convergence window. Exercises the append → seal → announcer →
// CmdCreateChunk/CmdSealChunk replication path end-to-end through real
// vault-ctl Raft.
func TestOrchRel_SealedChunk_ReplicatesCrossNode(t *testing.T) {
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	const records = 20
	now := time.Now()
	for i := range records {
		rec := chunk.Record{
			SourceTS: now,
			IngestTS: now,
			Raw:      []byte("msg-" + strconv.Itoa(i)),
		}
		if err := h.appendOnLeader(rec); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	h.sealOnLeader()

	h.eventuallyAllSeeSealedChunk(t)
}

// Append + seal on the leader, stop every node, restart every node,
// confirm the sealed chunk metadata is still visible from every node.
// WAL replay at the orchestrator layer — the tier FSM manifest must
// survive a full cluster crash.
func TestOrchRel_Restart_SealedChunkSurvives(t *testing.T) {
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	const records = 15
	now := time.Now()
	for i := range records {
		if err := h.appendOnLeader(chunk.Record{
			SourceTS: now,
			IngestTS: now,
			Raw:      []byte("pre-restart-" + strconv.Itoa(i)),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	h.sealOnLeader()
	h.eventuallyAllSeeSealedChunk(t)

	// Capture pre-restart chunk ID set from the leader.
	pre := h.chunkIDsOnNode(h.nodeIDs[0])
	if len(pre) == 0 {
		t.Fatal("no sealed chunks before restart")
	}

	// Full crash.
	for _, id := range h.nodeIDs {
		h.stopNode(id)
	}
	// Full restart.
	for _, id := range h.nodeIDs {
		h.startNode(id)
	}
	h.waitForAllReady()

	// Post-restart: same chunk IDs should be visible via vault-ctl Raft
	// replay and tier FSM restore.
	h.assertAllNodesSee(pre)
}

// eventuallyAllSeeSealedChunk polls until the leader reports at least one
// sealed chunk, then asserts all nodes see the same set. Used by scenarios
// that append + seal and care about replication success, not specific
// chunk IDs.
func (h *orchRelHarness) eventuallyAllSeeSealedChunk(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(orchHarnessConvWait)
	for time.Now().Before(deadline) {
		leader := h.chunkIDsOnNode(h.nodeIDs[0])
		if len(leader) == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		h.assertAllNodesSee(leader)
		return
	}
	t.Fatalf("no sealed chunk appeared on leader within %s", orchHarnessConvWait)
}
