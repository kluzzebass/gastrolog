package orchestrator_test

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/raftgroup"
)

// openManifestRecords reads the records held by the vault's open chunk
// manifest from a node's vault-ctl FSM; 0 when nothing is open.
func (h *orchRelHarness) openManifestRecords(v vaultSpec, nodeID string) (chunk.ChunkID, int64) {
	sub := h.vaultCtlSubFSM(v, nodeID)
	if sub == nil {
		return chunk.ChunkID{}, 0
	}
	open := sub.OpenChunk()
	if open == nil {
		return chunk.ChunkID{}, 0
	}
	return open.ChunkID, int64(open.TotalRecords) //nolint:gosec // G115: test record counts are tiny
}

// waitOpenManifestRecords waits until the vault's open chunk manifest, as seen
// on nodeID, holds exactly want records, and returns its chunk ID.
func (h *orchRelHarness) waitOpenManifestRecords(v vaultSpec, nodeID string, want int64) chunk.ChunkID {
	h.t.Helper()
	var id chunk.ChunkID
	h.waitProgress(fmt.Sprintf("vault %s: open manifest holding %d records", v.label, want), 30*time.Millisecond,
		func() (string, bool) {
			var got int64
			id, got = h.openManifestRecords(v, nodeID)
			return fmt.Sprintf("open=%s records=%d", id, got), got == want
		}, func() { h.dumpPipelineState(v) })
	return id
}

// vaultCtlLeadership is a node's view of the vault's vault-ctl Raft group:
// who leads, in which term. Two equal observations around an action prove
// leadership did not move while it ran.
func (h *orchRelHarness) vaultCtlLeadership(v vaultSpec, nodeID string) string {
	n := h.nodes[nodeID]
	if n == nil || n.groupMgr == nil {
		return ""
	}
	g := n.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(v.id))
	if g == nil {
		return ""
	}
	_, leaderID := g.Raft.LeaderWithID()
	return fmt.Sprintf("%s@t%s", leaderID, g.Raft.Stats()["term"])
}

// sealOnChunkingLeader runs SealActive on the vault's vault-ctl leader — the
// chunking leader, since leaders are drawn from the vault's homes. An
// ErrNotChunkingLeader answer means leadership moved between finding the
// leader and sealing, so the seal is re-issued on the new leader.
func (h *orchRelHarness) sealOnChunkingLeader(v vaultSpec) (int, *orchRelNode) {
	h.t.Helper()
	const attempts = 20
	for range attempts {
		leader := h.waitForVaultCtlLeaderForVault(v)
		sealed, err := leader.orch.SealActive(v.id)
		if errors.Is(err, orchestrator.ErrNotChunkingLeader) {
			continue
		}
		if err != nil {
			h.t.Fatalf("SealActive on vault-ctl leader %s: %v", leader.label, err)
		}
		return sealed, leader
	}
	h.t.Fatalf("SealActive: vault-ctl leadership moved on each of %d attempts", attempts)
	return 0, nil
}

// nonLeaderHome returns a home of the vault that is not the given leader.
func (h *orchRelHarness) nonLeaderHome(v vaultSpec, leader *orchRelNode) *orchRelNode {
	h.t.Helper()
	for _, idx := range v.nodeIdxs {
		if n := h.nodes[h.nodeIDs[idx]]; n != nil && n.id != leader.id {
			return n
		}
	}
	h.t.Fatalf("vault %s has no home besides the leader", v.label)
	return nil
}

// A seal of a pipeline vault seals the open chunk manifest when it holds
// records, reports exactly what it sealed, and never answers a silent zero
// from a node that cannot commit the seal. Four nodes, the vault homed on
// three, records ingested on the fourth.
func TestOrchPipeline_SealVaultSealsOpenManifest(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline seal test")
	}
	t.Parallel()
	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	enableVault(t, h, v)
	homeIdxs := []int{0, 1, 2}
	ingestNode := h.nodeIDs[3]

	// Nothing ingested: every home reports nothing to seal, leader or not.
	leader := h.waitForVaultCtlLeaderForVault(v)
	for _, idx := range homeIdxs {
		n := h.nodes[h.nodeIDs[idx]]
		if sealed, err := n.orch.SealActive(v.id); err != nil || sealed != 0 {
			t.Fatalf("SealActive on %s with nothing open = (%d, %v), want (0, nil)", n.label, sealed, err)
		}
	}

	// Fewer records than the rotation policy seals at, so the open manifest
	// stays open until the operator seals it.
	const ingested = pipelineChunkMaxRecords / 2
	h.submitIngestRecords(ingestNode, ingested, "seal-vault")
	openID := h.waitOpenManifestRecords(v, h.nodeIDs[0], ingested)

	// A home that is not the chunking leader cannot commit the seal; it must
	// say so rather than report that there was nothing to seal. Leadership is
	// observed on both sides of the call so a mid-call election cannot turn
	// the refusal into a legitimate seal.
	refused := false
	for range 20 {
		leader = h.waitForVaultCtlLeaderForVault(v)
		follower := h.nonLeaderHome(v, leader)
		before := h.vaultCtlLeadership(v, follower.id)
		sealed, err := follower.orch.SealActive(v.id)
		if h.vaultCtlLeadership(v, follower.id) != before {
			continue
		}
		if !errors.Is(err, orchestrator.ErrNotChunkingLeader) || sealed != 0 {
			t.Fatalf("SealActive on non-leader home %s with %d records open = (%d, %v), want (0, ErrNotChunkingLeader)",
				follower.label, ingested, sealed, err)
		}
		refused = true
		break
	}
	if !refused {
		t.Fatal("vault-ctl leadership never held still across a non-leader seal")
	}

	// A node that is not a home holds no vault instance and refuses outright.
	if sealed, err := h.nodes[ingestNode].orch.SealActive(v.id); sealed != 0 ||
		!(errors.Is(err, orchestrator.ErrVaultNotFound) || errors.Is(err, orchestrator.ErrNotChunkingLeader)) {
		t.Fatalf("SealActive on non-home %s with %d records open = (%d, %v), want a refusal",
			h.nodes[ingestNode].label, ingested, sealed, err)
	}
	if id, got := h.openManifestRecords(v, h.nodeIDs[0]); id != openID || got != ingested {
		t.Fatalf("refused seal changed the open manifest: open=%s records=%d, want open=%s records=%d", id, got, openID, ingested)
	}

	sealed, _ := h.sealOnChunkingLeader(v)
	if sealed != 1 {
		t.Fatalf("SealActive on chunking leader = %d, want 1 (the open manifest)", sealed)
	}

	// The sealed manifest becomes a Sealed chunk holding every ingested
	// record, built on every home.
	entries := h.waitSealedRecords(v, h.nodeIDs[0], ingested)
	if len(entries) != 1 || entries[0].ID != openID {
		t.Fatalf("sealed entries = %+v, want exactly the open manifest's chunk %s", entries, openID)
	}
	h.waitGLCBsOnHomes(v, homeIdxs, entries)
	h.waitSearchable(v, h.nodeIDs[1], ingested)

	// Everything is sealed now; a second seal reports zero, truthfully.
	if again, _ := h.sealOnChunkingLeader(v); again != 0 {
		t.Fatalf("second SealActive = %d, want 0 with nothing open", again)
	}
}

// A vault can carry a chunk-manager active chunk beside the pipeline's open
// manifest. Seal means every open chunk: both are sealed by one call and the
// count reports both.
func TestOrchPipeline_SealVaultSealsActiveChunkAndOpenManifest(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline seal test")
	}
	t.Parallel()
	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	enableVault(t, h, v)

	const ingested = pipelineChunkMaxRecords / 2
	h.submitIngestRecords(h.nodeIDs[3], ingested, "seal-both")
	openID := h.waitOpenManifestRecords(v, h.nodeIDs[0], ingested)

	// AppendToVault writes through the chunk manager on the leader, which is
	// how a chunk-manager active chunk comes to sit beside the manifest.
	var leader *orchRelNode
	var activeID chunk.ChunkID
	for range 20 {
		leader = h.waitForVaultCtlLeaderForVault(v)
		before := h.vaultCtlLeadership(v, leader.id)
		now := time.Now()
		for i := range 3 {
			if err := leader.orch.AppendToVault(v.id, chunk.ChunkID{}, chunk.Record{
				SourceTS: now, IngestTS: now, Raw: []byte("active-" + strconv.Itoa(i)),
			}); err != nil {
				t.Fatalf("AppendToVault %d on %s: %v", i, leader.label, err)
			}
		}
		inst := leader.orch.FindLocalVaultInstance(v.id)
		if inst == nil || inst.Chunks == nil {
			t.Fatalf("leader %s has no local vault instance", leader.label)
		}
		active := inst.Chunks.Active()
		if active == nil || active.RecordCount == 0 {
			t.Fatal("premise: no chunk-manager active chunk holding records")
		}
		activeID = active.ID

		sealed, err := leader.orch.SealActive(v.id)
		if h.vaultCtlLeadership(v, leader.id) != before || errors.Is(err, orchestrator.ErrNotChunkingLeader) {
			// Leadership moved under the seal: the active chunk on this node
			// may already be sealed, so start over on the new leader.
			continue
		}
		if err != nil {
			t.Fatalf("SealActive on %s: %v", leader.label, err)
		}
		if sealed != 2 {
			h.dumpPipelineState(v)
			t.Fatalf("SealActive with an active chunk and an open manifest = %d, want 2", sealed)
		}
		break
	}
	if activeID == (chunk.ChunkID{}) {
		t.Fatal("vault-ctl leadership never held still across the seal")
	}

	inst := leader.orch.FindLocalVaultInstance(v.id)
	if active := inst.Chunks.Active(); active != nil && active.ID == activeID {
		t.Fatalf("chunk-manager active chunk %s is still active after the seal", activeID)
	}

	h.waitProgress("both chunks reaching Sealed on every home", 50*time.Millisecond, func() (string, bool) {
		var views []string
		done := true
		for _, idx := range v.nodeIdxs {
			id := h.nodeIDs[idx]
			states := h.chunkStatesOnNodeForVault(v, id)
			a, m := states[activeID], states[openID]
			if a != chunk.ChunkStateSealed || m != chunk.ChunkStateSealed {
				done = false
			}
			views = append(views, fmt.Sprintf("%s active=%s manifest=%s", h.nodes[id].label, a, m))
		}
		return fmt.Sprintf("%v", views), done
	}, func() { h.dumpPipelineState(v) })

	if sub := h.vaultCtlSubFSM(v, h.nodeIDs[0]); sub != nil && sub.OpenChunk() != nil {
		t.Fatalf("open manifest %s still open after the seal", sub.OpenChunk().ChunkID)
	}
}
