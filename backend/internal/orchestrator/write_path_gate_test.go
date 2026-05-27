package orchestrator_test

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
)

// TestWritePathGateFourNodeAsymmetricIngest is the mandatory P0/P10 gate from
// docs/fan-out/v2/write-path-lock.md: RF≥3 sequenced vault, asymmetric
// ingesters on two routers, swath assign + direct spool replica fan-out.
func TestWritePathGateFourNodeAsymmetricIngest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 4-node write-path gate in short mode")
	}
	h := newOrchRelHarness(t, 4, withSequencedWritePath(3))
	h.setDefaultIngestRoute(t)

	nodeA := h.nodeIDs[0]
	nodeB := h.nodeIDs[1]
	ingesterA := glid.New()
	ingesterB := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	ingestAndAssert := func(nodeID string, rec chunk.Record) {
		t.Helper()
		if err := h.ingestOnNode(nodeID, rec); err != nil {
			t.Fatalf("ingest on %s: %v", h.nodes[nodeID].label, err)
		}
		seq := h.lastAssignedSeq(t, nodeID, rec.EventID)
		h.assertSpoolSlotOnAllReplicas(t, seq, rec)
	}

	for i := range 30 {
		rec := gateSequencedRecord("a-", ingesterA, now, uint32(i+1))
		ingestAndAssert(nodeA, rec)
	}
	for i := range 5 {
		rec := gateSequencedRecord("b-", ingesterB, now, uint32(i+1))
		ingestAndAssert(nodeB, rec)
	}

	// Cross-node routed ingest: remote NodeID in route must not block local assign.
	remoteNode := h.nodeIDs[3]
	crAll, _ := orchestrator.CompileRoute(glid.New(), "all", 0, "*",
		[]orchestrator.RouteDestination{{VaultID: h.vaultID}}, "gate-all")
	crRemote, err := orchestrator.CompileRoute(glid.New(), "remote", 0, `env="gate"`,
		[]orchestrator.RouteDestination{{VaultID: h.vaultID, NodeID: remoteNode}}, "gate-remote")
	if err != nil {
		t.Fatal(err)
	}
	h.nodes[nodeB].orch.SetRouteSet(orchestrator.NewRouteSet([]*orchestrator.CompiledRoute{crAll, crRemote}))
	recRemote := gateSequencedRecord("remote", ingesterB, now, 99)
	recRemote.Attrs = chunk.Attributes{"env": "gate"}
	ingestAndAssert(nodeB, recRemote)

	h.assertNoChunkAppendLanding(t)
}

// TestWritePathGateBurstAsymmetricIngesters exercises extreme rate skew between
// two ingesters on the same vault (slow A, burst B) while preserving per-seq
// spool parity on every replica.
func TestWritePathGateBurstAsymmetricIngesters(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping burst asymmetric write-path gate in short mode")
	}
	h := newOrchRelHarness(t, 4, withSequencedWritePath(3))
	h.setDefaultIngestRoute(t)

	nodeA := h.nodeIDs[0]
	nodeB := h.nodeIDs[1]
	ingesterA := glid.New()
	ingesterB := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	ingestAndAssert := func(nodeID string, rec chunk.Record) {
		t.Helper()
		if err := h.ingestOnNode(nodeID, rec); err != nil {
			t.Fatalf("ingest on %s: %v", h.nodes[nodeID].label, err)
		}
		seq := h.lastAssignedSeq(t, nodeID, rec.EventID)
		h.assertSpoolSlotOnAllReplicas(t, seq, rec)
	}

	for i := range 5 {
		rec := gateSequencedRecord("slow-", ingesterA, now, uint32(i+1))
		ingestAndAssert(nodeA, rec)
		time.Sleep(50 * time.Millisecond)
	}

	const burstRecords = 50
	for i := range burstRecords {
		rec := gateSequencedRecord("burst-", ingesterB, now, uint32(i+1))
		ingestAndAssert(nodeB, rec)
	}

	h.assertNoChunkAppendLanding(t)
}

// TestWritePathGateMultiVaultIndependentVaultSeq verifies route fan-out to two
// sequenced vaults assigns independent vault_seq counters per vault.
func TestWritePathGateMultiVaultIndependentVaultSeq(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-vault write-path gate in short mode")
	}
	h := newOrchRelHarness(t, 4, withSequencedWritePath(3), withExtraVault([]int{0, 1, 3}))
	vaultA := h.vaults[0]
	vaultB := h.vaults[1]
	h.setDualVaultIngestRoutes(t, vaultA, vaultB)

	nodeA := h.nodeIDs[0]
	nodeB := h.nodeIDs[1]
	ingesterA := glid.New()
	ingesterB := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	ingestVault := func(nodeID string, v vaultSpec, ingester glid.GLID, vaultTag string, n int) {
		t.Helper()
		for i := range n {
			rec := gateSequencedRecord(vaultTag+"-", ingester, now, uint32(i+1))
			rec.Attrs = chunk.Attributes{"vault": vaultTag}
			if err := h.ingestOnNode(nodeID, rec); err != nil {
				t.Fatalf("ingest %s-%d on %s: %v", vaultTag, i+1, h.nodes[nodeID].label, err)
			}
			seq := h.lastAssignedSeqForVault(t, v.id, nodeID, rec.EventID)
			if seq != uint64(i+1) {
				t.Fatalf("vault %s: seq = %d, want %d", v.label, seq, i+1)
			}
			h.assertSpoolSlotOnVaultNodes(t, v.id, h.vaultNodeIDs(v), seq, rec)
		}
	}

	ingestVault(nodeA, vaultA, ingesterA, "A", 6)
	ingestVault(nodeB, vaultB, ingesterB, "B", 4)

	h.assertNoChunkAppendOnVault(t, vaultA)
	h.assertNoChunkAppendOnVault(t, vaultB)
}

// TestWritePathGateSymmetricThreeNodeSameIngester mirrors scatterbox with
// --all-nodes=true: one ingester ID, every router ingesting into the same
// sequenced vault concurrently. EventIDs differ by router NodeID (as in
// lifecycle digest), not by random GLIDs.
func TestWritePathGateSymmetricThreeNodeSameIngester(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping symmetric 3-node write-path gate in short mode")
	}
	h := newOrchRelHarness(t, 3, withSequencedWritePath(3))
	h.setDefaultIngestRoute(t)

	ingester := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	ingestAndAssert := func(nodeID string, rec chunk.Record) {
		t.Helper()
		if err := h.ingestOnNode(nodeID, rec); err != nil {
			t.Fatalf("ingest on %s: %v", h.nodes[nodeID].label, err)
		}
		seq := h.lastAssignedSeq(t, nodeID, rec.EventID)
		h.assertSpoolSlotOnAllReplicas(t, seq, rec)
	}

	for i := range 30 {
		for _, nodeID := range h.nodeIDs {
			nodeGLID, err := glid.ParseUUID(nodeID)
			if err != nil {
				t.Fatalf("parse node id %s: %v", nodeID, err)
			}
			rec := gateSequencedRecordOnNode("sym-", ingester, nodeGLID, now, uint32(i+1))
			ingestAndAssert(nodeID, rec)
		}
	}

	h.assertNoChunkAppendLanding(t)
}

// TestWritePathGateSymmetricThreeNodeConcurrent crosses swath renewal (256-seq
// batches) with all three routers ingesting in parallel each wave.
func TestWritePathGateSymmetricThreeNodeConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent symmetric 3-node write-path gate in short mode")
	}
	h := newOrchRelHarness(t, 3, withSequencedWritePath(3))
	h.setDefaultIngestRoute(t)

	ingester := glid.New()
	now := time.Now().Truncate(time.Nanosecond)
	const waves = 280 // one full swath batch plus renewal margin

	var wg sync.WaitGroup
	for i := range waves {
		for _, nodeID := range h.nodeIDs {
			wg.Add(1)
			go func(nodeID string, wave int) {
				defer wg.Done()
				nodeGLID, err := glid.ParseUUID(nodeID)
				if err != nil {
					t.Errorf("parse node id %s: %v", nodeID, err)
					return
				}
				rec := gateSequencedRecordOnNode("symc-", ingester, nodeGLID, now, uint32(wave+1))
				if err := h.ingestOnNode(nodeID, rec); err != nil {
					t.Errorf("wave %d on %s: %v", wave, h.nodes[nodeID].label, err)
				}
			}(nodeID, i)
		}
		wg.Wait()
	}

	h.assertNoChunkAppendLanding(t)
}

func gateSequencedRecord(prefix string, ingesterID glid.GLID, now time.Time, ingestSeq uint32) chunk.Record {
	return gateSequencedRecordOnNode(prefix, ingesterID, glid.New(), now, ingestSeq)
}

func gateSequencedRecordOnNode(prefix string, ingesterID, nodeID glid.GLID, now time.Time, ingestSeq uint32) chunk.Record {
	body := prefix + strconv.FormatUint(uint64(ingestSeq), 10)
	return chunk.Record{
		SourceTS: now,
		IngestTS: now,
		Attrs:    chunk.Attributes{"msg": body},
		Raw:      []byte(body),
		EventID: chunk.EventID{
			IngesterID: ingesterID,
			NodeID:     nodeID,
			IngestTS:   now,
			IngestSeq:  ingestSeq,
		},
	}
}

func (h *orchRelHarness) vaultNodeIDs(v vaultSpec) []string {
	out := make([]string, len(v.nodeIdxs))
	for i, idx := range v.nodeIdxs {
		out[i] = h.nodeIDs[idx]
	}
	return out
}

func (h *orchRelHarness) setDualVaultIngestRoutes(t *testing.T, vaultA, vaultB vaultSpec) {
	t.Helper()
	crA, err := orchestrator.CompileRoute(glid.New(), "vault-a", 0, `vault="A"`,
		[]orchestrator.RouteDestination{{VaultID: vaultA.id}}, "gate-vault-a")
	if err != nil {
		t.Fatal(err)
	}
	crB, err := orchestrator.CompileRoute(glid.New(), "vault-b", 0, `vault="B"`,
		[]orchestrator.RouteDestination{{VaultID: vaultB.id}}, "gate-vault-b")
	if err != nil {
		t.Fatal(err)
	}
	rs := orchestrator.NewRouteSet([]*orchestrator.CompiledRoute{crA, crB})
	for _, id := range h.nodeIDs {
		h.nodes[id].orch.SetRouteSet(rs)
	}
}

func (h *orchRelHarness) lastAssignedSeq(t *testing.T, nodeID string, eventID chunk.EventID) uint64 {
	return h.lastAssignedSeqForVault(t, h.vaultID, nodeID, eventID)
}

func (h *orchRelHarness) lastAssignedSeqForVault(t *testing.T, vaultID glid.GLID, nodeID string, eventID chunk.EventID) uint64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for seq := uint64(1); seq <= 1024; seq++ {
			rec, err := h.nodes[nodeID].orch.ReadVaultSpoolSeq(vaultID, seq)
			if err != nil {
				continue
			}
			if rec.EventID == eventID {
				return seq
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("assigned seq for event on %s not found", nodeID)
	return 0
}

func (h *orchRelHarness) assertSpoolSlotOnAllReplicas(t *testing.T, seq uint64, want chunk.Record) {
	h.assertSpoolSlotOnVaultNodes(t, h.vaultID, h.nodeIDs, seq, want)
}

func (h *orchRelHarness) assertSpoolSlotOnVaultNodes(t *testing.T, vaultID glid.GLID, nodeIDs []string, seq uint64, want chunk.Record) {
	t.Helper()
	for _, id := range nodeIDs {
		rec, err := h.nodes[id].orch.ReadVaultSpoolSeq(vaultID, seq)
		if err != nil {
			t.Fatalf("node %s: spool seq %d missing: %v", h.nodes[id].label, seq, err)
		}
		if rec.VaultSeq != seq {
			t.Fatalf("node %s: VaultSeq = %d, want %d", h.nodes[id].label, rec.VaultSeq, seq)
		}
		if string(rec.Raw) != string(want.Raw) {
			t.Fatalf("node %s: payload mismatch at seq %d", h.nodes[id].label, seq)
		}
		if rec.EventID.IngesterID != want.EventID.IngesterID || rec.EventID.IngestSeq != want.EventID.IngestSeq {
			t.Fatalf("node %s: EventID identity mismatch at seq %d", h.nodes[id].label, seq)
		}
	}
}

func (h *orchRelHarness) assertNoChunkAppendLanding(t *testing.T) {
	h.assertNoChunkAppendOnVault(t, h.vaults[0])
}

func (h *orchRelHarness) assertNoChunkAppendOnVault(t *testing.T, vault vaultSpec) {
	t.Helper()
	for _, idx := range vault.nodeIdxs {
		id := h.nodeIDs[idx]
		metas, err := h.nodes[id].orch.ListLocalChunkMetas(vault.id)
		if err != nil {
			t.Fatalf("node %s ListLocalChunkMetas: %v", h.nodes[id].label, err)
		}
		for _, m := range metas {
			if !m.Sealed && m.RecordCount > 0 {
				t.Fatalf("node %s: active chunk has %d records (chunk append forbidden on sequenced path)",
					h.nodes[id].label, m.RecordCount)
			}
		}
	}
}
