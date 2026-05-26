package orchestrator_test

import (
	"strconv"
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

func gateSequencedRecord(prefix string, ingesterID glid.GLID, now time.Time, ingestSeq uint32) chunk.Record {
	body := prefix + strconv.FormatUint(uint64(ingestSeq), 10)
	return chunk.Record{
		SourceTS: now,
		IngestTS: now,
		Attrs:    chunk.Attributes{"msg": body},
		Raw:      []byte(body),
		EventID: chunk.EventID{
			IngesterID: ingesterID,
			NodeID:     glid.New(),
			IngestTS:   now,
			IngestSeq:  ingestSeq,
		},
	}
}

func (h *orchRelHarness) lastAssignedSeq(t *testing.T, nodeID string, eventID chunk.EventID) uint64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for seq := uint64(1); seq <= 1024; seq++ {
			rec, err := h.nodes[nodeID].orch.ReadVaultSpoolSeq(h.vaultID, seq)
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
	t.Helper()
	for _, id := range h.nodeIDs {
		rec, err := h.nodes[id].orch.ReadVaultSpoolSeq(h.vaultID, seq)
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
	t.Helper()
	for _, id := range h.nodeIDs {
		metas, err := h.nodes[id].orch.ListLocalChunkMetas(h.vaultID)
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
