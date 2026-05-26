package orchestrator_test

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
)

// TestSequencedCrossNodeRouteGuardrailNoChunkAppend asserts the P0 guardrail
// from docs/fan-out/v2/write-path-lock.md: a route destination with a remote
// NodeID must not use RecordForwarder→SetRecordAppender→chunk Append on
// sequenced vaults. Assignment and replica fan-out stay on the ingesting
// router's spool path.
func TestSequencedCrossNodeRouteGuardrailNoChunkAppend(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multinode sequenced guardrail in short mode")
	}
	h := newOrchRelHarness(t, 4, withSequencedWritePath(3))

	ingestNode := h.nodeIDs[0]
	remoteNode := h.nodeIDs[3]
	now := time.Now().Truncate(time.Nanosecond)

	cr, err := orchestrator.CompileRoute(glid.New(), "remote-residency", 0, `env="guardrail"`,
		[]orchestrator.RouteDestination{{VaultID: h.vaultID, NodeID: remoteNode}}, "guardrail-remote")
	if err != nil {
		t.Fatal(err)
	}
	h.nodes[ingestNode].orch.SetRouteSet(orchestrator.NewRouteSet([]*orchestrator.CompiledRoute{cr}))

	rec := gateSequencedRecord("guardrail", glid.New(), now, 1)
	rec.Attrs = chunk.Attributes{"env": "guardrail"}
	if err := h.ingestOnNode(ingestNode, rec); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	seq := h.lastAssignedSeq(t, ingestNode, rec.EventID)
	h.assertSpoolSlotOnAllReplicas(t, seq, rec)
	h.assertNoChunkAppendLanding(t)
	h.assertGuardrailNoRecordForwardAppend(t)
}
