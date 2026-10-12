package orchestrator_test

import (
	"fmt"
	"testing"
	"time"
)

// transferBetween ingests one sealed chunk into source, transfers it to dest
// by retention, and checks it arrives unchanged — same chunk, same records,
// served by dest's leader — and leaves the source.
func transferBetween(t *testing.T, h *orchRelHarness, source, dest vaultSpec) {
	t.Helper()
	enableVault(t, h, source)
	enableVault(t, h, dest)
	ingest := h.nodeIDs[3]
	h.submitIngestRecords(ingest, pipelineChunkMaxRecords, "transfer-"+source.label)
	entries := h.waitSealedRecords(source, h.nodeIDs[0], pipelineChunkMaxRecords)
	if len(entries) != 1 {
		t.Fatalf("source sealed %d chunks, want 1", len(entries))
	}
	moved := entries[0]

	policyID := putTransferRetentionPolicy(t, h, "transfer-"+source.label+"-policy")
	configureTransferDisposition(t, h, source, dest.id, policyID)

	h.waitProgress(fmt.Sprintf("chunk moved from vault %s to vault %s", source.label, dest.label), 50*time.Millisecond, func() (string, bool) {
		inDest := h.sealedPipelineChunks(dest, h.nodeIDs[0])
		inSource := h.sealedPipelineChunks(source, h.nodeIDs[0])
		done := len(inDest) == 1 && inDest[0].ID == moved.ID && inDest[0].RecordCount == moved.RecordCount && len(inSource) == 0
		return fmt.Sprintf("dest=%d source=%d", len(inDest), len(inSource)), done
	}, func() { h.dumpPipelineState(source); h.dumpPipelineState(dest) })
	h.waitServedOnLeader(dest, pipelineChunkMaxRecords)
}

// A memory vault's sealed chunks transfer out by retention like a file
// vault's.
func TestOrchPipeline_MemoryVaultTransfersToAFileVault(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	t.Parallel()
	homes := []int{0, 1, 2}
	h := newOrchRelHarness(t, 4,
		withMemoryVault(homes, "64MiB"),
		withExtraVault(homes),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	transferBetween(t, h, h.vaults[1], h.vaults[2])
}

// A memory vault receives transferred chunks like a file vault.
func TestOrchPipeline_FileVaultTransfersToAMemoryVault(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	t.Parallel()
	homes := []int{0, 1, 2}
	h := newOrchRelHarness(t, 4,
		withExtraVault(homes),
		withMemoryVault(homes, "64MiB"),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	transferBetween(t, h, h.vaults[1], h.vaults[2])
}
