package orchestrator_test

import (
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// TestOrchRel_ManifestReadsOnVoterWithoutInstance covers the FSM tier of the
// unified manifest read core (gastrolog-3w8qj): vault B is homed on nodes
// {0,1,2} only, but node 3 — the ingest origin — is still a voter of B's
// vault-ctl Raft group (symmetric seeding, gastrolog-292yi) and must serve
// sealed manifest reads from the replicated FSM without hosting any instance
// for the vault.
func TestOrchRel_ManifestReadsOnVoterWithoutInstance(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node reliability test")
	}
	t.Parallel()
	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	outsiderID := h.nodeIDs[3] // voter + ingest origin, no instance
	outsider := h.nodes[outsiderID]

	const total = 2 * pipelineChunkMaxRecords
	h.submitIngestRecords(outsiderID, total, "manifest-read")

	// Sealed truth as seen by a home node's FSM.
	sealed := h.waitSealedRecords(v, h.nodeIDs[0], total)
	if len(sealed) != 2 {
		t.Fatalf("expected 2 sealed chunks, got %d", len(sealed))
	}

	// The API under test doubles as the convergence probe: the sealed set
	// must become visible through the instance-less voter's ManifestReader.
	reader := outsider.orch.ManifestReader()
	h.waitProgress("sealed manifest visible on instance-less voter", 50*time.Millisecond, func() (string, bool) {
		var n int64
		entries := reader.EntriesForVault(v.id)
		for _, e := range entries {
			n += e.RecordCount
		}
		return fmt.Sprintf("sealed_chunks=%d records=%d", len(entries), n), len(entries) == len(sealed) && n == total
	}, func() { h.dumpPipelineState(v) })

	entries := reader.EntriesForVault(v.id)
	byID := make(map[chunk.ChunkID]vaultctlfsm.ManifestEntry, len(entries))
	for _, e := range entries {
		if !e.IsSealed() {
			t.Errorf("EntriesForVault returned unsealed entry %s state=%v", e.ID, e.State)
		}
		byID[e.ID] = e
	}
	for _, want := range sealed {
		got, ok := byID[want.ID]
		if !ok {
			t.Fatalf("sealed chunk %s missing from voter's EntriesForVault", want.ID)
		}
		if got.RecordCount != want.RecordCount {
			t.Errorf("chunk %s RecordCount on voter = %d, want %d", want.ID, got.RecordCount, want.RecordCount)
		}
		if got.IngestStart.IsZero() || got.IngestEnd.IsZero() {
			t.Errorf("chunk %s missing ingest bounds on voter: start=%v end=%v", want.ID, got.IngestStart, got.IngestEnd)
		}

		// Entry resolves the same chunk by ID on the instance-less voter.
		byChunk, ok := reader.Entry(want.ID)
		if !ok {
			t.Fatalf("Entry(%s) not found on voter without instance", want.ID)
		}
		if byChunk.RecordCount != want.RecordCount {
			t.Errorf("Entry(%s) RecordCount = %d, want %d", want.ID, byChunk.RecordCount, want.RecordCount)
		}
	}

	// The open-inclusive surface serves at least the sealed set on the same
	// voter — it is the same read core with the open-chunk overlay on top.
	incl := outsider.orch.VaultManifestEntriesIncludingOpen(v.id)
	if len(incl) < len(entries) {
		t.Errorf("VaultManifestEntriesIncludingOpen = %d entries, want >= %d", len(incl), len(entries))
	}

	// Unhappy paths on the voter: unknown chunk, unknown vault.
	if _, ok := reader.Entry(chunk.NewChunkID()); ok {
		t.Error("Entry(unknown chunk) resolved on voter")
	}
	if extra := reader.EntriesForVault(glid.New()); len(extra) != 0 {
		t.Errorf("EntriesForVault(unknown vault) = %d entries, want none", len(extra))
	}

	// A home node's Reader agrees with the instance-less voter's view.
	homeReader := h.nodes[h.nodeIDs[0]].orch.ManifestReader()
	home := homeReader.EntriesForVault(v.id)
	if len(home) != len(entries) {
		t.Errorf("home sealed set (%d) diverges from voter view (%d)", len(home), len(entries))
	}
	for _, e := range home {
		if _, ok := byID[e.ID]; !ok {
			t.Errorf("home chunk %s missing from voter view", e.ID)
		}
	}
}
