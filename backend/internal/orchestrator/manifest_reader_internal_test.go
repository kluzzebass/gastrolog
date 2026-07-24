package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// TestIndexReaderChunkRootTier covers the byte-local GLCB fallback of the
// FSM-grounded IndexReader (gastrolog-nlepn): a sealed pipeline chunk whose
// GLCB sits in this node's vault chunk root but is served by no chunk or
// index manager must still resolve IngestTS rank/pos lookups — and must
// report unresolvable (never fabricate) when the bytes are absent.
func TestIndexReaderChunkRootTier(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := buildSealedPipelineGLCB(t, ctx, 5, "rank-tier", nil)
	if fx.sealed.IngestStart.IsZero() || fx.sealed.IngestEnd.IsZero() {
		t.Fatalf("fixture sealed entry missing ingest bounds: %+v", fx.sealed)
	}

	base := t.TempDir()
	orch := newTestOrch(t, Config{LocalNodeID: "node-A", SegmentsDir: base})
	orch.pipelineVaults[fx.vaultID] = pipelineVaultReg{home: true}

	// Instance carrying only the manifest callbacks (backed by the fixture
	// FSM) — no chunk manager, no index manager. Ownership resolves through
	// the manifest; only the chunk-root tier can serve bytes.
	orch.RegisterVault(NewVault(fx.vaultID, &VaultInstance{
		VaultID: fx.vaultID,
		Type:    "file",
		ManifestEntry: func(id chunk.ChunkID) (vaultctlfsm.ManifestEntry, bool) {
			e := fx.fsm.Get(id)
			if e == nil {
				return vaultctlfsm.ManifestEntry{}, false
			}
			return *e, true
		},
		ManifestEntries: func() []vaultctlfsm.ManifestEntry { return fx.fsm.List() },
	}))

	ir := orch.IndexReader()

	// No bytes materialized yet: the lookup is unresolvable, not estimated.
	if _, ok := ir.FindIngestRank(fx.sealed.ID, fx.sealed.IngestStart); ok {
		t.Fatal("FindIngestRank resolved before any local GLCB bytes exist")
	}

	// Materialize the GLCB under the vault chunk root.
	chunkRoot := filepath.Join(base, fx.vaultID.String(), "chunks")
	dst := chunking.ChunkGLCBPath(chunkRoot, fx.sealed.ID)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatalf("mkdir chunk dir: %v", err)
	}
	blob, err := os.ReadFile(fx.glcbPath)
	if err != nil {
		t.Fatalf("read fixture GLCB: %v", err)
	}
	if err := os.WriteFile(dst, blob, 0o644); err != nil {
		t.Fatalf("write GLCB into chunk root: %v", err)
	}

	// Happy path: rank and pos at the chunk's earliest IngestTS resolve to 0
	// straight from the chunk-root ITSI section.
	rank, ok := ir.FindIngestRank(fx.sealed.ID, fx.sealed.IngestStart)
	if !ok || rank != 0 {
		t.Errorf("FindIngestRank(IngestStart) = (%d, %v), want (0, true)", rank, ok)
	}
	pos, ok := ir.FindIngestPos(fx.sealed.ID, fx.sealed.IngestStart)
	if !ok || pos != 0 {
		t.Errorf("FindIngestPos(IngestStart) = (%d, %v), want (0, true)", pos, ok)
	}

	// The last ingest timestamp resolves to a rank inside the chunk.
	rank, ok = ir.FindIngestRank(fx.sealed.ID, fx.sealed.IngestEnd)
	if !ok {
		t.Error("FindIngestRank(IngestEnd) unresolvable with local GLCB present")
	}
	if int64(rank) >= fx.sealed.RecordCount {
		t.Errorf("FindIngestRank(IngestEnd) = %d, want < RecordCount %d", rank, fx.sealed.RecordCount)
	}

	// Past all entries: same "not found" answer the ITSI section gives.
	if got, ok := ir.FindIngestRank(fx.sealed.ID, fx.sealed.IngestEnd.Add(time.Second)); ok {
		t.Errorf("FindIngestRank(past end) = (%d, true), want unresolved", got)
	}

	// Unknown chunk stays unresolvable.
	if _, ok := ir.FindIngestRank(chunk.NewChunkID(), fx.sealed.IngestStart); ok {
		t.Error("FindIngestRank(unknown chunk) resolved")
	}

	// Bytes deleted (retention, eviction): unresolvable again — the
	// FSM-estimate residual (gastrolog-1952x) is the caller's business.
	if err := os.Remove(dst); err != nil {
		t.Fatalf("remove GLCB: %v", err)
	}
	if _, ok := ir.FindIngestRank(fx.sealed.ID, fx.sealed.IngestStart); ok {
		t.Error("FindIngestRank resolved after GLCB bytes were removed")
	}
}
