package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// TestIndexReaderMetadataBoundaryTier covers the byte-free tier of the
// FSM-grounded IndexReader (gastrolog-enfwd): a sealed monotonic chunk
// answers rank/pos 0 for timestamps strictly before IngestStart from
// replicated metadata alone — on any voter, with no local ITSI bytes.
// Everything at or past IngestStart, non-monotonic chunks, empty chunks
// and unsealed chunks stay unresolvable.
func TestIndexReaderMetadataBoundaryTier(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	vaultID := glid.New()

	start := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Second)
	mono := vaultctlfsm.ManifestEntry{
		ID: chunk.NewChunkID(), State: chunk.ChunkStateSealed,
		RecordCount: 42, IngestStart: start, IngestEnd: end, IngestTSMonotonic: true,
	}
	nonMono := vaultctlfsm.ManifestEntry{
		ID: chunk.NewChunkID(), State: chunk.ChunkStateSealed,
		RecordCount: 42, IngestStart: start, IngestEnd: end, IngestTSMonotonic: false,
	}
	empty := vaultctlfsm.ManifestEntry{
		ID: chunk.NewChunkID(), State: chunk.ChunkStateSealed,
		RecordCount: 0, IngestStart: start, IngestEnd: end, IngestTSMonotonic: true,
	}
	active := vaultctlfsm.ManifestEntry{
		ID: chunk.NewChunkID(), State: chunk.ChunkStateActive,
		RecordCount: 42, IngestStart: start, IngestEnd: end, IngestTSMonotonic: true,
	}
	entries := map[chunk.ChunkID]vaultctlfsm.ManifestEntry{
		mono.ID: mono, nonMono.ID: nonMono, empty.ID: empty, active.ID: active,
	}
	orch.RegisterVault(NewVault(vaultID, &VaultInstance{
		VaultID: vaultID,
		Type:    "file",
		ManifestReadFacet: ManifestReadFacet{
			ManifestEntry: func(id chunk.ChunkID) (vaultctlfsm.ManifestEntry, bool) {
				e, ok := entries[id]
				return e, ok
			},
			ManifestEntries: func() []vaultctlfsm.ManifestEntry {
				var out []vaultctlfsm.ManifestEntry
				for _, e := range entries {
					out = append(out, e)
				}
				return out
			},
		},
	}))

	ir := orch.IndexReader()
	before := start.Add(-time.Second)

	// Happy: strictly-before on a sealed monotonic chunk → (0, true).
	if rank, ok := ir.FindIngestRank(mono.ID, before); !ok || rank != 0 {
		t.Errorf("FindIngestRank(mono, before start) = (%d, %v), want (0, true)", rank, ok)
	}
	if pos, ok := ir.FindIngestPos(mono.ID, before); !ok || pos != 0 {
		t.Errorf("FindIngestPos(mono, before start) = (%d, %v), want (0, true)", pos, ok)
	}

	// Unresolvable without bytes: at IngestStart, interior, past end.
	for name, ts := range map[string]time.Time{
		"at start": start,
		"interior": start.Add(5 * time.Second),
		"past end": end.Add(time.Second),
	} {
		if got, ok := ir.FindIngestRank(mono.ID, ts); ok {
			t.Errorf("FindIngestRank(mono, %s) = (%d, true), want unresolvable without bytes", name, got)
		}
	}

	// Non-monotonic: IngestStart is not the minimum — no boundary answer.
	if got, ok := ir.FindIngestRank(nonMono.ID, before); ok {
		t.Errorf("FindIngestRank(non-monotonic, before start) = (%d, true), want unresolvable", got)
	}
	// Empty and active chunks: no boundary answer.
	if got, ok := ir.FindIngestRank(empty.ID, before); ok {
		t.Errorf("FindIngestRank(empty, before start) = (%d, true), want unresolvable", got)
	}
	if got, ok := ir.FindIngestRank(active.ID, before); ok {
		t.Errorf("FindIngestRank(active, before start) = (%d, true), want unresolvable", got)
	}
}

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
	orch.setPipelineVaultLocked(fx.vaultID, pipelineVaultReg{home: true})

	// Instance carrying only the manifest callbacks (backed by the fixture
	// FSM) — no chunk manager, no index manager. Ownership resolves through
	// the manifest; only the chunk-root tier can serve bytes.
	orch.RegisterVault(NewVault(fx.vaultID, &VaultInstance{
		VaultID: fx.vaultID,
		Type:    "file",
		ManifestReadFacet: ManifestReadFacet{
			ManifestEntry: func(id chunk.ChunkID) (vaultctlfsm.ManifestEntry, bool) {
				e := fx.fsm.Get(id)
				if e == nil {
					return vaultctlfsm.ManifestEntry{}, false
				}
				return *e, true
			},
			ManifestEntries: func() []vaultctlfsm.ManifestEntry { return fx.fsm.List() },
		},
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
