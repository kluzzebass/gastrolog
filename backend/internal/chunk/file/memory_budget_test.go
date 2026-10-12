package file

import (
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// newBudgetHome is a pipeline home whose manager has the given memory budget;
// zero is an ordinary file vault's manager.
func newBudgetHome(t *testing.T, budget uint64) *pipelineHome {
	t.Helper()
	cm, err := NewManager(Config{
		Dir:               t.TempDir(),
		RotationPolicy:    chunk.NewRecordCountPolicy(10000),
		VaultID:           glid.New(),
		MemoryBudgetBytes: budget,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	h := &pipelineHome{cm: cm, chunkRoot: t.TempDir()}
	h.installLocator()
	return h
}

// readAll reads every record of id through the manager, so its GLCB is opened.
func readAll(t *testing.T, cm *Manager, id chunk.ChunkID) int {
	t.Helper()
	cur, err := cm.OpenCursor(id)
	if err != nil {
		t.Fatalf("OpenCursor %s: %v", id, err)
	}
	defer func() { _ = cur.Close() }()
	n := 0
	for {
		if _, _, err := cur.Next(); err != nil {
			break
		}
		n++
	}
	return n
}

// openBlob reports whether id's GLCB is open in the manager, and whether it
// is held in RAM.
func openBlob(cm *Manager, id chunk.ChunkID) (open, held bool) {
	v, ok := cm.glcbMapped.Load(id)
	if !ok {
		return false, false
	}
	return true, v.(*mappedGLCBEntry).blob.Held()
}

// A memory vault's sealed GLCBs are held in RAM and stay resident: more of
// them than the mapping cap stays open, and none is in the mapping LRU. An
// ordinary file vault maps its GLCBs and the cap closes the oldest mappings.
func TestMemoryVaultHoldsEverySealedGLCBInRAM(t *testing.T) {
	t.Parallel()
	chunks := glcbMappedCap + 6
	for _, tc := range []struct {
		name   string
		budget uint64
	}{
		{"memory vault", 1 << 30},
		{"file vault", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newBudgetHome(t, tc.budget)
			ids := make([]chunk.ChunkID, chunks)
			for i := range ids {
				ids[i] = chunk.NewChunkID()
				h.buildPipelineChunk(t, ids[i], 3)
				if got := readAll(t, h.cm, ids[i]); got != 3 {
					t.Fatalf("chunk %d served %d records, want 3", i, got)
				}
			}
			openCount, heldCount := 0, 0
			for _, id := range ids {
				open, held := openBlob(h.cm, id)
				if open {
					openCount++
				}
				if held {
					heldCount++
				}
			}
			h.cm.glcbMapMu.Lock()
			lru := len(h.cm.glcbMapLRU)
			h.cm.glcbMapMu.Unlock()
			if tc.budget > 0 {
				if heldCount != chunks || openCount != chunks {
					t.Fatalf("held %d and open %d of %d sealed GLCBs, want all held and open", heldCount, openCount, chunks)
				}
				if lru != 0 {
					t.Fatalf("%d held GLCBs in the mapping LRU, want none", lru)
				}
				return
			}
			if heldCount != 0 {
				t.Fatalf("a file vault held %d GLCBs in RAM, want them mapped", heldCount)
			}
			if openCount > glcbMappedCap {
				t.Fatalf("a file vault kept %d GLCBs mapped, over the cap of %d", openCount, glcbMappedCap)
			}
		})
	}
}

// BudgetExceeded is the bytes by which a memory vault's sealed chunks exceed
// its budget, and zero within it or without a budget.
func TestMemoryVaultBudgetCountsItsSealedChunks(t *testing.T) {
	t.Parallel()
	probe := newBudgetHome(t, 0)
	probeID := chunk.NewChunkID()
	_, info := probe.buildPipelineChunk(t, probeID, 3)
	size := info.Bytes
	if size <= 0 {
		t.Fatalf("premise: a sealed chunk's registration carries its size, got %d", size)
	}

	register := func(h *pipelineHome, n int) {
		t.Helper()
		for range n {
			_, got := h.buildPipelineChunk(t, chunk.NewChunkID(), 3)
			if got.Bytes != size {
				t.Fatalf("premise: equal chunks have equal sizes, got %d and %d", got.Bytes, size)
			}
		}
	}
	for _, tc := range []struct {
		name   string
		budget uint64
		chunks int
		want   int64
	}{
		{"within budget", uint64(3 * size), 3, 0},
		{"over budget by one chunk", uint64(2 * size), 3, size},
		{"over budget by part of a chunk", uint64(2*size + size/2), 3, size - size/2},
		{"no budget", 0, 3, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newBudgetHome(t, tc.budget)
			register(h, tc.chunks)
			if got := h.cm.BudgetExceeded(); got != tc.want {
				t.Fatalf("BudgetExceeded = %d, want %d (chunk size %d, budget %d)", got, tc.want, size, tc.budget)
			}
		})
	}
}
