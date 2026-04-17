package orchestrator

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/notify"
)

func quietOrchLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func nowFn() time.Time { return time.Now() }

// progressFakeCM is a minimal chunk.ChunkManager for ticker tests — only
// Active() is exercised; every other method delegates to cronFakeChunkManager's
// no-op implementations via embedding.
type progressFakeCM struct {
	cronFakeChunkManager
}

func newProgressOrch(t *testing.T) *Orchestrator {
	t.Helper()
	sched, err := newScheduler(quietOrchLogger(), 4, nowFn)
	if err != nil {
		t.Fatalf("newScheduler: %v", err)
	}
	o := &Orchestrator{
		vaults:      make(map[glid.GLID]*Vault),
		chunkSignal: notify.NewSignal(),
		scheduler:   sched,
		logger:      quietOrchLogger(),
	}
	return o
}

// helper to install a fake tier on a vault with a given active chunk.
func addFakeTier(v *Vault, active *chunk.ChunkMeta) *progressFakeCM {
	cm := &progressFakeCM{}
	cm.active = active
	v.Tiers = append(v.Tiers, &TierInstance{
		TierID: glid.New(),
		Type:   "memory",
		Chunks: cm,
	})
	return cm
}

// TestSnapshotActiveChunkProgress_FirstSightingWithRecords fires change
// when an active chunk is seen for the first time with records already on
// it.
func TestSnapshotActiveChunkProgress_FirstSightingWithRecords(t *testing.T) {
	o := newProgressOrch(t)
	v := NewVault(glid.New())
	addFakeTier(v, &chunk.ChunkMeta{ID: chunk.NewChunkID(), RecordCount: 5})
	o.vaults[v.ID] = v

	last := map[chunk.ChunkID]int64{}
	changed, seen := o.snapshotActiveChunkProgress(last)
	if !changed {
		t.Error("expected changed=true for first sighting with records")
	}
	if len(seen) != 1 {
		t.Errorf("seen count: got %d, want 1", len(seen))
	}
	if len(last) != 1 {
		t.Errorf("last-counts population: got %d entries, want 1", len(last))
	}
}

// TestSnapshotActiveChunkProgress_FirstSightingEmpty does NOT fire
// change when an active chunk is brand new and empty — nothing to tell
// subscribers about yet.
func TestSnapshotActiveChunkProgress_FirstSightingEmpty(t *testing.T) {
	o := newProgressOrch(t)
	v := NewVault(glid.New())
	addFakeTier(v, &chunk.ChunkMeta{ID: chunk.NewChunkID(), RecordCount: 0})
	o.vaults[v.ID] = v

	last := map[chunk.ChunkID]int64{}
	changed, _ := o.snapshotActiveChunkProgress(last)
	if changed {
		t.Error("expected changed=false for first sighting of empty chunk")
	}
}

// TestSnapshotActiveChunkProgress_NoGrowthNoChange verifies that a
// second tick without growth reports no change.
func TestSnapshotActiveChunkProgress_NoGrowthNoChange(t *testing.T) {
	o := newProgressOrch(t)
	v := NewVault(glid.New())
	addFakeTier(v, &chunk.ChunkMeta{ID: chunk.NewChunkID(), RecordCount: 3})
	o.vaults[v.ID] = v

	last := map[chunk.ChunkID]int64{}
	// Prime with first tick.
	if changed, _ := o.snapshotActiveChunkProgress(last); !changed {
		t.Fatal("prime tick should fire")
	}
	// Second tick — no growth.
	if changed, _ := o.snapshotActiveChunkProgress(last); changed {
		t.Error("expected changed=false when record count unchanged")
	}
}

// TestSnapshotActiveChunkProgress_GrowthFires verifies that a growing
// chunk fires change on the tick that observes the new count.
func TestSnapshotActiveChunkProgress_GrowthFires(t *testing.T) {
	o := newProgressOrch(t)
	v := NewVault(glid.New())
	cm := addFakeTier(v, &chunk.ChunkMeta{ID: chunk.NewChunkID(), RecordCount: 3})
	o.vaults[v.ID] = v

	last := map[chunk.ChunkID]int64{}
	_, _ = o.snapshotActiveChunkProgress(last) // prime
	// Grow.
	cm.active.RecordCount = 7
	if changed, _ := o.snapshotActiveChunkProgress(last); !changed {
		t.Error("expected changed=true after record count grew")
	}
	if last[cm.active.ID] != 7 {
		t.Errorf("last-counts not updated: got %d, want 7", last[cm.active.ID])
	}
}

// TestSnapshotActiveChunkProgress_NoActiveChunkSkips verifies tiers with
// no active chunk don't contribute to seen/changed.
func TestSnapshotActiveChunkProgress_NoActiveChunkSkips(t *testing.T) {
	o := newProgressOrch(t)
	v := NewVault(glid.New())
	addFakeTier(v, nil) // no active chunk
	o.vaults[v.ID] = v

	last := map[chunk.ChunkID]int64{}
	changed, seen := o.snapshotActiveChunkProgress(last)
	if changed || len(seen) != 0 {
		t.Errorf("tier without active chunk should be skipped; changed=%v seen=%d", changed, len(seen))
	}
}

// TestSnapshotActiveChunkProgress_Rotation verifies that after a chunk
// rotates (active ID changes), the new chunk is tracked and the old one
// is eligible for eviction via the seen-set returned to the caller.
func TestSnapshotActiveChunkProgress_Rotation(t *testing.T) {
	o := newProgressOrch(t)
	v := NewVault(glid.New())
	cm := addFakeTier(v, &chunk.ChunkMeta{ID: chunk.NewChunkID(), RecordCount: 10})
	o.vaults[v.ID] = v

	last := map[chunk.ChunkID]int64{}
	oldID := cm.active.ID
	_, _ = o.snapshotActiveChunkProgress(last) // prime
	if _, ok := last[oldID]; !ok {
		t.Fatal("primed tick should have tracked old chunk")
	}

	// Simulate rotation: new active chunk with a different ID.
	newID := chunk.NewChunkID()
	cm.active = &chunk.ChunkMeta{ID: newID, RecordCount: 1}

	changed, seen := o.snapshotActiveChunkProgress(last)
	if !changed {
		t.Error("expected changed=true for newly-rotated chunk with records")
	}
	if _, ok := seen[newID]; !ok {
		t.Error("new chunk ID not in seen set")
	}
	if _, ok := seen[oldID]; ok {
		t.Error("old chunk ID should no longer be in seen set after rotation")
	}
}

// TestSnapshotActiveChunkProgress_MultipleVaultsAndTiers verifies all
// active chunks across all vaults and tiers are observed.
func TestSnapshotActiveChunkProgress_MultipleVaultsAndTiers(t *testing.T) {
	o := newProgressOrch(t)
	v1 := NewVault(glid.New())
	addFakeTier(v1, &chunk.ChunkMeta{ID: chunk.NewChunkID(), RecordCount: 1})
	addFakeTier(v1, &chunk.ChunkMeta{ID: chunk.NewChunkID(), RecordCount: 2})
	v2 := NewVault(glid.New())
	addFakeTier(v2, &chunk.ChunkMeta{ID: chunk.NewChunkID(), RecordCount: 3})
	o.vaults[v1.ID] = v1
	o.vaults[v2.ID] = v2

	last := map[chunk.ChunkID]int64{}
	changed, seen := o.snapshotActiveChunkProgress(last)
	if !changed {
		t.Error("expected changed=true")
	}
	if len(seen) != 3 {
		t.Errorf("seen count: got %d, want 3", len(seen))
	}
}
