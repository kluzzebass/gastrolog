package orchestrator

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
)

// ---------- fake chunk manager ----------

type retentionFakeChunkManager struct {
	chunks  []chunk.ChunkMeta
	deleted []chunk.ChunkID
}

func (f *retentionFakeChunkManager) Append(record chunk.Record) (chunk.ChunkID, uint64, error) {
	return chunk.ChunkID{}, 0, nil
}
func (f *retentionFakeChunkManager) Seal() error              { return nil }
func (f *retentionFakeChunkManager) Active() *chunk.ChunkMeta { return nil }
func (f *retentionFakeChunkManager) Meta(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	return chunk.ChunkMeta{}, nil
}
func (f *retentionFakeChunkManager) List() ([]chunk.ChunkMeta, error) {
	return f.chunks, nil
}
func (f *retentionFakeChunkManager) Delete(id chunk.ChunkID) error {
	f.deleted = append(f.deleted, id)
	return nil
}
func (f *retentionFakeChunkManager) OpenCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	return nil, nil
}
func (f *retentionFakeChunkManager) FindStartPosition(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	return 0, false, nil
}
func (f *retentionFakeChunkManager) ReadWriteTimestamps(id chunk.ChunkID, positions []uint64) ([]time.Time, error) {
	return nil, nil
}
func (f *retentionFakeChunkManager) SetRotationPolicy(policy chunk.RotationPolicy) {}

// ---------- fake index manager ----------

type retentionFakeIndexManager struct {
	deleted []chunk.ChunkID
}

func (f *retentionFakeIndexManager) BuildIndexes(ctx context.Context, chunkID chunk.ChunkID) error {
	return nil
}
func (f *retentionFakeIndexManager) DeleteIndexes(chunkID chunk.ChunkID) error {
	f.deleted = append(f.deleted, chunkID)
	return nil
}
func (f *retentionFakeIndexManager) OpenTokenIndex(chunkID chunk.ChunkID) (*index.Index[index.TokenIndexEntry], error) {
	return nil, nil
}
func (f *retentionFakeIndexManager) OpenAttrKeyIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrKeyIndexEntry], error) {
	return nil, nil
}
func (f *retentionFakeIndexManager) OpenAttrValueIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrValueIndexEntry], error) {
	return nil, nil
}
func (f *retentionFakeIndexManager) OpenAttrKVIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrKVIndexEntry], error) {
	return nil, nil
}
func (f *retentionFakeIndexManager) OpenKVKeyIndex(chunkID chunk.ChunkID) (*index.Index[index.KVKeyIndexEntry], index.KVIndexStatus, error) {
	return nil, index.KVComplete, nil
}
func (f *retentionFakeIndexManager) OpenKVValueIndex(chunkID chunk.ChunkID) (*index.Index[index.KVValueIndexEntry], index.KVIndexStatus, error) {
	return nil, index.KVComplete, nil
}
func (f *retentionFakeIndexManager) OpenKVIndex(chunkID chunk.ChunkID) (*index.Index[index.KVIndexEntry], index.KVIndexStatus, error) {
	return nil, index.KVComplete, nil
}
func (f *retentionFakeIndexManager) IndexesComplete(chunkID chunk.ChunkID) (bool, error) {
	return true, nil
}

// ---------- helpers ----------

func chunkIDAt(t time.Time) chunk.ChunkID {
	return chunk.ChunkIDFromTime(t)
}

func newRetentionRunner(cm chunk.ChunkManager, im index.IndexManager, policy chunk.RetentionPolicy) *retentionRunner {
	return &retentionRunner{
		storeID: "test-store",
		cm:      cm,
		im:      im,
		policy:  policy,
		now:     time.Now,
		logger:  slog.Default(),
	}
}

// ---------- tests ----------

func TestSweepDeletesExpiredChunks(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	id0 := chunkIDAt(base)
	id1 := chunkIDAt(base.Add(1 * time.Hour))
	id2 := chunkIDAt(base.Add(2 * time.Hour))
	id3 := chunkIDAt(base.Add(3 * time.Hour))

	cm := &retentionFakeChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: id0, StartTS: base, EndTS: base.Add(30 * time.Minute), Sealed: true},
			{ID: id1, StartTS: base.Add(1 * time.Hour), EndTS: base.Add(90 * time.Minute), Sealed: true},
			{ID: id2, StartTS: base.Add(2 * time.Hour), EndTS: base.Add(150 * time.Minute), Sealed: true},
			{ID: id3, StartTS: base.Add(3 * time.Hour), EndTS: base.Add(210 * time.Minute), Sealed: true},
		},
	}
	im := &retentionFakeIndexManager{}

	policy := chunk.NewCountRetentionPolicy(2)
	r := newRetentionRunner(cm, im, policy)

	r.sweep()

	// With max 2, the 2 oldest (id0, id1) should be deleted.
	if len(cm.deleted) != 2 {
		t.Fatalf("expected 2 chunk deletions, got %d", len(cm.deleted))
	}
	if cm.deleted[0] != id0 {
		t.Errorf("expected first deleted chunk %s, got %s", id0, cm.deleted[0])
	}
	if cm.deleted[1] != id1 {
		t.Errorf("expected second deleted chunk %s, got %s", id1, cm.deleted[1])
	}

	// Indexes should be deleted first (same IDs, same order).
	if len(im.deleted) != 2 {
		t.Fatalf("expected 2 index deletions, got %d", len(im.deleted))
	}
	if im.deleted[0] != id0 {
		t.Errorf("expected first deleted index %s, got %s", id0, im.deleted[0])
	}
	if im.deleted[1] != id1 {
		t.Errorf("expected second deleted index %s, got %s", id1, im.deleted[1])
	}
}

func TestSweepSkipsActiveChunks(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	idSealed0 := chunkIDAt(base)
	idSealed1 := chunkIDAt(base.Add(1 * time.Hour))
	idSealed2 := chunkIDAt(base.Add(2 * time.Hour))
	idActive := chunkIDAt(base.Add(3 * time.Hour))

	cm := &retentionFakeChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: idSealed0, StartTS: base, EndTS: base.Add(30 * time.Minute), Sealed: true},
			{ID: idSealed1, StartTS: base.Add(1 * time.Hour), EndTS: base.Add(90 * time.Minute), Sealed: true},
			{ID: idSealed2, StartTS: base.Add(2 * time.Hour), EndTS: base.Add(150 * time.Minute), Sealed: true},
			{ID: idActive, StartTS: base.Add(3 * time.Hour), Sealed: false}, // active, unsealed
		},
	}
	im := &retentionFakeIndexManager{}

	// Keep max 2 sealed chunks. With 3 sealed, oldest 1 should be deleted.
	// The active chunk must not be considered.
	policy := chunk.NewCountRetentionPolicy(2)
	r := newRetentionRunner(cm, im, policy)

	r.sweep()

	if len(cm.deleted) != 1 {
		t.Fatalf("expected 1 chunk deletion, got %d", len(cm.deleted))
	}
	if cm.deleted[0] != idSealed0 {
		t.Errorf("expected deleted chunk %s, got %s", idSealed0, cm.deleted[0])
	}

	// Verify the active chunk was not deleted.
	for _, id := range cm.deleted {
		if id == idActive {
			t.Error("active (unsealed) chunk should not be deleted")
		}
	}
	for _, id := range im.deleted {
		if id == idActive {
			t.Error("active (unsealed) chunk indexes should not be deleted")
		}
	}
}

func TestSweepWithNoPolicy(t *testing.T) {
	cm := &retentionFakeChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunkIDAt(time.Now()), Sealed: true},
		},
	}
	im := &retentionFakeIndexManager{}

	r := newRetentionRunner(cm, im, nil)

	r.sweep()

	if len(cm.deleted) != 0 {
		t.Errorf("expected no chunk deletions with nil policy, got %d", len(cm.deleted))
	}
	if len(im.deleted) != 0 {
		t.Errorf("expected no index deletions with nil policy, got %d", len(im.deleted))
	}
}

func TestSetPolicyHotSwap(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	id0 := chunkIDAt(base)
	id1 := chunkIDAt(base.Add(1 * time.Hour))
	id2 := chunkIDAt(base.Add(2 * time.Hour))

	cm := &retentionFakeChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: id0, StartTS: base, EndTS: base.Add(30 * time.Minute), Sealed: true},
			{ID: id1, StartTS: base.Add(1 * time.Hour), EndTS: base.Add(90 * time.Minute), Sealed: true},
			{ID: id2, StartTS: base.Add(2 * time.Hour), EndTS: base.Add(150 * time.Minute), Sealed: true},
		},
	}
	im := &retentionFakeIndexManager{}

	// Start with keep-all (max 10) so nothing gets deleted.
	r := newRetentionRunner(cm, im, chunk.NewCountRetentionPolicy(10))

	r.sweep()

	if len(cm.deleted) != 0 {
		t.Fatalf("expected no deletions with generous policy, got %d", len(cm.deleted))
	}

	// Hot-swap to keep-1 policy. Next sweep should delete the 2 oldest.
	r.setPolicy(chunk.NewCountRetentionPolicy(1))

	r.sweep()

	if len(cm.deleted) != 2 {
		t.Fatalf("expected 2 chunk deletions after policy swap, got %d", len(cm.deleted))
	}
	if cm.deleted[0] != id0 {
		t.Errorf("expected first deleted chunk %s, got %s", id0, cm.deleted[0])
	}
	if cm.deleted[1] != id1 {
		t.Errorf("expected second deleted chunk %s, got %s", id1, cm.deleted[1])
	}
}
