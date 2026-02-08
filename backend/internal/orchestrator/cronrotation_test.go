package orchestrator

import (
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// ---------- fake chunk manager for cron rotation ----------

type cronFakeChunkManager struct {
	active  *chunk.ChunkMeta
	sealed  bool
	sealErr error
}

func (f *cronFakeChunkManager) Append(record chunk.Record) (chunk.ChunkID, uint64, error) {
	return chunk.ChunkID{}, 0, nil
}
func (f *cronFakeChunkManager) Seal() error {
	f.sealed = true
	return f.sealErr
}
func (f *cronFakeChunkManager) Active() *chunk.ChunkMeta { return f.active }
func (f *cronFakeChunkManager) Meta(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	return chunk.ChunkMeta{}, nil
}
func (f *cronFakeChunkManager) List() ([]chunk.ChunkMeta, error) { return nil, nil }
func (f *cronFakeChunkManager) Delete(id chunk.ChunkID) error    { return nil }
func (f *cronFakeChunkManager) OpenCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	return nil, nil
}
func (f *cronFakeChunkManager) FindStartPosition(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	return 0, false, nil
}
func (f *cronFakeChunkManager) ReadWriteTimestamps(id chunk.ChunkID, positions []uint64) ([]time.Time, error) {
	return nil, nil
}
func (f *cronFakeChunkManager) SetRotationPolicy(policy chunk.RotationPolicy) {}

// ---------- tests ----------

func TestRotateStoreSealsNonEmptyChunk(t *testing.T) {
	cm := &cronFakeChunkManager{
		active: &chunk.ChunkMeta{
			ID:          chunkIDAt(time.Now()),
			RecordCount: 42,
			Bytes:       1024,
		},
	}

	m, err := newCronRotationManager(slog.Default())
	if err != nil {
		t.Fatal(err)
	}

	m.rotateStore("test-store", cm)

	if !cm.sealed {
		t.Error("expected chunk to be sealed")
	}
}

func TestRotateStoreSkipsEmptyChunk(t *testing.T) {
	cm := &cronFakeChunkManager{
		active: &chunk.ChunkMeta{
			ID:          chunkIDAt(time.Now()),
			RecordCount: 0,
		},
	}

	m, err := newCronRotationManager(slog.Default())
	if err != nil {
		t.Fatal(err)
	}

	m.rotateStore("test-store", cm)

	if cm.sealed {
		t.Error("expected empty chunk to NOT be sealed")
	}
}

func TestRotateStoreSkipsNilActive(t *testing.T) {
	cm := &cronFakeChunkManager{
		active: nil,
	}

	m, err := newCronRotationManager(slog.Default())
	if err != nil {
		t.Fatal(err)
	}

	m.rotateStore("test-store", cm)

	if cm.sealed {
		t.Error("expected nil active to NOT trigger seal")
	}
}

func TestAddAndRemoveJob(t *testing.T) {
	cm := &cronFakeChunkManager{}

	m, err := newCronRotationManager(slog.Default())
	if err != nil {
		t.Fatal(err)
	}

	if err := m.addJob("store-a", "* * * * *", cm); err != nil {
		t.Fatalf("addJob failed: %v", err)
	}

	if _, ok := m.jobs["store-a"]; !ok {
		t.Error("expected job to be registered")
	}

	// Adding the same store again should fail.
	if err := m.addJob("store-a", "0 * * * *", cm); err == nil {
		t.Error("expected error when adding duplicate job")
	}

	m.removeJob("store-a")

	if _, ok := m.jobs["store-a"]; ok {
		t.Error("expected job to be removed")
	}

	// Removing a non-existent job should be a no-op.
	m.removeJob("store-nonexistent")
}

func TestUpdateJob(t *testing.T) {
	cm := &cronFakeChunkManager{}

	m, err := newCronRotationManager(slog.Default())
	if err != nil {
		t.Fatal(err)
	}

	if err := m.addJob("store-a", "* * * * *", cm); err != nil {
		t.Fatalf("addJob failed: %v", err)
	}

	oldJobID := m.jobs["store-a"].ID()

	if err := m.updateJob("store-a", "0 * * * *", cm); err != nil {
		t.Fatalf("updateJob failed: %v", err)
	}

	newJobID := m.jobs["store-a"].ID()
	if oldJobID == newJobID {
		t.Error("expected job ID to change after update")
	}
}

func TestAddJobRejectsInvalidCron(t *testing.T) {
	cm := &cronFakeChunkManager{}

	m, err := newCronRotationManager(slog.Default())
	if err != nil {
		t.Fatal(err)
	}

	if err := m.addJob("store-a", "not a cron", cm); err == nil {
		t.Error("expected error for invalid cron expression")
	}

	if _, ok := m.jobs["store-a"]; ok {
		t.Error("expected no job to be registered for invalid cron")
	}
}
