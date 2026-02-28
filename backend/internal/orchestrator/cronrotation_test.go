package orchestrator

import (
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"

	"github.com/google/uuid"
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
func (f *cronFakeChunkManager) AppendPreserved(record chunk.Record) (chunk.ChunkID, uint64, error) {
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
func (f *cronFakeChunkManager) CheckRotation() *string                        { return nil }
func (f *cronFakeChunkManager) Close() error                                  { return nil }

// ---------- helpers ----------

func newTestCronManager(t *testing.T) *cronRotationManager {
	t.Helper()
	sched, err := newScheduler(slog.Default(), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	return newCronRotationManager(sched, slog.Default())
}

// ---------- tests ----------

func TestRotateVaultSealsNonEmptyChunk(t *testing.T) {
	cm := &cronFakeChunkManager{
		active: &chunk.ChunkMeta{
			ID:          chunkIDAt(time.Now()),
			RecordCount: 42,
			Bytes:       1024,
		},
	}

	vaultID := uuid.Must(uuid.NewV7())
	m := newTestCronManager(t)
	m.rotateVault(vaultID, "test-vault", cm)

	if !cm.sealed {
		t.Error("expected chunk to be sealed")
	}
}

func TestRotateVaultSkipsEmptyChunk(t *testing.T) {
	cm := &cronFakeChunkManager{
		active: &chunk.ChunkMeta{
			ID:          chunkIDAt(time.Now()),
			RecordCount: 0,
		},
	}

	vaultID := uuid.Must(uuid.NewV7())
	m := newTestCronManager(t)
	m.rotateVault(vaultID, "test-vault", cm)

	if cm.sealed {
		t.Error("expected empty chunk to NOT be sealed")
	}
}

func TestRotateVaultSkipsNilActive(t *testing.T) {
	cm := &cronFakeChunkManager{
		active: nil,
	}

	vaultID := uuid.Must(uuid.NewV7())
	m := newTestCronManager(t)
	m.rotateVault(vaultID, "test-vault", cm)

	if cm.sealed {
		t.Error("expected nil active to NOT trigger seal")
	}
}

func TestAddAndRemoveJob(t *testing.T) {
	cm := &cronFakeChunkManager{}
	m := newTestCronManager(t)

	vaultA := uuid.Must(uuid.NewV7())
	if err := m.addJob(vaultA, "vault-a", "* * * * *", cm); err != nil {
		t.Fatalf("addJob failed: %v", err)
	}

	if !m.hasJob(vaultA) {
		t.Error("expected job to be registered")
	}

	// Adding the same vault again should fail.
	if err := m.addJob(vaultA, "vault-a", "0 * * * *", cm); err == nil {
		t.Error("expected error when adding duplicate job")
	}

	m.removeJob(vaultA)

	if m.hasJob(vaultA) {
		t.Error("expected job to be removed")
	}

	// Removing a non-existent job should be a no-op.
	nonexistent := uuid.Must(uuid.NewV7())
	m.removeJob(nonexistent)
}

func TestUpdateJob(t *testing.T) {
	cm := &cronFakeChunkManager{}
	m := newTestCronManager(t)

	vaultA := uuid.Must(uuid.NewV7())
	if err := m.addJob(vaultA, "vault-a", "* * * * *", cm); err != nil {
		t.Fatalf("addJob failed: %v", err)
	}

	if err := m.updateJob(vaultA, "vault-a", "0 * * * *", cm); err != nil {
		t.Fatalf("updateJob failed: %v", err)
	}

	if !m.hasJob(vaultA) {
		t.Error("expected job to still exist after update")
	}
}

func TestAddJobRejectsInvalidCron(t *testing.T) {
	cm := &cronFakeChunkManager{}
	m := newTestCronManager(t)

	vaultA := uuid.Must(uuid.NewV7())
	if err := m.addJob(vaultA, "vault-a", "not a cron", cm); err == nil {
		t.Error("expected error for invalid cron expression")
	}

	if m.hasJob(vaultA) {
		t.Error("expected no job to be registered for invalid cron")
	}
}

func TestSchedulerListJobs(t *testing.T) {
	cm := &cronFakeChunkManager{}
	m := newTestCronManager(t)

	vaultA := uuid.Must(uuid.NewV7())
	vaultB := uuid.Must(uuid.NewV7())
	if err := m.addJob(vaultA, "vault-a", "* * * * *", cm); err != nil {
		t.Fatal(err)
	}
	if err := m.addJob(vaultB, "vault-b", "0 * * * *", cm); err != nil {
		t.Fatal(err)
	}

	jobs := m.scheduler.ListJobs()
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}

	names := map[string]bool{}
	for _, j := range jobs {
		names[j.Name] = true
		if j.Schedule == "" {
			t.Errorf("expected non-empty schedule for job %s", j.Name)
		}
	}

	if !names[cronJobName(vaultA)] {
		t.Error("expected job for vault-a")
	}
	if !names[cronJobName(vaultB)] {
		t.Error("expected job for vault-b")
	}
}
