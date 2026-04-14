package orchestrator

import (
	"gastrolog/internal/glid"
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
func (f *cronFakeChunkManager) FindIngestStartPosition(_ chunk.ChunkID, _ time.Time) (uint64, bool, error) {
	return 0, false, nil
}
func (f *cronFakeChunkManager) FindSourceStartPosition(_ chunk.ChunkID, _ time.Time) (uint64, bool, error) {
	return 0, false, nil
}
func (f *cronFakeChunkManager) ReadWriteTimestamps(id chunk.ChunkID, positions []uint64) ([]time.Time, error) {
	return nil, nil
}
func (f *cronFakeChunkManager) SetRotationPolicy(policy chunk.RotationPolicy) {}
func (f *cronFakeChunkManager) CheckRotation() *string                                    { return nil }
func (f *cronFakeChunkManager) ImportRecords(chunk.RecordIterator) (chunk.ChunkMeta, error) { return chunk.ChunkMeta{}, nil }
func (f *cronFakeChunkManager) ScanAttrs(_ chunk.ChunkID, _ uint64, _ func(time.Time, chunk.Attributes) bool) error {
	return nil
}
func (f *cronFakeChunkManager) SetNextChunkID(_ chunk.ChunkID) {}
func (f *cronFakeChunkManager) Close() error                   { return nil }

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

	vaultID := glid.New()
	tierID := glid.New()
	m := newTestCronManager(t)
	m.rotateVault(vaultID, tierID, "test-vault", cm)

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

	vaultID := glid.New()
	tierID := glid.New()
	m := newTestCronManager(t)
	m.rotateVault(vaultID, tierID, "test-vault", cm)

	if cm.sealed {
		t.Error("expected empty chunk to NOT be sealed")
	}
}

func TestRotateVaultSkipsNilActive(t *testing.T) {
	cm := &cronFakeChunkManager{
		active: nil,
	}

	vaultID := glid.New()
	tierID := glid.New()
	m := newTestCronManager(t)
	m.rotateVault(vaultID, tierID, "test-vault", cm)

	if cm.sealed {
		t.Error("expected nil active to NOT trigger seal")
	}
}

func TestEnsureCreatesAndUpdatesJob(t *testing.T) {
	cm := &cronFakeChunkManager{}
	m := newTestCronManager(t)

	vaultA := glid.New()
	tierA := glid.New()

	// First ensure creates the job.
	m.ensure(vaultA, tierA, "vault-a", "* * * * *", cm)

	name := cronJobName(vaultA, tierA)
	if !m.scheduler.HasJob(name) {
		t.Error("expected job to be registered after ensure")
	}
	if m.schedules[name] != "* * * * *" {
		t.Errorf("expected schedule '* * * * *', got %q", m.schedules[name])
	}

	// Ensure with same schedule is a no-op.
	m.ensure(vaultA, tierA, "vault-a", "* * * * *", cm)
	if m.schedules[name] != "* * * * *" {
		t.Error("schedule should be unchanged")
	}

	// Ensure with new schedule updates.
	m.ensure(vaultA, tierA, "vault-a", "0 * * * *", cm)
	if m.schedules[name] != "0 * * * *" {
		t.Errorf("expected updated schedule '0 * * * *', got %q", m.schedules[name])
	}
	if !m.scheduler.HasJob(name) {
		t.Error("job should still exist after schedule update")
	}
}

func TestPruneExceptRemovesStaleJobs(t *testing.T) {
	cm := &cronFakeChunkManager{}
	m := newTestCronManager(t)

	vaultA := glid.New()
	vaultB := glid.New()
	tierA := glid.New()
	tierB := glid.New()

	m.ensure(vaultA, tierA, "vault-a", "* * * * *", cm)
	m.ensure(vaultB, tierB, "vault-b", "0 * * * *", cm)

	nameA := cronJobName(vaultA, tierA)
	nameB := cronJobName(vaultB, tierB)

	// Prune everything except vault-a's job.
	m.pruneExcept(map[string]bool{nameA: true})

	if !m.scheduler.HasJob(nameA) {
		t.Error("vault-a job should survive prune")
	}
	if m.scheduler.HasJob(nameB) {
		t.Error("vault-b job should be pruned")
	}
	if _, ok := m.schedules[nameB]; ok {
		t.Error("vault-b schedule should be removed from tracking map")
	}
}

func TestRemoveAllForVault(t *testing.T) {
	cm := &cronFakeChunkManager{}
	m := newTestCronManager(t)

	vaultA := glid.New()
	tierA := glid.New()
	tierB := glid.New()

	m.ensure(vaultA, tierA, "vault-a", "* * * * *", cm)
	m.ensure(vaultA, tierB, "vault-a", "0 * * * *", cm)

	m.removeAllForVault(vaultA)

	if m.scheduler.HasJob(cronJobName(vaultA, tierA)) {
		t.Error("tier-a job should be removed")
	}
	if m.scheduler.HasJob(cronJobName(vaultA, tierB)) {
		t.Error("tier-b job should be removed")
	}
	if len(m.schedules) != 0 {
		t.Errorf("expected empty schedules map, got %d entries", len(m.schedules))
	}
}
