package file

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// corruptCloudDiskBytes directly overwrites a cloud-index entry's persisted
// diskBytes, bypassing updateCloudDiskBytes — simulating the crash window
// the reconciliation pass exists to close: a local mutation (eviction's
// os.Remove, a re-warm's rename) that landed without its paired index
// update (process died in between).
func corruptCloudDiskBytes(t *testing.T, cm *Manager, id chunk.ChunkID, diskBytes int64) {
	t.Helper()
	cm.cloudIdxMu.Lock()
	defer cm.cloudIdxMu.Unlock()
	meta, ok := cm.cloudIdx.Lookup(id)
	if !ok {
		t.Fatalf("corruptCloudDiskBytes: %s not in cloud index", id)
	}
	meta.diskBytes = diskBytes
	if _, err := cm.cloudIdx.Delete(id); err != nil {
		t.Fatalf("corruptCloudDiskBytes: delete: %v", err)
	}
	if err := cm.cloudIdx.Insert(id, meta); err != nil {
		t.Fatalf("corruptCloudDiskBytes: insert: %v", err)
	}
	if err := cm.cloudIdx.Sync(); err != nil {
		t.Fatalf("corruptCloudDiskBytes: sync: %v", err)
	}
}

func newRestartableCloudManager(t *testing.T, dir string, vaultID glid.GLID, store *blobstore.Memory) *Manager {
	t.Helper()
	cm, err := NewManager(Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		CloudStore:     store,
		VaultID:        vaultID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return cm
}

// TestStartupReconcilesStalePositiveDiskBytes pins the fix for the crash
// window between eviction's os.Remove and its paired updateCloudDiskBytes(0)
// call: if the process dies in between, the persisted cloud-index entry
// keeps claiming a positive local footprint for a file that no longer
// exists — and nothing else self-heals it, since runEvictionSweep's own
// candidate set only considers chunks whose data.glcb stat still succeeds.
// A restart must stat the file and correct the claim to 0.
func TestStartupReconcilesStalePositiveDiskBytes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()
	store := blobstore.NewMemory()

	cm := newRestartableCloudManager(t, dir, vaultID, store)
	id := appendSealAndUpload(t, cm, 50)

	meta, err := cm.Meta(id)
	if err != nil {
		t.Fatalf("Meta before crash simulation: %v", err)
	}
	if meta.DiskBytes <= 0 {
		t.Fatalf("fixture setup: expected a cached warm copy, DiskBytes = %d", meta.DiskBytes)
	}

	// Simulate the crash: the local file is gone (as eviction would leave
	// it) but the persisted diskBytes was never updated to 0 because the
	// process died between the two steps.
	glcbPath := filepath.Join(cm.chunkDir(id), dataGLCBFileName)
	if err := os.Remove(glcbPath); err != nil {
		t.Fatalf("simulate crash: remove local file: %v", err)
	}
	if err := cm.Close(); err != nil {
		t.Fatalf("close before restart: %v", err)
	}

	cm2 := newRestartableCloudManager(t, dir, vaultID, store)
	defer func() { _ = cm2.Close() }()

	got, err := cm2.Meta(id)
	if err != nil {
		t.Fatalf("Meta after restart: %v", err)
	}
	if got.DiskBytes != 0 {
		t.Errorf("DiskBytes after restart = %d, want 0 (reconciled: file is missing)", got.DiskBytes)
	}
	if chunk.DiskClaim(got, nil) != 0 {
		t.Errorf("DiskClaim after restart = %d, want 0", chunk.DiskClaim(got, nil))
	}
}

// TestStartupReconcilesStaleZeroDiskBytes pins the mirror crash window: a
// completed re-warm (or upload) whose rename landed but whose paired
// updateCloudDiskBytes call didn't leaves a persisted 0 next to a real,
// present file. A restart must stat the file and correct the claim up to
// its actual size.
func TestStartupReconcilesStaleZeroDiskBytes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()
	store := blobstore.NewMemory()

	cm := newRestartableCloudManager(t, dir, vaultID, store)
	id := appendSealAndUpload(t, cm, 50)

	meta, err := cm.Meta(id)
	if err != nil {
		t.Fatalf("Meta before crash simulation: %v", err)
	}
	wantSize := meta.DiskBytes
	if wantSize <= 0 {
		t.Fatalf("fixture setup: expected a cached warm copy, DiskBytes = %d", wantSize)
	}

	// Simulate the crash: the persisted entry regressed to 0 (as if the
	// paired update from a prior eviction landed) even though the file is
	// actually still present on disk.
	corruptCloudDiskBytes(t, cm, id, 0)
	if err := cm.Close(); err != nil {
		t.Fatalf("close before restart: %v", err)
	}

	cm2 := newRestartableCloudManager(t, dir, vaultID, store)
	defer func() { _ = cm2.Close() }()

	got, err := cm2.Meta(id)
	if err != nil {
		t.Fatalf("Meta after restart: %v", err)
	}
	if got.DiskBytes != wantSize {
		t.Errorf("DiskBytes after restart = %d, want %d (reconciled: file is present)", got.DiskBytes, wantSize)
	}
	if chunk.DiskClaim(got, nil) != wantSize {
		t.Errorf("DiskClaim after restart = %d, want %d", chunk.DiskClaim(got, nil), wantSize)
	}
}

// TestStartupReconcileNoOpWhenAlreadyCorrect confirms a clean restart with
// no crash window doesn't rewrite every cloud-index entry — the common
// case should be cheap (computeDiskBytes' ReadDir cost only, no
// Delete+Insert+Sync churn).
func TestStartupReconcileNoOpWhenAlreadyCorrect(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	vaultID := glid.New()
	store := blobstore.NewMemory()

	cm := newRestartableCloudManager(t, dir, vaultID, store)
	id := appendSealAndUpload(t, cm, 50)
	want, err := cm.Meta(id)
	if err != nil {
		t.Fatalf("Meta: %v", err)
	}
	if err := cm.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	cm2 := newRestartableCloudManager(t, dir, vaultID, store)
	defer func() { _ = cm2.Close() }()

	got, err := cm2.Meta(id)
	if err != nil {
		t.Fatalf("Meta after restart: %v", err)
	}
	if got.DiskBytes != want.DiskBytes {
		t.Errorf("DiskBytes after clean restart = %d, want unchanged %d", got.DiskBytes, want.DiskBytes)
	}
}
