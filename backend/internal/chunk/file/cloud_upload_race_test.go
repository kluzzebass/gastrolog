package file

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// TestCloudUploadWaitsForInFlightCursor is the regression test for
// gastrolog-2owzp. The 26zu1 per-chunk RWMutex was missing from
// uploadToCloud's removeLocalDataFiles call, allowing a backfill
// upload to remove a chunk's mmap'd files while an indexer Build
// cursor was iterating them — SIGBUS in production on macOS, silent
// data race on Linux (FDs pin inodes; mmap stays valid). The Linux
// inode semantics make the SIGBUS itself cross-platform-untestable;
// instead the test pins the lock CONTRACT.
//
// Causal/sequencing assertion (not timing-based, so the test is
// hardware-independent and CI-stable): UploadToCloud must NOT
// complete while a cursor's RLock is held. The test:
//
//   1. Opens a cursor (RLock taken).
//   2. Spawns UploadToCloud in a goroutine; tracks completion via
//      a done channel that closes when UploadToCloud returns.
//   3. Asserts the done channel is NOT closed by the time we've
//      let UploadToCloud's S3 path run to completion (memory
//      blobstore: instantaneous; this is just a "B has had every
//      chance to finish if it was going to" hand-off).
//   4. Closes the cursor (releases RLock).
//   5. Asserts the done channel closes promptly after.
//
// Pre-fix: step 3 fails because UploadToCloud sails through
// removeLocalDataFiles concurrently with the cursor's mmap reads.
// Post-fix: step 3 holds (UploadToCloud is blocked at chunkLock.Lock
// inside the removal phase) and step 5 confirms it unblocks once
// the RLock is released.
func TestCloudUploadWaitsForInFlightCursor(t *testing.T) {
	t.Parallel()

	const records = 200

	dir := t.TempDir()
	cacheDir := t.TempDir()
	vaultID := glid.New()
	store := blobstore.NewMemory()

	cm, err := NewManager(Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(uint64(records * 10)),
		CloudStore:     store,
		VaultID:        vaultID,
		CacheDir:       cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range records {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts,
			Raw: []byte(fmt.Sprintf("payload-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	active := cm.Active()
	if active == nil {
		t.Fatal("no active chunk")
	}
	chunkID := active.ID
	if err := cm.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := cm.CompressChunk(chunkID); err != nil {
		t.Fatal(err)
	}

	// Open and hold the cursor — its RLock pins the per-chunk lock.
	cursor, err := cm.OpenCursor(chunkID)
	if err != nil {
		t.Fatal(err)
	}

	uploadDone := make(chan error, 1)
	go func() {
		uploadDone <- cm.UploadToCloud(chunkID)
	}()

	// Wait for UploadToCloud to either complete (pre-fix) or block
	// at chunkLock.Lock (post-fix). The polling deadline is generous;
	// what's being asserted is the eventual state after the deadline,
	// not how fast UploadToCloud reaches it. A passing pre-fix run
	// would have UploadToCloud finished long before this deadline
	// regardless of hardware speed (memory blobstore + a few hundred
	// records). A passing post-fix run has UploadToCloud still in
	// flight at the deadline because chunkLock.Lock is blocked on
	// the cursor's RLock.
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	select {
	case err := <-uploadDone:
		t.Fatalf("UploadToCloud completed (err=%v) while cursor's RLock was still held — per-chunk lock contract violated; removal raced with mmap reads",
			err)
	case <-deadline.C:
		// Expected post-fix: UploadToCloud is blocked on chunkLock.
	}

	// Release the cursor's RLock. UploadToCloud should unblock and
	// complete promptly. The deadline here is just a watchdog —
	// success is "completes at all", not "completes within X ms".
	if err := cursor.Close(); err != nil {
		t.Fatalf("cursor.Close: %v", err)
	}

	completionDeadline := time.NewTimer(10 * time.Second)
	defer completionDeadline.Stop()
	select {
	case err := <-uploadDone:
		if err != nil && !errors.Is(err, chunk.ErrChunkNotFound) {
			t.Errorf("UploadToCloud after cursor close: %v", err)
		}
	case <-completionDeadline.C:
		t.Fatalf("UploadToCloud did not complete within 10s after cursor close — chunkLock release path is broken")
	}
}
