package file

import (
	"context"
	"errors"
	"gastrolog/internal/glid"
	"io"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
)

// blockingStore is a blobstore.Store whose Upload method blocks until the
// context is cancelled. Used to verify that cloud operations in the chunk
// manager respect per-call timeouts — without a deadline they would block
// indefinitely. Other methods delegate to the embedded blobstore.Memory so
// the store is otherwise usable.
type blockingStore struct {
	blobstore.Store
	uploadCalled chan struct{} // closed on first Upload call, for sync
}

func (s *blockingStore) Upload(ctx context.Context, _ string, _ io.Reader, _ map[string]string) error {
	select {
	case <-s.uploadCalled:
	default:
		close(s.uploadCalled)
	}
	<-ctx.Done()
	return ctx.Err()
}

// TestCloudUploadTimeout is the regression test for gastrolog-21xs8.
// Before the fix, uploadToCloud passed context.Background() to the
// CloudStore.Upload call, so a slow or unresponsive S3 would block the
// post-seal pipeline indefinitely. The fix wraps the call with a
// per-call deadline (cloudUploadTimeout). This test uses a mock Store
// whose Upload method blocks forever, and asserts that PostSealProcess
// returns within cloudUploadTimeout + margin rather than hanging.
//
// The production cloudUploadTimeout is 60s, which is too slow for a
// test. Monkey-patch a short value for just this test so the whole
// scenario runs in a fraction of a second.
func TestCloudUploadTimeout(t *testing.T) {
	// NOT parallel: this test overwrites a package-level var. Running
	// parallel with other tests that call uploadToCloud would be a data
	// race. The other existing chunk/file tests that use CloudStore
	// (cache_test.go) don't hit the Upload path, so this should be safe,
	// but not sharing the var is still the right call.
	prevTimeout := cloudUploadTimeout
	cloudUploadTimeout = 200 * time.Millisecond
	t.Cleanup(func() { cloudUploadTimeout = prevTimeout })

	vaultID := glid.New()
	blocking := &blockingStore{
		Store:        blobstore.NewMemory(),
		uploadCalled: make(chan struct{}),
	}

	dir := t.TempDir()
	cm, err := NewManager(Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		CloudStore:     blocking,
		VaultID:        vaultID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	// Append one record and seal so we have a chunk to upload.
	if _, _, err := cm.Append(chunk.Record{
		IngestTS: time.Now(),
		WriteTS:  time.Now(),
		Raw:      []byte("x"),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	active := cm.Active()
	if active == nil {
		t.Fatal("no active chunk after append")
	}
	chunkID := active.ID
	if err := cm.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// PostSealProcess will run compress → index → upload. The upload
	// will block on the blocking store until cloudUploadTimeout fires
	// and the mock returns ctx.Err().
	start := time.Now()
	err = cm.PostSealProcess(context.Background(), chunkID)
	elapsed := time.Since(start)

	// PostSealProcess logs and swallows upload failures, so we check
	// elapsed time rather than error. If the timeout fires, elapsed
	// should be ≈ cloudUploadTimeout. Without the fix, elapsed would
	// be unbounded and this test would hang.
	if elapsed > cloudUploadTimeout+2*time.Second {
		t.Fatalf("PostSealProcess elapsed %v > cloudUploadTimeout %v + margin — timeout not applied",
			elapsed, cloudUploadTimeout)
	}
	if elapsed < cloudUploadTimeout/2 {
		t.Fatalf("PostSealProcess returned in %v, much less than cloudUploadTimeout %v — did upload block at all?",
			elapsed, cloudUploadTimeout)
	}
	// Confirm the blocking store's Upload was actually invoked.
	select {
	case <-blocking.uploadCalled:
	default:
		t.Error("Upload was never called — test setup broken")
	}

	// PostSealProcess itself should return nil even when the upload
	// times out — upload is best-effort ("keeping local" warn) and
	// the pipeline must not fail the caller.
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Logf("PostSealProcess returned %v (acceptable: DeadlineExceeded or nil)", err)
	}
}
