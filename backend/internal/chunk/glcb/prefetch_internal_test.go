package glcb

import (
	"testing"

	"gastrolog/internal/chunk"
)

// buildBlobFile (toc_hardening_test.go) writes a small valid GLCB and returns
// its path + size; reused here for the prefetch wiring tests.

// TestPrefetchSequentialReachesMadvise proves the chunk.SequentialPrefetcher
// method actually reaches the prefetch call on a live mapping.
func TestPrefetchSequentialReachesMadvise(t *testing.T) {
	path, size := buildBlobFile(t)
	blob, err := OpenMappedBlob(path)
	if err != nil {
		t.Fatal(err)
	}
	defer blob.Close()

	rd, err := blob.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()

	var calls int
	var gotLen int
	orig := prefetchMadvise
	prefetchMadvise = func(data []byte) {
		calls++
		gotLen = len(data)
	}
	defer func() { prefetchMadvise = orig }()

	// Call through the interface the retention drain uses, not the concrete
	// method, so the wiring (cursor -> reader -> madvise) is what's exercised.
	var prefetcher chunk.SequentialPrefetcher = NewGLCBCursor(rd, chunk.NewChunkID(), nil).(chunk.SequentialPrefetcher)
	prefetcher.PrefetchSequential()

	if calls != 1 {
		t.Fatalf("prefetch reached madvise %d times, want 1", calls)
	}
	// The prefetch must cover the whole file mapping, not a sub-slice.
	if int64(gotLen) != size {
		t.Fatalf("prefetch advised %d bytes, want whole mapping %d", gotLen, size)
	}
}

// TestPrefetchSequentialSafeAfterClose ensures the prefetch is a no-op (not a crash
// or an madvise against freed address space) once the cursor mapping is gone.
func TestPrefetchSequentialSafeAfterClose(t *testing.T) {
	path, _ := buildBlobFile(t)
	blob, err := OpenMappedBlob(path)
	if err != nil {
		t.Fatal(err)
	}
	rd, err := blob.Reader()
	if err != nil {
		t.Fatal(err)
	}
	_ = rd.Close() // nils mmapData
	_ = blob.Close()

	// Real madvise path (not the seam) — must tolerate the emptied mapping.
	rd.PrefetchSequential()
}

// TestMadviseSequentialEmptyInput guards the platform helper against nil/empty
// input directly.
func TestMadviseSequentialEmptyInput(t *testing.T) {
	madviseSequential(nil)
	madviseSequential([]byte{})
}
