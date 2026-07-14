package cloud

// glcbCursor boundary tests (gastrolog-5do8sh): Seek past the end and a
// zero-record GLCB. Complements TestSeekableCursor (cloud_test.go), which
// pins Seek within range + Prev and Prev-past-start.

import (
	"errors"
	"testing"

	"gastrolog/internal/chunk"
)

// openBoundaryCursor maps the GLCB at path and returns a seekable cursor
// over it; blob, reader, and cursor are all closed via t.Cleanup.
func openBoundaryCursor(t *testing.T, path string) chunk.RecordCursor {
	t.Helper()
	blob, err := OpenMappedBlob(path)
	if err != nil {
		t.Fatalf("OpenMappedBlob: %v", err)
	}
	t.Cleanup(func() { _ = blob.Close() })
	rd, err := blob.Reader()
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	cur := NewSeekableCursorWithClose(rd, chunk.ChunkID{}, nil)
	t.Cleanup(func() { _ = cur.Close() })
	return cur
}

// TestGLCBCursorSeekPastEnd pins current behavior for a Seek strictly past
// the last record: Next reports ErrNoMoreRecords via the fwdIndex bound,
// and Prev reports ErrNoMoreRecords passed through from the reader's
// position bound (revIndex-1 is still >= recordCount). Note that Prev's
// error path never sets revDone, so repeated Prev calls re-read the same
// out-of-range position and keep returning the error — current behavior,
// pinned here, not changed.
func TestGLCBCursorSeekPastEnd(t *testing.T) {
	t.Parallel()

	const recordCount = 4
	path := writeFanOutTestGLCB(t, recordCount)
	cur := openBoundaryCursor(t, path)

	if err := cur.Seek(chunk.RecordRef{Pos: recordCount + 10}); err != nil {
		t.Fatalf("Seek past end: %v", err)
	}

	if _, _, err := cur.Next(); !errors.Is(err, chunk.ErrNoMoreRecords) {
		t.Fatalf("Next after Seek past end: err = %v, want ErrNoMoreRecords", err)
	}
	if _, _, err := cur.Prev(); !errors.Is(err, chunk.ErrNoMoreRecords) {
		t.Fatalf("Prev after Seek past end: err = %v, want ErrNoMoreRecords", err)
	}
	// Repeated Prev re-reads the same out-of-range position (revDone is
	// never set on this path) and must keep returning the same error.
	if _, _, err := cur.Prev(); !errors.Is(err, chunk.ErrNoMoreRecords) {
		t.Fatalf("second Prev after Seek past end: err = %v, want ErrNoMoreRecords", err)
	}
}

// TestGLCBCursorEmptyBlob iterates a zero-record GLCB: both directions
// are immediately exhausted, before and after a Seek(0).
func TestGLCBCursorEmptyBlob(t *testing.T) {
	t.Parallel()

	path := writeFanOutTestGLCB(t, 0)
	cur := openBoundaryCursor(t, path)

	if _, _, err := cur.Next(); !errors.Is(err, chunk.ErrNoMoreRecords) {
		t.Fatalf("Next on empty GLCB: err = %v, want ErrNoMoreRecords", err)
	}
	if _, _, err := cur.Prev(); !errors.Is(err, chunk.ErrNoMoreRecords) {
		t.Fatalf("Prev on empty GLCB: err = %v, want ErrNoMoreRecords", err)
	}

	if err := cur.Seek(chunk.RecordRef{Pos: 0}); err != nil {
		t.Fatalf("Seek(0) on empty GLCB: %v", err)
	}
	if _, _, err := cur.Next(); !errors.Is(err, chunk.ErrNoMoreRecords) {
		t.Fatalf("Next after Seek(0) on empty GLCB: err = %v, want ErrNoMoreRecords", err)
	}
	if _, _, err := cur.Prev(); !errors.Is(err, chunk.ErrNoMoreRecords) {
		t.Fatalf("Prev after Seek(0) on empty GLCB: err = %v, want ErrNoMoreRecords", err)
	}
}
