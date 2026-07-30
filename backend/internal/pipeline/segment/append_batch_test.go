package segment_test

// AppendFrames commits a whole batch with one data write and one header
// rewrite. On-disk layout, header fields, and the running checksum must be
// byte-identical to sequential single appends, and crash recovery must
// reconcile a torn batch exactly like a torn single.

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segment"
)

func encodeSample(t *testing.T, seq uint32) segment.Frame {
	t.Helper()
	rec := sampleRecord(seq)
	body, err := segment.EncodeFrame(rec, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return segment.Frame{Rec: rec, Body: body}
}

// TestAppendFramesEquivalentToSingles: one batch of N == N single appends,
// byte for byte.
func TestAppendFramesEquivalentToSingles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	meta := segment.Meta{ID: glid.New(), VaultID: glid.New()}

	frames := make([]segment.Frame, 8)
	for i := range frames {
		frames[i] = encodeSample(t, uint32(i)) //nolint:gosec // G115: small test index
	}

	batchPath := filepath.Join(dir, "batch")
	sfBatch, err := segment.Create(batchPath, meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := sfBatch.AppendFrames(frames); err != nil {
		t.Fatal(err)
	}
	if err := sfBatch.Close(); err != nil {
		t.Fatal(err)
	}

	singlePath := filepath.Join(dir, "single")
	sfSingle, err := segment.Create(singlePath, meta)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range frames {
		if err := sfSingle.AppendFrame(f.Rec, time.Time{}, f.Body); err != nil {
			t.Fatal(err)
		}
	}
	if err := sfSingle.Close(); err != nil {
		t.Fatal(err)
	}

	batchBytes, err := os.ReadFile(batchPath) //ok:io-readall small test fixture
	if err != nil {
		t.Fatal(err)
	}
	singleBytes, err := os.ReadFile(singlePath) //ok:io-readall small test fixture
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(batchBytes, singleBytes) {
		t.Fatalf("batched append diverges from sequential singles: %d vs %d bytes", len(batchBytes), len(singleBytes))
	}

	// Reopen and verify the reconciled view too.
	sf, err := segment.Open(batchPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sf.Close()
	recs, err := sf.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != len(frames) {
		t.Fatalf("ReadAll = %d records, want %d", len(recs), len(frames))
	}
}

// TestAppendFramesRecoversTornBatchTail: a crash can tear the tail of a batch
// write. Recovery must keep every complete frame — including frames from the
// torn batch that landed intact — and drop only the torn suffix.
func TestAppendFramesRecoversTornBatchTail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg")
	sf, err := segment.Create(path, segment.Meta{ID: glid.New(), VaultID: glid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if err := sf.AppendFrames([]segment.Frame{encodeSample(t, 0), encodeSample(t, 1)}); err != nil {
		t.Fatal(err)
	}
	if err := sf.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := sf.AppendFrames([]segment.Frame{encodeSample(t, 2), encodeSample(t, 3)}); err != nil {
		t.Fatal(err)
	}
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}

	// Tear the last frame: chop 3 bytes off the file tail, as a crash mid
	// data-write would.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}

	sf2, err := segment.Open(path)
	if err != nil {
		t.Fatalf("Open after torn batch tail: %v", err)
	}
	defer sf2.Close()
	recs, err := sf2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("recovered %d records, want 3 (two synced + first intact frame of torn batch)", len(recs))
	}
	if sf2.Header().RecordCount != 3 {
		t.Fatalf("reconciled RecordCount = %d, want 3", sf2.Header().RecordCount)
	}
}

// TestFinalizeFromMemoryMatchesDiskScan: writer-created segments finalize via
// the in-memory index build; crash-recovered segments (Open path, no
// capture) use the disk scan. Both must produce byte-identical files — same
// tails, same sort, same checksums, same header.
func TestFinalizeFromMemoryMatchesDiskScan(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	meta := segment.Meta{ID: glid.New(), VaultID: glid.New()}

	frames := make([]segment.Frame, 32)
	for i := range frames {
		frames[i] = encodeSample(t, uint32(len(frames)-i)) //nolint:gosec // G115: small test index
	}

	write := func(name string) string {
		path := filepath.Join(dir, name)
		sf, err := segment.Create(path, meta)
		if err != nil {
			t.Fatal(err)
		}
		if err := sf.AppendFrames(frames); err != nil {
			t.Fatal(err)
		}
		if err := sf.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := sf.Close(); err != nil {
			t.Fatal(err)
		}
		return path
	}

	// Fast path: reopen the writer's File? No — Create+Append keeps the
	// in-memory capture, so finalize directly on a fresh writer instance.
	fastPath := filepath.Join(dir, "fast")
	sfFast, err := segment.Create(fastPath, meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := sfFast.AppendFrames(frames); err != nil {
		t.Fatal(err)
	}
	if err := sfFast.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := sfFast.Close(); err != nil {
		t.Fatal(err)
	}

	// Disk path: identical unfinalized file, reopened via Open (no capture).
	diskPath := write("disk")
	sfDisk, err := segment.Open(diskPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := sfDisk.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := sfDisk.Close(); err != nil {
		t.Fatal(err)
	}

	fastBytes, err := os.ReadFile(fastPath) //ok:io-readall small test fixture
	if err != nil {
		t.Fatal(err)
	}
	diskBytes, err := os.ReadFile(diskPath) //ok:io-readall small test fixture
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fastBytes, diskBytes) {
		t.Fatalf("in-memory finalize diverges from disk-scan finalize: %d vs %d bytes", len(fastBytes), len(diskBytes))
	}

	// Both must pass full open-time verification (layout + checksums).
	for _, p := range []string{fastPath, diskPath} {
		sf, err := segment.Open(p)
		if err != nil {
			t.Fatalf("Open(%s) after finalize: %v", p, err)
		}
		if sf.Header().SourceIndexCount != 32 {
			t.Fatalf("SourceIndexCount = %d, want 32", sf.Header().SourceIndexCount)
		}
		_ = sf.Close()
	}
}
