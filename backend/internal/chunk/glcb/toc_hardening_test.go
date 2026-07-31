package glcb

// TOC hardening tests: oversized TOC entries fail the seal with an error
// instead of panicking, and corrupt/truncated blob tails are rejected at
// ReadTOC time so section mmap windows never map past EOF and SIGBUS on
// first access.

import (
	"crypto/sha256"
	"encoding/binary"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// buildBlobFile writes a small valid GLCB to a temp file and returns its
// path and size.
func buildBlobFile(t *testing.T) (string, int64) {
	t.Helper()

	dir := t.TempDir()
	w, err := NewWriter(chunk.NewChunkID(), glid.New(), dir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	now := time.Now()
	for i := range uint32(3) {
		rec := chunk.Record{
			SourceTS: now.Add(-time.Second),
			IngestTS: now,
			WriteTS:  now.Add(time.Duration(i) * time.Millisecond),
			EventID:  chunk.EventID{IngesterID: glid.New(), IngestTS: now, IngestSeq: i + 1},
			Attrs:    chunk.Attributes{"host": "web-1"},
			Raw:      []byte("hardening test record"),
		}
		if err := w.Add(rec); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	path := filepath.Join(dir, "data.glcb")
	f, err := os.Create(path) //nolint:gosec // test temp path
	if err != nil {
		t.Fatalf("create blob: %v", err)
	}
	n, err := w.WriteTo(f)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close blob: %v", err)
	}
	return path, n
}

// patchFile overwrites len(b) bytes at off in the file at path.
func patchFile(t *testing.T, path string, off int64, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0) //nolint:gosec // test temp path
	if err != nil {
		t.Fatalf("open for patch: %v", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatalf("patch at %d: %v", off, err)
	}
}

// readTOCFromPath opens the blob and runs ReadTOC against its real size.
func readTOCFromPath(t *testing.T, path string) (BlobTOC, error) {
	t.Helper()
	f, err := os.Open(path) //nolint:gosec // test temp path
	if err != nil {
		t.Fatalf("open blob: %v", err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("stat blob: %v", err)
	}
	return ReadTOC(f, info.Size())
}

// --- Writer-side range guards ---

// TestEncodeTOCEntry_RangeValidation pins the u32 range guards: values
// outside [0, MaxUint32] return an error (never panic), and both
// boundaries encode successfully.
func TestEncodeTOCEntry_RangeValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		offset  int64
		size    int64
		wantErr bool
	}{
		{"offset over u32", math.MaxUint32 + 1, 10, true},
		{"size over u32", 10, math.MaxUint32 + 1, true},
		{"negative offset", -1, 10, true},
		{"negative size", 10, -1, true},
		{"boundary offset at MaxUint32", math.MaxUint32, 10, false},
		{"boundary size at MaxUint32", 10, math.MaxUint32, false},
		{"zero offset and size", 0, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			buf, err := encodeTOCEntry(TOCEntry{
				Type:    SectionIngestTSIndex,
				Version: 1,
				Offset:  tc.offset,
				Size:    tc.size,
			})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("encodeTOCEntry(offset=%d, size=%d): expected error, got nil", tc.offset, tc.size)
				}
				return
			}
			if err != nil {
				t.Fatalf("encodeTOCEntry(offset=%d, size=%d): %v", tc.offset, tc.size, err)
			}
			if len(buf) != tocEntrySize {
				t.Fatalf("encoded length = %d, want %d", len(buf), tocEntrySize)
			}
		})
	}
}

// TestFinalizeTOC_OversizedEntryReturnsError proves the seal-time error
// path: an entry outside u32 range makes finalizeTOC return an error
// (which propagates through Writer.WriteTo / Finish to the seal caller)
// rather than panicking and taking the node down.
func TestFinalizeTOC_OversizedEntryReturnsError(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("finalizeTOC panicked: %v", r)
		}
	}()

	w, err := NewWriter(chunk.NewChunkID(), glid.New(), t.TempDir())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	cw := &countWriter{w: io.Discard, hash: sha256.New()}
	err = w.finalizeTOC(cw, []TOCEntry{{
		Type:    SectionIngestTSIndex,
		Version: 1,
		Offset:  math.MaxUint32 + 1, // projected blob past 4 GiB
		Size:    12,
	}})
	if err == nil {
		t.Fatal("finalizeTOC with oversized offset: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "outside u32 range") {
		t.Fatalf("finalizeTOC error = %q, want mention of u32 range", err)
	}
}

// --- Reader-side corruption guards ---

// TestReadTOC_ValidBlob is the happy path: a freshly written blob's TOC
// parses and every section lies within the file.
func TestReadTOC_ValidBlob(t *testing.T) {
	t.Parallel()

	path, size := buildBlobFile(t)
	toc, err := readTOCFromPath(t, path)
	if err != nil {
		t.Fatalf("ReadTOC: %v", err)
	}
	if len(toc.Entries) != 3 {
		t.Fatalf("entries = %d, want 3 (layout + ITSI + STSI)", len(toc.Entries))
	}
	for _, e := range toc.Entries {
		if e.Offset+e.Size > size {
			t.Errorf("entry type 0x%02x: [%d, %d) exceeds size %d", e.Type, e.Offset, e.Offset+e.Size, size)
		}
	}
}

// TestReadTOC_TruncatedFooter rejects a blob shorter than the footer.
func TestReadTOC_TruncatedFooter(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "tiny.glcb")
	if err := os.WriteFile(path, make([]byte, tocFooterSize-1), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := readTOCFromPath(t, path); err == nil {
		t.Fatal("ReadTOC on sub-footer blob: expected error, got nil")
	}
}

// TestReadTOC_TruncatedBlob rejects a valid blob whose tail was cut off
// (the bytes at EOF are no longer a footer).
func TestReadTOC_TruncatedBlob(t *testing.T) {
	t.Parallel()

	path, size := buildBlobFile(t)
	if err := os.Truncate(path, size-5); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if _, err := readTOCFromPath(t, path); err == nil {
		t.Fatal("ReadTOC on truncated blob: expected error, got nil")
	}
}

// TestReadTOC_OversizedEntryCount rejects a footer whose entry count
// implies more entry bytes than the file holds.
func TestReadTOC_OversizedEntryCount(t *testing.T) {
	t.Parallel()

	path, size := buildBlobFile(t)
	var countBuf [4]byte
	binary.LittleEndian.PutUint32(countBuf[:], math.MaxUint32)
	patchFile(t, path, size-int64(tocFooterSize), countBuf[:])
	if _, err := readTOCFromPath(t, path); err == nil {
		t.Fatal("ReadTOC with oversized entry count: expected error, got nil")
	}
}

// TestReadTOC_OutOfRangeSection rejects an entry whose Offset+Size runs
// past EOF, on all three read paths (ReadTOC, LoadSection, MapSection).
// Before the guard, MapSection/LoadSection would mmap past EOF and the
// first access would SIGBUS.
func TestReadTOC_OutOfRangeSection(t *testing.T) {
	t.Parallel()

	path, size := buildBlobFile(t)

	// Locate the ITSI entry in on-disk TOC order, then patch its size
	// field (u32 at entry offset +6) to MaxUint32 so Offset+Size sails
	// past EOF.
	toc, err := readTOCFromPath(t, path)
	if err != nil {
		t.Fatalf("ReadTOC before patch: %v", err)
	}
	itsiIdx := -1
	for i, e := range toc.Entries {
		if e.Type == SectionIngestTSIndex {
			itsiIdx = i
		}
	}
	if itsiIdx < 0 {
		t.Fatal("no ITSI entry in fresh blob TOC")
	}
	entriesStart := size - int64(tocFooterSize) - int64(len(toc.Entries))*int64(tocEntrySize)
	itsiEntryOff := entriesStart + int64(itsiIdx)*int64(tocEntrySize)
	var sizeBuf [4]byte
	binary.LittleEndian.PutUint32(sizeBuf[:], math.MaxUint32)
	patchFile(t, path, itsiEntryOff+6, sizeBuf[:])

	if _, err := readTOCFromPath(t, path); err == nil {
		t.Fatal("ReadTOC with out-of-range section: expected error, got nil")
	} else if !strings.Contains(err.Error(), "exceeds blob size") {
		t.Fatalf("ReadTOC error = %q, want mention of exceeding blob size", err)
	}

	if _, err := LoadSection(path, SectionIngestTSIndex,
		func(data []byte) (int, error) { return len(data), nil }); err == nil {
		t.Fatal("LoadSection with out-of-range section: expected error, got nil")
	}

	if data, closer, err := MapSection(path, SectionIngestTSIndex); err == nil {
		_ = data[0] // would SIGBUS without the guard
		_ = closer()
		t.Fatal("MapSection with out-of-range section: expected error, got nil")
	}
}

// TestReadTOC_SectionBoundary pins the exact boundary: a section ending
// exactly at EOF is accepted; one byte past EOF is rejected. Uses a
// synthetic tail (payload + 1 entry + footer) built with the writer's
// own encoders.
func TestReadTOC_SectionBoundary(t *testing.T) {
	t.Parallel()

	const payloadLen = 100
	// fileSize = payload + 1 entry + footer.
	fileSize := int64(payloadLen + tocEntrySize + tocFooterSize)

	writeSynthetic := func(t *testing.T, entry TOCEntry) string {
		t.Helper()
		enc, err := encodeTOCEntry(entry)
		if err != nil {
			t.Fatalf("encodeTOCEntry: %v", err)
		}
		buf := make([]byte, 0, fileSize)
		buf = append(buf, make([]byte, payloadLen)...)
		buf = append(buf, enc...)
		buf = append(buf, encodeTOCFooter(1, [32]byte{})...)
		path := filepath.Join(t.TempDir(), "synthetic.glcb")
		if err := os.WriteFile(path, buf, 0o600); err != nil {
			t.Fatalf("write synthetic blob: %v", err)
		}
		return path
	}

	t.Run("section ends exactly at EOF", func(t *testing.T) {
		t.Parallel()
		path := writeSynthetic(t, TOCEntry{
			Type:    SectionIngestTSIndex,
			Version: 1,
			Offset:  fileSize - payloadLen,
			Size:    payloadLen, // Offset+Size == fileSize
		})
		toc, err := readTOCFromPath(t, path)
		if err != nil {
			t.Fatalf("ReadTOC at boundary: %v", err)
		}
		if len(toc.Entries) != 1 {
			t.Fatalf("entries = %d, want 1", len(toc.Entries))
		}
	})

	t.Run("section ends one byte past EOF", func(t *testing.T) {
		t.Parallel()
		path := writeSynthetic(t, TOCEntry{
			Type:    SectionIngestTSIndex,
			Version: 1,
			Offset:  fileSize - payloadLen + 1,
			Size:    payloadLen, // Offset+Size == fileSize+1
		})
		if _, err := readTOCFromPath(t, path); err == nil {
			t.Fatal("ReadTOC one byte past EOF: expected error, got nil")
		}
	})
}
