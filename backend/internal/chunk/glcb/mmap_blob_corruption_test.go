package glcb

// GLCB mmap-open corruption tests (gastrolog-5do8sh): deterministic
// patched/truncated copies of a valid blob exercise every rejection
// branch in parseMappedBlob (size guard, preamble validation, dict and
// record-index range checks), the ParseTOC declared-entry-count guard,
// and the TOC section-range asymmetry between ParseTOC and ReadTOC.
//
// Layout-meta byte offsets below come from encodeBlobLayoutMeta
// (layout.go): the 128-byte meta block starts at preambleSize (4), so a
// field at meta offset N sits at absolute offset preambleSize+N.

import (
	"encoding/binary"
	"errors"
	"math"
	"os"
	"strings"
	"testing"

	"gastrolog/internal/format"
)

// Absolute file offsets of the layout-meta fields patched below, derived
// from encodeBlobLayoutMeta (layout.go).
const (
	dictSizeFileOff  = preambleSize + 88  // DictSize:  meta[88:92]
	indexSizeFileOff = preambleSize + 108 // IndexSize: meta[108:112]
)

// TestOpenMappedBlob_TooSmall truncates a valid blob below the fixed
// prefix + TOC footer minimum so parseMappedBlob rejects it before any
// field decode.
func TestOpenMappedBlob_TooSmall(t *testing.T) {
	t.Parallel()

	path, _ := buildBlobFile(t)
	if err := os.Truncate(path, int64(headerSize+tocFooterSize-1)); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	_, err := OpenMappedBlob(path)
	if err == nil {
		t.Fatal("OpenMappedBlob on sub-minimum blob: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "GLCB too small") {
		t.Fatalf("error = %q, want mention of GLCB too small", err)
	}
}

// TestOpenMappedBlob_CorruptPreamble corrupts each preamble byte that
// DecodeAndValidate checks (signature, type, version) and pins the
// matching format sentinel wrapped in the "GLCB preamble" error.
func TestOpenMappedBlob_CorruptPreamble(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		off  int64
		b    byte
		want error
	}{
		{"bad signature", 0, 'X', format.ErrSignatureMismatch},
		{"bad type", 1, 'z', format.ErrTypeMismatch},
		{"bad version", 2, 0x7f, format.ErrVersionMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path, _ := buildBlobFile(t)
			patchFile(t, path, tc.off, []byte{tc.b})
			_, err := OpenMappedBlob(path)
			if err == nil {
				t.Fatal("OpenMappedBlob with corrupt preamble: expected error, got nil")
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if !strings.Contains(err.Error(), "GLCB preamble") {
				t.Fatalf("error = %q, want mention of GLCB preamble", err)
			}
		})
	}
}

// TestOpenMappedBlob_DictOutOfRange patches the layout meta's DictSize to
// MaxUint32 so DictOff+DictSize sails past the mapping.
func TestOpenMappedBlob_DictOutOfRange(t *testing.T) {
	t.Parallel()

	path, _ := buildBlobFile(t)
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], math.MaxUint32)
	patchFile(t, path, dictSizeFileOff, buf[:])
	_, err := OpenMappedBlob(path)
	if err == nil {
		t.Fatal("OpenMappedBlob with out-of-range dict: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "dict out of range") {
		t.Fatalf("error = %q, want mention of dict out of range", err)
	}
}

// TestOpenMappedBlob_RecordIndexOutOfRange patches the layout meta's
// IndexSize to MaxUint32 (dict left intact so the dict check passes)
// so IndexOff+IndexSize sails past the mapping.
func TestOpenMappedBlob_RecordIndexOutOfRange(t *testing.T) {
	t.Parallel()

	path, _ := buildBlobFile(t)
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], math.MaxUint32)
	patchFile(t, path, indexSizeFileOff, buf[:])
	_, err := OpenMappedBlob(path)
	if err == nil {
		t.Fatal("OpenMappedBlob with out-of-range record index: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "record index out of range") {
		t.Fatalf("error = %q, want mention of record index out of range", err)
	}
}

// TestOpenMappedBlob_TOCEntryPastEOF pins CURRENT behavior for a TOC
// entry whose Offset+Size runs past EOF: ParseTOC (the mmap open path)
// does NOT validate section ranges, so OpenMappedBlob succeeds and the
// out-of-range section degrades to a Section() miss via the mmap bounds
// guard (mmap_blob.go Section). ReadTOC on the same bytes rejects the
// blob — that asymmetry between the two TOC parse paths is a follow-up
// candidate; this test only documents it, it must not change ParseTOC.
func TestOpenMappedBlob_TOCEntryPastEOF(t *testing.T) {
	t.Parallel()

	path, size := buildBlobFile(t)

	// Locate the ITSI entry in on-disk TOC order, then patch its size
	// field (u32 at entry offset +6) to MaxUint32 so Offset+Size sails
	// past EOF. Same technique as TestReadTOC_OutOfRangeSection.
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

	// The fd path (ReadTOC) validates section ranges and rejects the blob.
	if _, err := readTOCFromPath(t, path); err == nil {
		t.Fatal("ReadTOC with out-of-range section: expected error, got nil")
	}

	// The mmap path (ParseTOC) does not: open succeeds.
	blob, err := OpenMappedBlob(path)
	if err != nil {
		t.Fatalf("OpenMappedBlob with out-of-range TOC entry: %v (current behavior is to succeed)", err)
	}
	defer func() { _ = blob.Close() }()

	// The out-of-range section degrades to a lookup miss...
	if data, ok := blob.Section(SectionIngestTSIndex); ok || data != nil {
		t.Fatalf("Section(ITSI) = (%d bytes, %v), want (nil, false)", len(data), ok)
	}
	// ...while an in-range section from the same TOC still resolves.
	if data, ok := blob.Section(SectionBlobLayout); !ok || len(data) != layoutMetaSize {
		t.Fatalf("Section(layout) = (%d bytes, %v), want (%d bytes, true)", len(data), ok, layoutMetaSize)
	}
}

// TestParseTOC_BufferTooSmallForEntryCount hands ParseTOC a footer-only
// tail whose declared entry count implies entry bytes the buffer does not
// hold, pinning the deterministic guard behind the fuzz coverage.
func TestParseTOC_BufferTooSmallForEntryCount(t *testing.T) {
	t.Parallel()

	// A valid 44-byte footer declaring 1 entry, with zero entry bytes
	// preceding it.
	buf := encodeTOCFooter(1, [32]byte{})
	_, err := ParseTOC(buf)
	if err == nil {
		t.Fatal("ParseTOC with missing entry bytes: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "too small for declared entry count") {
		t.Fatalf("error = %q, want mention of declared entry count", err)
	}
}
