package cloud_test

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"

	"gastrolog/internal/chunk/cloud"
)

// TestLoadSection_ReadsITSIAndSTSI verifies that LoadSection finds the
// embedded TS index sections by type byte, mmaps a window covering the
// section, and hands the decoder bytes that match the section's
// recorded SHA-256 in the TOC.
func TestLoadSection_ReadsITSIAndSTSI(t *testing.T) {
	t.Parallel()

	chunkID, vaultID, records := testRecords()
	tmp := writeBlobToTempFile(t, chunkID, vaultID, records)
	defer func() { _ = tmp.Close() }()

	// Read the TOC tail directly so we don't go through NewReader (whose
	// default Close removes the temp file).
	stat, err := tmp.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	tailLen := int64(2)*42 + 44 // 2 entries × 42 bytes + footer
	tail := make([]byte, tailLen)
	if _, err := tmp.ReadAt(tail, stat.Size()-tailLen); err != nil {
		t.Fatalf("read TOC tail: %v", err)
	}
	parsed, err := cloud.ParseTOC(tail)
	if err != nil {
		t.Fatalf("ParseTOC: %v", err)
	}

	for _, ty := range []byte{cloud.SectionIngestTSIndex, cloud.SectionSourceTSIndex} {
		gotHash, err := cloud.LoadSection(tmp.Name(), ty,
			func(data []byte) ([32]byte, error) { return sha256.Sum256(data), nil })
		if err != nil {
			t.Fatalf("LoadSection 0x%02x: %v", ty, err)
		}

		// Re-read the same byte range via the file handle and compare hashes.
		// LoadSection's correctness reduces to "the bytes you got match the
		// bytes that live at TOC.Find(ty).{Offset,Size}".
		entry, ok := parsed.Find(ty)
		if !ok {
			t.Fatalf("section 0x%02x: TOC entry missing", ty)
		}
		off, size := entry.Offset, entry.Size
		if size <= 0 {
			t.Fatalf("section 0x%02x has size %d in meta", ty, size)
		}
		buf := make([]byte, size)
		if _, err := tmp.ReadAt(buf, off); err != nil {
			t.Fatalf("ReadAt section 0x%02x: %v", ty, err)
		}
		if sha256.Sum256(buf) != gotHash {
			t.Errorf("section 0x%02x: LoadSection bytes hash != ReadAt bytes hash", ty)
		}

		// Sanity: each TS index entry is [tsNano:i64][pos:u32] = 12 bytes.
		// recordCount is small; we just check the size is a multiple.
		if size%12 != 0 {
			t.Errorf("section 0x%02x size %d not a multiple of 12 (TS entry size)", ty, size)
		}
		// Decode the first entry to ensure the bytes are sensible.
		if size >= 12 {
			ts := int64(binary.LittleEndian.Uint64(buf[0:8])) //nolint:gosec // round-trip
			if ts <= 0 {
				t.Errorf("section 0x%02x: first ts = %d, want positive", ty, ts)
			}
		}
	}
}

// TestLoadSection_NotFound verifies the typed error for a section type
// that exists in format.Type but isn't present in this blob's TOC.
func TestLoadSection_NotFound(t *testing.T) {
	t.Parallel()

	chunkID, vaultID, records := testRecords()
	tmp := writeBlobToTempFile(t, chunkID, vaultID, records)
	defer func() { _ = tmp.Close() }()

	// Token index isn't emitted by the writer yet — its TOC entry doesn't
	// exist in this blob, so LoadSection must report ErrSectionNotFound.
	_, err := cloud.LoadSection(tmp.Name(), cloud.SectionTokenIndex,
		func(data []byte) (int, error) { return len(data), nil })
	if !errors.Is(err, cloud.ErrSectionNotFound) {
		t.Fatalf("LoadSection: err = %v, want ErrSectionNotFound", err)
	}
}
