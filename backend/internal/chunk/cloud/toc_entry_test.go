package cloud

import (
	"bytes"
	"encoding/binary"
	"testing"

	"gastrolog/internal/format"
)

// TestTOCEntryBinaryLayout pins the on-disk encoding of a TOCEntry so a
// future field reorder cannot silently corrupt blobs already on disk.
//
// Layout (42 bytes):
//
//	[type:u8][version:u8][offset:u32][size:u32][hash:32]
func TestTOCEntryBinaryLayout(t *testing.T) {
	t.Parallel()

	var hash [32]byte
	for i := range hash {
		hash[i] = byte(0xA0 + i)
	}
	e := TOCEntry{
		Type:    format.TypeIngestIndex,
		Version: 0x07,
		Offset:  0x11223344,
		Size:    0x55667788,
		Hash:    hash,
	}

	got := encodeTOCEntry(e)
	if len(got) != tocEntrySize {
		t.Fatalf("encoded length = %d, want %d", len(got), tocEntrySize)
	}
	if got[0] != format.TypeIngestIndex {
		t.Errorf("byte 0 = 0x%02x, want 0x%02x", got[0], format.TypeIngestIndex)
	}
	if got[1] != 0x07 {
		t.Errorf("version byte = 0x%02x, want 0x07", got[1])
	}
	if v := binary.LittleEndian.Uint32(got[2:6]); v != 0x11223344 {
		t.Errorf("offset field = 0x%x", v)
	}
	if v := binary.LittleEndian.Uint32(got[6:10]); v != 0x55667788 {
		t.Errorf("size field = 0x%x", v)
	}
	if !bytes.Equal(got[10:42], hash[:]) {
		t.Errorf("hash field mismatch")
	}
}

// TestTOCEntryRoundTripAllSectionTypes encodes one TOCEntry per declared
// section type and verifies parseTOCRegion decodes the type byte back
// correctly. Guards against accidental type-byte aliasing across the
// section constants.
func TestTOCEntryRoundTripAllSectionTypes(t *testing.T) {
	t.Parallel()

	types := []byte{
		SectionIngestTSIndex, SectionSourceTSIndex,
		SectionTokenIndex, SectionJSONIndex,
		SectionKVKeyIndex, SectionKVValueIndex, SectionKVKVIndex,
		SectionAttrKeyIndex, SectionAttrValueIndex, SectionAttrKVIndex,
	}
	seen := make(map[byte]bool)
	for _, ty := range types {
		if seen[ty] {
			t.Fatalf("section type 0x%02x declared twice", ty)
		}
		seen[ty] = true
	}

	var entryBuf bytes.Buffer
	for i, ty := range types {
		var hash [32]byte
		hash[0] = byte(i)
		entryBuf.Write(encodeTOCEntry(TOCEntry{
			Type:    ty,
			Version: 1,
			Offset:  int64(i) * 100,
			Size:    int64(i + 1),
			Hash:    hash,
		}))
	}
	footer := encodeTOCFooter(uint32(len(types)), [32]byte{})

	toc, err := parseTOCRegion(entryBuf.Bytes(), footer)
	if err != nil {
		t.Fatalf("parseTOCRegion: %v", err)
	}
	if len(toc.Entries) != len(types) {
		t.Fatalf("entries = %d, want %d", len(toc.Entries), len(types))
	}
	for i, ty := range types {
		e, ok := toc.Find(ty)
		if !ok {
			t.Errorf("Find(0x%02x) = !ok", ty)
			continue
		}
		if e.Type != ty {
			t.Errorf("entry type = 0x%02x, want 0x%02x", e.Type, ty)
		}
		if e.Offset != int64(i)*100 {
			t.Errorf("entry %d offset = %d, want %d", i, e.Offset, i*100)
		}
	}
}
