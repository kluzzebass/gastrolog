package segment_test

// gastrolog-1vepg0: the segment record checksum must be content-sensitive.
// The previous rolling CRC32 consumed lenPrefix ++ body ++ bodyCRC per frame,
// and CRC(M ++ CRC(M)) cancels the content contribution by CRC linearity —
// the checksum pinned only frame-length structure (count, lengths,
// truncation), so a same-length content substitution with a fixed-up frame
// CRC passed segment.Open, publish, and collection's published-checksum
// verify. These tests pin content sensitivity.

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

// fixedRecord is sampleRecord with caller-pinned identity and raw payload so
// two segments can share exact frame geometry while differing in content.
func fixedRecord(ingester, node glid.GLID, seq uint32, raw string) *record.Record {
	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	return &record.Record{
		SourceTS: ts.Add(-time.Second),
		IngestTS: ts,
		EventID: record.EventID{
			IngesterID: ingester,
			NodeID:     node,
			IngestTS:   ts,
			IngestSeq:  seq,
		},
		Attrs: record.Attributes{"env": "prod"},
		Raw:   []byte(raw),
	}
}

func buildFinalizedSegment(t *testing.T, path string, recs ...*record.Record) segment.Header {
	t.Helper()
	sf, err := segment.Create(path, segment.Meta{ID: glid.New(), VaultID: glid.New()})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	writeTS := time.Date(2024, 6, 1, 12, 1, 0, 0, time.UTC)
	for _, rec := range recs {
		if err := sf.Append(rec, writeTS); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := sf.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	hdr := sf.Header()
	if err := sf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return hdr
}

// tamperFirstFrameSameLength substitutes content inside the first record
// frame WITHOUT changing any frame length, and fixes up the frame's embedded
// CRC32 so the frame stays internally valid — the shape of a holder serving
// corrupted-in-place or substituted bytes with matching frame geometry.
func tamperFirstFrameSameLength(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	bodyStart := segment.HeaderSize + 4
	bodyLen := int(binary.LittleEndian.Uint32(data[segment.HeaderSize:]))
	if bodyStart+bodyLen > len(data) {
		t.Fatalf("frame body out of range: start %d len %d file %d", bodyStart, bodyLen, len(data))
	}
	body := data[bodyStart : bodyStart+bodyLen]
	// Flip the last raw payload byte (just before the 4-byte embedded CRC).
	body[bodyLen-5] ^= 0xFF
	// Recompute the embedded frame CRC over the substituted body.
	binary.LittleEndian.PutUint32(body[bodyLen-4:], crc32.ChecksumIEEE(body[:bodyLen-4]))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

// TestSegmentChecksumIsContentSensitive: two segments with identical frame
// geometry (same EventIDs, same lengths) but different record bytes MUST
// publish different record checksums.
func TestSegmentChecksumIsContentSensitive(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ingester, node := glid.New(), glid.New()

	hdrA := buildFinalizedSegment(t, filepath.Join(dir, "a"),
		fixedRecord(ingester, node, 0, "payload-one"),
		fixedRecord(ingester, node, 1, "payload-two"))
	hdrB := buildFinalizedSegment(t, filepath.Join(dir, "b"),
		fixedRecord(ingester, node, 0, "payload-0ne"),
		fixedRecord(ingester, node, 1, "payload-tw0"))

	if hdrA.SegmentChecksum == hdrB.SegmentChecksum {
		t.Fatalf("content-blind checksum: same-geometry segments with different record bytes share checksum %x", hdrA.SegmentChecksum)
	}
}

// TestOpenRejectsEqualLengthFrameReorder: swapping two equal-length frames
// preserves frame geometry and every embedded frame CRC, but must still fail
// the bulk verify — the content-blind checksum was reorder-blind too.
func TestOpenRejectsEqualLengthFrameReorder(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg")
	ingester, node := glid.New(), glid.New()
	buildFinalizedSegment(t, path,
		fixedRecord(ingester, node, 0, "frame-alpha"),
		fixedRecord(ingester, node, 1, "frame-bravo"))

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	firstLen := 4 + int(binary.LittleEndian.Uint32(data[segment.HeaderSize:]))
	secondStart := segment.HeaderSize + firstLen
	secondLen := 4 + int(binary.LittleEndian.Uint32(data[secondStart:]))
	if firstLen != secondLen {
		t.Fatalf("test needs equal-length frames: %d vs %d", firstLen, secondLen)
	}
	first := append([]byte(nil), data[segment.HeaderSize:secondStart]...)
	copy(data[segment.HeaderSize:], data[secondStart:secondStart+secondLen])
	copy(data[secondStart:], first)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	sf, err := segment.Open(path)
	if err == nil {
		_ = sf.Close()
		t.Fatal("segment.Open accepted an equal-length frame reorder")
	}
}

// TestOpenRejectsSameLengthContentSubstitution: substituted record bytes with
// matching frame geometry and a fixed-up frame CRC must fail segment.Open's
// bulk verify against the header checksum.
func TestOpenRejectsSameLengthContentSubstitution(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg")
	ingester, node := glid.New(), glid.New()
	buildFinalizedSegment(t, path,
		fixedRecord(ingester, node, 0, "authentic bytes"),
		fixedRecord(ingester, node, 1, "more authentic bytes"))

	tamperFirstFrameSameLength(t, path)

	sf, err := segment.Open(path)
	if err == nil {
		_ = sf.Close()
		t.Fatal("segment.Open accepted a same-length content substitution")
	}
}
