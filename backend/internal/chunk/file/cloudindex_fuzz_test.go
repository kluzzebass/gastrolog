package file

import (
	"encoding/binary"
	"gastrolog/internal/glid"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// FuzzCloudMetaRoundTrip verifies that encodeCloudMeta/decodeCloudMeta
// round-trip correctly for arbitrary field values.
func FuzzCloudMetaRoundTrip(f *testing.F) {
	// Seed: 9 int64s (timestamps/counts) + 1 byte (flags) + 4 int64s (offsets) = 13*8 + 1 = 105 bytes.
	seed := make([]byte, 105)
	f.Add(seed)

	// Seed with some realistic-ish values.
	realistic := make([]byte, 105)
	now := time.Now().UnixNano()
	binary.LittleEndian.PutUint64(realistic[0:8], uint64(now))
	binary.LittleEndian.PutUint64(realistic[8:16], uint64(now+int64(time.Hour)))
	binary.LittleEndian.PutUint64(realistic[16:24], 1000)  // recordCount
	binary.LittleEndian.PutUint64(realistic[24:32], 65536) // bytes
	binary.LittleEndian.PutUint64(realistic[32:40], 32768) // diskBytes
	realistic[104] = 0x03                                  // sealed + compressed
	f.Add(realistic)

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 105 {
			return
		}

		// Parse 13 int64 fields from the fuzz data.
		readI64 := func(off int) int64 {
			return int64(binary.LittleEndian.Uint64(data[off : off+8]))
		}

		flags := data[104]

		m := &chunkMeta{
			id:              chunk.ChunkID(glid.GLID{}), // zero ID for round-trip
			writeStart:      time.Unix(0, readI64(0)),
			writeEnd:        time.Unix(0, readI64(8)),
			recordCount:     readI64(16),
			bytes:           readI64(24),
			diskBytes:       readI64(32),
			ingestStart:     time.Unix(0, readI64(40)),
			ingestEnd:       time.Unix(0, readI64(48)),
			sourceStart:     time.Unix(0, readI64(56)),
			sourceEnd:       time.Unix(0, readI64(64)),
			sealed:          flags&0x01 != 0,
			compressed:      flags&0x02 != 0,
			ingestIdxOffset: readI64(72),
			ingestIdxSize:   readI64(80),
			sourceIdxOffset: readI64(88),
			sourceIdxSize:   readI64(96),
		}

		id := m.id
		encoded := encodeCloudMeta(m)
		decoded := decodeCloudMeta(id, encoded)

		// Verify all fields round-trip.
		if !decoded.writeStart.Equal(m.writeStart) {
			t.Fatalf("writeStart: got %v, want %v", decoded.writeStart, m.writeStart)
		}
		if !decoded.writeEnd.Equal(m.writeEnd) {
			t.Fatalf("writeEnd: got %v, want %v", decoded.writeEnd, m.writeEnd)
		}
		if decoded.recordCount != m.recordCount {
			t.Fatalf("recordCount: got %d, want %d", decoded.recordCount, m.recordCount)
		}
		if decoded.bytes != m.bytes {
			t.Fatalf("bytes: got %d, want %d", decoded.bytes, m.bytes)
		}
		if decoded.diskBytes != m.diskBytes {
			t.Fatalf("diskBytes: got %d, want %d", decoded.diskBytes, m.diskBytes)
		}
		if !decoded.ingestStart.Equal(m.ingestStart) {
			t.Fatalf("ingestStart: got %v, want %v", decoded.ingestStart, m.ingestStart)
		}
		if !decoded.ingestEnd.Equal(m.ingestEnd) {
			t.Fatalf("ingestEnd: got %v, want %v", decoded.ingestEnd, m.ingestEnd)
		}
		if !decoded.sourceStart.Equal(m.sourceStart) {
			t.Fatalf("sourceStart: got %v, want %v", decoded.sourceStart, m.sourceStart)
		}
		if !decoded.sourceEnd.Equal(m.sourceEnd) {
			t.Fatalf("sourceEnd: got %v, want %v", decoded.sourceEnd, m.sourceEnd)
		}
		if decoded.sealed != m.sealed {
			t.Fatalf("sealed: got %v, want %v", decoded.sealed, m.sealed)
		}
		if decoded.compressed != m.compressed {
			t.Fatalf("compressed: got %v, want %v", decoded.compressed, m.compressed)
		}
		if !decoded.cloudBacked {
			t.Fatalf("cloudBacked: got false, want true")
		}
		if decoded.ingestIdxOffset != m.ingestIdxOffset {
			t.Fatalf("ingestIdxOffset: got %d, want %d", decoded.ingestIdxOffset, m.ingestIdxOffset)
		}
		if decoded.ingestIdxSize != m.ingestIdxSize {
			t.Fatalf("ingestIdxSize: got %d, want %d", decoded.ingestIdxSize, m.ingestIdxSize)
		}
		if decoded.sourceIdxOffset != m.sourceIdxOffset {
			t.Fatalf("sourceIdxOffset: got %d, want %d", decoded.sourceIdxOffset, m.sourceIdxOffset)
		}
		if decoded.sourceIdxSize != m.sourceIdxSize {
			t.Fatalf("sourceIdxSize: got %d, want %d", decoded.sourceIdxSize, m.sourceIdxSize)
		}
		if decoded.id != id {
			t.Fatalf("id: got %v, want %v", decoded.id, id)
		}
	})
}
