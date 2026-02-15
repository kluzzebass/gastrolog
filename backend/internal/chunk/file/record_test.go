package file

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

// =============================================================================
// IdxEntry Encoding/Decoding Tests
// =============================================================================

func TestIdxEntryEncodeDecode(t *testing.T) {
	entry := IdxEntry{
		SourceTS:   time.Unix(0, 1234567890123455000),
		IngestTS:   time.Unix(0, 1234567890123456000),
		WriteTS:    time.Unix(0, 1234567890123457000),
		RawOffset:  12345,
		RawSize:    67890,
		AttrOffset: 11111,
		AttrSize:   222,
	}

	buf := make([]byte, IdxEntrySize)
	EncodeIdxEntry(entry, buf)

	decoded := DecodeIdxEntry(buf)

	if decoded.SourceTS.UnixNano() != entry.SourceTS.UnixNano() {
		t.Fatalf("SourceTS: want %d, got %d", entry.SourceTS.UnixNano(), decoded.SourceTS.UnixNano())
	}
	if decoded.IngestTS.UnixNano() != entry.IngestTS.UnixNano() {
		t.Fatalf("IngestTS: want %d, got %d", entry.IngestTS.UnixNano(), decoded.IngestTS.UnixNano())
	}
	if decoded.WriteTS.UnixNano() != entry.WriteTS.UnixNano() {
		t.Fatalf("WriteTS: want %d, got %d", entry.WriteTS.UnixNano(), decoded.WriteTS.UnixNano())
	}
	if decoded.RawOffset != entry.RawOffset {
		t.Fatalf("RawOffset: want %d, got %d", entry.RawOffset, decoded.RawOffset)
	}
	if decoded.RawSize != entry.RawSize {
		t.Fatalf("RawSize: want %d, got %d", entry.RawSize, decoded.RawSize)
	}
	if decoded.AttrOffset != entry.AttrOffset {
		t.Fatalf("AttrOffset: want %d, got %d", entry.AttrOffset, decoded.AttrOffset)
	}
	if decoded.AttrSize != entry.AttrSize {
		t.Fatalf("AttrSize: want %d, got %d", entry.AttrSize, decoded.AttrSize)
	}
}

func TestIdxEntryBinaryFormat(t *testing.T) {
	// Test exact binary layout for a known entry.
	// Values are Unix nanoseconds, so we use time.Unix(0, v).
	entry := IdxEntry{
		SourceTS:   time.Unix(0, 0x00d00d00d00d00d0),
		IngestTS:   time.Unix(0, 0x0102030405060708),
		WriteTS:    time.Unix(0, 0x1112131415161718),
		RawOffset:  0x21222324,
		RawSize:    0x31323334,
		AttrOffset: 0x41424344,
		AttrSize:   0x5152,
	}

	buf := make([]byte, IdxEntrySize)
	EncodeIdxEntry(entry, buf)

	// Verify each field at its expected offset
	// SourceTS at offset 0, 8 bytes
	sourceTS := binary.LittleEndian.Uint64(buf[0:8])
	if sourceTS != 0x00d00d00d00d00d0 {
		t.Fatalf("SourceTS at wrong offset or encoding: %x", sourceTS)
	}

	// IngestTS at offset 8, 8 bytes
	ingestTS := binary.LittleEndian.Uint64(buf[8:16])
	if ingestTS != 0x0102030405060708 {
		t.Fatalf("IngestTS at wrong offset or encoding: %x", ingestTS)
	}

	// WriteTS at offset 16, 8 bytes
	writeTS := binary.LittleEndian.Uint64(buf[16:24])
	if writeTS != 0x1112131415161718 {
		t.Fatalf("WriteTS at wrong offset or encoding: %x", writeTS)
	}

	// RawOffset at offset 24, 4 bytes
	rawOffset := binary.LittleEndian.Uint32(buf[24:28])
	if rawOffset != 0x21222324 {
		t.Fatalf("RawOffset at wrong offset or encoding: %x", rawOffset)
	}

	// RawSize at offset 28, 4 bytes
	rawSize := binary.LittleEndian.Uint32(buf[28:32])
	if rawSize != 0x31323334 {
		t.Fatalf("RawSize at wrong offset or encoding: %x", rawSize)
	}

	// AttrOffset at offset 32, 4 bytes
	attrOffset := binary.LittleEndian.Uint32(buf[32:36])
	if attrOffset != 0x41424344 {
		t.Fatalf("AttrOffset at wrong offset or encoding: %x", attrOffset)
	}

	// AttrSize at offset 36, 2 bytes
	attrSize := binary.LittleEndian.Uint16(buf[36:38])
	if attrSize != 0x5152 {
		t.Fatalf("AttrSize at wrong offset or encoding: %x", attrSize)
	}
}

func TestIdxEntrySize(t *testing.T) {
	// Verify the constant matches expected layout:
	// 8 (SourceTS) + 8 (IngestTS) + 8 (WriteTS) + 4 (RawOffset) + 4 (RawSize) + 4 (AttrOffset) + 2 (AttrSize) = 38
	if IdxEntrySize != 38 {
		t.Fatalf("IdxEntrySize should be 38, got %d", IdxEntrySize)
	}
}

func TestIdxEntryEncodeDecodeNano(t *testing.T) {
	// Nano precision round-trip.
	entry := IdxEntry{
		SourceTS:   time.Unix(0, 1234567890123456789),
		IngestTS:   time.Unix(0, 1234567890123456790),
		WriteTS:    time.Unix(0, 1234567890123456791),
		RawOffset:  1,
		RawSize:    2,
		AttrOffset: 3,
		AttrSize:   4,
	}

	buf := make([]byte, IdxEntrySize)
	EncodeIdxEntry(entry, buf)

	decoded := DecodeIdxEntry(buf)

	if decoded.SourceTS.UnixNano() != entry.SourceTS.UnixNano() {
		t.Fatalf("SourceTS: want %d, got %d", entry.SourceTS.UnixNano(), decoded.SourceTS.UnixNano())
	}
	if decoded.IngestTS.UnixNano() != entry.IngestTS.UnixNano() {
		t.Fatalf("IngestTS: want %d, got %d", entry.IngestTS.UnixNano(), decoded.IngestTS.UnixNano())
	}
	if decoded.WriteTS.UnixNano() != entry.WriteTS.UnixNano() {
		t.Fatalf("WriteTS: want %d, got %d", entry.WriteTS.UnixNano(), decoded.WriteTS.UnixNano())
	}
}

func TestIdxEntryZeroValues(t *testing.T) {
	entry := IdxEntry{}

	buf := make([]byte, IdxEntrySize)
	EncodeIdxEntry(entry, buf)

	decoded := DecodeIdxEntry(buf)

	// Zero time.Time in Go is year 1, not Unix epoch.
	// The encoding stores UnixNano(), so we verify round-trip correctness.
	if decoded.SourceTS.UnixNano() != entry.SourceTS.UnixNano() {
		t.Fatalf("Zero SourceTS round-trip failed: want %v, got %v", entry.SourceTS, decoded.SourceTS)
	}
	if decoded.IngestTS.UnixNano() != entry.IngestTS.UnixNano() {
		t.Fatalf("Zero IngestTS round-trip failed: want %v, got %v", entry.IngestTS, decoded.IngestTS)
	}
	if decoded.WriteTS.UnixNano() != entry.WriteTS.UnixNano() {
		t.Fatalf("Zero WriteTS round-trip failed: want %v, got %v", entry.WriteTS, decoded.WriteTS)
	}
	if decoded.RawOffset != 0 {
		t.Fatalf("Zero RawOffset: want 0, got %d", decoded.RawOffset)
	}
	if decoded.RawSize != 0 {
		t.Fatalf("Zero RawSize: want 0, got %d", decoded.RawSize)
	}
	if decoded.AttrOffset != 0 {
		t.Fatalf("Zero AttrOffset: want 0, got %d", decoded.AttrOffset)
	}
	if decoded.AttrSize != 0 {
		t.Fatalf("Zero AttrSize: want 0, got %d", decoded.AttrSize)
	}
}

func TestIdxEntryMaxValues(t *testing.T) {
	entry := IdxEntry{
		SourceTS:   time.Unix(0, 1<<63-1), // max positive int64
		IngestTS:   time.Unix(0, 1<<63-1),
		WriteTS:    time.Unix(0, 1<<63-1),
		RawOffset:  0xFFFFFFFF, // max uint32
		RawSize:    0xFFFFFFFF,
		AttrOffset: 0xFFFFFFFF,
		AttrSize:   0xFFFF, // max uint16
	}

	buf := make([]byte, IdxEntrySize)
	EncodeIdxEntry(entry, buf)

	decoded := DecodeIdxEntry(buf)

	if decoded.RawOffset != 0xFFFFFFFF {
		t.Fatalf("Max RawOffset: want %d, got %d", uint32(0xFFFFFFFF), decoded.RawOffset)
	}
	if decoded.AttrSize != 0xFFFF {
		t.Fatalf("Max AttrSize: want %d, got %d", uint16(0xFFFF), decoded.AttrSize)
	}
}

// =============================================================================
// IdxFileOffset and RecordCount Tests
// =============================================================================

func TestIdxFileOffsetCalculation(t *testing.T) {
	testCases := []struct {
		recordIndex uint64
		expected    int64
	}{
		{0, int64(IdxHeaderSize)},                             // First record: header only
		{1, int64(IdxHeaderSize) + int64(IdxEntrySize)},       // Second record
		{10, int64(IdxHeaderSize) + 10*int64(IdxEntrySize)},   // 11th record
		{100, int64(IdxHeaderSize) + 100*int64(IdxEntrySize)}, // 101st record
	}

	for _, tc := range testCases {
		offset := IdxFileOffset(tc.recordIndex)
		if offset != tc.expected {
			t.Fatalf("IdxFileOffset(%d): want %d, got %d", tc.recordIndex, tc.expected, offset)
		}
	}
}

func TestRecordCountCalculation(t *testing.T) {
	testCases := []struct {
		fileSize int64
		expected uint64
	}{
		{0, 0},                    // Empty file
		{int64(IdxHeaderSize), 0}, // Header only
		{int64(IdxHeaderSize) + int64(IdxEntrySize), 1},       // One record
		{int64(IdxHeaderSize) + 2*int64(IdxEntrySize), 2},     // Two records
		{int64(IdxHeaderSize) + 100*int64(IdxEntrySize), 100}, // 100 records
		{int64(IdxHeaderSize) + int64(IdxEntrySize) + 10, 1},  // Partial entry ignored
		{int64(IdxHeaderSize) - 1, 0},                         // Less than header
	}

	for _, tc := range testCases {
		count := RecordCount(tc.fileSize)
		if count != tc.expected {
			t.Fatalf("RecordCount(%d): want %d, got %d", tc.fileSize, tc.expected, count)
		}
	}
}

func TestRawDataOffset(t *testing.T) {
	offset := RawDataOffset()
	if offset != int64(format.HeaderSize) {
		t.Fatalf("RawDataOffset: want %d, got %d", format.HeaderSize, offset)
	}
}

// =============================================================================
// BuildRecord Tests
// =============================================================================

func TestBuildRecord(t *testing.T) {
	entry := IdxEntry{
		IngestTS:   time.UnixMicro(1000),
		WriteTS:    time.UnixMicro(2000),
		RawOffset:  100,
		RawSize:    50,
		AttrOffset: 200,
		AttrSize:   30,
	}
	raw := []byte("test data")
	attrs := chunk.Attributes{"key": "value"}

	rec := BuildRecord(entry, raw, attrs)

	if rec.IngestTS.UnixMicro() != 1000 {
		t.Fatalf("IngestTS: want 1000, got %d", rec.IngestTS.UnixMicro())
	}
	if rec.WriteTS.UnixMicro() != 2000 {
		t.Fatalf("WriteTS: want 2000, got %d", rec.WriteTS.UnixMicro())
	}
	if string(rec.Raw) != "test data" {
		t.Fatalf("Raw: want 'test data', got %q", rec.Raw)
	}
	if rec.Attrs["key"] != "value" {
		t.Fatalf("Attrs: want value, got %q", rec.Attrs["key"])
	}

	// Verify no copy (same underlying slice)
	raw[0] = 'X'
	if rec.Raw[0] != 'X' {
		t.Fatal("BuildRecord should not copy raw slice")
	}
}

func TestBuildRecordCopy(t *testing.T) {
	entry := IdxEntry{
		IngestTS:   time.UnixMicro(1000),
		WriteTS:    time.UnixMicro(2000),
		RawOffset:  100,
		RawSize:    50,
		AttrOffset: 200,
		AttrSize:   30,
	}
	raw := []byte("test data")
	attrs := chunk.Attributes{"key": "value"}

	rec := BuildRecordCopy(entry, raw, attrs)

	if rec.IngestTS.UnixMicro() != 1000 {
		t.Fatalf("IngestTS: want 1000, got %d", rec.IngestTS.UnixMicro())
	}
	if string(rec.Raw) != "test data" {
		t.Fatalf("Raw: want 'test data', got %q", rec.Raw)
	}

	// Verify copy was made (modifying original doesn't affect record)
	raw[0] = 'X'
	if rec.Raw[0] == 'X' {
		t.Fatal("BuildRecordCopy should copy raw slice")
	}

	attrs["key"] = "modified"
	if rec.Attrs["key"] != "value" {
		t.Fatal("BuildRecordCopy should copy attrs")
	}
}

// =============================================================================
// Constants Tests
// =============================================================================

func TestFileVersionConstants(t *testing.T) {
	// Verify version constants are set correctly
	if RawLogVersion != 0x01 {
		t.Fatalf("RawLogVersion: want 0x01, got 0x%02x", RawLogVersion)
	}
	if IdxLogVersion != 0x01 {
		t.Fatalf("IdxLogVersion: want 0x01, got 0x%02x", IdxLogVersion)
	}
	if AttrLogVersion != 0x01 {
		t.Fatalf("AttrLogVersion: want 0x01, got 0x%02x", AttrLogVersion)
	}
	// IdxHeaderSize should be 12 (4 byte header + 8 byte createdAt)
	if IdxHeaderSize != 12 {
		t.Fatalf("IdxHeaderSize: want 12, got %d", IdxHeaderSize)
	}
}

func TestMaxSizeConstants(t *testing.T) {
	// Both raw.log and attr.log use uint32 offsets, so max size is 2^32-1
	expectedMax := uint64(1<<32 - 1)

	if MaxRawLogSize != expectedMax {
		t.Fatalf("MaxRawLogSize: want %d, got %d", expectedMax, MaxRawLogSize)
	}
	if MaxAttrLogSize != expectedMax {
		t.Fatalf("MaxAttrLogSize: want %d, got %d", expectedMax, MaxAttrLogSize)
	}
}

// =============================================================================
// Multiple Entry Round-Trip Tests
// =============================================================================

func TestMultipleEntriesRoundTrip(t *testing.T) {
	entries := []IdxEntry{
		{
			IngestTS:   time.Unix(0, 1000),
			WriteTS:    time.Unix(0, 1001),
			RawOffset:  0,
			RawSize:    100,
			AttrOffset: 0,
			AttrSize:   10,
		},
		{
			IngestTS:   time.Unix(0, 2000),
			WriteTS:    time.Unix(0, 2001),
			RawOffset:  100,
			RawSize:    200,
			AttrOffset: 10,
			AttrSize:   20,
		},
		{
			IngestTS:   time.Unix(0, 3000),
			WriteTS:    time.Unix(0, 3001),
			RawOffset:  300,
			RawSize:    300,
			AttrOffset: 30,
			AttrSize:   30,
		},
	}

	// Encode all entries into a buffer
	buf := make([]byte, len(entries)*IdxEntrySize)
	for i, entry := range entries {
		EncodeIdxEntry(entry, buf[i*IdxEntrySize:])
	}

	// Decode and verify
	for i, expected := range entries {
		decoded := DecodeIdxEntry(buf[i*IdxEntrySize:])

		if decoded.IngestTS.UnixNano() != expected.IngestTS.UnixNano() {
			t.Fatalf("Entry %d IngestTS mismatch", i)
		}
		if decoded.WriteTS.UnixNano() != expected.WriteTS.UnixNano() {
			t.Fatalf("Entry %d WriteTS mismatch", i)
		}
		if decoded.RawOffset != expected.RawOffset {
			t.Fatalf("Entry %d RawOffset mismatch", i)
		}
		if decoded.RawSize != expected.RawSize {
			t.Fatalf("Entry %d RawSize mismatch", i)
		}
		if decoded.AttrOffset != expected.AttrOffset {
			t.Fatalf("Entry %d AttrOffset mismatch", i)
		}
		if decoded.AttrSize != expected.AttrSize {
			t.Fatalf("Entry %d AttrSize mismatch", i)
		}
	}
}

// =============================================================================
// Timestamp Precision Tests
// =============================================================================

func TestTimestampNanosecondPrecision(t *testing.T) {
	// Test that nanosecond precision is preserved
	testTimes := []time.Time{
		time.Date(2025, 1, 15, 10, 30, 45, 123456789, time.UTC),
		time.Date(2025, 1, 15, 10, 30, 45, 999999999, time.UTC),
		time.Date(2025, 1, 15, 10, 30, 45, 1, time.UTC),
		time.Date(2025, 1, 15, 10, 30, 45, 0, time.UTC),
	}

	for _, ts := range testTimes {
		entry := IdxEntry{
			IngestTS: ts,
			WriteTS:  ts,
		}

		buf := make([]byte, IdxEntrySize)
		EncodeIdxEntry(entry, buf)
		decoded := DecodeIdxEntry(buf)

		if decoded.IngestTS.UnixNano() != ts.UnixNano() {
			t.Fatalf("Timestamp %v: want %d nanos, got %d", ts, ts.UnixNano(), decoded.IngestTS.UnixNano())
		}
	}
}

func TestNegativeTimestamp(t *testing.T) {
	// Test timestamps before Unix epoch (negative nanoseconds)
	ts := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)
	entry := IdxEntry{
		IngestTS: ts,
		WriteTS:  ts,
	}

	buf := make([]byte, IdxEntrySize)
	EncodeIdxEntry(entry, buf)
	decoded := DecodeIdxEntry(buf)

	if decoded.IngestTS.UnixNano() != ts.UnixNano() {
		t.Fatalf("Negative timestamp: want %d, got %d", ts.UnixNano(), decoded.IngestTS.UnixNano())
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkEncodeIdxEntry(b *testing.B) {
	entry := IdxEntry{
		IngestTS:   time.Unix(0, 1234567890123456000),
		WriteTS:    time.Unix(0, 1234567890123457000),
		RawOffset:  12345,
		RawSize:    67890,
		AttrOffset: 11111,
		AttrSize:   222,
	}
	buf := make([]byte, IdxEntrySize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeIdxEntry(entry, buf)
	}
}

func BenchmarkDecodeIdxEntry(b *testing.B) {
	entry := IdxEntry{
		IngestTS:   time.Unix(0, 1234567890123456000),
		WriteTS:    time.Unix(0, 1234567890123457000),
		RawOffset:  12345,
		RawSize:    67890,
		AttrOffset: 11111,
		AttrSize:   222,
	}
	buf := make([]byte, IdxEntrySize)
	EncodeIdxEntry(entry, buf)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DecodeIdxEntry(buf)
	}
}

// =============================================================================
// Fuzzing
// =============================================================================

func FuzzIdxEntryRoundTrip(f *testing.F) {
	// Seed with various values
	f.Add(int64(0), int64(0), uint32(0), uint32(0), uint32(0), uint16(0))
	f.Add(int64(1234567890), int64(1234567891), uint32(1000), uint32(500), uint32(100), uint16(50))
	f.Add(int64(-1000000), int64(-999999), uint32(0xFFFFFFFF), uint32(0xFFFFFFFF), uint32(0xFFFFFFFF), uint16(0xFFFF))

	f.Fuzz(func(t *testing.T, ingestNano, writeNano int64, rawOff, rawSize, attrOff uint32, attrSize uint16) {
		entry := IdxEntry{
			IngestTS:   time.Unix(0, ingestNano),
			WriteTS:    time.Unix(0, writeNano),
			RawOffset:  rawOff,
			RawSize:    rawSize,
			AttrOffset: attrOff,
			AttrSize:   attrSize,
		}

		buf := make([]byte, IdxEntrySize)
		EncodeIdxEntry(entry, buf)
		decoded := DecodeIdxEntry(buf)

		if decoded.IngestTS.UnixNano() != ingestNano {
			t.Fatalf("IngestTS mismatch")
		}
		if decoded.WriteTS.UnixNano() != writeNano {
			t.Fatalf("WriteTS mismatch")
		}
		if decoded.RawOffset != rawOff {
			t.Fatalf("RawOffset mismatch")
		}
		if decoded.RawSize != rawSize {
			t.Fatalf("RawSize mismatch")
		}
		if decoded.AttrOffset != attrOff {
			t.Fatalf("AttrOffset mismatch")
		}
		if decoded.AttrSize != attrSize {
			t.Fatalf("AttrSize mismatch")
		}
	})
}

// =============================================================================
// Offset Calculation Consistency Tests
// =============================================================================

func TestOffsetCalculationConsistency(t *testing.T) {
	// Verify that offset constants match the actual byte positions
	if idxSourceTSOffset != 0 {
		t.Fatalf("idxSourceTSOffset should be 0, got %d", idxSourceTSOffset)
	}
	if idxIngestTSOffset != 8 {
		t.Fatalf("idxIngestTSOffset should be 8, got %d", idxIngestTSOffset)
	}
	if idxWriteTSOffset != 16 {
		t.Fatalf("idxWriteTSOffset should be 16, got %d", idxWriteTSOffset)
	}
	if idxRawOffsetOffset != 24 {
		t.Fatalf("idxRawOffsetOffset should be 24, got %d", idxRawOffsetOffset)
	}
	if idxRawSizeOffset != 28 {
		t.Fatalf("idxRawSizeOffset should be 28, got %d", idxRawSizeOffset)
	}
	if idxAttrOffsetOffset != 32 {
		t.Fatalf("idxAttrOffsetOffset should be 32, got %d", idxAttrOffsetOffset)
	}
	if idxAttrSizeOffset != 36 {
		t.Fatalf("idxAttrSizeOffset should be 36, got %d", idxAttrSizeOffset)
	}

	// Verify total size matches
	totalFromOffsets := idxAttrSizeOffset + 2 // last field offset + field size
	if totalFromOffsets != IdxEntrySize {
		t.Fatalf("Offset calculation: %d != IdxEntrySize %d", totalFromOffsets, IdxEntrySize)
	}
}

// =============================================================================
// Edge Case: Empty/Nil Data
// =============================================================================

func TestBuildRecordEmptyRaw(t *testing.T) {
	entry := IdxEntry{
		IngestTS: time.UnixMicro(1000),
		WriteTS:  time.UnixMicro(2000),
	}

	rec := BuildRecord(entry, []byte{}, chunk.Attributes{})

	if len(rec.Raw) != 0 {
		t.Fatalf("Expected empty raw, got %d bytes", len(rec.Raw))
	}
	if len(rec.Attrs) != 0 {
		t.Fatalf("Expected empty attrs, got %d entries", len(rec.Attrs))
	}
}

func TestBuildRecordNilData(t *testing.T) {
	entry := IdxEntry{
		IngestTS: time.UnixMicro(1000),
		WriteTS:  time.UnixMicro(2000),
	}

	rec := BuildRecord(entry, nil, nil)

	if rec.Raw != nil {
		t.Fatal("Expected nil raw")
	}
	if rec.Attrs != nil {
		t.Fatal("Expected nil attrs")
	}
}

func TestBuildRecordCopyNilData(t *testing.T) {
	entry := IdxEntry{
		IngestTS: time.UnixMicro(1000),
		WriteTS:  time.UnixMicro(2000),
	}

	rec := BuildRecordCopy(entry, nil, nil)

	// Copy of nil creates empty slice/map
	if rec.Raw == nil {
		t.Fatal("BuildRecordCopy should create empty slice for nil raw")
	}
	if len(rec.Raw) != 0 {
		t.Fatalf("Expected empty raw, got %d bytes", len(rec.Raw))
	}
	// nil attrs copy returns nil
	if rec.Attrs != nil {
		t.Fatal("BuildRecordCopy should return nil for nil attrs")
	}
}

// =============================================================================
// Buffer Reuse Tests
// =============================================================================

func TestEncodeIdxEntryBufferReuse(t *testing.T) {
	// Test that encoding into a reused buffer works correctly
	buf := make([]byte, IdxEntrySize)

	entry1 := IdxEntry{
		IngestTS:   time.Unix(0, 1000),
		RawOffset:  100,
		AttrOffset: 10,
		AttrSize:   5,
	}
	EncodeIdxEntry(entry1, buf)
	decoded1 := DecodeIdxEntry(buf)

	entry2 := IdxEntry{
		IngestTS:   time.Unix(0, 2000),
		RawOffset:  200,
		AttrOffset: 20,
		AttrSize:   10,
	}
	EncodeIdxEntry(entry2, buf)
	decoded2 := DecodeIdxEntry(buf)

	// First entry should have been overwritten
	if decoded2.IngestTS.UnixNano() != 2000 {
		t.Fatalf("Buffer reuse: IngestTS should be 2000, got %d", decoded2.IngestTS.UnixNano())
	}
	if decoded2.AttrOffset != 20 {
		t.Fatalf("Buffer reuse: AttrOffset should be 20, got %d", decoded2.AttrOffset)
	}

	// decoded1 should still have old values (it's a copy of the values)
	if decoded1.IngestTS.UnixNano() != 1000 {
		t.Fatalf("decoded1 IngestTS should still be 1000, got %d", decoded1.IngestTS.UnixNano())
	}
}

// Test that decoding from a buffer slice works correctly
func TestDecodeIdxEntryFromSlice(t *testing.T) {
	// Create a larger buffer with multiple entries
	numEntries := 5
	buf := make([]byte, numEntries*IdxEntrySize)

	for i := range numEntries {
		entry := IdxEntry{
			IngestTS:   time.Unix(0, int64(i*1000)),
			WriteTS:    time.Unix(0, int64(i*1000+1)),
			RawOffset:  uint32(i * 100),
			RawSize:    uint32(i * 10),
			AttrOffset: uint32(i * 50),
			AttrSize:   uint16(i * 5),
		}
		EncodeIdxEntry(entry, buf[i*IdxEntrySize:])
	}

	// Decode each entry from its slice
	for i := range numEntries {
		decoded := DecodeIdxEntry(buf[i*IdxEntrySize:])

		if decoded.IngestTS.UnixNano() != int64(i*1000) {
			t.Fatalf("Entry %d: IngestTS want %d, got %d", i, i*1000, decoded.IngestTS.UnixNano())
		}
		if decoded.RawOffset != uint32(i*100) {
			t.Fatalf("Entry %d: RawOffset want %d, got %d", i, i*100, decoded.RawOffset)
		}
		if decoded.AttrSize != uint16(i*5) {
			t.Fatalf("Entry %d: AttrSize want %d, got %d", i, i*5, decoded.AttrSize)
		}
	}
}

// =============================================================================
// Verify Entry Doesn't Reference Buffer
// =============================================================================

func TestDecodeIdxEntryNoBufferReference(t *testing.T) {
	entry := IdxEntry{
		IngestTS:   time.Unix(0, 12345),
		WriteTS:    time.Unix(0, 12346),
		RawOffset:  1000,
		RawSize:    500,
		AttrOffset: 100,
		AttrSize:   50,
	}

	buf := make([]byte, IdxEntrySize)
	EncodeIdxEntry(entry, buf)

	decoded := DecodeIdxEntry(buf)

	// Zero out the buffer
	for i := range buf {
		buf[i] = 0
	}

	// Decoded entry should still have correct values (no reference to buf)
	if decoded.IngestTS.UnixNano() != 12345 {
		t.Fatal("Decoded entry references buffer for IngestTS")
	}
	if decoded.RawOffset != 1000 {
		t.Fatal("Decoded entry references buffer for RawOffset")
	}
	if decoded.AttrSize != 50 {
		t.Fatal("Decoded entry references buffer for AttrSize")
	}
}

// =============================================================================
// Verify bytes package not needed
// =============================================================================

func init() {
	// This test file uses bytes.Equal - verify it's imported
	_ = bytes.Equal
}
