package file

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

// =============================================================================
// Full Record Round-Trip Tests with Attributes
// =============================================================================

func TestRecordRoundTripWithAttributes(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	testCases := []struct {
		name  string
		attrs chunk.Attributes
		raw   []byte
	}{
		{
			name:  "single_attr",
			attrs: chunk.Attributes{"host": "server-001"},
			raw:   []byte("log message 1"),
		},
		{
			name:  "multiple_attrs",
			attrs: chunk.Attributes{"host": "server-001", "env": "prod", "service": "api"},
			raw:   []byte("log message 2"),
		},
		{
			name:  "empty_attrs",
			attrs: chunk.Attributes{},
			raw:   []byte("log message 3"),
		},
		{
			name:  "unicode_attrs",
			attrs: chunk.Attributes{"location": "东京", "team": "日本語チーム"},
			raw:   []byte("unicode log: こんにちは"),
		},
		{
			name:  "long_values",
			attrs: chunk.Attributes{"long_key": strings.Repeat("v", 1000)},
			raw:   bytes.Repeat([]byte("x"), 5000),
		},
		{
			name:  "special_chars",
			attrs: chunk.Attributes{"path": "/var/log/app.log", "query": "a=1&b=2"},
			raw:   []byte("special: \t\n\r"),
		},
	}

	var chunkID chunk.ChunkID
	recordRefs := make([]chunk.RecordRef, len(testCases))

	// Append all records
	for i, tc := range testCases {
		rec := chunk.Record{
			IngestTS: time.UnixMicro(int64((i + 1) * 1000)),
			Attrs:    tc.attrs,
			Raw:      tc.raw,
		}
		id, pos, err := manager.Append(rec)
		if err != nil {
			t.Fatalf("%s: append: %v", tc.name, err)
		}
		chunkID = id
		recordRefs[i] = chunk.RecordRef{ChunkID: id, Pos: pos}
	}

	// Read back from unsealed chunk (stdio reader)
	t.Run("unsealed_forward", func(t *testing.T) {
		cursor, err := manager.OpenCursor(chunkID)
		if err != nil {
			t.Fatalf("open cursor: %v", err)
		}
		defer cursor.Close()

		for i, tc := range testCases {
			got, ref, err := cursor.Next()
			if err != nil {
				t.Fatalf("%s: next: %v", tc.name, err)
			}

			verifyRecord(t, tc.name, got, tc.attrs, tc.raw)

			if ref.Pos != recordRefs[i].Pos {
				t.Fatalf("%s: pos mismatch: want %d, got %d", tc.name, recordRefs[i].Pos, ref.Pos)
			}
		}

		if _, _, err := cursor.Next(); err != chunk.ErrNoMoreRecords {
			t.Fatalf("expected ErrNoMoreRecords, got %v", err)
		}
	})

	// Read backward from unsealed chunk
	t.Run("unsealed_backward", func(t *testing.T) {
		cursor, err := manager.OpenCursor(chunkID)
		if err != nil {
			t.Fatalf("open cursor: %v", err)
		}
		defer cursor.Close()

		for i := len(testCases) - 1; i >= 0; i-- {
			tc := testCases[i]
			got, _, err := cursor.Prev()
			if err != nil {
				t.Fatalf("%s: prev: %v", tc.name, err)
			}
			verifyRecord(t, tc.name, got, tc.attrs, tc.raw)
		}

		if _, _, err := cursor.Prev(); err != chunk.ErrNoMoreRecords {
			t.Fatalf("expected ErrNoMoreRecords, got %v", err)
		}
	})

	// Seal the chunk
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Read back from sealed chunk (mmap reader)
	t.Run("sealed_forward", func(t *testing.T) {
		cursor, err := manager.OpenCursor(chunkID)
		if err != nil {
			t.Fatalf("open cursor: %v", err)
		}
		defer cursor.Close()

		for _, tc := range testCases {
			got, _, err := cursor.Next()
			if err != nil {
				t.Fatalf("%s: next: %v", tc.name, err)
			}
			verifyRecord(t, tc.name, got, tc.attrs, tc.raw)
		}
	})

	// Read backward from sealed chunk
	t.Run("sealed_backward", func(t *testing.T) {
		cursor, err := manager.OpenCursor(chunkID)
		if err != nil {
			t.Fatalf("open cursor: %v", err)
		}
		defer cursor.Close()

		for i := len(testCases) - 1; i >= 0; i-- {
			tc := testCases[i]
			got, _, err := cursor.Prev()
			if err != nil {
				t.Fatalf("%s: prev: %v", tc.name, err)
			}
			verifyRecord(t, tc.name, got, tc.attrs, tc.raw)
		}
	})

	// Test seeking
	t.Run("seek", func(t *testing.T) {
		cursor, err := manager.OpenCursor(chunkID)
		if err != nil {
			t.Fatalf("open cursor: %v", err)
		}
		defer cursor.Close()

		// Seek to middle record
		midIdx := len(testCases) / 2
		if err := cursor.Seek(recordRefs[midIdx]); err != nil {
			t.Fatalf("seek: %v", err)
		}

		got, _, err := cursor.Next()
		if err != nil {
			t.Fatalf("next after seek: %v", err)
		}
		verifyRecord(t, testCases[midIdx].name, got, testCases[midIdx].attrs, testCases[midIdx].raw)
	})
}

func verifyRecord(t *testing.T, name string, got chunk.Record, wantAttrs chunk.Attributes, wantRaw []byte) {
	t.Helper()

	if len(got.Attrs) != len(wantAttrs) {
		t.Fatalf("%s: attrs count: want %d, got %d", name, len(wantAttrs), len(got.Attrs))
	}

	for k, v := range wantAttrs {
		gotV, ok := got.Attrs[k]
		if !ok {
			t.Fatalf("%s: missing attr key %q", name, k)
		}
		if gotV != v {
			t.Fatalf("%s: attr %q: want %q, got %q", name, k, v, gotV)
		}
	}

	if !bytes.Equal(got.Raw, wantRaw) {
		t.Fatalf("%s: raw mismatch: want %q, got %q", name, wantRaw, got.Raw)
	}
}

// =============================================================================
// attr.log File Format Tests
// =============================================================================

func TestAttrLogFileFormat(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	attrs := chunk.Attributes{"key": "value"}
	rec := chunk.Record{
		IngestTS: time.UnixMicro(1000),
		Attrs:    attrs,
		Raw:      []byte("test"),
	}

	chunkID, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Read the attr.log file directly
	attrPath := filepath.Join(dir, chunkID.String(), attrLogFileName)
	data, err := os.ReadFile(attrPath)
	if err != nil {
		t.Fatalf("read attr.log: %v", err)
	}

	// Verify header
	if len(data) < format.HeaderSize {
		t.Fatalf("attr.log too small: %d bytes", len(data))
	}

	header, err := format.Decode(data[:format.HeaderSize])
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}

	if header.Type != format.TypeAttrLog {
		t.Fatalf("header type: want %c, got %c", format.TypeAttrLog, header.Type)
	}

	if header.Version != AttrLogVersion {
		t.Fatalf("header version: want %d, got %d", AttrLogVersion, header.Version)
	}

	// Verify sealed flag is set
	if header.Flags&format.FlagSealed == 0 {
		t.Fatal("sealed flag not set in attr.log header")
	}

	// Verify we can decode the attributes from the data section
	attrData := data[format.HeaderSize:]
	decoded, err := chunk.DecodeAttributes(attrData)
	if err != nil {
		t.Fatalf("decode attrs from file: %v", err)
	}

	if decoded["key"] != "value" {
		t.Fatalf("decoded attrs: want key=value, got key=%q", decoded["key"])
	}
}

func TestAttrLogMultipleRecords(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	records := []chunk.Record{
		{IngestTS: time.UnixMicro(1000), Attrs: chunk.Attributes{"a": "1"}, Raw: []byte("r1")},
		{IngestTS: time.UnixMicro(2000), Attrs: chunk.Attributes{"b": "2", "c": "3"}, Raw: []byte("r2")},
		{IngestTS: time.UnixMicro(3000), Attrs: chunk.Attributes{}, Raw: []byte("r3")},
		{IngestTS: time.UnixMicro(4000), Attrs: chunk.Attributes{"d": strings.Repeat("x", 100)}, Raw: []byte("r4")},
	}

	var chunkID chunk.ChunkID
	for _, rec := range records {
		id, _, err := manager.Append(rec)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		chunkID = id
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Verify via cursor
	cursor, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()

	for i, expected := range records {
		got, _, err := cursor.Next()
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}

		if len(got.Attrs) != len(expected.Attrs) {
			t.Fatalf("record %d: attrs count mismatch: want %d, got %d", i, len(expected.Attrs), len(got.Attrs))
		}

		for k, v := range expected.Attrs {
			if got.Attrs[k] != v {
				t.Fatalf("record %d: attr %q mismatch", i, k)
			}
		}
	}
}

// =============================================================================
// idx.log Entry Verification Tests
// =============================================================================

func TestIdxLogEntryContainsAttrOffsets(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	records := []chunk.Record{
		{IngestTS: time.UnixMicro(1000), Attrs: chunk.Attributes{"a": "1"}, Raw: []byte("raw1")},
		{IngestTS: time.UnixMicro(2000), Attrs: chunk.Attributes{"bb": "22"}, Raw: []byte("raw2")},
	}

	var chunkID chunk.ChunkID
	for _, rec := range records {
		id, _, err := manager.Append(rec)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		chunkID = id
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Read idx.log directly
	idxPath := filepath.Join(dir, chunkID.String(), idxLogFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read idx.log: %v", err)
	}

	// Skip header and decode entries
	if len(data) < IdxHeaderSize+2*IdxEntrySize {
		t.Fatalf("idx.log too small: %d bytes", len(data))
	}

	entry0 := DecodeIdxEntry(data[IdxHeaderSize:])
	entry1 := DecodeIdxEntry(data[IdxHeaderSize+IdxEntrySize:])

	// Entry 0 should have AttrOffset 0
	if entry0.AttrOffset != 0 {
		t.Fatalf("entry 0 AttrOffset: want 0, got %d", entry0.AttrOffset)
	}

	// Entry 0's AttrSize should be the encoded size of {"a": "1"}
	enc0, _ := records[0].Attrs.Encode()
	if entry0.AttrSize != uint16(len(enc0)) {
		t.Fatalf("entry 0 AttrSize: want %d, got %d", len(enc0), entry0.AttrSize)
	}

	// Entry 1's AttrOffset should be entry0.AttrSize
	if entry1.AttrOffset != uint32(entry0.AttrSize) {
		t.Fatalf("entry 1 AttrOffset: want %d, got %d", entry0.AttrSize, entry1.AttrOffset)
	}

	// Verify raw offsets too
	if entry0.RawOffset != 0 {
		t.Fatalf("entry 0 RawOffset: want 0, got %d", entry0.RawOffset)
	}
	if entry0.RawSize != uint32(len(records[0].Raw)) {
		t.Fatalf("entry 0 RawSize: want %d, got %d", len(records[0].Raw), entry0.RawSize)
	}
}

// =============================================================================
// Persistence Tests - Close and Reopen
// =============================================================================

func TestAttributesPersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	attrs := chunk.Attributes{
		"host":    "server-001.example.com",
		"env":     "production",
		"service": "api-gateway",
	}
	raw := []byte("important log message")

	var chunkID chunk.ChunkID

	// First manager instance - write data
	{
		manager, err := NewManager(Config{Dir: dir})
		if err != nil {
			t.Fatalf("new manager: %v", err)
		}

		rec := chunk.Record{
			IngestTS: time.UnixMicro(1234567890),
			Attrs:    attrs,
			Raw:      raw,
		}

		id, _, err := manager.Append(rec)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		chunkID = id

		if err := manager.Seal(); err != nil {
			t.Fatalf("seal: %v", err)
		}

		if err := manager.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}

	// Second manager instance - read data back
	{
		manager, err := NewManager(Config{Dir: dir})
		if err != nil {
			t.Fatalf("reopen manager: %v", err)
		}
		defer manager.Close()

		cursor, err := manager.OpenCursor(chunkID)
		if err != nil {
			t.Fatalf("open cursor: %v", err)
		}
		defer cursor.Close()

		got, _, err := cursor.Next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}

		// Verify all attributes survived the restart
		for k, v := range attrs {
			if got.Attrs[k] != v {
				t.Fatalf("attr %q: want %q, got %q", k, v, got.Attrs[k])
			}
		}

		if !bytes.Equal(got.Raw, raw) {
			t.Fatalf("raw mismatch")
		}

		if got.IngestTS.UnixMicro() != 1234567890 {
			t.Fatalf("IngestTS: want 1234567890, got %d", got.IngestTS.UnixMicro())
		}
	}
}

func TestMultipleChunksWithAttributes(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	type testRecord struct {
		attrs chunk.Attributes
		raw   []byte
	}

	chunk1Records := []testRecord{
		{chunk.Attributes{"chunk": "1", "idx": "0"}, []byte("c1r0")},
		{chunk.Attributes{"chunk": "1", "idx": "1"}, []byte("c1r1")},
	}

	chunk2Records := []testRecord{
		{chunk.Attributes{"chunk": "2", "idx": "0"}, []byte("c2r0")},
		{chunk.Attributes{"chunk": "2", "idx": "1"}, []byte("c2r1")},
		{chunk.Attributes{"chunk": "2", "idx": "2"}, []byte("c2r2")},
	}

	// Write chunk 1
	var chunk1ID chunk.ChunkID
	for i, tr := range chunk1Records {
		rec := chunk.Record{
			IngestTS: time.UnixMicro(int64(1000 + i)),
			Attrs:    tr.attrs,
			Raw:      tr.raw,
		}
		id, _, err := manager.Append(rec)
		if err != nil {
			t.Fatalf("append chunk1 %d: %v", i, err)
		}
		chunk1ID = id
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal chunk1: %v", err)
	}

	// Write chunk 2
	var chunk2ID chunk.ChunkID
	for i, tr := range chunk2Records {
		rec := chunk.Record{
			IngestTS: time.UnixMicro(int64(2000 + i)),
			Attrs:    tr.attrs,
			Raw:      tr.raw,
		}
		id, _, err := manager.Append(rec)
		if err != nil {
			t.Fatalf("append chunk2 %d: %v", i, err)
		}
		chunk2ID = id
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal chunk2: %v", err)
	}

	// Verify chunk 1
	cursor1, err := manager.OpenCursor(chunk1ID)
	if err != nil {
		t.Fatalf("open cursor chunk1: %v", err)
	}
	for i, tr := range chunk1Records {
		got, _, err := cursor1.Next()
		if err != nil {
			t.Fatalf("chunk1 record %d: %v", i, err)
		}
		if got.Attrs["chunk"] != "1" {
			t.Fatalf("chunk1 record %d: wrong chunk attr", i)
		}
		if !bytes.Equal(got.Raw, tr.raw) {
			t.Fatalf("chunk1 record %d: raw mismatch", i)
		}
	}
	cursor1.Close()

	// Verify chunk 2
	cursor2, err := manager.OpenCursor(chunk2ID)
	if err != nil {
		t.Fatalf("open cursor chunk2: %v", err)
	}
	for i, tr := range chunk2Records {
		got, _, err := cursor2.Next()
		if err != nil {
			t.Fatalf("chunk2 record %d: %v", i, err)
		}
		if got.Attrs["chunk"] != "2" {
			t.Fatalf("chunk2 record %d: wrong chunk attr", i)
		}
		if !bytes.Equal(got.Raw, tr.raw) {
			t.Fatalf("chunk2 record %d: raw mismatch", i)
		}
	}
	cursor2.Close()
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

func TestConcurrentReadWithAttributes(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	// Write some records
	numRecords := 100
	var chunkID chunk.ChunkID
	for i := range numRecords {
		rec := chunk.Record{
			IngestTS: time.UnixMicro(int64(i * 1000)),
			Attrs:    chunk.Attributes{"idx": string(rune('0' + i%10))},
			Raw:      []byte(strings.Repeat("x", i+1)),
		}
		id, _, err := manager.Append(rec)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		chunkID = id
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Concurrent readers
	numReaders := 10
	errCh := make(chan error, numReaders)

	for r := range numReaders {
		go func(readerID int) {
			cursor, err := manager.OpenCursor(chunkID)
			if err != nil {
				errCh <- err
				return
			}
			defer cursor.Close()

			count := 0
			for {
				rec, _, err := cursor.Next()
				if err == chunk.ErrNoMoreRecords {
					break
				}
				if err != nil {
					errCh <- err
					return
				}

				// Verify the record has expected attributes
				if _, ok := rec.Attrs["idx"]; !ok {
					errCh <- err
					return
				}

				count++
			}

			if count != numRecords {
				errCh <- err
				return
			}

			errCh <- nil
		}(r)
	}

	for range numReaders {
		if err := <-errCh; err != nil {
			t.Fatalf("reader error: %v", err)
		}
	}
}

// =============================================================================
// Edge Cases
// =============================================================================

func TestEmptyAttributesRoundTrip(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	rec := chunk.Record{
		IngestTS: time.UnixMicro(1000),
		Attrs:    chunk.Attributes{}, // empty
		Raw:      []byte("data"),
	}

	chunkID, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	cursor, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()

	got, _, err := cursor.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}

	if len(got.Attrs) != 0 {
		t.Fatalf("expected empty attrs, got %d entries", len(got.Attrs))
	}
}

func TestNilAttributesRoundTrip(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	rec := chunk.Record{
		IngestTS: time.UnixMicro(1000),
		Attrs:    nil, // nil
		Raw:      []byte("data"),
	}

	chunkID, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	cursor, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()

	got, _, err := cursor.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}

	// nil attrs should come back as empty map
	if got.Attrs == nil {
		t.Fatal("expected non-nil attrs (empty map)")
	}
	if len(got.Attrs) != 0 {
		t.Fatalf("expected empty attrs, got %d entries", len(got.Attrs))
	}
}

func TestLargeAttributesRoundTrip(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	// Create attrs near the 64KB limit
	// 65535 - 2 (count) - 2 (keyLen) - 2 (valLen) = 65529 max for key+value
	attrs := chunk.Attributes{
		"k": strings.Repeat("v", 60000), // Large but under limit
	}

	rec := chunk.Record{
		IngestTS: time.UnixMicro(1000),
		Attrs:    attrs,
		Raw:      []byte("data"),
	}

	chunkID, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	cursor, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()

	got, _, err := cursor.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}

	if got.Attrs["k"] != attrs["k"] {
		t.Fatal("large attrs value mismatch")
	}
}

func TestBinaryDataInAttributes(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	// Binary data including null bytes
	attrs := chunk.Attributes{
		"binary": string([]byte{0x00, 0x01, 0x02, 0xff, 0xfe}),
		"null":   string([]byte{0x00, 0x00, 0x00}),
	}

	rec := chunk.Record{
		IngestTS: time.UnixMicro(1000),
		Attrs:    attrs,
		Raw:      []byte{0x00, 0x01, 0x02},
	}

	chunkID, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	cursor, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()

	got, _, err := cursor.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}

	if got.Attrs["binary"] != attrs["binary"] {
		t.Fatal("binary attr mismatch")
	}
	if got.Attrs["null"] != attrs["null"] {
		t.Fatal("null attr mismatch")
	}
}

// =============================================================================
// Record Copy Test
// =============================================================================

func TestRecordCopyWithAttributes(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	attrs := chunk.Attributes{"key": "value"}
	raw := []byte("test data")

	rec := chunk.Record{
		IngestTS: time.UnixMicro(1000),
		Attrs:    attrs,
		Raw:      raw,
	}

	chunkID, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	cursor, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}

	got, _, err := cursor.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}

	// Make a copy before closing the cursor
	copied := got.Copy()

	// Close cursor (this would invalidate mmap'd memory)
	cursor.Close()

	// The copy should still be valid
	if copied.Attrs["key"] != "value" {
		t.Fatalf("copy attrs: want 'value', got %q", copied.Attrs["key"])
	}
	if string(copied.Raw) != "test data" {
		t.Fatalf("copy raw: want 'test data', got %q", copied.Raw)
	}

	// Modifying copy shouldn't affect original attrs (which were copied)
	copied.Attrs["key"] = "modified"
	if attrs["key"] != "value" {
		t.Fatal("original attrs was modified")
	}
}

// =============================================================================
// Stress Test
// =============================================================================

func TestManyRecordsWithVariedAttributes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	numRecords := 10000
	var chunkID chunk.ChunkID

	for i := range numRecords {
		attrs := chunk.Attributes{
			"idx":     string(rune('A' + i%26)),
			"service": []string{"api", "web", "worker", "cron"}[i%4],
			"env":     []string{"dev", "staging", "prod"}[i%3],
		}

		if i%10 == 0 {
			attrs["extra"] = strings.Repeat("x", i%100+1)
		}

		rec := chunk.Record{
			IngestTS: time.UnixMicro(int64(i)),
			Attrs:    attrs,
			Raw:      []byte(strings.Repeat("d", i%500+1)),
		}

		id, _, err := manager.Append(rec)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		chunkID = id
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Verify all records
	cursor, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()

	count := 0
	for {
		rec, _, err := cursor.Next()
		if err == chunk.ErrNoMoreRecords {
			break
		}
		if err != nil {
			t.Fatalf("record %d: %v", count, err)
		}

		// Verify basic structure
		if _, ok := rec.Attrs["service"]; !ok {
			t.Fatalf("record %d: missing service attr", count)
		}
		if _, ok := rec.Attrs["env"]; !ok {
			t.Fatalf("record %d: missing env attr", count)
		}

		count++
	}

	if count != numRecords {
		t.Fatalf("record count: want %d, got %d", numRecords, count)
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkAppendWithAttributes(b *testing.B) {
	dir := b.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		b.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	attrs := chunk.Attributes{
		"host":    "server-001.example.com",
		"service": "api-gateway",
		"env":     "production",
	}
	raw := bytes.Repeat([]byte("x"), 200)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := chunk.Record{
			IngestTS: time.UnixMicro(int64(i)),
			Attrs:    attrs,
			Raw:      raw,
		}
		if _, _, err := manager.Append(rec); err != nil {
			b.Fatalf("append: %v", err)
		}
	}
}

func BenchmarkReadWithAttributes(b *testing.B) {
	dir := b.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		b.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	attrs := chunk.Attributes{
		"host":    "server-001.example.com",
		"service": "api-gateway",
		"env":     "production",
	}
	raw := bytes.Repeat([]byte("x"), 200)

	var chunkID chunk.ChunkID
	for i := range 1000 {
		rec := chunk.Record{
			IngestTS: time.UnixMicro(int64(i)),
			Attrs:    attrs,
			Raw:      raw,
		}
		id, _, _ := manager.Append(rec)
		chunkID = id
	}

	if err := manager.Seal(); err != nil {
		b.Fatalf("seal: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor, _ := manager.OpenCursor(chunkID)
		for {
			_, _, err := cursor.Next()
			if err == chunk.ErrNoMoreRecords {
				break
			}
		}
		cursor.Close()
	}
}
