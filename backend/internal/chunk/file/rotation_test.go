package file

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// =============================================================================
// Chunk Rotation Tests
// =============================================================================

func TestRotationOnMaxChunkBytesWithAttributes(t *testing.T) {
	dir := t.TempDir()

	// Small size policy to trigger rotation quickly
	manager, err := NewManager(Config{
		Dir:            dir,
		RotationPolicy: chunk.NewSizePolicy(500), // Very small
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	attrs := chunk.Attributes{"test": "rotation"}
	raw := bytes.Repeat([]byte("x"), 100) // 100 bytes per record

	var lastChunkID chunk.ChunkID
	chunkIDs := make(map[chunk.ChunkID]bool)

	// Append records until we've rotated at least once
	for i := range 20 {
		rec := chunk.Record{
			IngestTS: time.UnixMicro(int64(i * 1000)),
			Attrs:    attrs,
			Raw:      raw,
		}

		id, _, err := manager.Append(rec)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}

		chunkIDs[id] = true
		lastChunkID = id
	}

	// We should have rotated to multiple chunks
	if len(chunkIDs) < 2 {
		t.Fatalf("expected rotation to create multiple chunks, got %d", len(chunkIDs))
	}

	// Seal and verify we can read from all chunks
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	metas, err := manager.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(metas) != len(chunkIDs) {
		t.Fatalf("chunk count mismatch: want %d, got %d", len(chunkIDs), len(metas))
	}

	// Verify we can read all records across all chunks
	totalRecords := 0
	for _, meta := range metas {
		cursor, err := manager.OpenCursor(meta.ID)
		if err != nil {
			t.Fatalf("open cursor %s: %v", meta.ID, err)
		}

		for {
			rec, _, err := cursor.Next()
			if err == chunk.ErrNoMoreRecords {
				break
			}
			if err != nil {
				t.Fatalf("next: %v", err)
			}

			// Verify record integrity
			if rec.Attrs["test"] != "rotation" {
				t.Fatal("attrs mismatch after rotation")
			}
			if !bytes.Equal(rec.Raw, raw) {
				t.Fatal("raw mismatch after rotation")
			}

			totalRecords++
		}
		cursor.Close()
	}

	if totalRecords != 20 {
		t.Fatalf("total records: want 20, got %d", totalRecords)
	}

	_ = lastChunkID // Used to track current chunk
}

func TestRotationPreservesAttributesAcrossChunks(t *testing.T) {
	dir := t.TempDir()

	manager, err := NewManager(Config{
		Dir:            dir,
		RotationPolicy: chunk.NewSizePolicy(200), // Small to force rotation
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	// Different attributes for each record
	records := []struct {
		attrs chunk.Attributes
		raw   []byte
	}{
		{chunk.Attributes{"idx": "0", "special": "first"}, []byte("record 0")},
		{chunk.Attributes{"idx": "1"}, bytes.Repeat([]byte("x"), 50)},
		{chunk.Attributes{"idx": "2", "unicode": "日本語"}, []byte("record 2")},
		{chunk.Attributes{"idx": "3"}, bytes.Repeat([]byte("y"), 100)},
		{chunk.Attributes{"idx": "4", "special": "last"}, []byte("record 4")},
	}

	for i, r := range records {
		rec := chunk.Record{
			IngestTS: time.UnixMicro(int64(i * 1000)),
			Attrs:    r.attrs,
			Raw:      r.raw,
		}
		if _, _, err := manager.Append(rec); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Read all records from all chunks and verify
	metas, err := manager.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	var allRecords []chunk.Record
	for _, meta := range metas {
		cursor, err := manager.OpenCursor(meta.ID)
		if err != nil {
			t.Fatalf("open cursor: %v", err)
		}

		for {
			rec, _, err := cursor.Next()
			if err == chunk.ErrNoMoreRecords {
				break
			}
			if err != nil {
				t.Fatalf("next: %v", err)
			}
			allRecords = append(allRecords, rec.Copy())
		}
		cursor.Close()
	}

	if len(allRecords) != len(records) {
		t.Fatalf("record count: want %d, got %d", len(records), len(allRecords))
	}

	// Verify each record's attributes
	for i, expected := range records {
		got := allRecords[i]

		for k, v := range expected.attrs {
			if got.Attrs[k] != v {
				t.Fatalf("record %d attr %q: want %q, got %q", i, k, v, got.Attrs[k])
			}
		}

		if !bytes.Equal(got.Raw, expected.raw) {
			t.Fatalf("record %d raw mismatch", i)
		}
	}
}

// =============================================================================
// Attribute Size Limit Tests
// =============================================================================

func TestAttributesTooLargeFails(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	// Create attributes that exceed 64KB when dict-encoded.
	// Dict encoding per entry: 2 (keyID) + 2 (valLen) + value.
	// Total: 2 (count) + 4 + valLen > 65535 → valLen > 65529.
	largeAttrs := chunk.Attributes{
		"k": strings.Repeat("v", 65530),
	}

	rec := chunk.Record{
		IngestTS: time.UnixMicro(1000),
		Attrs:    largeAttrs,
		Raw:      []byte("data"),
	}

	_, _, err = manager.Append(rec)
	if err == nil {
		t.Fatal("expected error for oversized attributes")
	}

	// The error should be about attrs being too large
	if err != chunk.ErrAttrsTooLarge {
		t.Fatalf("expected ErrAttrsTooLarge, got %v", err)
	}
}

func TestAttributesNearMaxSize(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	// Create attributes near but under 64KB limit
	// 65535 - 2 (count) - 2 (keyLen) - 2 (valLen) - 1 (key) = 65528 max for value
	nearMaxAttrs := chunk.Attributes{
		"k": strings.Repeat("v", 65000), // Under limit
	}

	rec := chunk.Record{
		IngestTS: time.UnixMicro(1000),
		Attrs:    nearMaxAttrs,
		Raw:      []byte("data"),
	}

	chunkID, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append near-max attrs: %v", err)
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Verify we can read it back
	cursor, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()

	got, _, err := cursor.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}

	if got.Attrs["k"] != nearMaxAttrs["k"] {
		t.Fatal("near-max attrs value mismatch")
	}
}

// =============================================================================
// File Structure Tests
// =============================================================================

func TestAllThreeFilesCreated(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	rec := chunk.Record{
		IngestTS: time.UnixMicro(1000),
		Attrs:    chunk.Attributes{"test": "files"},
		Raw:      []byte("data"),
	}

	chunkID, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	manager.Close()

	// Check all four files exist
	chunkDir := filepath.Join(dir, chunkID.String())

	files := []string{rawLogFileName, idxLogFileName, attrLogFileName, attrDictFileName}
	for _, f := range files {
		path := filepath.Join(chunkDir, f)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("file %s: %v", f, err)
		}
		if info.Size() == 0 {
			t.Fatalf("file %s is empty", f)
		}
	}
}

func TestFileHeadersAreCorrect(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	rec := chunk.Record{
		IngestTS: time.UnixMicro(1000),
		Attrs:    chunk.Attributes{"test": "headers"},
		Raw:      []byte("data"),
	}

	chunkID, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	manager.Close()

	chunkDir := filepath.Join(dir, chunkID.String())

	// Verify raw.log header
	rawData, err := os.ReadFile(filepath.Join(chunkDir, rawLogFileName))
	if err != nil {
		t.Fatalf("read raw.log: %v", err)
	}
	if rawData[0] != 0x69 { // 'i' signature
		t.Fatalf("raw.log signature: want 0x69, got 0x%02x", rawData[0])
	}
	if rawData[1] != 0x72 { // 'r' type
		t.Fatalf("raw.log type: want 0x72, got 0x%02x", rawData[1])
	}

	// Verify idx.log header
	idxData, err := os.ReadFile(filepath.Join(chunkDir, idxLogFileName))
	if err != nil {
		t.Fatalf("read idx.log: %v", err)
	}
	if idxData[0] != 0x69 {
		t.Fatalf("idx.log signature: want 0x69, got 0x%02x", idxData[0])
	}
	if idxData[1] != 0x69 { // 'i' type
		t.Fatalf("idx.log type: want 0x69, got 0x%02x", idxData[1])
	}

	// Verify attr.log header
	attrData, err := os.ReadFile(filepath.Join(chunkDir, attrLogFileName))
	if err != nil {
		t.Fatalf("read attr.log: %v", err)
	}
	if attrData[0] != 0x69 {
		t.Fatalf("attr.log signature: want 0x69, got 0x%02x", attrData[0])
	}
	if attrData[1] != 0x61 { // 'a' type
		t.Fatalf("attr.log type: want 0x61, got 0x%02x", attrData[1])
	}

	// Verify attr_dict.log header
	dictData, err := os.ReadFile(filepath.Join(chunkDir, attrDictFileName))
	if err != nil {
		t.Fatalf("read attr_dict.log: %v", err)
	}
	if dictData[0] != 0x69 {
		t.Fatalf("attr_dict.log signature: want 0x69, got 0x%02x", dictData[0])
	}
	if dictData[1] != 0x64 { // 'd' type
		t.Fatalf("attr_dict.log type: want 0x64, got 0x%02x", dictData[1])
	}

	// All files should have sealed flag set
	for name, data := range map[string][]byte{
		"raw.log":       rawData,
		"idx.log":       idxData,
		"attr.log":      attrData,
		"attr_dict.log": dictData,
	} {
		if data[3]&0x01 == 0 {
			t.Fatalf("%s sealed flag not set", name)
		}
	}
}

// =============================================================================
// Empty Chunk Tests
// =============================================================================

func TestEmptyChunkHasAllFiles(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	// Seal without any records
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	metas, err := manager.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(metas) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(metas))
	}

	manager.Close()

	// Verify all files exist even for empty chunk
	chunkDir := filepath.Join(dir, metas[0].ID.String())

	for _, f := range []string{rawLogFileName, idxLogFileName, attrLogFileName, attrDictFileName} {
		path := filepath.Join(chunkDir, f)
		_, err := os.Stat(path)
		if err != nil {
			t.Fatalf("file %s missing in empty chunk: %v", f, err)
		}
	}
}

// =============================================================================
// Corruption Resistance Tests
// =============================================================================

func TestTruncatedAttrLogDetected(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	rec := chunk.Record{
		IngestTS: time.UnixMicro(1000),
		Attrs:    chunk.Attributes{"key": "value"},
		Raw:      []byte("data"),
	}

	chunkID, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	manager.Close()

	// Truncate attr.log to corrupt it
	attrPath := filepath.Join(dir, chunkID.String(), attrLogFileName)
	f, err := os.OpenFile(attrPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open attr.log: %v", err)
	}
	// Truncate to just the header (4 bytes)
	if err := f.Truncate(4); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	f.Close()

	// Reopen manager
	manager2, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen manager: %v", err)
	}
	defer manager2.Close()

	// Try to read - should fail or return error
	cursor, err := manager2.OpenCursor(chunkID)
	if err != nil {
		// Error on open is acceptable
		return
	}
	defer cursor.Close()

	_, _, err = cursor.Next()
	if err == nil {
		t.Fatal("expected error reading from corrupted attr.log")
	}
}

// =============================================================================
// Interleaved Read/Write Tests
// =============================================================================

func TestReadWhileWriting(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	// Write initial records
	var chunkID chunk.ChunkID
	for i := range 5 {
		rec := chunk.Record{
			IngestTS: time.UnixMicro(int64(i * 1000)),
			Attrs:    chunk.Attributes{"phase": "initial", "idx": string(rune('0' + i))},
			Raw:      []byte("initial"),
		}
		id, _, err := manager.Append(rec)
		if err != nil {
			t.Fatalf("append initial %d: %v", i, err)
		}
		chunkID = id
	}

	// Open cursor while chunk is still active
	cursor, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}

	// Read some records
	for i := range 3 {
		rec, _, err := cursor.Next()
		if err != nil {
			t.Fatalf("next %d: %v", i, err)
		}
		if rec.Attrs["phase"] != "initial" {
			t.Fatalf("record %d: wrong phase", i)
		}
	}

	// Write more records while cursor is open
	for i := 5; i < 10; i++ {
		rec := chunk.Record{
			IngestTS: time.UnixMicro(int64(i * 1000)),
			Attrs:    chunk.Attributes{"phase": "additional", "idx": string(rune('0' + i%10))},
			Raw:      []byte("additional"),
		}
		if _, _, err := manager.Append(rec); err != nil {
			t.Fatalf("append additional %d: %v", i, err)
		}
	}

	// Continue reading - should see remaining records
	// Note: behavior depends on implementation - stdio cursor should see new records
	count := 3 // already read 3
	for {
		_, _, err := cursor.Next()
		if err == chunk.ErrNoMoreRecords {
			break
		}
		if err != nil {
			t.Fatalf("next after write: %v", err)
		}
		count++
	}

	cursor.Close()

	// Total should be at least 5 (initial records)
	// May be more if cursor sees newly written records
	if count < 5 {
		t.Fatalf("expected at least 5 records, got %d", count)
	}
}

// =============================================================================
// Timestamp Ordering Tests
// =============================================================================

func TestWriteTSIsMonotonic(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	// Write records with out-of-order IngestTS
	var chunkID chunk.ChunkID
	ingestTimes := []int64{5000, 1000, 3000, 2000, 4000}

	for _, ts := range ingestTimes {
		rec := chunk.Record{
			IngestTS: time.UnixMicro(ts),
			Attrs:    chunk.Attributes{"ingest_ts": string(rune('0' + ts/1000))},
			Raw:      []byte("data"),
		}
		id, _, err := manager.Append(rec)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		chunkID = id
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Read back and verify WriteTS is monotonically increasing
	cursor, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()

	var lastWriteTS time.Time
	for i := range ingestTimes {
		rec, _, err := cursor.Next()
		if err != nil {
			t.Fatalf("next %d: %v", i, err)
		}

		if i > 0 && !rec.WriteTS.After(lastWriteTS) && rec.WriteTS != lastWriteTS {
			t.Fatalf("record %d: WriteTS not monotonic: %v <= %v", i, rec.WriteTS, lastWriteTS)
		}

		// Verify IngestTS matches what we wrote
		expectedIngest := ingestTimes[i]
		if rec.IngestTS.UnixMicro() != expectedIngest {
			t.Fatalf("record %d: IngestTS mismatch: want %d, got %d", i, expectedIngest, rec.IngestTS.UnixMicro())
		}

		lastWriteTS = rec.WriteTS
	}
}

// =============================================================================
// Reopen After Partial Write Tests
// =============================================================================

func TestReopenUnsealedChunk(t *testing.T) {
	dir := t.TempDir()

	var chunkID chunk.ChunkID

	// First session - write but don't seal
	{
		manager, err := NewManager(Config{Dir: dir})
		if err != nil {
			t.Fatalf("new manager: %v", err)
		}

		for i := range 5 {
			rec := chunk.Record{
				IngestTS: time.UnixMicro(int64(i * 1000)),
				Attrs:    chunk.Attributes{"session": "1", "idx": string(rune('0' + i))},
				Raw:      []byte("session 1 data"),
			}
			id, _, err := manager.Append(rec)
			if err != nil {
				t.Fatalf("append: %v", err)
			}
			chunkID = id
		}

		// Close without sealing
		if err := manager.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}

	// Second session - should be able to read the unsealed chunk
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

		count := 0
		for {
			rec, _, err := cursor.Next()
			if err == chunk.ErrNoMoreRecords {
				break
			}
			if err != nil {
				t.Fatalf("next: %v", err)
			}

			if rec.Attrs["session"] != "1" {
				t.Fatalf("wrong session attr")
			}
			count++
		}

		cursor.Close()

		if count != 5 {
			t.Fatalf("expected 5 records, got %d", count)
		}

		// Should be able to append more
		rec := chunk.Record{
			IngestTS: time.UnixMicro(5000),
			Attrs:    chunk.Attributes{"session": "2"},
			Raw:      []byte("session 2 data"),
		}
		if _, _, err := manager.Append(rec); err != nil {
			t.Fatalf("append in session 2: %v", err)
		}
	}
}
