package file

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

func TestFileChunkManagerUseSmallTime(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir, UseSmallTime: true})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	// Append a record with nanosecond precision.
	ts := time.Unix(0, 1234567890123456789)
	rec := chunk.Record{
		SourceTS: ts,
		IngestTS: ts.Add(time.Nanosecond),
		Attrs:    chunk.Attributes{"src": "test"},
		Raw:      []byte("nano"),
	}
	chunkID, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Verify idx.log header has FlagSmallTime.
	idxPath := filepath.Join(dir, chunkID.String(), idxLogFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read idx.log: %v", err)
	}
	if len(data) < format.HeaderSize {
		t.Fatalf("idx.log too small")
	}
	header, err := format.DecodeAndValidate(data[:format.HeaderSize], format.TypeIdxLog, IdxLogVersion)
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}
	if header.Flags&format.FlagSmallTime == 0 {
		t.Error("expected FlagSmallTime in idx header")
	}

	// Read back via cursor and verify nanosecond precision preserved.
	cursor, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()
	got, _, err := cursor.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if got.SourceTS.UnixNano() != ts.UnixNano() {
		t.Errorf("SourceTS: want %d, got %d", ts.UnixNano(), got.SourceTS.UnixNano())
	}
	if string(got.Raw) != "nano" {
		t.Errorf("Raw: want %q, got %q", "nano", string(got.Raw))
	}
}

func TestFileChunkManagerIngestSourceTSBounds(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	// Append records with varying IngestTS and SourceTS.
	ing1 := time.UnixMicro(1000)
	ing2 := time.UnixMicro(2000)
	ing3 := time.UnixMicro(1500)
	src1 := time.UnixMicro(500)
	src2 := time.UnixMicro(3000)
	attrs := chunk.Attributes{"src": "test"}

	for _, r := range []chunk.Record{
		{IngestTS: ing1, SourceTS: src1, Attrs: attrs, Raw: []byte("a")},
		{IngestTS: ing2, SourceTS: src2, Attrs: attrs, Raw: []byte("b")},
		{IngestTS: ing3, Attrs: attrs, Raw: []byte("c")}, // no SourceTS
	} {
		if _, _, err := manager.Append(r); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Unsealed chunks: meta from active. List returns active + sealed.
	meta, err := manager.Meta(manager.Active().ID)
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	if meta.IngestStart != ing1 {
		t.Errorf("IngestStart: want %v, got %v", ing1, meta.IngestStart)
	}
	if meta.IngestEnd != ing2 {
		t.Errorf("IngestEnd: want %v, got %v", ing2, meta.IngestEnd)
	}
	if meta.SourceStart != src1 {
		t.Errorf("SourceStart: want %v, got %v", src1, meta.SourceStart)
	}
	if meta.SourceEnd != src2 {
		t.Errorf("SourceEnd: want %v, got %v", src2, meta.SourceEnd)
	}

	// Seal and verify loadChunkMeta also populates bounds.
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	meta, err = manager.Meta(meta.ID)
	if err != nil {
		t.Fatalf("meta after seal: %v", err)
	}
	if meta.IngestStart.IsZero() || meta.IngestEnd.IsZero() {
		t.Error("sealed chunk should have IngestTS bounds")
	}
	if meta.SourceStart != src1 || meta.SourceEnd != src2 {
		t.Errorf("SourceTS bounds: want %v-%v, got %v-%v", src1, src2, meta.SourceStart, meta.SourceEnd)
	}
}

func TestFileChunkManagerDirectoryLayout(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	// Append to first chunk, seal it, then append to a second chunk and seal.
	attrs := chunk.Attributes{"source": "test"}
	rec := chunk.Record{IngestTS: time.UnixMicro(1), Attrs: attrs, Raw: []byte("one")}
	chunkID1, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append chunk 1: %v", err)
	}
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal chunk 1: %v", err)
	}

	rec2 := chunk.Record{IngestTS: time.UnixMicro(2), Attrs: attrs, Raw: []byte("two")}
	chunkID2, _, err := manager.Append(rec2)
	if err != nil {
		t.Fatalf("append chunk 2: %v", err)
	}
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal chunk 2: %v", err)
	}

	if chunkID1 == chunkID2 {
		t.Fatalf("expected different chunk IDs, both are %s", chunkID1.String())
	}

	// Top-level directory should contain exactly two subdirectories named by chunk ID,
	// plus the .lock file for directory locking.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries in root dir (2 chunks + .lock), got %d", len(entries))
	}
	names := map[string]bool{}
	for _, e := range entries {
		if e.Name() == ".lock" {
			if e.IsDir() {
				t.Fatalf(".lock should be a file, not a directory")
			}
			continue
		}
		if !e.IsDir() {
			t.Fatalf("unexpected non-directory entry: %s", e.Name())
		}
		names[e.Name()] = true
	}
	if !names[chunkID1.String()] {
		t.Fatalf("missing directory for chunk %s", chunkID1.String())
	}
	if !names[chunkID2.String()] {
		t.Fatalf("missing directory for chunk %s", chunkID2.String())
	}

	// Each chunk directory should contain exactly the three expected files.
	expectedFiles := []string{rawLogFileName, idxLogFileName, attrLogFileName}
	for _, id := range []chunk.ChunkID{chunkID1, chunkID2} {
		chunkDir := filepath.Join(dir, id.String())
		files, err := os.ReadDir(chunkDir)
		if err != nil {
			t.Fatalf("read chunk dir %s: %v", id.String(), err)
		}
		if len(files) != len(expectedFiles) {
			t.Fatalf("chunk %s: expected %d files, got %d", id.String(), len(expectedFiles), len(files))
		}
		fileNames := map[string]bool{}
		for _, f := range files {
			if f.IsDir() {
				t.Fatalf("chunk %s: unexpected subdirectory %s", id.String(), f.Name())
			}
			fileNames[f.Name()] = true
		}
		for _, name := range expectedFiles {
			if !fileNames[name] {
				t.Fatalf("chunk %s: missing file %s", id.String(), name)
			}
		}
	}
}

func TestFileChunkManagerAppendSealOpenReader(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	attrs := chunk.Attributes{"source": "test"}
	record := chunk.Record{IngestTS: time.UnixMicro(100), Attrs: attrs, Raw: []byte("alpha")}
	chunkID, offset, err := manager.Append(record)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if offset != 0 {
		t.Fatalf("expected offset 0, got %d", offset)
	}

	// Read from the unsealed (active) chunk via file I/O reader.
	unsealedReader, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open unsealed reader: %v", err)
	}
	unsealedGot, _, err := unsealedReader.Next()
	if err != nil {
		t.Fatalf("unsealed next: %v", err)
	}
	if unsealedGot.Attrs["source"] != record.Attrs["source"] {
		t.Fatalf("unsealed attrs: expected %v got %v", record.Attrs, unsealedGot.Attrs)
	}
	if string(unsealedGot.Raw) != string(record.Raw) {
		t.Fatalf("unsealed raw: expected %q got %q", record.Raw, unsealedGot.Raw)
	}
	if _, _, err := unsealedReader.Next(); err != chunk.ErrNoMoreRecords {
		t.Fatalf("unsealed: expected end of records, got %v", err)
	}
	unsealedReader.Close()

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	reader, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	got, _, err := reader.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if got.Attrs["source"] != record.Attrs["source"] {
		t.Fatalf("expected attrs %v got %v", record.Attrs, got.Attrs)
	}
	if _, _, err := reader.Next(); err != chunk.ErrNoMoreRecords {
		t.Fatalf("expected end of records, got %v", err)
	}

	chunkDir := filepath.Join(dir, chunkID.String())
	if _, err := os.Stat(filepath.Join(chunkDir, rawLogFileName)); err != nil {
		t.Fatalf("raw.log file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(chunkDir, idxLogFileName)); err != nil {
		t.Fatalf("idx.log file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(chunkDir, attrLogFileName)); err != nil {
		t.Fatalf("attr.log file missing: %v", err)
	}
}

func TestFileChunkManagerReverseReader(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: time.UnixMicro(100), Attrs: attrs, Raw: []byte("first")},
		{IngestTS: time.UnixMicro(200), Attrs: attrs, Raw: []byte("second")},
		{IngestTS: time.UnixMicro(300), Attrs: attrs, Raw: []byte("third")},
	}

	var chunkID chunk.ChunkID
	for _, rec := range records {
		id, _, err := manager.Append(rec)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		chunkID = id
	}

	// Reverse read from unsealed chunk (file I/O reader).
	reader, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open reader (unsealed): %v", err)
	}
	for i := len(records) - 1; i >= 0; i-- {
		got, _, err := reader.Prev()
		if err != nil {
			t.Fatalf("prev (unsealed) record %d: %v", i, err)
		}
		if got.Attrs["source"] != attrs["source"] {
			t.Fatalf("record %d: attrs want %v got %v", i, attrs, got.Attrs)
		}
		if string(got.Raw) != string(records[i].Raw) {
			t.Fatalf("record %d: raw want %q got %q", i, records[i].Raw, got.Raw)
		}
	}
	if _, _, err := reader.Prev(); err != chunk.ErrNoMoreRecords {
		t.Fatalf("prev (unsealed): expected ErrNoMoreRecords, got %v", err)
	}
	reader.Close()

	// Seal and reverse read again (mmap reader).
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	reader, err = manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open reader (sealed): %v", err)
	}
	defer reader.Close()
	for i := len(records) - 1; i >= 0; i-- {
		got, _, err := reader.Prev()
		if err != nil {
			t.Fatalf("prev (sealed) record %d: %v", i, err)
		}
		if got.Attrs["source"] != attrs["source"] {
			t.Fatalf("record %d: attrs want %v got %v", i, attrs, got.Attrs)
		}
		if string(got.Raw) != string(records[i].Raw) {
			t.Fatalf("record %d: raw want %q got %q", i, records[i].Raw, got.Raw)
		}
	}
	if _, _, err := reader.Prev(); err != chunk.ErrNoMoreRecords {
		t.Fatalf("prev (sealed): expected ErrNoMoreRecords, got %v", err)
	}
}

func TestFileChunkManagerCursorSeek(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: time.UnixMicro(100), Attrs: attrs, Raw: []byte("alpha")},
		{IngestTS: time.UnixMicro(200), Attrs: attrs, Raw: []byte("beta")},
		{IngestTS: time.UnixMicro(300), Attrs: attrs, Raw: []byte("gamma")},
		{IngestTS: time.UnixMicro(400), Attrs: attrs, Raw: []byte("delta")},
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

	cursor, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()

	// Read forward to collect refs for all records.
	refs := make([]chunk.RecordRef, len(records))
	for i := range records {
		_, ref, err := cursor.Next()
		if err != nil {
			t.Fatalf("next record %d: %v", i, err)
		}
		refs[i] = ref
	}

	// Seek to the second record and read forward from there.
	if err := cursor.Seek(refs[1]); err != nil {
		t.Fatalf("seek to record 1: %v", err)
	}
	got, ref, err := cursor.Next()
	if err != nil {
		t.Fatalf("next after seek: %v", err)
	}
	if string(got.Raw) != "beta" {
		t.Fatalf("expected %q after seek, got %q", "beta", got.Raw)
	}
	if ref.Pos != refs[1].Pos {
		t.Fatalf("ref pos: want %d got %d", refs[1].Pos, ref.Pos)
	}

	// Continue forward — should get gamma.
	got, _, err = cursor.Next()
	if err != nil {
		t.Fatalf("next after seek+1: %v", err)
	}
	if string(got.Raw) != "gamma" {
		t.Fatalf("expected %q, got %q", "gamma", got.Raw)
	}

	// Seek to the third record and read backward from there.
	if err := cursor.Seek(refs[2]); err != nil {
		t.Fatalf("seek to record 2: %v", err)
	}
	got, ref, err = cursor.Prev()
	if err != nil {
		t.Fatalf("prev after seek: %v", err)
	}
	// Prev from the start of record 2 should return record 1.
	if string(got.Raw) != "beta" {
		t.Fatalf("expected %q from prev after seek, got %q", "beta", got.Raw)
	}
	if ref.Pos != refs[1].Pos {
		t.Fatalf("prev ref pos: want %d got %d", refs[1].Pos, ref.Pos)
	}

	// Seek to beginning (first record ref), Prev should return ErrNoMoreRecords.
	if err := cursor.Seek(refs[0]); err != nil {
		t.Fatalf("seek to record 0: %v", err)
	}
	if _, _, err := cursor.Prev(); err != chunk.ErrNoMoreRecords {
		t.Fatalf("expected ErrNoMoreRecords at start, got %v", err)
	}
}

func TestFileChunkManagerCursorMixedNextPrev(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: time.UnixMicro(100), Attrs: attrs, Raw: []byte("one")},
		{IngestTS: time.UnixMicro(200), Attrs: attrs, Raw: []byte("two")},
		{IngestTS: time.UnixMicro(300), Attrs: attrs, Raw: []byte("three")},
		{IngestTS: time.UnixMicro(400), Attrs: attrs, Raw: []byte("four")},
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

	cursor, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()

	// Next: one
	got, _, err := cursor.Next()
	if err != nil {
		t.Fatalf("next 1: %v", err)
	}
	if string(got.Raw) != "one" {
		t.Fatalf("expected %q, got %q", "one", got.Raw)
	}

	// Next: two
	got, ref, err := cursor.Next()
	if err != nil {
		t.Fatalf("next 2: %v", err)
	}
	if string(got.Raw) != "two" {
		t.Fatalf("expected %q, got %q", "two", got.Raw)
	}

	// Seek to the ref returned by "two", then Prev should give "one".
	if err := cursor.Seek(ref); err != nil {
		t.Fatalf("seek: %v", err)
	}
	got, _, err = cursor.Prev()
	if err != nil {
		t.Fatalf("prev after seek: %v", err)
	}
	if string(got.Raw) != "one" {
		t.Fatalf("expected %q, got %q", "one", got.Raw)
	}

	// Prev again should be ErrNoMoreRecords (at start of file).
	if _, _, err := cursor.Prev(); err != chunk.ErrNoMoreRecords {
		t.Fatalf("expected ErrNoMoreRecords, got %v", err)
	}

	// Seek back to "two" ref, Next should give "two", then "three", then "four".
	if err := cursor.Seek(ref); err != nil {
		t.Fatalf("seek back: %v", err)
	}
	for _, expected := range []string{"two", "three", "four"} {
		got, _, err = cursor.Next()
		if err != nil {
			t.Fatalf("next %q: %v", expected, err)
		}
		if string(got.Raw) != expected {
			t.Fatalf("expected %q, got %q", expected, got.Raw)
		}
	}

	// Next past end should be ErrNoMoreRecords.
	if _, _, err := cursor.Next(); err != chunk.ErrNoMoreRecords {
		t.Fatalf("expected ErrNoMoreRecords at end, got %v", err)
	}
}

func TestFileChunkManagerEmptyChunk(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	// Seal with no prior append creates an empty sealed chunk.
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
	meta := metas[0]
	if !meta.Sealed {
		t.Fatal("expected chunk to be sealed")
	}

	// Open a cursor on the empty sealed chunk.
	cursor, err := manager.OpenCursor(meta.ID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()

	// Next should immediately return ErrNoMoreRecords.
	if _, _, err := cursor.Next(); err != chunk.ErrNoMoreRecords {
		t.Fatalf("expected ErrNoMoreRecords, got %v", err)
	}

	// Prev should immediately return ErrNoMoreRecords.
	if _, _, err := cursor.Prev(); err != chunk.ErrNoMoreRecords {
		t.Fatalf("expected ErrNoMoreRecords from Prev, got %v", err)
	}
}

// TestListReturnsSortedChunks verifies that List() returns chunks sorted by StartTS.
func TestListReturnsSortedChunks(t *testing.T) {
	dir := t.TempDir()
	attrs := chunk.Attributes{"source": "test"}

	// Controlled clock - increments by 1 second each call
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time {
		result := ts
		ts = ts.Add(time.Second)
		return result
	}

	manager, err := NewManager(Config{Dir: dir, Now: clock})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	// Create 3 chunks with 1 record each
	for i := 0; i < 3; i++ {
		rec := chunk.Record{
			IngestTS: clock(),
			Attrs:    attrs,
			Raw:      []byte("data"),
		}
		if _, _, err := manager.Append(rec); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if err := manager.Seal(); err != nil {
			t.Fatalf("seal %d: %v", i, err)
		}
	}

	metas, err := manager.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(metas) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(metas))
	}

	// Verify sorted by StartTS ascending
	for i := 1; i < len(metas); i++ {
		if !metas[i].StartTS.After(metas[i-1].StartTS) {
			t.Errorf("not sorted: metas[%d].StartTS=%v <= metas[%d].StartTS=%v",
				i, metas[i].StartTS, i-1, metas[i-1].StartTS)
		}
	}
}
