package file

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kluzzebass/gastrolog/internal/chunk"
)

func TestFileChunkManagerDirectoryLayout(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	// Append to first chunk, seal it, then append to a second chunk and seal.
	sourceID := chunk.NewSourceID()
	rec := chunk.Record{IngestTS: time.UnixMicro(1), SourceID: sourceID, Raw: []byte("one")}
	chunkID1, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append chunk 1: %v", err)
	}
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal chunk 1: %v", err)
	}

	rec2 := chunk.Record{IngestTS: time.UnixMicro(2), SourceID: sourceID, Raw: []byte("two")}
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

	// Top-level directory should contain exactly two subdirectories named by chunk ID.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries in root dir, got %d", len(entries))
	}
	names := map[string]bool{}
	for _, e := range entries {
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
	expectedFiles := []string{recordsFileName, metaFileName, sourcesFileName}
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

	sourceID := chunk.NewSourceID()
	record := chunk.Record{IngestTS: time.UnixMicro(100), SourceID: sourceID, Raw: []byte("alpha")}
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
	if unsealedGot.SourceID != record.SourceID {
		t.Fatalf("unsealed source id: expected %s got %s", record.SourceID.String(), unsealedGot.SourceID.String())
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
	if got.SourceID != record.SourceID {
		t.Fatalf("expected source id %s got %s", record.SourceID.String(), got.SourceID.String())
	}
	if _, _, err := reader.Next(); err != chunk.ErrNoMoreRecords {
		t.Fatalf("expected end of records, got %v", err)
	}

	chunkDir := filepath.Join(dir, chunkID.String())
	if _, err := os.Stat(filepath.Join(chunkDir, recordsFileName)); err != nil {
		t.Fatalf("records file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(chunkDir, metaFileName)); err != nil {
		t.Fatalf("meta file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(chunkDir, sourcesFileName)); err != nil {
		t.Fatalf("sources file missing: %v", err)
	}
}

func TestFileChunkManagerReverseReader(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: time.UnixMicro(100), SourceID: sourceID, Raw: []byte("first")},
		{IngestTS: time.UnixMicro(200), SourceID: sourceID, Raw: []byte("second")},
		{IngestTS: time.UnixMicro(300), SourceID: sourceID, Raw: []byte("third")},
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
		if got.SourceID != sourceID {
			t.Fatalf("record %d: source id want %s got %s", i, sourceID.String(), got.SourceID.String())
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
		if got.SourceID != sourceID {
			t.Fatalf("record %d: source id want %s got %s", i, sourceID.String(), got.SourceID.String())
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

	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: time.UnixMicro(100), SourceID: sourceID, Raw: []byte("alpha")},
		{IngestTS: time.UnixMicro(200), SourceID: sourceID, Raw: []byte("beta")},
		{IngestTS: time.UnixMicro(300), SourceID: sourceID, Raw: []byte("gamma")},
		{IngestTS: time.UnixMicro(400), SourceID: sourceID, Raw: []byte("delta")},
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

	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: time.UnixMicro(100), SourceID: sourceID, Raw: []byte("one")},
		{IngestTS: time.UnixMicro(200), SourceID: sourceID, Raw: []byte("two")},
		{IngestTS: time.UnixMicro(300), SourceID: sourceID, Raw: []byte("three")},
		{IngestTS: time.UnixMicro(400), SourceID: sourceID, Raw: []byte("four")},
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
