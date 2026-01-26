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
	unsealedReader, err := manager.OpenReader(chunkID)
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

	reader, err := manager.OpenReader(chunkID)
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
	reader, err := manager.OpenReader(chunkID)
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
	reader, err = manager.OpenReader(chunkID)
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
