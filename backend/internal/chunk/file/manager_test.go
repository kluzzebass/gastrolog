package file

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

func TestFileChunkManagerNanosecondPrecision(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
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
	t.Parallel()
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
	t.Parallel()
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

	// Each chunk directory should contain exactly the four expected files.
	expectedFiles := []string{rawLogFileName, idxLogFileName, attrLogFileName, attrDictFileName}
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
	t.Parallel()
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
	if _, err := os.Stat(filepath.Join(chunkDir, attrDictFileName)); err != nil {
		t.Fatalf("attr_dict.log file missing: %v", err)
	}
}

func TestFileChunkManagerReverseReader(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

// TestMissingDirectoryWarning verifies that a warning is logged when a previously
// existing store's directory is missing and gets recreated empty.
func TestMissingDirectoryWarning(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	storeDir := filepath.Join(root, "mystore")

	// Create a manager, write data, seal, close — establishes the directory.
	m1, err := NewManager(Config{Dir: storeDir})
	if err != nil {
		t.Fatalf("create initial manager: %v", err)
	}
	rec := chunk.Record{
		IngestTS: time.UnixMicro(1000),
		Attrs:    chunk.Attributes{"src": "test"},
		Raw:      []byte("important data"),
	}
	if _, _, err := m1.Append(rec); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := m1.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	if err := m1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Nuke the directory (simulates accidental deletion / /tmp cleanup).
	if err := os.RemoveAll(storeDir); err != nil {
		t.Fatalf("remove store dir: %v", err)
	}

	// Re-open with a capturing logger, expecting existing data.
	h := &capturingHandler{}
	logger := slog.New(h)

	m2, err := NewManager(Config{Dir: storeDir, Logger: logger, ExpectExisting: true})
	if err != nil {
		t.Fatalf("reopen manager: %v", err)
	}
	defer m2.Close()

	// Should have zero chunks.
	metas, err := m2.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(metas) != 0 {
		t.Fatalf("expected 0 chunks after dir loss, got %d", len(metas))
	}

	// Should have logged a warning about the missing directory.
	if !h.hasWarn("missing") {
		t.Error("expected a WARN log about missing directory, got none")
	}
}

// TestNewDirectoryNoWarning verifies that creating a brand-new store (directory
// never existed) does NOT emit a spurious warning.
func TestNewDirectoryNoWarning(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	storeDir := filepath.Join(root, "brand-new-store")

	h := &capturingHandler{}
	logger := slog.New(h)

	m, err := NewManager(Config{Dir: storeDir, Logger: logger})
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}
	defer m.Close()

	// A new empty store should NOT warn — there's nothing to have lost.
	if h.hasWarn("missing") {
		t.Error("unexpected warning for brand-new store directory")
	}
}

// capturingHandler is a minimal slog.Handler that records log records for assertions.
type capturingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capturingHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}
func (h *capturingHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(_ string) slog.Handler      { return h }

func (h *capturingHandler) hasWarn(substr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Level == slog.LevelWarn && contains(r.Message, substr) {
			return true
		}
	}
	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestChunkDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	rec := chunk.Record{IngestTS: time.UnixMicro(1), Attrs: chunk.Attributes{"src": "test"}, Raw: []byte("a")}
	chunkID, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	got := manager.ChunkDir(chunkID)
	want := filepath.Join(dir, chunkID.String())
	if got != want {
		t.Errorf("ChunkDir: got %q, want %q", got, want)
	}
}

func TestDisown(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	rec := chunk.Record{IngestTS: time.UnixMicro(1), Attrs: chunk.Attributes{"src": "test"}, Raw: []byte("a")}
	chunkID, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Disown the sealed chunk.
	if err := manager.Disown(chunkID); err != nil {
		t.Fatalf("disown: %v", err)
	}

	// Chunk should no longer be tracked.
	if _, err := manager.Meta(chunkID); err != chunk.ErrChunkNotFound {
		t.Errorf("expected ErrChunkNotFound after disown, got %v", err)
	}

	// Files should still exist on disk.
	chunkDir := filepath.Join(dir, chunkID.String())
	if _, err := os.Stat(chunkDir); os.IsNotExist(err) {
		t.Error("chunk directory should still exist after disown")
	}
}

func TestDisownActiveChunkFails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	rec := chunk.Record{IngestTS: time.UnixMicro(1), Attrs: chunk.Attributes{"src": "test"}, Raw: []byte("a")}
	chunkID, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// Disown the active chunk should fail.
	if err := manager.Disown(chunkID); err != chunk.ErrActiveChunk {
		t.Errorf("expected ErrActiveChunk, got %v", err)
	}
}

func TestDisownUnsealedChunkFails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	// Disown nonexistent chunk.
	fakeID := chunk.NewChunkID()
	if err := manager.Disown(fakeID); err != chunk.ErrChunkNotFound {
		t.Errorf("expected ErrChunkNotFound, got %v", err)
	}
}

func TestAdopt(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	rec := chunk.Record{IngestTS: time.UnixMicro(1000), Attrs: chunk.Attributes{"src": "test"}, Raw: []byte("adopt-me")}
	chunkID, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Disown the chunk.
	if err := manager.Disown(chunkID); err != nil {
		t.Fatalf("disown: %v", err)
	}

	// Adopt it back.
	meta, err := manager.Adopt(chunkID)
	if err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if meta.ID != chunkID {
		t.Errorf("adopted chunk ID: got %s, want %s", meta.ID, chunkID)
	}
	if !meta.Sealed {
		t.Error("adopted chunk should be sealed")
	}
	if meta.RecordCount != 1 {
		t.Errorf("adopted chunk record count: got %d, want 1", meta.RecordCount)
	}

	// Should be listed again.
	metas, err := manager.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	found := false
	for _, m := range metas {
		if m.ID == chunkID {
			found = true
		}
	}
	if !found {
		t.Error("adopted chunk should appear in list")
	}

	manager.Close()
}

func TestAdoptMissingDirFails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	fakeID := chunk.NewChunkID()
	if _, err := manager.Adopt(fakeID); err == nil {
		t.Fatal("expected error adopting nonexistent chunk")
	}
}

func TestAdoptAlreadyTrackedFails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	rec := chunk.Record{IngestTS: time.UnixMicro(1), Attrs: chunk.Attributes{"src": "test"}, Raw: []byte("a")}
	chunkID, _, err := manager.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Adopt a chunk that's already tracked should fail.
	if _, err := manager.Adopt(chunkID); err == nil {
		t.Fatal("expected error adopting already-tracked chunk")
	}
}

// TestListReturnsSortedChunks verifies that List() returns chunks sorted by WriteStart.
func TestListReturnsSortedChunks(t *testing.T) {
	t.Parallel()
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
	for i := range 3 {
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

	// Verify sorted by WriteStart ascending
	for i := 1; i < len(metas); i++ {
		if !metas[i].WriteStart.After(metas[i-1].WriteStart) {
			t.Errorf("not sorted: metas[%d].WriteStart=%v <= metas[%d].WriteStart=%v",
				i, metas[i].WriteStart, i-1, metas[i-1].WriteStart)
		}
	}
}

func TestConcurrentAppend(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	const numGoroutines = 10
	const recordsPerGoroutine = 100
	total := numGoroutines * recordsPerGoroutine

	var wg sync.WaitGroup
	errCh := make(chan error, total)

	for g := range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range recordsPerGoroutine {
				rec := chunk.Record{
					IngestTS: time.Now(),
					Attrs:    chunk.Attributes{"src": "test", "g": fmt.Sprintf("%d", g)},
					Raw:      []byte(fmt.Sprintf("g%d-r%d", g, i)),
				}
				if _, _, err := manager.Append(rec); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent append: %v", err)
	}

	// Seal and verify all records are readable.
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	metas, err := manager.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var totalRecords int64
	for _, meta := range metas {
		totalRecords += meta.RecordCount
	}
	if totalRecords != int64(total) {
		t.Fatalf("expected %d records, got %d", total, totalRecords)
	}

	// Read every record from every chunk and verify non-empty raw data.
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
				cursor.Close()
				t.Fatalf("next: %v", err)
			}
			if len(rec.Raw) == 0 {
				cursor.Close()
				t.Fatalf("empty raw data in chunk %s", meta.ID)
			}
			if rec.Attrs["src"] != "test" {
				cursor.Close()
				t.Fatalf("wrong attrs: %v", rec.Attrs)
			}
		}
		cursor.Close()
	}
}

func TestActiveChunkBTreeSeeking(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer mgr.Close()

	// Insert records with non-monotonic IngestTS and SourceTS.
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	records := []chunk.Record{
		{IngestTS: base.Add(300 * time.Millisecond), SourceTS: base.Add(100 * time.Millisecond), Attrs: chunk.Attributes{"src": "a"}, Raw: []byte("r0")},
		{IngestTS: base.Add(100 * time.Millisecond), SourceTS: base.Add(500 * time.Millisecond), Attrs: chunk.Attributes{"src": "b"}, Raw: []byte("r1")},
		{IngestTS: base.Add(500 * time.Millisecond), SourceTS: base.Add(200 * time.Millisecond), Attrs: chunk.Attributes{"src": "c"}, Raw: []byte("r2")},
		{IngestTS: base.Add(200 * time.Millisecond), SourceTS: base.Add(400 * time.Millisecond), Attrs: chunk.Attributes{"src": "d"}, Raw: []byte("r3")},
		{IngestTS: base.Add(400 * time.Millisecond), Raw: []byte("r4"), Attrs: chunk.Attributes{"src": "e"}}, // no SourceTS
	}

	var chunkID chunk.ChunkID
	for _, rec := range records {
		id, _, err := mgr.Append(rec)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		chunkID = id
	}

	// FindIngestStartPosition should find the earliest record with IngestTS >= target.
	tests := []struct {
		name    string
		method  string
		target  time.Time
		wantPos uint64
		wantOK  bool
	}{
		// IngestTS order by value: 100ms(r1), 200ms(r3), 300ms(r0), 400ms(r4), 500ms(r2)
		{"ingest_before_all", "ingest", base, 1, true},                              // r1 at 100ms
		{"ingest_exact_match", "ingest", base.Add(200 * time.Millisecond), 3, true}, // r3 at 200ms
		{"ingest_between", "ingest", base.Add(250 * time.Millisecond), 0, true},     // r0 at 300ms
		{"ingest_after_all", "ingest", base.Add(600 * time.Millisecond), 0, false},

		// SourceTS order by value: 100ms(r0), 200ms(r2), 400ms(r3), 500ms(r1) — r4 excluded (zero)
		{"source_before_all", "source", base, 0, true},                              // r0 at 100ms
		{"source_exact_match", "source", base.Add(200 * time.Millisecond), 2, true}, // r2 at 200ms
		{"source_between", "source", base.Add(300 * time.Millisecond), 3, true},     // r3 at 400ms
		{"source_after_all", "source", base.Add(600 * time.Millisecond), 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var pos uint64
			var found bool
			var err error
			if tt.method == "ingest" {
				pos, found, err = mgr.FindIngestStartPosition(chunkID, tt.target)
			} else {
				pos, found, err = mgr.FindSourceStartPosition(chunkID, tt.target)
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if found != tt.wantOK {
				t.Fatalf("found = %v, want %v", found, tt.wantOK)
			}
			if found && pos != tt.wantPos {
				t.Fatalf("pos = %d, want %d", pos, tt.wantPos)
			}
		})
	}
}

func TestBTreeRecoveryAfterReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	base := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	records := []chunk.Record{
		{IngestTS: base.Add(300 * time.Millisecond), SourceTS: base.Add(100 * time.Millisecond), Attrs: chunk.Attributes{"src": "a"}, Raw: []byte("r0")},
		{IngestTS: base.Add(100 * time.Millisecond), SourceTS: base.Add(500 * time.Millisecond), Attrs: chunk.Attributes{"src": "b"}, Raw: []byte("r1")},
		{IngestTS: base.Add(500 * time.Millisecond), SourceTS: base.Add(200 * time.Millisecond), Attrs: chunk.Attributes{"src": "c"}, Raw: []byte("r2")},
	}

	for _, rec := range records {
		if _, _, err := mgr.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Close without sealing — simulates crash recovery.
	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen — should rebuild B+ trees from idx.log.
	mgr, err = NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr.Close()

	active := mgr.Active()
	if active == nil {
		t.Fatal("expected active chunk after reopen")
	}

	// Verify IngestTS seeking works after recovery.
	// IngestTS order: 100ms(r1), 300ms(r0), 500ms(r2)
	pos, found, err := mgr.FindIngestStartPosition(active.ID, base.Add(200*time.Millisecond))
	if err != nil {
		t.Fatalf("FindIngestStartPosition: %v", err)
	}
	if !found || pos != 0 {
		t.Fatalf("FindIngestStartPosition: found=%v pos=%d, want found=true pos=0 (r0 at 300ms)", found, pos)
	}

	// Verify SourceTS seeking works after recovery.
	// SourceTS order: 100ms(r0), 200ms(r2), 500ms(r1)
	pos, found, err = mgr.FindSourceStartPosition(active.ID, base.Add(150*time.Millisecond))
	if err != nil {
		t.Fatalf("FindSourceStartPosition: %v", err)
	}
	if !found || pos != 2 {
		t.Fatalf("FindSourceStartPosition: found=%v pos=%d, want found=true pos=2 (r2 at 200ms)", found, pos)
	}
}

func TestBTreeSeekingLargeNonMonotonic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer mgr.Close()

	// Insert 2000 records with randomized IngestTS and SourceTS.
	// This forces multiple B+ tree leaf splits.
	rng := rand.New(rand.NewPCG(42, 0))
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	n := 2000

	type record struct {
		ingestTS time.Time
		sourceTS time.Time
		pos      uint32
	}
	records := make([]record, n)
	var chunkID chunk.ChunkID

	for i := range n {
		ingestOffset := time.Duration(rng.Int64N(1_000_000)) * time.Microsecond
		sourceOffset := time.Duration(rng.Int64N(1_000_000)) * time.Microsecond
		rec := chunk.Record{
			IngestTS: base.Add(ingestOffset),
			SourceTS: base.Add(sourceOffset),
			Attrs:    chunk.Attributes{"src": "test"},
			Raw:      []byte(fmt.Sprintf("record-%d", i)),
		}
		id, _, err := mgr.Append(rec)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		chunkID = id
		records[i] = record{
			ingestTS: rec.IngestTS,
			sourceTS: rec.SourceTS,
			pos:      uint32(i),
		}
	}

	// Sort records by IngestTS to build expected results.
	ingestSorted := slices.Clone(records)
	slices.SortFunc(ingestSorted, func(a, b record) int {
		if c := a.ingestTS.Compare(b.ingestTS); c != 0 {
			return c
		}
		return int(a.pos) - int(b.pos)
	})

	sourceSorted := slices.Clone(records)
	slices.SortFunc(sourceSorted, func(a, b record) int {
		if c := a.sourceTS.Compare(b.sourceTS); c != 0 {
			return c
		}
		return int(a.pos) - int(b.pos)
	})

	// Spot-check 200 random IngestTS seeks.
	for range 200 {
		target := base.Add(time.Duration(rng.Int64N(1_000_000)) * time.Microsecond)
		pos, found, err := mgr.FindIngestStartPosition(chunkID, target)
		if err != nil {
			t.Fatalf("FindIngestStartPosition(%v): %v", target, err)
		}

		// Find expected: first record in ingestSorted with IngestTS >= target.
		expectedIdx := -1
		for i, r := range ingestSorted {
			if !r.ingestTS.Before(target) {
				expectedIdx = i
				break
			}
		}

		if expectedIdx == -1 {
			if found {
				t.Fatalf("FindIngestStartPosition(%v): found=%v pos=%d, want not found", target, found, pos)
			}
		} else {
			if !found {
				t.Fatalf("FindIngestStartPosition(%v): not found, want pos=%d (ts=%v)", target, ingestSorted[expectedIdx].pos, ingestSorted[expectedIdx].ingestTS)
			}
			if pos != uint64(ingestSorted[expectedIdx].pos) {
				t.Fatalf("FindIngestStartPosition(%v): pos=%d, want %d (ts=%v)", target, pos, ingestSorted[expectedIdx].pos, ingestSorted[expectedIdx].ingestTS)
			}
		}
	}

	// Spot-check 200 random SourceTS seeks.
	for range 200 {
		target := base.Add(time.Duration(rng.Int64N(1_000_000)) * time.Microsecond)
		pos, found, err := mgr.FindSourceStartPosition(chunkID, target)
		if err != nil {
			t.Fatalf("FindSourceStartPosition(%v): %v", target, err)
		}

		expectedIdx := -1
		for i, r := range sourceSorted {
			if !r.sourceTS.Before(target) {
				expectedIdx = i
				break
			}
		}

		if expectedIdx == -1 {
			if found {
				t.Fatalf("FindSourceStartPosition(%v): found=%v pos=%d, want not found", target, found, pos)
			}
		} else {
			if !found {
				t.Fatalf("FindSourceStartPosition(%v): not found, want pos=%d (ts=%v)", target, sourceSorted[expectedIdx].pos, sourceSorted[expectedIdx].sourceTS)
			}
			if pos != uint64(sourceSorted[expectedIdx].pos) {
				t.Fatalf("FindSourceStartPosition(%v): pos=%d, want %d (ts=%v)", target, pos, sourceSorted[expectedIdx].pos, sourceSorted[expectedIdx].sourceTS)
			}
		}
	}
}

func TestBTreeSeekSingleRecord(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer mgr.Close()

	ts := time.Date(2025, 3, 15, 12, 0, 0, 0, time.UTC)
	chunkID, _, err := mgr.Append(chunk.Record{
		IngestTS: ts,
		SourceTS: ts,
		Attrs:    chunk.Attributes{"src": "only"},
		Raw:      []byte("single"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Exact match.
	pos, found, err := mgr.FindIngestStartPosition(chunkID, ts)
	if err != nil || !found || pos != 0 {
		t.Fatalf("exact match: found=%v pos=%d err=%v", found, pos, err)
	}

	// Before the record.
	pos, found, err = mgr.FindIngestStartPosition(chunkID, ts.Add(-time.Second))
	if err != nil || !found || pos != 0 {
		t.Fatalf("before: found=%v pos=%d err=%v", found, pos, err)
	}

	// After the record.
	_, found, err = mgr.FindIngestStartPosition(chunkID, ts.Add(time.Second))
	if err != nil || found {
		t.Fatalf("after: found=%v err=%v, want not found", found, err)
	}
}

func TestBTreeSeekEmptyActiveChunk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer mgr.Close()

	// Force creation of an active chunk by appending then sealing, then creating new.
	chunkID, _, err := mgr.Append(chunk.Record{
		IngestTS: time.Now(),
		SourceTS: time.Now(),
		Attrs:    chunk.Attributes{"src": "t"},
		Raw:      []byte("x"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Seeking on a chunk that IS active should work.
	_, found, err := mgr.FindIngestStartPosition(chunkID, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("seek active: %v", err)
	}
	if !found {
		t.Fatal("expected to find the record")
	}

	// Seal it, then seek on the now-sealed chunk — should return not found (sealed chunks
	// use the index manager, not the chunk manager).
	if err := mgr.Seal(); err != nil {
		t.Fatal(err)
	}
	_, found, err = mgr.FindIngestStartPosition(chunkID, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("seek sealed: %v", err)
	}
	if found {
		t.Fatal("sealed chunk should return not-found from chunk manager")
	}
}

func TestBTreeDuplicateTimestamps(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer mgr.Close()

	// Insert 100 records all with the same IngestTS.
	ts := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	var chunkID chunk.ChunkID
	for i := range 100 {
		id, _, err := mgr.Append(chunk.Record{
			IngestTS: ts,
			SourceTS: ts.Add(time.Duration(i) * time.Millisecond), // unique SourceTS
			Attrs:    chunk.Attributes{"src": "dup"},
			Raw:      []byte(fmt.Sprintf("dup-%d", i)),
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		chunkID = id
	}

	// FindGE for the exact timestamp should return the first record (position 0).
	pos, found, err := mgr.FindIngestStartPosition(chunkID, ts)
	if err != nil {
		t.Fatal(err)
	}
	if !found || pos != 0 {
		t.Fatalf("FindIngestStartPosition: found=%v pos=%d, want found=true pos=0", found, pos)
	}

	// One nanosecond after should find nothing (all records have the same ts).
	_, found, err = mgr.FindIngestStartPosition(chunkID, ts.Add(time.Nanosecond))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected not found for ts after all duplicates")
	}

	// SourceTS seeking should also work — each record has a unique SourceTS.
	// Seek to the 50th millisecond.
	target := ts.Add(50 * time.Millisecond)
	pos, found, err = mgr.FindSourceStartPosition(chunkID, target)
	if err != nil {
		t.Fatal(err)
	}
	if !found || pos != 50 {
		t.Fatalf("FindSourceStartPosition(%v): found=%v pos=%d, want found=true pos=50", target, found, pos)
	}
}

func TestBTreeRecoveryPreservesCorrectness(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	// Insert 500 records with random timestamps.
	rng := rand.New(rand.NewPCG(99, 0))
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	n := 500

	type entry struct {
		ingestTS time.Time
		sourceTS time.Time
		pos      uint32
	}
	entries := make([]entry, n)

	for i := range n {
		ingestOffset := time.Duration(rng.Int64N(1_000_000)) * time.Microsecond
		sourceOffset := time.Duration(rng.Int64N(1_000_000)) * time.Microsecond
		rec := chunk.Record{
			IngestTS: base.Add(ingestOffset),
			SourceTS: base.Add(sourceOffset),
			Attrs:    chunk.Attributes{"src": "test"},
			Raw:      []byte(fmt.Sprintf("r%d", i)),
		}
		if _, _, err := mgr.Append(rec); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		entries[i] = entry{
			ingestTS: rec.IngestTS,
			sourceTS: rec.SourceTS,
			pos:      uint32(i),
		}
	}

	active := mgr.Active()
	if active == nil {
		t.Fatal("no active chunk")
	}
	chunkID := active.ID

	// Collect pre-crash seek results for 50 random targets.
	type seekResult struct {
		target  time.Time
		iPos    uint64
		iFound  bool
		sPos    uint64
		sFound  bool
	}
	var preResults []seekResult
	for range 50 {
		target := base.Add(time.Duration(rng.Int64N(1_000_000)) * time.Microsecond)
		iPos, iFound, _ := mgr.FindIngestStartPosition(chunkID, target)
		sPos, sFound, _ := mgr.FindSourceStartPosition(chunkID, target)
		preResults = append(preResults, seekResult{target, iPos, iFound, sPos, sFound})
	}

	// Close (simulates crash) and reopen.
	mgr.Close()
	mgr, err = NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr.Close()

	active = mgr.Active()
	if active == nil {
		t.Fatal("no active chunk after reopen")
	}

	// Verify all pre-crash results match post-recovery results.
	for i, pre := range preResults {
		iPos, iFound, err := mgr.FindIngestStartPosition(active.ID, pre.target)
		if err != nil {
			t.Fatalf("post-recovery ingest seek %d: %v", i, err)
		}
		if iFound != pre.iFound || iPos != pre.iPos {
			t.Fatalf("post-recovery ingest seek %d (target=%v): got (pos=%d,found=%v), want (pos=%d,found=%v)",
				i, pre.target, iPos, iFound, pre.iPos, pre.iFound)
		}

		sPos, sFound, err := mgr.FindSourceStartPosition(active.ID, pre.target)
		if err != nil {
			t.Fatalf("post-recovery source seek %d: %v", i, err)
		}
		if sFound != pre.sFound || sPos != pre.sPos {
			t.Fatalf("post-recovery source seek %d (target=%v): got (pos=%d,found=%v), want (pos=%d,found=%v)",
				i, pre.target, sPos, sFound, pre.sPos, pre.sFound)
		}
	}
}

func TestBTreeSeekZeroSourceTS(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer mgr.Close()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// All records have zero SourceTS.
	var chunkID chunk.ChunkID
	for i := range 10 {
		id, _, err := mgr.Append(chunk.Record{
			IngestTS: base.Add(time.Duration(i) * time.Millisecond),
			// SourceTS is zero
			Attrs: chunk.Attributes{"src": "no-source"},
			Raw:   []byte(fmt.Sprintf("r%d", i)),
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		chunkID = id
	}

	// IngestTS seeking should work normally.
	pos, found, err := mgr.FindIngestStartPosition(chunkID, base.Add(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if !found || pos != 5 {
		t.Fatalf("ingest seek: found=%v pos=%d, want found=true pos=5", found, pos)
	}

	// SourceTS seeking should find nothing — no records inserted into source B+ tree.
	_, found, err = mgr.FindSourceStartPosition(chunkID, base)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("source seek should find nothing when all SourceTS are zero")
	}
}

func TestBTreeSeekAfterRotation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr, err := NewManager(Config{
		Dir: dir,
		RotationPolicy: chunk.NewCompositePolicy(
			chunk.NewRecordCountPolicy(10), // Rotate after 10 records
		),
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer mgr.Close()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Insert 25 records to trigger multiple rotations.
	var lastChunkID chunk.ChunkID
	chunkIDs := map[chunk.ChunkID]bool{}
	for i := range 25 {
		id, _, err := mgr.Append(chunk.Record{
			IngestTS: base.Add(time.Duration(i*100) * time.Millisecond),
			SourceTS: base.Add(time.Duration(i*50) * time.Millisecond),
			Attrs:    chunk.Attributes{"src": "rot"},
			Raw:      []byte(fmt.Sprintf("r%d", i)),
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		chunkIDs[id] = true
		lastChunkID = id
	}

	// Should have at least 3 chunks (25 records / 10 per chunk).
	if len(chunkIDs) < 3 {
		t.Fatalf("expected >= 3 chunks, got %d", len(chunkIDs))
	}

	// Only the last chunk should be active and seekable via B+ tree.
	active := mgr.Active()
	if active == nil {
		t.Fatal("no active chunk")
	}
	if active.ID != lastChunkID {
		t.Fatalf("active chunk %s != last chunk %s", active.ID, lastChunkID)
	}

	// B+ tree seek on the active chunk should work.
	// The active chunk has records 20-24 (IngestTS: 2000ms-2400ms).
	pos, found, err := mgr.FindIngestStartPosition(lastChunkID, base.Add(2100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected to find record in active chunk")
	}
	// Position within the active chunk (0-based from chunk start).
	if pos > 4 {
		t.Fatalf("pos=%d, expected <= 4 (5 records in active chunk)", pos)
	}

	// Sealed chunk seek should return not-found (handled by index manager, not chunk manager).
	for id := range chunkIDs {
		if id == lastChunkID {
			continue
		}
		_, found, err := mgr.FindIngestStartPosition(id, base)
		if err != nil {
			t.Fatalf("sealed chunk seek: %v", err)
		}
		if found {
			t.Fatalf("sealed chunk %s should return not-found from chunk manager", id)
		}
	}
}

func TestBTreeCleanedUpOnSeal(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer mgr.Close()

	rec := chunk.Record{
		IngestTS: time.Now(),
		SourceTS: time.Now(),
		Attrs:    chunk.Attributes{"src": "test"},
		Raw:      []byte("data"),
	}
	chunkID, _, err := mgr.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// Verify B+ tree files exist before seal.
	chunkDir := filepath.Join(dir, chunkID.String())
	for _, name := range []string{ingestBTFileName, sourceBTFileName} {
		if _, err := os.Stat(filepath.Join(chunkDir, name)); err != nil {
			t.Fatalf("btree file %s should exist before seal: %v", name, err)
		}
	}

	if err := mgr.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Verify B+ tree files are removed after seal.
	for _, name := range []string{ingestBTFileName, sourceBTFileName} {
		if _, err := os.Stat(filepath.Join(chunkDir, name)); !os.IsNotExist(err) {
			t.Fatalf("btree file %s should be removed after seal, err=%v", name, err)
		}
	}
}

// TestDeleteDuringPostSealOtherChunk verifies that Delete on chunk A does not
// panic when PostSealProcess is running concurrently on a different chunk B.
//
// The old code called a global postSealWg.Wait() inside Delete, which panicked
// with "sync: WaitGroup is reused before previous Wait has returned" when
// another goroutine's PostSealProcess called postSealWg.Add(1) concurrently.
// The fix uses a per-chunk channel (postSealActive sync.Map) so Delete only
// waits for the specific chunk being deleted.
func TestDeleteDuringPostSealOtherChunk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	manager, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	attrs := chunk.Attributes{"src": "test"}

	// Create and seal chunk A.
	recA := chunk.Record{IngestTS: time.UnixMicro(100), Attrs: attrs, Raw: []byte("chunk-a")}
	chunkA, _, err := manager.Append(recA)
	if err != nil {
		t.Fatalf("append chunk A: %v", err)
	}
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal chunk A: %v", err)
	}

	// Create and seal chunk B.
	recB := chunk.Record{IngestTS: time.UnixMicro(200), Attrs: attrs, Raw: []byte("chunk-b")}
	chunkB, _, err := manager.Append(recB)
	if err != nil {
		t.Fatalf("append chunk B: %v", err)
	}
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal chunk B: %v", err)
	}

	// Start PostSealProcess for chunk B in a background goroutine.
	// This will run compression (and index builds if any) which takes
	// non-trivial time, keeping the postSealActive entry alive.
	ctx := context.Background()
	postSealErr := make(chan error, 1)
	go func() {
		postSealErr <- manager.PostSealProcess(ctx, chunkB)
	}()

	// Concurrently delete chunk A. With the old global WaitGroup approach,
	// this would panic because PostSealProcess on chunk B calls wg.Add(1)
	// while Delete calls wg.Wait(). The per-chunk channel approach must
	// allow Delete(A) to proceed immediately since no post-seal work is
	// active for chunk A.
	if err := manager.Delete(chunkA); err != nil {
		t.Fatalf("delete chunk A: %v", err)
	}

	// Wait for PostSealProcess on chunk B to finish.
	if err := <-postSealErr; err != nil {
		t.Fatalf("post-seal chunk B: %v", err)
	}

	// Verify chunk A is gone.
	if _, err := manager.Meta(chunkA); err != chunk.ErrChunkNotFound {
		t.Fatalf("chunk A should be deleted, got err: %v", err)
	}
	chunkADir := filepath.Join(dir, chunkA.String())
	if _, err := os.Stat(chunkADir); !os.IsNotExist(err) {
		t.Fatalf("chunk A directory should not exist, stat err: %v", err)
	}

	// Verify chunk B still exists and is accessible.
	metaB, err := manager.Meta(chunkB)
	if err != nil {
		t.Fatalf("chunk B meta: %v", err)
	}
	if !metaB.Sealed {
		t.Fatal("chunk B should be sealed (gastrolog-24m1t: sealed = data.glcb on disk)")
	}
	chunkBDir := filepath.Join(dir, chunkB.String())
	if _, err := os.Stat(chunkBDir); err != nil {
		t.Fatalf("chunk B directory should exist: %v", err)
	}
}

// ── openActiveChunk / crash recovery tests ──────────────────────────

// appendAndClose creates a manager, appends records, and closes without sealing.
// Returns the directory and the chunk ID for reopening.
func appendAndClose(t *testing.T, records []chunk.Record) (string, chunk.ChunkID) {
	t.Helper()
	dir := t.TempDir()
	mgr, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	var id chunk.ChunkID
	for _, rec := range records {
		id, _, err = mgr.Append(rec)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return dir, id
}

func TestOpenActiveChunkHappyPath(t *testing.T) {
	t.Parallel()
	records := []chunk.Record{
		{IngestTS: time.UnixMicro(100), SourceTS: time.UnixMicro(50), Attrs: chunk.Attributes{"src": "a"}, Raw: []byte("hello")},
		{IngestTS: time.UnixMicro(200), SourceTS: time.UnixMicro(150), Attrs: chunk.Attributes{"src": "b"}, Raw: []byte("world")},
	}
	dir, chunkID := appendAndClose(t, records)

	// Reopen — openActiveChunk runs during NewManager.
	mgr, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr.Close()

	active := mgr.Active()
	if active == nil {
		t.Fatal("expected active chunk after reopen")
	}
	if active.ID != chunkID {
		t.Fatalf("chunk ID mismatch: want %s, got %s", chunkID, active.ID)
	}
	if active.RecordCount != 2 {
		t.Fatalf("record count: want 2, got %d", active.RecordCount)
	}

	// Verify records are readable.
	cursor, err := mgr.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()
	rec, _, err := cursor.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if string(rec.Raw) != "hello" {
		t.Fatalf("first record: want %q, got %q", "hello", string(rec.Raw))
	}
}

func TestOpenActiveChunkCrashTruncation(t *testing.T) {
	t.Parallel()
	records := []chunk.Record{
		{IngestTS: time.UnixMicro(100), Attrs: chunk.Attributes{"src": "a"}, Raw: []byte("rec1")},
		{IngestTS: time.UnixMicro(200), Attrs: chunk.Attributes{"src": "b"}, Raw: []byte("rec2")},
	}
	dir, chunkID := appendAndClose(t, records)

	// Simulate a crash: append garbage to raw.log and attr.log beyond what
	// idx.log accounts for.
	chunkDir := filepath.Join(dir, chunkID.String())
	for _, name := range []string{rawLogFileName, attrLogFileName} {
		path := filepath.Join(chunkDir, name)
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatalf("open %s: %v", name, err)
		}
		if _, err := f.Write([]byte("ORPHANED-CRASH-DATA")); err != nil {
			t.Fatalf("write orphan data to %s: %v", name, err)
		}
		f.Close()
	}

	// Reopen — should truncate the orphaned data.
	mgr, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen with orphaned data: %v", err)
	}
	defer mgr.Close()

	active := mgr.Active()
	if active == nil {
		t.Fatal("expected active chunk after reopen")
	}
	if active.RecordCount != 2 {
		t.Fatalf("record count: want 2, got %d", active.RecordCount)
	}

	// Verify original records survived truncation.
	cursor, err := mgr.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()
	for i, want := range []string{"rec1", "rec2"} {
		rec, _, err := cursor.Next()
		if err != nil {
			t.Fatalf("next[%d]: %v", i, err)
		}
		if string(rec.Raw) != want {
			t.Fatalf("record[%d]: want %q, got %q", i, want, string(rec.Raw))
		}
	}
}

func TestOpenActiveChunkEmptyChunk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	// Force-create an active chunk by appending then deleting the record's
	// chunk won't work — instead, just close an empty manager and verify
	// that reopening an empty directory is fine.
	mgr.Close()

	mgr, err = NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen empty: %v", err)
	}
	defer mgr.Close()

	if active := mgr.Active(); active != nil {
		t.Fatal("expected no active chunk in empty manager")
	}
}

func TestOpenActiveChunkCorruptIdxHeader(t *testing.T) {
	t.Parallel()
	records := []chunk.Record{
		{IngestTS: time.UnixMicro(100), Attrs: chunk.Attributes{"src": "a"}, Raw: []byte("rec")},
	}
	dir, chunkID := appendAndClose(t, records)

	// Corrupt the idx.log header.
	idxPath := filepath.Join(dir, chunkID.String(), idxLogFileName)
	if err := os.WriteFile(idxPath, []byte("BADHDR"), 0o644); err != nil {
		t.Fatalf("corrupt idx.log: %v", err)
	}

	// Reopen should fail with a descriptive error.
	_, err := NewManager(Config{Dir: dir})
	if err == nil {
		t.Fatal("expected error when reopening with corrupt idx.log header")
	}
}

func TestOpenActiveChunkMissingFile(t *testing.T) {
	t.Parallel()
	records := []chunk.Record{
		{IngestTS: time.UnixMicro(100), Attrs: chunk.Attributes{"src": "a"}, Raw: []byte("rec")},
	}
	dir, chunkID := appendAndClose(t, records)

	// Delete raw.log — openChunkFiles should fail.
	rawPath := filepath.Join(dir, chunkID.String(), rawLogFileName)
	os.Remove(rawPath)

	_, err := NewManager(Config{Dir: dir})
	if err == nil {
		t.Fatal("expected error when reopening with missing raw.log")
	}
}

func TestOpenActiveChunkDictRecovery(t *testing.T) {
	t.Parallel()
	// Append records with distinct attribute keys to populate the dictionary.
	records := []chunk.Record{
		{IngestTS: time.UnixMicro(100), Attrs: chunk.Attributes{"host": "web-1", "env": "prod"}, Raw: []byte("r1")},
		{IngestTS: time.UnixMicro(200), Attrs: chunk.Attributes{"host": "web-2", "env": "staging"}, Raw: []byte("r2")},
	}
	dir, chunkID := appendAndClose(t, records)

	// Reopen and verify dictionary survived.
	mgr, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr.Close()

	// Append a new record with the same keys — should reuse dict entries.
	rec := chunk.Record{
		IngestTS: time.UnixMicro(300),
		Attrs:    chunk.Attributes{"host": "web-3", "env": "dev"},
		Raw:      []byte("r3"),
	}
	_, _, err = mgr.Append(rec)
	if err != nil {
		t.Fatalf("append after reopen: %v", err)
	}

	active := mgr.Active()
	if active.RecordCount != 3 {
		t.Fatalf("record count: want 3, got %d", active.RecordCount)
	}

	// Verify all 3 records are readable with correct attrs.
	cursor, err := mgr.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()
	for i, wantRaw := range []string{"r1", "r2", "r3"} {
		rec, _, err := cursor.Next()
		if err != nil {
			t.Fatalf("next[%d]: %v", i, err)
		}
		if string(rec.Raw) != wantRaw {
			t.Fatalf("record[%d]: want %q, got %q", i, wantRaw, string(rec.Raw))
		}
		if rec.Attrs["host"] == "" {
			t.Fatalf("record[%d]: missing 'host' attr", i)
		}
	}
}

// Regression: cloud backfill can call UploadToCloud after the file manager is
// closed during tier removal; must return ErrManagerClosed instead of
// panicking on a nil zstd encoder (gastrolog: RF churn / node crash).
func TestUploadToCloudAfterClose(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	dir := t.TempDir()
	cm, err := NewManager(Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		CloudStore:     blobstore.NewMemory(),
		VaultID:        vaultID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cm.Close(); err != nil {
		t.Fatal(err)
	}
	err = cm.UploadToCloud(chunk.NewChunkID())
	if !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("UploadToCloud after Close: got %v, want ErrManagerClosed", err)
	}
}
