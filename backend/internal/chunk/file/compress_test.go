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

	"github.com/klauspost/compress/zstd"
)

// =============================================================================
// Low-Level Compression Round-Trip Tests
// =============================================================================

func TestCompressDecompressRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Write a file with a standard header + data section.
	path := filepath.Join(dir, "test.log")
	header := format.Header{Type: format.TypeRawLog, Version: RawLogVersion, Flags: format.FlagSealed}
	headerBytes := header.Encode()
	data := bytes.Repeat([]byte("hello world "), 1000)

	if err := os.WriteFile(path, append(headerBytes[:], data...), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Create encoder for test.
	enc, err := newTestEncoder()
	if err != nil {
		t.Fatalf("new encoder: %v", err)
	}
	defer enc.Close()

	if err := compressFile(path, enc, 0o644); err != nil {
		t.Fatalf("compress: %v", err)
	}

	// Read back via readFileData.
	got, compressed, err := readFileData(path)
	if err != nil {
		t.Fatalf("readFileData: %v", err)
	}
	if !compressed {
		t.Fatal("expected compressed=true")
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch: want %d bytes, got %d bytes", len(data), len(got))
	}
}

func TestCompressFlagSet(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")

	header := format.Header{Type: format.TypeRawLog, Version: RawLogVersion, Flags: format.FlagSealed}
	headerBytes := header.Encode()
	if err := os.WriteFile(path, append(headerBytes[:], []byte("data")...), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	enc, err := newTestEncoder()
	if err != nil {
		t.Fatalf("new encoder: %v", err)
	}
	defer enc.Close()

	if err := compressFile(path, enc, 0o644); err != nil {
		t.Fatalf("compress: %v", err)
	}

	// Read raw header bytes and check flag.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	h, err := format.Decode(raw[:format.HeaderSize])
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}

	if h.Flags&format.FlagCompressed == 0 {
		t.Fatal("FlagCompressed not set in header")
	}
	if h.Flags&format.FlagSealed == 0 {
		t.Fatal("FlagSealed was lost after compression")
	}
}

func TestUncompressedPassthrough(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")

	header := format.Header{Type: format.TypeRawLog, Version: RawLogVersion, Flags: format.FlagSealed}
	headerBytes := header.Encode()
	data := []byte("uncompressed data")
	if err := os.WriteFile(path, append(headerBytes[:], data...), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, compressed, err := readFileData(path)
	if err != nil {
		t.Fatalf("readFileData: %v", err)
	}
	if compressed {
		t.Fatal("expected compressed=false for uncompressed file")
	}
	if got != nil {
		t.Fatal("expected nil data for uncompressed file (caller should mmap)")
	}
}

func TestCompressEmptyDataSection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")

	header := format.Header{Type: format.TypeRawLog, Version: RawLogVersion, Flags: format.FlagSealed}
	headerBytes := header.Encode()
	if err := os.WriteFile(path, headerBytes[:], 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	enc, err := newTestEncoder()
	if err != nil {
		t.Fatalf("new encoder: %v", err)
	}
	defer enc.Close()

	if err := compressFile(path, enc, 0o644); err != nil {
		t.Fatalf("compress: %v", err)
	}

	got, compressed, err := readFileData(path)
	if err != nil {
		t.Fatalf("readFileData: %v", err)
	}
	if !compressed {
		t.Fatal("expected compressed=true")
	}
	if len(got) != 0 {
		t.Fatalf("expected empty data, got %d bytes", len(got))
	}
}

// =============================================================================
// End-to-End Compression Tests (via Manager)
// =============================================================================

func TestCompressedChunkRoundTrip(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{
		Dir:         dir,
		Compression: CompressionZstd,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	records := []chunk.Record{
		{IngestTS: time.UnixMicro(1000), Attrs: chunk.Attributes{"host": "server-1", "env": "prod"}, Raw: []byte("log line 1")},
		{IngestTS: time.UnixMicro(2000), Attrs: chunk.Attributes{"host": "server-2"}, Raw: []byte("log line 2")},
		{IngestTS: time.UnixMicro(3000), Attrs: chunk.Attributes{}, Raw: []byte("log line 3")},
		{IngestTS: time.UnixMicro(4000), Attrs: chunk.Attributes{"path": "/var/log"}, Raw: bytes.Repeat([]byte("x"), 5000)},
	}

	var chunkID chunk.ChunkID
	for i, rec := range records {
		id, _, err := manager.Append(rec)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		chunkID = id
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Verify compressed files have FlagCompressed set.
	chunkDir := filepath.Join(dir, chunkID.String())
	for _, name := range []string{rawLogFileName, attrLogFileName} {
		data, err := os.ReadFile(filepath.Join(chunkDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		h, err := format.Decode(data[:format.HeaderSize])
		if err != nil {
			t.Fatalf("decode %s header: %v", name, err)
		}
		if h.Flags&format.FlagCompressed == 0 {
			t.Fatalf("%s: FlagCompressed not set", name)
		}
	}

	// Verify idx.log and attr_dict.log do NOT have FlagCompressed.
	for _, name := range []string{idxLogFileName, attrDictFileName} {
		data, err := os.ReadFile(filepath.Join(chunkDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		h, err := format.Decode(data[:format.HeaderSize])
		if err != nil {
			t.Fatalf("decode %s header: %v", name, err)
		}
		if h.Flags&format.FlagCompressed != 0 {
			t.Fatalf("%s: FlagCompressed should NOT be set", name)
		}
	}

	// Read back all records via cursor.
	cursor, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()

	for i, expected := range records {
		got, _, err := cursor.Next()
		if err != nil {
			t.Fatalf("record %d: next: %v", i, err)
		}
		if !bytes.Equal(got.Raw, expected.Raw) {
			t.Fatalf("record %d: raw mismatch", i)
		}
		for k, v := range expected.Attrs {
			if got.Attrs[k] != v {
				t.Fatalf("record %d: attr %q: want %q, got %q", i, k, v, got.Attrs[k])
			}
		}
	}
	if _, _, err := cursor.Next(); err != chunk.ErrNoMoreRecords {
		t.Fatalf("expected ErrNoMoreRecords, got %v", err)
	}
}

func TestCompressedChunkReverseCursor(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{
		Dir:         dir,
		Compression: CompressionZstd,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	records := []chunk.Record{
		{IngestTS: time.UnixMicro(100), Attrs: chunk.Attributes{"src": "test"}, Raw: []byte("first")},
		{IngestTS: time.UnixMicro(200), Attrs: chunk.Attributes{"src": "test"}, Raw: []byte("second")},
		{IngestTS: time.UnixMicro(300), Attrs: chunk.Attributes{"src": "test"}, Raw: []byte("third")},
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

	for i := len(records) - 1; i >= 0; i-- {
		got, _, err := cursor.Prev()
		if err != nil {
			t.Fatalf("prev record %d: %v", i, err)
		}
		if string(got.Raw) != string(records[i].Raw) {
			t.Fatalf("record %d: want %q, got %q", i, records[i].Raw, got.Raw)
		}
	}
	if _, _, err := cursor.Prev(); err != chunk.ErrNoMoreRecords {
		t.Fatalf("expected ErrNoMoreRecords, got %v", err)
	}
}

func TestCompressedFilesAreSmaller(t *testing.T) {
	// Write identical data with and without compression, compare sizes.
	records := make([]chunk.Record, 100)
	for i := range records {
		records[i] = chunk.Record{
			IngestTS: time.UnixMicro(int64(i * 1000)),
			Attrs:    chunk.Attributes{"host": "server-001", "env": "prod"},
			Raw:      []byte(strings.Repeat("x", 200)),
		}
	}

	// Uncompressed.
	dirNone := t.TempDir()
	mNone, err := NewManager(Config{Dir: dirNone, Compression: CompressionNone})
	if err != nil {
		t.Fatalf("new manager (none): %v", err)
	}
	var idNone chunk.ChunkID
	for _, rec := range records {
		id, _, err := mNone.Append(rec)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		idNone = id
	}
	if err := mNone.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	mNone.Close()

	// Compressed.
	dirZstd := t.TempDir()
	mZstd, err := NewManager(Config{Dir: dirZstd, Compression: CompressionZstd})
	if err != nil {
		t.Fatalf("new manager (zstd): %v", err)
	}
	var idZstd chunk.ChunkID
	for _, rec := range records {
		id, _, err := mZstd.Append(rec)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		idZstd = id
	}
	if err := mZstd.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	mZstd.Close()

	// Compare raw.log sizes.
	noneRawInfo, err := os.Stat(filepath.Join(dirNone, idNone.String(), rawLogFileName))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	zstdRawInfo, err := os.Stat(filepath.Join(dirZstd, idZstd.String(), rawLogFileName))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	if zstdRawInfo.Size() >= noneRawInfo.Size() {
		t.Fatalf("compressed raw.log (%d) should be smaller than uncompressed (%d)",
			zstdRawInfo.Size(), noneRawInfo.Size())
	}
	t.Logf("raw.log: %d -> %d (%.0f%% reduction)",
		noneRawInfo.Size(), zstdRawInfo.Size(),
		100*(1-float64(zstdRawInfo.Size())/float64(noneRawInfo.Size())))
}

func TestCompressedChunkRotation(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{
		Dir:            dir,
		Compression:    CompressionZstd,
		RotationPolicy: chunk.NewSizePolicy(500),
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer manager.Close()

	raw := bytes.Repeat([]byte("x"), 100)
	attrs := chunk.Attributes{"test": "rotation"}
	chunkIDs := make(map[chunk.ChunkID]bool)

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
	}

	if len(chunkIDs) < 2 {
		t.Fatalf("expected rotation, got %d chunk(s)", len(chunkIDs))
	}

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Verify all records across all chunks.
	metas, err := manager.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}

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
			if rec.Attrs["test"] != "rotation" {
				t.Fatal("attrs mismatch")
			}
			if !bytes.Equal(rec.Raw, raw) {
				t.Fatal("raw mismatch")
			}
			totalRecords++
		}
		cursor.Close()
	}

	if totalRecords != 20 {
		t.Fatalf("total records: want 20, got %d", totalRecords)
	}
}

func TestCompressedChunkPersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	attrs := chunk.Attributes{"host": "server-001", "env": "prod"}
	raw := []byte("important compressed log")
	var chunkID chunk.ChunkID

	// First session: write, seal (with compression), close.
	{
		m, err := NewManager(Config{Dir: dir, Compression: CompressionZstd})
		if err != nil {
			t.Fatalf("new manager: %v", err)
		}
		id, _, err := m.Append(chunk.Record{IngestTS: time.UnixMicro(1000), Attrs: attrs, Raw: raw})
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		chunkID = id
		if err := m.Seal(); err != nil {
			t.Fatalf("seal: %v", err)
		}
		if err := m.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}

	// Second session: reopen WITHOUT compression config (reads must still work).
	{
		m, err := NewManager(Config{Dir: dir})
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer m.Close()

		cursor, err := m.OpenCursor(chunkID)
		if err != nil {
			t.Fatalf("open cursor: %v", err)
		}
		defer cursor.Close()

		got, _, err := cursor.Next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if !bytes.Equal(got.Raw, raw) {
			t.Fatalf("raw mismatch: want %q, got %q", raw, got.Raw)
		}
		for k, v := range attrs {
			if got.Attrs[k] != v {
				t.Fatalf("attr %q: want %q, got %q", k, v, got.Attrs[k])
			}
		}
	}
}

func TestCompressedEmptyChunk(t *testing.T) {
	dir := t.TempDir()
	manager, err := NewManager(Config{
		Dir:         dir,
		Compression: CompressionZstd,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	// Seal without any records.
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

	cursor, err := manager.OpenCursor(metas[0].ID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()

	if _, _, err := cursor.Next(); err != chunk.ErrNoMoreRecords {
		t.Fatalf("expected ErrNoMoreRecords, got %v", err)
	}

	manager.Close()
}

// =============================================================================
// Helpers
// =============================================================================

func newTestEncoder() (*zstd.Encoder, error) {
	return zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
}
