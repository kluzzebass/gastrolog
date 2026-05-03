package file

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
)

// TestSealToGLCB_ProducesValidBlob verifies that sealToGLCB writes a
// data.glcb file in the chunk directory and that it parses cleanly via
// the chunkcloud reader. Capability test for the gastrolog-24m1t step
// 7c machinery — sealToGLCB is not yet wired into the seal pipeline.
func TestSealToGLCB_ProducesValidBlob(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()

	now := time.Now().Truncate(time.Microsecond)
	const recordCount = 25
	var chunkID chunk.ChunkID
	for i := range recordCount {
		id, _, err := m.Append(chunk.Record{
			IngestTS: now.Add(time.Duration(i) * time.Millisecond),
			Attrs:    chunk.Attributes{"level": "info"},
			Raw:      []byte("payload"),
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		chunkID = id
	}
	if err := m.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	w, written, err := m.sealToGLCB(chunkID)
	if err != nil {
		t.Fatalf("sealToGLCB: %v", err)
	}
	if w == nil {
		t.Fatal("sealToGLCB returned nil writer")
	}
	if written <= 0 {
		t.Fatalf("expected non-zero bytes written, got %d", written)
	}

	glcbPath := filepath.Join(m.chunkDir(chunkID), dataGLCBFileName)
	info, err := os.Stat(glcbPath)
	if err != nil {
		t.Fatalf("stat data.glcb: %v", err)
	}
	if info.Size() != written {
		t.Fatalf("file size %d != reported written %d", info.Size(), written)
	}

	// data.glcb.tmp must NOT exist after a successful rename.
	if _, err := os.Stat(filepath.Join(m.chunkDir(chunkID), dataGLCBTmpFileName)); !os.IsNotExist(err) {
		t.Fatalf("data.glcb.tmp should be gone after seal, got err=%v", err)
	}

	// The TOC offsets should be sane and non-zero.
	toc := w.TOC()
	if toc.IngestIdxOffset <= 0 || toc.IngestIdxSize <= 0 {
		t.Fatalf("ingest TS index missing in TOC: offset=%d size=%d", toc.IngestIdxOffset, toc.IngestIdxSize)
	}

	// The blob should be readable via the chunkcloud reader; the metadata
	// it returns should match what we appended.
	f, err := os.Open(filepath.Clean(glcbPath))
	if err != nil {
		t.Fatalf("open data.glcb: %v", err)
	}
	defer func() { _ = f.Close() }()
	rd, err := chunkcloud.NewReader(f)
	if err != nil {
		t.Fatalf("open GLCB reader: %v", err)
	}
	defer func() { _ = rd.Close() }()
	if got := rd.Meta().RecordCount; got != recordCount {
		t.Fatalf("record count: got %d want %d", got, recordCount)
	}
}

// TestPostSealProcess_ProducesGLCB verifies that after PostSealProcess
// runs, the chunk directory contains a data.glcb file alongside the
// multi-file artifacts. Stage 2a of gastrolog-24m1t step 7c — sealToGLCB
// is now wired in but read paths still consume multi-file.
func TestPostSealProcess_ProducesGLCB(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()

	now := time.Now().Truncate(time.Microsecond)
	const recordCount = 10
	var chunkID chunk.ChunkID
	for i := range recordCount {
		id, _, err := m.Append(chunk.Record{
			IngestTS: now.Add(time.Duration(i) * time.Millisecond),
			Attrs:    chunk.Attributes{"level": "info"},
			Raw:      []byte("payload"),
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		chunkID = id
	}
	if err := m.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	if err := m.PostSealProcess(t.Context(), chunkID); err != nil {
		t.Fatalf("PostSealProcess: %v", err)
	}

	glcbPath := filepath.Join(m.chunkDir(chunkID), dataGLCBFileName)
	if _, err := os.Stat(glcbPath); err != nil {
		t.Fatalf("data.glcb missing after PostSealProcess: %v", err)
	}
	if _, err := os.Stat(filepath.Join(m.chunkDir(chunkID), dataGLCBTmpFileName)); !os.IsNotExist(err) {
		t.Fatalf("data.glcb.tmp should be gone after rename, got err=%v", err)
	}
}

// TestLoadChunkMetaFromGLCB verifies that a sealed chunk's metadata can
// be reconstructed from its data.glcb file alone, without reading
// idx.log. Capability test for the loadExisting migration in stage 3b
// of gastrolog-24m1t step 7c.
func TestLoadChunkMetaFromGLCB(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()

	now := time.Now().Truncate(time.Microsecond)
	const recordCount = 7
	var chunkID chunk.ChunkID
	for i := range recordCount {
		id, _, err := m.Append(chunk.Record{
			IngestTS: now.Add(time.Duration(i) * time.Millisecond),
			Attrs:    chunk.Attributes{"level": "info"},
			Raw:      []byte("payload"),
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		chunkID = id
	}
	if err := m.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	if err := m.PostSealProcess(t.Context(), chunkID); err != nil {
		t.Fatalf("PostSealProcess: %v", err)
	}

	got, err := m.loadChunkMetaFromGLCB(chunkID)
	if err != nil {
		t.Fatalf("loadChunkMetaFromGLCB: %v", err)
	}
	if got.id != chunkID {
		t.Errorf("id: got %v want %v", got.id, chunkID)
	}
	if got.recordCount != recordCount {
		t.Errorf("recordCount: got %d want %d", got.recordCount, recordCount)
	}
	if !got.sealed {
		t.Error("expected sealed=true")
	}
	if got.ingestIdxSize <= 0 {
		t.Errorf("expected non-zero ingestIdxSize, got %d", got.ingestIdxSize)
	}
}

// TestLoadExisting_GLCBOnlyChunk simulates the future state where
// sealed chunks live as data.glcb only (no multi-file artifacts). The
// test seals a chunk via the normal pipeline, then deletes the
// multi-file artifacts manually, restarts the manager, and verifies
// the sealed chunk is loaded from data.glcb. Regression-protection for
// gastrolog-24m1t step 7c stage 3b — once multi-file generation goes
// away, this is the steady-state load path.
func TestLoadExisting_GLCBOnlyChunk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	now := time.Now().Truncate(time.Microsecond)
	const recordCount = 5
	var chunkID chunk.ChunkID
	for i := range recordCount {
		id, _, err := m.Append(chunk.Record{
			IngestTS: now.Add(time.Duration(i) * time.Millisecond),
			Attrs:    chunk.Attributes{"level": "info"},
			Raw:      []byte("payload"),
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		chunkID = id
	}
	if err := m.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	if err := m.PostSealProcess(t.Context(), chunkID); err != nil {
		t.Fatalf("PostSealProcess: %v", err)
	}
	chunkDirPath := m.chunkDir(chunkID)
	_ = m.Close()

	// Delete the multi-file artifacts; leave only data.glcb.
	for _, name := range dataFileNames {
		_ = os.Remove(filepath.Join(chunkDirPath, name))
	}
	if _, err := os.Stat(filepath.Join(chunkDirPath, dataGLCBFileName)); err != nil {
		t.Fatalf("data.glcb missing after manual cleanup: %v", err)
	}

	// Re-open the manager. loadExisting should pick up the chunk via
	// the GLCB fallback rather than treating it as cloud-backed-only.
	m2, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("re-open manager: %v", err)
	}
	defer func() { _ = m2.Close() }()

	metas, err := m2.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var found *chunk.ChunkMeta
	for i := range metas {
		if metas[i].ID == chunkID {
			found = &metas[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("chunk %v not found after restart with GLCB-only layout", chunkID)
	}
	if !found.Sealed {
		t.Errorf("expected sealed=true, got %+v", found)
	}
	if found.RecordCount != recordCount {
		t.Errorf("recordCount: got %d want %d", found.RecordCount, recordCount)
	}

	// And the GLCB cursor must read records back correctly.
	cursor, err := m2.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("OpenCursor after restart: %v", err)
	}
	defer func() { _ = cursor.Close() }()
	var got int
	for {
		_, _, err := cursor.Next()
		if err != nil {
			break
		}
		got++
	}
	if got != recordCount {
		t.Errorf("read %d records via GLCB cursor, want %d", got, recordCount)
	}
}

// TestSealToGLCB_RefusesUnsealedChunk verifies that sealToGLCB on an
// unsealed (still-active) chunk does not silently produce a bogus
// blob — the OpenCursor call should fail because cursors against the
// active chunk follow a different path.
func TestSealToGLCB_RefusesUnsealedChunk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()

	id, _, err := m.Append(chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"level": "info"},
		Raw:      []byte("active"),
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	// Intentionally do NOT seal.

	if _, _, err := m.sealToGLCB(id); err == nil {
		t.Fatal("expected sealToGLCB to fail on unsealed chunk")
	}
}
