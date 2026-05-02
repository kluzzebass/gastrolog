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
