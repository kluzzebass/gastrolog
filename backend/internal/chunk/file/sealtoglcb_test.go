package file

import (
	"crypto/sha256"
	"io"
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

// TestAdoptSealedBlob_RoundTrip writes a real GLCB on a "leader" manager,
// reads its bytes back as if they came over the wire, hands them to a
// "follower" manager's AdoptSealedBlob, and verifies the follower ends
// up with an equivalent sealed chunk and the digests match end to end.
func TestAdoptSealedBlob_RoundTrip(t *testing.T) {
	t.Parallel()

	// --- Leader: build a sealed chunk + a real data.glcb file. ---
	leaderDir := t.TempDir()
	leader, err := NewManager(Config{Dir: leaderDir})
	if err != nil {
		t.Fatalf("new leader: %v", err)
	}
	defer func() { _ = leader.Close() }()

	now := time.Now().Truncate(time.Microsecond)
	const recordCount = 50
	var chunkID chunk.ChunkID
	for i := range recordCount {
		id, _, err := leader.Append(chunk.Record{
			IngestTS: now.Add(time.Duration(i) * time.Millisecond),
			Attrs:    chunk.Attributes{"level": "info"},
			Raw:      []byte("payload"),
		})
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		chunkID = id
	}
	if err := leader.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	if _, _, err := leader.sealToGLCB(chunkID); err != nil {
		t.Fatalf("sealToGLCB: %v", err)
	}

	leaderBlobPath := filepath.Join(leader.chunkDir(chunkID), dataGLCBFileName)
	blobBytes, err := os.ReadFile(filepath.Clean(leaderBlobPath))
	if err != nil {
		t.Fatalf("read leader's data.glcb: %v", err)
	}
	leaderDigest := sha256Bytes(blobBytes)

	// --- Follower: install the blob via AdoptSealedBlob. ---
	followerDir := t.TempDir()
	follower, err := NewManager(Config{Dir: followerDir})
	if err != nil {
		t.Fatalf("new follower: %v", err)
	}
	defer func() { _ = follower.Close() }()

	digest, err := follower.AdoptSealedBlob(chunkID, int64(len(blobBytes)), bytesReader(blobBytes))
	if err != nil {
		t.Fatalf("AdoptSealedBlob: %v", err)
	}
	if digest != leaderDigest {
		t.Errorf("digest mismatch:\n got %x\nwant %x", digest[:], leaderDigest[:])
	}

	// data.glcb should exist at the canonical path; .tmp should not.
	followerBlobPath := filepath.Join(follower.chunkDir(chunkID), dataGLCBFileName)
	if _, err := os.Stat(followerBlobPath); err != nil {
		t.Fatalf("follower data.glcb missing: %v", err)
	}
	if _, err := os.Stat(followerBlobPath + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("follower data.glcb.tmp should be gone after rename, got err=%v", err)
	}

	// Bytes on disk must be byte-identical to what the leader had.
	got, err := os.ReadFile(filepath.Clean(followerBlobPath))
	if err != nil {
		t.Fatalf("read follower data.glcb: %v", err)
	}
	if sha256Bytes(got) != leaderDigest {
		t.Errorf("follower's on-disk digest differs from leader's")
	}

	// Chunk should be queryable on the follower.
	metas, err := follower.List()
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
		t.Fatalf("chunk %s not in follower's List", chunkID)
	}
	if !found.Sealed {
		t.Errorf("expected chunk to be sealed on follower")
	}
	if found.RecordCount != recordCount {
		t.Errorf("RecordCount: got %d want %d", found.RecordCount, recordCount)
	}
}

// TestAdoptSealedBlob_RejectsDuplicateLocalChunk pins the precondition
// that AdoptSealedBlob refuses to overwrite an existing local chunk.
func TestAdoptSealedBlob_RejectsDuplicateLocalChunk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()

	id, _, err := m.Append(chunk.Record{
		IngestTS: time.Now(),
		Raw:      []byte("p"),
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := m.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Pretend a peer is trying to push the same chunk.
	if _, err := m.AdoptSealedBlob(id, 100, bytesReader([]byte("not used"))); err == nil {
		t.Fatal("expected error when chunk is already present locally")
	}
}

// TestAdoptSealedBlob_RejectsCorruptBlob pins behaviour on bad input:
// random bytes should fail TOC validation, the .tmp file should be
// removed, no meta entry should appear.
func TestAdoptSealedBlob_RejectsCorruptBlob(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()

	garbage := make([]byte, 1024)
	for i := range garbage {
		garbage[i] = byte(i % 256)
	}

	var id chunk.ChunkID
	if _, err := m.AdoptSealedBlob(id, int64(len(garbage)), bytesReader(garbage)); err == nil {
		t.Fatal("expected AdoptSealedBlob to reject random bytes")
	}

	// .tmp must be cleaned up.
	tmpPath := filepath.Join(m.chunkDir(id), dataGLCBTmpFileName)
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("data.glcb.tmp should be removed on failed adopt, got err=%v", err)
	}
}

// sha256Bytes is a tiny helper — local to keep the test file self-contained
// rather than reaching into another package's helper.
func sha256Bytes(b []byte) [32]byte {
	return sha256.Sum256(b)
}

func bytesReader(b []byte) io.Reader {
	return &slicedReader{p: b}
}

type slicedReader struct {
	p   []byte
	off int
}

func (r *slicedReader) Read(p []byte) (int, error) {
	if r.off >= len(r.p) {
		return 0, io.EOF
	}
	n := copy(p, r.p[r.off:])
	r.off += n
	return n, nil
}

// TestAdoptSealedBlob_TruncatedBodyRejected pins the partial-RPC path:
// the leader said `totalSize=N` in the header but the stream EOFs early.
// AdoptSealedBlob must reject without leaving half-state on disk.
func TestAdoptSealedBlob_TruncatedBodyRejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()

	// Build a real GLCB to know what valid bytes look like, then truncate.
	leaderDir := t.TempDir()
	leader, err := NewManager(Config{Dir: leaderDir})
	if err != nil {
		t.Fatalf("new leader: %v", err)
	}
	defer func() { _ = leader.Close() }()

	id, _, err := leader.Append(chunk.Record{
		IngestTS: time.Now(), Raw: []byte("rec"),
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := leader.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	if _, _, err := leader.sealToGLCB(id); err != nil {
		t.Fatalf("sealToGLCB: %v", err)
	}
	blobBytes, err := os.ReadFile(filepath.Clean(filepath.Join(leader.chunkDir(id), dataGLCBFileName)))
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}

	// Lie about totalSize: declare full length but only feed half.
	half := len(blobBytes) / 2
	if _, err := m.AdoptSealedBlob(id, int64(len(blobBytes)), bytesReader(blobBytes[:half])); err == nil {
		t.Fatal("expected AdoptSealedBlob to reject truncated body")
	}

	// .tmp must be cleaned up.
	tmpPath := filepath.Join(m.chunkDir(id), dataGLCBTmpFileName)
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("data.glcb.tmp should be removed on size mismatch, got err=%v", err)
	}
	// data.glcb must NOT exist (the rename never happened).
	finalPath := filepath.Join(m.chunkDir(id), dataGLCBFileName)
	if _, err := os.Stat(finalPath); !os.IsNotExist(err) {
		t.Errorf("data.glcb should not exist after a rejected adopt, got err=%v", err)
	}
}

// TestAdoptSealedBlob_StaleTmpRejected pins the follower-restart-
// mid-receive path: a prior AdoptSealedBlob aborted, leaving a
// data.glcb.tmp on disk. A retry by the leader must surface the stale
// tmp clearly (O_EXCL fails) instead of silently clobbering it.
//
// The cleanup of stale .tmp on startup is loadExisting's job (existing
// behaviour for the seal pipeline's tmp files); within a single
// process AdoptSealedBlob refusing to overwrite is the correct local
// invariant.
func TestAdoptSealedBlob_StaleTmpRejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()

	id := chunk.ChunkID{1, 2, 3}
	chunkDir := m.chunkDir(id)
	if err := os.MkdirAll(chunkDir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	tmpPath := filepath.Join(chunkDir, dataGLCBTmpFileName)
	if err := os.WriteFile(tmpPath, []byte("stale-prior-attempt"), 0o644); err != nil {
		t.Fatalf("write stale tmp: %v", err)
	}

	if _, err := m.AdoptSealedBlob(id, 100, bytesReader([]byte("doesn't matter"))); err == nil {
		t.Fatal("expected AdoptSealedBlob to refuse a stale data.glcb.tmp")
	}
}
