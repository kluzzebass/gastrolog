package file

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// buildSealedGLCB appends recordCount records, seals, and runs sealToGLCB so the
// chunk directory holds a data.glcb. Returns the chunk ID and the absolute GLCB
// path. Used by the external-GLCB registration tests as a stand-in for a
// pipeline-built blob (the pipeline builder lives in a package that imports
// chunk, so it can't be used from here without a cycle).
func buildSealedGLCB(t *testing.T, m *Manager, recordCount int) (chunk.ChunkID, string) {
	t.Helper()
	now := time.Now().Truncate(time.Microsecond)
	var chunkID chunk.ChunkID
	for i := range recordCount {
		id, _, err := m.Append(chunk.Record{
			IngestTS: now.Add(time.Duration(i) * time.Millisecond),
			Attrs:    chunk.Attributes{"level": "info"},
			Raw:      []byte("external-glcb-payload"),
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		chunkID = id
	}
	if err := m.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	if _, _, err := m.sealToGLCB(chunkID); err != nil {
		t.Fatalf("sealToGLCB: %v", err)
	}
	return chunkID, filepath.Join(m.chunkDir(chunkID), dataGLCBFileName)
}

func drainCursorRaw(t *testing.T, m *Manager, id chunk.ChunkID) [][]byte {
	t.Helper()
	cursor, err := m.OpenCursor(id)
	if err != nil {
		t.Fatalf("OpenCursor(%s): %v", id, err)
	}
	defer func() { _ = cursor.Close() }()
	var out [][]byte
	for {
		rec, _, err := cursor.Next()
		if err == chunk.ErrNoMoreRecords {
			return out
		}
		if err != nil {
			t.Fatalf("cursor Next: %v", err)
		}
		out = append(out, append([]byte(nil), rec.Raw...))
	}
}

// TestRegisterExternalGLCB_OpensByExternalPath: a sealed data.glcb living under
// a path OUTSIDE this manager's Dir becomes openable after RegisterExternalGLCB,
// reading the records from the registered path without any copy. Mirrors the
// pipeline case where the GLCB is owned by the vault ChunkRoot.
func TestRegisterExternalGLCB_OpensByExternalPath(t *testing.T) {
	t.Parallel()

	// Source manager owns the real bytes (stands in for the pipeline ChunkRoot).
	srcDir := t.TempDir()
	src, err := NewManager(Config{Dir: srcDir})
	if err != nil {
		t.Fatalf("new src manager: %v", err)
	}
	defer func() { _ = src.Close() }()
	const recordCount = 12
	chunkID, glcbPath := buildSealedGLCB(t, src, recordCount)
	want := drainCursorRaw(t, src, chunkID)
	if len(want) != recordCount {
		t.Fatalf("source read %d records, want %d", len(want), recordCount)
	}

	// Consumer manager (distinct Dir, like the legacy vault chunk-manager dir).
	cm, err := NewManager(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("new consumer manager: %v", err)
	}
	defer func() { _ = cm.Close() }()

	// Before registration the chunk is unknown.
	if _, err := cm.OpenCursor(chunkID); err != chunk.ErrChunkNotFound {
		t.Fatalf("OpenCursor before register = %v, want ErrChunkNotFound", err)
	}

	if err := cm.RegisterExternalGLCB(chunkID, glcbPath, chunk.ExternalGLCBInfo{RecordCount: recordCount}); err != nil {
		t.Fatalf("RegisterExternalGLCB: %v", err)
	}

	// hasLocalGLCB must resolve the external path, not chunkDir(id).
	if !cm.hasLocalGLCB(chunkID) {
		t.Fatal("hasLocalGLCB false after external registration")
	}
	if got := cm.glcbPath(chunkID); got != glcbPath {
		t.Fatalf("glcbPath = %q, want external %q", got, glcbPath)
	}

	got := drainCursorRaw(t, cm, chunkID)
	if len(got) != len(want) {
		t.Fatalf("consumer read %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("record %d raw = %q, want %q", i, got[i], want[i])
		}
	}

	// The chunk appears in List() (so the inspector/FSM-projection sees it).
	metas, err := cm.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var found bool
	for _, mm := range metas {
		if mm.ID == chunkID {
			found = true
			if !mm.Sealed {
				t.Error("external chunk should be sealed")
			}
		}
	}
	if !found {
		t.Error("external chunk missing from List()")
	}
}

// TestRegisterExternalGLCB_DoesNotShadowLocalChunk: a chunk this manager owns
// locally (sealed under its own Dir) must not be overridden by an external
// registration — its own bytes keep serving reads.
func TestRegisterExternalGLCB_DoesNotShadowLocalChunk(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cm, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = cm.Close() }()
	chunkID, _ := buildSealedGLCB(t, cm, 8)
	localPath := filepath.Join(cm.chunkDir(chunkID), dataGLCBFileName)

	// Attempt to register an external path for the SAME id (points elsewhere).
	bogus := filepath.Join(t.TempDir(), "elsewhere.glcb")
	if err := cm.RegisterExternalGLCB(chunkID, bogus, chunk.ExternalGLCBInfo{RecordCount: 8}); err != nil {
		t.Fatalf("RegisterExternalGLCB: %v", err)
	}

	// The locally-owned bytes still serve: glcbPath stays the local chunkDir.
	if got := cm.glcbPath(chunkID); got != localPath {
		t.Fatalf("glcbPath = %q, want local %q (external must not shadow local)", got, localPath)
	}
	if _, err := os.Stat(localPath); err != nil {
		t.Fatalf("local GLCB missing: %v", err)
	}
	if got := drainCursorRaw(t, cm, chunkID); len(got) != 8 {
		t.Fatalf("local chunk read %d records, want 8", len(got))
	}
}

// TestRegisterExternalGLCB_RefreshesPath: re-registering an external chunk
// updates the recorded path (idempotent refresh), and a missing external file
// is reported via hasLocalGLCB=false.
func TestRegisterExternalGLCB_RefreshesPath(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	src, err := NewManager(Config{Dir: srcDir})
	if err != nil {
		t.Fatalf("new src manager: %v", err)
	}
	defer func() { _ = src.Close() }()
	chunkID, glcbPath := buildSealedGLCB(t, src, 6)

	cm, err := NewManager(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("new consumer manager: %v", err)
	}
	defer func() { _ = cm.Close() }()

	// First register a non-existent path: known to the manager, bytes absent.
	missing := filepath.Join(t.TempDir(), "not-built-yet.glcb")
	if err := cm.RegisterExternalGLCB(chunkID, missing, chunk.ExternalGLCBInfo{RecordCount: 6}); err != nil {
		t.Fatalf("first RegisterExternalGLCB: %v", err)
	}
	if cm.hasLocalGLCB(chunkID) {
		t.Fatal("hasLocalGLCB true for a missing external path")
	}

	// Refresh with the real path: now the bytes resolve.
	if err := cm.RegisterExternalGLCB(chunkID, glcbPath, chunk.ExternalGLCBInfo{RecordCount: 6}); err != nil {
		t.Fatalf("refresh RegisterExternalGLCB: %v", err)
	}
	if got := cm.glcbPath(chunkID); got != glcbPath {
		t.Fatalf("glcbPath after refresh = %q, want %q", got, glcbPath)
	}
	if got := drainCursorRaw(t, cm, chunkID); len(got) != 6 {
		t.Fatalf("read %d records after refresh, want 6", len(got))
	}
}

// TestDeleteSilent_RemovesExternalGLCBDir: deleting a pipeline-registered external
// chunk removes the GLCB directory at the registered path (the vault ChunkRoot
// layout), not just the chunk-manager metadata entry.
func TestDeleteSilent_RemovesExternalGLCBDir(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	chunkRoot := filepath.Join(base, "chunks")
	builder, err := NewManager(Config{Dir: chunkRoot})
	if err != nil {
		t.Fatalf("new builder manager: %v", err)
	}
	defer func() { _ = builder.Close() }()
	chunkID, glcbPath := buildSealedGLCB(t, builder, 5)
	chunkDir := filepath.Dir(glcbPath)

	cm, err := NewManager(Config{Dir: filepath.Join(base, "vault-chunks")})
	if err != nil {
		t.Fatalf("new consumer manager: %v", err)
	}
	defer func() { _ = cm.Close() }()
	if err := cm.RegisterExternalGLCB(chunkID, glcbPath, chunk.ExternalGLCBInfo{RecordCount: 5}); err != nil {
		t.Fatalf("RegisterExternalGLCB: %v", err)
	}

	if err := cm.DeleteSilent(chunkID); err != nil {
		t.Fatalf("DeleteSilent: %v", err)
	}
	if _, err := os.Stat(chunkDir); !os.IsNotExist(err) {
		t.Fatalf("pipeline chunk dir %q still exists after delete: %v", chunkDir, err)
	}
	if _, err := cm.Meta(chunkID); !errors.Is(err, chunk.ErrChunkNotFound) {
		t.Fatalf("Meta after delete = %v, want ErrChunkNotFound", err)
	}
}

// TestUploadToCloud_ExternalGLCBPath verifies pipeline-registered GLCBs upload
// from their external path, not chunkDir(id). See gastrolog-34azvz.
func TestUploadToCloud_ExternalGLCBPath(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	src, err := NewManager(Config{Dir: srcDir})
	if err != nil {
		t.Fatalf("new src manager: %v", err)
	}
	defer func() { _ = src.Close() }()
	chunkID, glcbPath := buildSealedGLCB(t, src, 6)

	store := blobstore.NewMemory()
	vaultID := glid.New()
	cm, err := NewManager(Config{
		Dir:            t.TempDir(),
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
		CloudStore:     store,
		VaultID:        vaultID,
	})
	if err != nil {
		t.Fatalf("new consumer manager: %v", err)
	}
	defer func() { _ = cm.Close() }()

	if err := cm.RegisterExternalGLCB(chunkID, glcbPath, chunk.ExternalGLCBInfo{RecordCount: 6}); err != nil {
		t.Fatalf("RegisterExternalGLCB: %v", err)
	}
	if err := cm.UploadToCloud(chunkID); err != nil {
		t.Fatalf("UploadToCloud: %v", err)
	}

	var count int
	if err := store.List(context.Background(), "", func(blobstore.BlobInfo) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("blob count = %d, want 1", count)
	}
}
