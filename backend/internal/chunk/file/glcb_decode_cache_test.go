package file

import (
	"testing"

	"gastrolog/internal/chunk"
)

// TestGLCBDecodeLRUEvictsUnpinned verifies that opening a new decode evicts
// another chunk's dict/index when it is in the LRU without active pins.
func TestGLCBDecodeLRUEvictsUnpinned(t *testing.T) {
	t.Parallel()

	oldCap := glcbDecodedTablesCap
	glcbDecodedTablesCap = 1
	t.Cleanup(func() { glcbDecodedTablesCap = oldCap })

	srcDir := t.TempDir()
	src, err := NewManager(Config{Dir: srcDir})
	if err != nil {
		t.Fatalf("src manager: %v", err)
	}
	defer func() { _ = src.Close() }()

	idA, glcbA := buildSealedGLCB(t, src, 4)
	idB, glcbB := buildSealedGLCB(t, src, 4)

	cmDir := t.TempDir()
	cm, err := NewManager(Config{Dir: cmDir})
	if err != nil {
		t.Fatalf("consumer manager: %v", err)
	}
	defer func() { _ = cm.Close() }()

	if err := cm.RegisterExternalGLCB(idA, glcbA, chunk.ExternalGLCBInfo{RecordCount: 4}); err != nil {
		t.Fatalf("register A: %v", err)
	}
	if err := cm.RegisterExternalGLCB(idB, glcbB, chunk.ExternalGLCBInfo{RecordCount: 4}); err != nil {
		t.Fatalf("register B: %v", err)
	}

	blobA, err := cm.mappedGLCB(idA)
	if err != nil {
		t.Fatalf("mappedGLCB A: %v", err)
	}
	blobA.Retain()
	if _, err := blobA.Reader(); err != nil {
		t.Fatalf("Reader A: %v", err)
	}
	cm.noteGLCBDecoded(idA)
	blobA.Release()
	if !blobA.RecordTablesLoaded() {
		t.Fatal("A should keep decode tables until LRU evict or cursor close")
	}

	blobB, err := cm.mappedGLCB(idB)
	if err != nil {
		t.Fatalf("mappedGLCB B: %v", err)
	}
	blobB.Retain()
	if _, err := blobB.Reader(); err != nil {
		t.Fatalf("Reader B: %v", err)
	}
	cm.noteGLCBDecoded(idB)
	if blobA.RecordTablesLoaded() {
		t.Fatal("A decode tables should have been LRU-evicted when B opened")
	}
	blobB.Release()
	cm.releaseGLCBDecodeTables(idB, blobB)
	if blobB.RecordTablesLoaded() {
		t.Fatal("B tables should release on cursor close path")
	}
}

func TestOpenLocalGLCBReleasesDecodeAfterClose(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	src, err := NewManager(Config{Dir: srcDir})
	if err != nil {
		t.Fatalf("src manager: %v", err)
	}
	defer func() { _ = src.Close() }()

	id, glcbPath := buildSealedGLCB(t, src, 6)

	cmDir := t.TempDir()
	cm, err := NewManager(Config{Dir: cmDir})
	if err != nil {
		t.Fatalf("consumer manager: %v", err)
	}
	defer func() { _ = cm.Close() }()

	if err := cm.RegisterExternalGLCB(id, glcbPath, chunk.ExternalGLCBInfo{RecordCount: 6}); err != nil {
		t.Fatalf("RegisterExternalGLCB: %v", err)
	}

	cursor, err := cm.openLocalGLCBCursor(id)
	if err != nil {
		t.Fatalf("openLocalGLCBCursor: %v", err)
	}
	blob := cm.mappedGLCBBlob(id)
	if blob == nil || !blob.RecordTablesLoaded() {
		t.Fatal("expected decode tables while cursor open")
	}
	if err := cursor.Close(); err != nil {
		t.Fatalf("close cursor: %v", err)
	}
	if blob.RecordTablesLoaded() {
		t.Fatal("decode tables should be released after cursor close")
	}
}
