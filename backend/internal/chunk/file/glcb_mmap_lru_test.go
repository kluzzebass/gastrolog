package file

import (
	"testing"

	"gastrolog/internal/chunk"
)

func TestGLCBMapLRUEvictsUnpinned(t *testing.T) {

	oldCap := glcbMappedCap
	glcbMappedCap = 1
	t.Cleanup(func() { glcbMappedCap = oldCap })

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
	if _, ok := cm.glcbMapped.Load(idA); !ok {
		t.Fatal("A should stay mapped")
	}

	blobB, err := cm.mappedGLCB(idB)
	if err != nil {
		t.Fatalf("mappedGLCB B: %v", err)
	}
	if _, ok := cm.glcbMapped.Load(idA); ok {
		t.Fatal("A mmap should be LRU-evicted when B opens")
	}
	if _, ok := cm.glcbMapped.Load(idB); !ok {
		t.Fatal("B should stay mapped")
	}
	_ = blobA
	_ = blobB
}
