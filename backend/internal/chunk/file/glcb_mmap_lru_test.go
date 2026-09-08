package file

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"gastrolog/internal/chunk"
)

func lowerMappedCap(t *testing.T, n int) {
	t.Helper()
	old := glcbMappedCap
	glcbMappedCap = n
	t.Cleanup(func() { glcbMappedCap = old })
}

// registerExternalCopies builds n sealed GLCBs in a source manager and
// registers each as an external chunk of the consumer; returns the IDs.
func registerExternalCopies(t *testing.T, n int) (*Manager, []chunk.ChunkID) {
	t.Helper()
	src, err := NewManager(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("src manager: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	cm, err := NewManager(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("consumer manager: %v", err)
	}
	t.Cleanup(func() { _ = cm.Close() })

	ids := make([]chunk.ChunkID, 0, n)
	for range n {
		id, path := buildSealedGLCB(t, src, 4)
		if err := cm.RegisterExternalGLCB(id, path, chunk.ExternalGLCBInfo{RecordCount: 4}); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
		ids = append(ids, id)
	}
	return cm, ids
}

// A mapping handed to a caller is pinned, so the cap cannot close it under
// them; once released it is ordinary LRU fodder again.
func TestGLCBMapLRUNeverEvictsAPinnedMapping(t *testing.T) {
	lowerMappedCap(t, 1)
	cm, ids := registerExternalCopies(t, 3)
	idA, idB, idC := ids[0], ids[1], ids[2]

	blobA, err := cm.mappedGLCB(idA)
	if err != nil {
		t.Fatalf("mappedGLCB A: %v", err)
	}
	blobB, err := cm.mappedGLCB(idB)
	if err != nil {
		t.Fatalf("mappedGLCB B: %v", err)
	}
	if _, ok := cm.glcbMapped.Load(idA); !ok {
		t.Fatal("A is pinned by its caller and must survive B exceeding the cap")
	}
	if _, err := blobA.Reader(); err != nil {
		t.Fatalf("A must stay readable while pinned: %v", err)
	}

	blobA.Release()
	blobB.Release()
	// A fresh mapping enforces the cap; A and B, now unpinned, are the ones
	// to go, and the newly pinned C stays.
	blobC, err := cm.mappedGLCB(idC)
	if err != nil {
		t.Fatalf("mappedGLCB C: %v", err)
	}
	defer blobC.Release()
	for _, id := range []chunk.ChunkID{idA, idB} {
		if _, ok := cm.glcbMapped.Load(id); ok {
			t.Fatalf("%s should be LRU-evicted once released and over cap", id)
		}
	}
	if _, ok := cm.glcbMapped.Load(idC); !ok {
		t.Fatal("C is pinned and must stay mapped")
	}
}

// A search holds one cursor per chunk for its whole run. Opening more of them
// than the map cap must not fail the newest one; the failure on old code was
// laundered into "open attr_dict: no such file" by the multi-file fallback.
func TestOpenCursorsAcrossMoreChunksThanTheMapCap(t *testing.T) {
	lowerMappedCap(t, 3)
	cm, ids := registerExternalCopies(t, 6)

	cursors := make([]chunk.RecordCursor, 0, len(ids))
	defer func() {
		for _, c := range cursors {
			_ = c.Close()
		}
	}()
	for i, id := range ids {
		c, err := cm.OpenCursor(id)
		if err != nil {
			t.Fatalf("OpenCursor #%d (%s) with %d cursors already open: %v", i+1, id, len(cursors), err)
		}
		cursors = append(cursors, c)
	}
	for i, c := range cursors {
		rec, _, err := c.Next()
		if err != nil {
			t.Fatalf("cursor #%d Next: %v", i+1, err)
		}
		if string(rec.Raw) != "external-glcb-payload" {
			t.Fatalf("cursor #%d read %q", i+1, rec.Raw)
		}
	}
}

// When only data.glcb exists and it cannot be opened, that is the error the
// caller must see — not a complaint about a legacy file that was never there.
func TestOpenCursorFailsLoudlyWhenTheGLCBIsBroken(t *testing.T) {
	cm, ids := registerExternalCopies(t, 1)
	id := ids[0]
	path := cm.glcbPath(id)
	if err := os.Truncate(path, 8); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	_, err := cm.OpenCursor(id)
	if err == nil {
		t.Fatal("OpenCursor succeeded on a truncated data.glcb")
	}
	if !strings.Contains(err.Error(), dataGLCBFileName) {
		t.Fatalf("error must name the GLCB that failed, got: %v", err)
	}
	if strings.Contains(err.Error(), "attr_dict") {
		t.Fatalf("error blames a legacy file that does not exist: %v", err)
	}
	if !errors.Is(err, chunk.ErrChunkNotFound) && strings.Contains(err.Error(), "no such file") {
		t.Fatalf("error must not read as a missing file: %v", err)
	}
}

// Many readers racing against a cap of one: every mapping a reader receives
// must be readable until that reader releases it.
func TestMappedGLCBConcurrentPinAndEvict(t *testing.T) {
	lowerMappedCap(t, 1)
	cm, ids := registerExternalCopies(t, 3)

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 60 {
				id := ids[(g+i)%len(ids)]
				blob, err := cm.mappedGLCB(id)
				if err != nil {
					errs <- fmt.Errorf("g%d i%d mappedGLCB: %w", g, i, err)
					return
				}
				if _, err := blob.Reader(); err != nil {
					errs <- fmt.Errorf("g%d i%d Reader on a mapping we hold: %w", g, i, err)
					blob.Release()
					return
				}
				blob.Release()
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
