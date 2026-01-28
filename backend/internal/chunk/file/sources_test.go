package file

import (
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/chunk"
)

func TestSourceMapPersistAndReload(t *testing.T) {
	dir := t.TempDir()

	map1 := NewSourceMap(dir, 0o644)
	sourceID := chunk.NewSourceID()

	localID, created, err := map1.GetOrAssign(sourceID)
	if err != nil {
		t.Fatalf("get or assign: %v", err)
	}
	if !created {
		t.Fatalf("expected new mapping")
	}
	if localID == 0 {
		t.Fatalf("expected non-zero local id")
	}

	localID2, created, err := map1.GetOrAssign(sourceID)
	if err != nil {
		t.Fatalf("get or assign (repeat): %v", err)
	}
	if created {
		t.Fatalf("expected existing mapping")
	}
	if localID2 != localID {
		t.Fatalf("expected local id %d got %d", localID, localID2)
	}

	map2 := NewSourceMap(dir, 0o644)
	if err := map2.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	resolved, ok := map2.Resolve(localID)
	if !ok {
		t.Fatalf("expected resolve to succeed")
	}
	if resolved != sourceID {
		t.Fatalf("expected source id %s got %s", sourceID.String(), resolved.String())
	}

	if _, err := os.Stat(filepath.Join(dir, sourcesFileName)); err != nil {
		t.Fatalf("sources file missing: %v", err)
	}
}
