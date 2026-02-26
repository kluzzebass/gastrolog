package memory

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

func testMeta() chunk.ChunkMeta {
	return chunk.ChunkMeta{
		ID:          chunk.NewChunkID(),
		StartTS:     time.UnixMicro(1000),
		EndTS:       time.UnixMicro(2000),
		RecordCount: 4096,
		Sealed:      true,
	}
}

func TestMetaStoreSaveLoad(t *testing.T) {
	store := NewMetaStore()

	meta := testMeta()
	if err := store.Save(meta); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := store.Load(meta.ID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if got.ID != meta.ID {
		t.Fatalf("ID: expected %s, got %s", meta.ID, got.ID)
	}
	if !got.StartTS.Equal(meta.StartTS) {
		t.Fatalf("StartTS: expected %v, got %v", meta.StartTS, got.StartTS)
	}
	if !got.EndTS.Equal(meta.EndTS) {
		t.Fatalf("EndTS: expected %v, got %v", meta.EndTS, got.EndTS)
	}
	if got.RecordCount != meta.RecordCount {
		t.Fatalf("RecordCount: expected %d, got %d", meta.RecordCount, got.RecordCount)
	}
	if got.Sealed != meta.Sealed {
		t.Fatalf("Sealed: expected %v, got %v", meta.Sealed, got.Sealed)
	}
}

func TestMetaStoreOverwrite(t *testing.T) {
	store := NewMetaStore()

	meta := testMeta()
	meta.Sealed = false
	if err := store.Save(meta); err != nil {
		t.Fatalf("save: %v", err)
	}

	meta.Sealed = true
	meta.RecordCount = 8192
	if err := store.Save(meta); err != nil {
		t.Fatalf("save overwrite: %v", err)
	}

	got, err := store.Load(meta.ID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if !got.Sealed {
		t.Fatal("expected Sealed=true after overwrite")
	}
	if got.RecordCount != 8192 {
		t.Fatalf("expected RecordCount=8192, got %d", got.RecordCount)
	}
}

func TestMetaStoreLoadNotFound(t *testing.T) {
	store := NewMetaStore()

	_, err := store.Load(chunk.NewChunkID())
	if err != chunk.ErrChunkNotFound {
		t.Fatalf("expected ErrChunkNotFound, got %v", err)
	}
}

func TestMetaStoreListEmpty(t *testing.T) {
	store := NewMetaStore()

	metas, err := store.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(metas) != 0 {
		t.Fatalf("expected 0 metas, got %d", len(metas))
	}
}

func TestMetaStoreListMultiple(t *testing.T) {
	store := NewMetaStore()

	m1 := testMeta()
	m2 := testMeta()
	m3 := testMeta()

	for _, m := range []chunk.ChunkMeta{m1, m2, m3} {
		if err := store.Save(m); err != nil {
			t.Fatalf("save: %v", err)
		}
	}

	metas, err := store.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(metas) != 3 {
		t.Fatalf("expected 3 metas, got %d", len(metas))
	}

	ids := make(map[chunk.ChunkID]bool)
	for _, m := range metas {
		ids[m.ID] = true
	}
	for _, expected := range []chunk.ChunkID{m1.ID, m2.ID, m3.ID} {
		if !ids[expected] {
			t.Fatalf("missing chunk %s in list", expected)
		}
	}
}

func TestMetaStoreSaveMultipleLoadEach(t *testing.T) {
	store := NewMetaStore()

	m1 := testMeta()
	m2 := testMeta()

	store.Save(m1)
	store.Save(m2)

	got1, err := store.Load(m1.ID)
	if err != nil {
		t.Fatalf("load m1: %v", err)
	}
	if got1.ID != m1.ID {
		t.Fatalf("expected %s, got %s", m1.ID, got1.ID)
	}

	got2, err := store.Load(m2.ID)
	if err != nil {
		t.Fatalf("load m2: %v", err)
	}
	if got2.ID != m2.ID {
		t.Fatalf("expected %s, got %s", m2.ID, got2.ID)
	}
}
