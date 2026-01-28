package file

import (
	"os"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

func testMeta() chunk.ChunkMeta {
	return chunk.ChunkMeta{
		ID:      chunk.NewChunkID(),
		StartTS: time.UnixMicro(1000),
		EndTS:   time.UnixMicro(2000),
		Size:    4096,
		Sealed:  true,
	}
}

func TestMetaStoreSaveLoad(t *testing.T) {
	dir := t.TempDir()
	store := NewMetaStore(dir, 0)

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
	if got.Size != meta.Size {
		t.Fatalf("Size: expected %d, got %d", meta.Size, got.Size)
	}
	if got.Sealed != meta.Sealed {
		t.Fatalf("Sealed: expected %v, got %v", meta.Sealed, got.Sealed)
	}
}

func TestMetaStoreSaveUnsealed(t *testing.T) {
	dir := t.TempDir()
	store := NewMetaStore(dir, 0)

	meta := testMeta()
	meta.Sealed = false

	if err := store.Save(meta); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := store.Load(meta.ID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if got.Sealed {
		t.Fatal("expected Sealed=false, got true")
	}
}

func TestMetaStoreOverwrite(t *testing.T) {
	dir := t.TempDir()
	store := NewMetaStore(dir, 0)

	meta := testMeta()
	meta.Sealed = false
	if err := store.Save(meta); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Update and save again.
	meta.Sealed = true
	meta.Size = 8192
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
	if got.Size != 8192 {
		t.Fatalf("expected Size=8192, got %d", got.Size)
	}
}

func TestMetaStoreLoadNotFound(t *testing.T) {
	dir := t.TempDir()
	store := NewMetaStore(dir, 0)

	_, err := store.Load(chunk.NewChunkID())
	if err == nil {
		t.Fatal("expected error loading nonexistent meta, got nil")
	}
}

func TestMetaStoreListEmpty(t *testing.T) {
	dir := t.TempDir()
	store := NewMetaStore(dir, 0)

	metas, err := store.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(metas) != 0 {
		t.Fatalf("expected 0 metas, got %d", len(metas))
	}
}

func TestMetaStoreListMultiple(t *testing.T) {
	dir := t.TempDir()
	store := NewMetaStore(dir, 0)

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

func TestMetaStoreListIgnoresNonUUIDDirs(t *testing.T) {
	dir := t.TempDir()
	store := NewMetaStore(dir, 0)

	meta := testMeta()
	if err := store.Save(meta); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Create a non-UUID directory — should be ignored by List.
	if err := os.MkdirAll(dir+"/not-a-uuid", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	metas, err := store.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(metas) != 1 {
		t.Fatalf("expected 1 meta, got %d", len(metas))
	}
}

func TestEncodeDecodeMeta(t *testing.T) {
	meta := testMeta()
	data := encodeMeta(meta)
	got, err := decodeMeta(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.ID != meta.ID {
		t.Fatalf("ID mismatch")
	}
	if !got.StartTS.Equal(meta.StartTS) {
		t.Fatalf("StartTS mismatch")
	}
	if !got.EndTS.Equal(meta.EndTS) {
		t.Fatalf("EndTS mismatch")
	}
	if got.Size != meta.Size {
		t.Fatalf("Size mismatch")
	}
	if got.Sealed != meta.Sealed {
		t.Fatalf("Sealed mismatch")
	}
}

func TestDecodeMetaTooSmall(t *testing.T) {
	_, err := decodeMeta([]byte{0x01, 0x02})
	if err != ErrMetaTooSmall {
		t.Fatalf("expected ErrMetaTooSmall, got %v", err)
	}
}

func TestDecodeMetaSignatureMismatch(t *testing.T) {
	data := encodeMeta(testMeta())
	data[0] = 0xFF
	_, err := decodeMeta(data)
	if err != ErrMetaSignatureMismatch {
		t.Fatalf("expected ErrMetaSignatureMismatch, got %v", err)
	}
}

func TestDecodeMetaTypeMismatch(t *testing.T) {
	data := encodeMeta(testMeta())
	data[1] = 'x'
	_, err := decodeMeta(data)
	if err != ErrMetaSignatureMismatch {
		t.Fatalf("expected ErrMetaSignatureMismatch, got %v", err)
	}
}

func TestDecodeMetaVersionMismatch(t *testing.T) {
	data := encodeMeta(testMeta())
	data[2] = 0xFF
	_, err := decodeMeta(data)
	if err != ErrMetaVersionMismatch {
		t.Fatalf("expected ErrMetaVersionMismatch, got %v", err)
	}
}

func TestMetaStoreReloadFromDisk(t *testing.T) {
	dir := t.TempDir()

	meta := testMeta()
	store1 := NewMetaStore(dir, 0)
	if err := store1.Save(meta); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Create a fresh store pointing at the same directory.
	store2 := NewMetaStore(dir, 0)
	got, err := store2.Load(meta.ID)
	if err != nil {
		t.Fatalf("load from new store: %v", err)
	}
	if got.ID != meta.ID {
		t.Fatalf("expected %s, got %s", meta.ID, got.ID)
	}
}
