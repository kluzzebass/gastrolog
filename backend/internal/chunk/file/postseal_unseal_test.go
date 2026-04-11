package file

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	filetoken "gastrolog/internal/index/file/token"
)

// TestPostSealProcessRejectsUnsealedChunk verifies that PostSealProcess
// does not produce a WARN when called with an unsealed chunk ID.
// Reproduces gastrolog-89k15.
func TestPostSealProcessRejectsUnsealedChunk(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cm, err := NewManager(Config{
		Dir: dir,
		Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	tokenIndexer := filetoken.NewIndexer(dir, cm, nil)
	im := indexfile.NewManager(dir, []index.Indexer{tokenIndexer}, nil)
	cm.SetIndexBuilders([]chunk.ChunkIndexBuilder{im.BuildAdapter()})

	// Append records but do NOT seal.
	for i := range 5 {
		ts := time.Now().Add(time.Duration(i) * time.Microsecond)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: []byte(fmt.Sprintf("record-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	active := cm.Active()
	if active == nil {
		t.Fatal("no active chunk")
	}

	// PostSealProcess on an unsealed chunk should return an error,
	// not silently run CompressChunk (no-op) then fail on index build.
	err = cm.PostSealProcess(context.Background(), active.ID)
	if err == nil {
		t.Error("PostSealProcess should reject unsealed chunks with an error")
	}
}

// TestPostSealProcessSealedChunkSucceeds is the positive control —
// PostSealProcess on a properly sealed chunk should succeed.
func TestPostSealProcessSealedChunkSucceeds(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cm, err := NewManager(Config{
		Dir: dir,
		Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	tokenIndexer := filetoken.NewIndexer(dir, cm, nil)
	im := indexfile.NewManager(dir, []index.Indexer{tokenIndexer}, nil)
	cm.SetIndexBuilders([]chunk.ChunkIndexBuilder{im.BuildAdapter()})

	for i := range 5 {
		ts := time.Now().Add(time.Duration(i) * time.Microsecond)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: []byte(fmt.Sprintf("record-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	active := cm.Active()
	if active == nil {
		t.Fatal("no active chunk")
	}
	chunkID := active.ID

	if err := cm.Seal(); err != nil {
		t.Fatal(err)
	}

	if err := cm.PostSealProcess(context.Background(), chunkID); err != nil {
		t.Fatalf("PostSealProcess on sealed chunk should succeed: %v", err)
	}
}
