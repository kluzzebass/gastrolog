package query_test

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/chunk/glcb/glcbtest"
	"gastrolog/internal/glid"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/query"
)

// A search holds a cursor on every chunk in range for its whole run. When
// that is more chunks than the manager keeps mapped, every one of them must
// still read: the cap bounds idle mappings, not a query's working set.
func TestSearchSpansMoreSealedChunksThanTheMappedBlobCap(t *testing.T) {
	const perChunk = 3
	chunks := chunkfile.MappedBlobCap() + 6

	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{Dir: dir, Now: time.Now})
	if err != nil {
		t.Fatalf("chunk manager: %v", err)
	}
	t.Cleanup(func() { _ = cm.Close() })

	vaultID, ingester := glid.New(), glid.New()
	root := t.TempDir()
	base := time.Now().Add(-time.Hour).Truncate(time.Microsecond)
	want := make(map[chunk.ChunkID]bool, chunks)
	for i := range chunks {
		id := chunk.NewChunkID()
		recs := glcbtest.Records(perChunk, base.Add(time.Duration(i)*time.Second), ingester, "payload")
		path, info := glcbtest.WriteSealedBlob(t, root, id, vaultID, recs)
		if err := cm.RegisterExternalGLCB(id, path, info); err != nil {
			t.Fatalf("register chunk %d: %v", i, err)
		}
		want[id] = true
	}

	eng := query.New(cm, indexfile.NewManager(dir, nil, nil, cm), nil)
	it, _ := eng.Search(context.Background(), query.Query{}, nil)
	seen := make(map[chunk.ChunkID]int, chunks)
	total := 0
	for rec, err := range it {
		if err != nil {
			t.Fatalf("search failed after %d records across %d chunks: %v", total, len(seen), err)
		}
		seen[rec.Ref.ChunkID]++
		total++
	}
	if total != chunks*perChunk {
		t.Fatalf("search returned %d records, want %d", total, chunks*perChunk)
	}
	for id := range want {
		if seen[id] != perChunk {
			t.Errorf("chunk %s contributed %d records, want %d", id, seen[id], perChunk)
		}
	}
}
