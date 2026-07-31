package file

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	filetoken "gastrolog/internal/index/file/token"
)

// Regression for whole-file GLCB mmap: histogram TS lookups must not SIGSEGV
// when retention deletes a chunk concurrently.
func TestGLCBMmapSurvivesConcurrentDeleteAndTSLookup(t *testing.T) {
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
	im := indexfile.NewManager(dir, []index.Indexer{tokenIndexer}, nil, cm)
	cm.SetIndexBuilders([]chunk.ChunkIndexBuilder{im.BuildAdapter()})

	base := time.Now()
	for i := range 50 {
		ts := base.Add(time.Duration(i) * time.Millisecond)
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
		t.Fatalf("PostSealProcess: %v", err)
	}

	done := make(chan struct{})
	errs := make(chan error, 1)
	go func() {
		defer close(done)
		for range 500 {
			_, _, err := im.FindIngestEntryIndex(chunkID, base.Add(25*time.Millisecond))
			if err == nil {
				continue
			}
			if errors.Is(err, index.ErrIndexNotFound) {
				return
			}
			errs <- err
			return
		}
	}()

	time.Sleep(2 * time.Millisecond)
	if err := cm.DeleteSilent(chunkID); err != nil {
		t.Fatalf("DeleteSilent: %v", err)
	}
	<-done
	select {
	case err := <-errs:
		t.Fatalf("TS lookup during delete: %v", err)
	default:
	}
}
