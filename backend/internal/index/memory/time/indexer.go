package time

import (
	"context"
	"fmt"
	"sync"

	"github.com/kluzzebass/gastrolog/internal/chunk"
	"github.com/kluzzebass/gastrolog/internal/index"
)

// Indexer builds a sparse time index for sealed chunks,
// storing the result in memory.
type Indexer struct {
	manager  chunk.ChunkManager
	sparsity int
	mu       sync.Mutex
	indices  map[chunk.ChunkID][]index.TimeIndexEntry
}

func NewIndexer(manager chunk.ChunkManager, sparsity int) *Indexer {
	return &Indexer{
		manager:  manager,
		sparsity: sparsity,
		indices:  make(map[chunk.ChunkID][]index.TimeIndexEntry),
	}
}

func (t *Indexer) Name() string {
	return "time"
}

func (t *Indexer) Build(ctx context.Context, chunkID chunk.ChunkID) error {
	meta, err := t.manager.Meta(chunkID)
	if err != nil {
		return fmt.Errorf("get chunk meta: %w", err)
	}
	if !meta.Sealed {
		return chunk.ErrChunkNotSealed
	}

	cursor, err := t.manager.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	defer cursor.Close()

	var entries []index.TimeIndexEntry
	n := 0

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		rec, ref, err := cursor.Next()
		if err != nil {
			if err == chunk.ErrNoMoreRecords {
				break
			}
			return fmt.Errorf("read record: %w", err)
		}

		if n == 0 || n%t.sparsity == 0 {
			entries = append(entries, index.TimeIndexEntry{
				Timestamp: rec.WriteTS,
				RecordPos: ref.Pos,
			})
		}
		n++
	}

	t.mu.Lock()
	t.indices[chunkID] = entries
	t.mu.Unlock()

	return nil
}

func (t *Indexer) Get(chunkID chunk.ChunkID) ([]index.TimeIndexEntry, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entries, ok := t.indices[chunkID]
	return entries, ok
}
