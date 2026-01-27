package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/kluzzebass/gastrolog/internal/chunk"
	indextime "github.com/kluzzebass/gastrolog/internal/index/time"
)

// TimeIndexer builds a sparse time index for sealed chunks,
// storing the result in memory.
type TimeIndexer struct {
	manager  chunk.ChunkManager
	sparsity int
	mu       sync.Mutex
	indices  map[chunk.ChunkID][]indextime.IndexEntry
}

func NewTimeIndexer(manager chunk.ChunkManager, sparsity int) *TimeIndexer {
	return &TimeIndexer{
		manager:  manager,
		sparsity: sparsity,
		indices:  make(map[chunk.ChunkID][]indextime.IndexEntry),
	}
}

func (t *TimeIndexer) Name() string {
	return "time"
}

func (t *TimeIndexer) Build(ctx context.Context, chunkID chunk.ChunkID) error {
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

	var entries []indextime.IndexEntry
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
			entries = append(entries, indextime.IndexEntry{
				Timestamp: rec.IngestTS,
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

func (t *TimeIndexer) Get(chunkID chunk.ChunkID) ([]indextime.IndexEntry, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entries, ok := t.indices[chunkID]
	return entries, ok
}
