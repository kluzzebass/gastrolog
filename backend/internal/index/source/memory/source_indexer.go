package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/google/uuid"
	"github.com/kluzzebass/gastrolog/internal/chunk"
	indexsource "github.com/kluzzebass/gastrolog/internal/index/source"
)

// SourceIndexer builds a source index for sealed chunks,
// storing the result in memory.
type SourceIndexer struct {
	manager chunk.ChunkManager
	mu      sync.Mutex
	indices map[chunk.ChunkID][]indexsource.IndexEntry
}

func NewSourceIndexer(manager chunk.ChunkManager) *SourceIndexer {
	return &SourceIndexer{
		manager: manager,
		indices: make(map[chunk.ChunkID][]indexsource.IndexEntry),
	}
}

func (s *SourceIndexer) Name() string {
	return "source"
}

func (s *SourceIndexer) Build(ctx context.Context, chunkID chunk.ChunkID) error {
	meta, err := s.manager.Meta(chunkID)
	if err != nil {
		return fmt.Errorf("get chunk meta: %w", err)
	}
	if !meta.Sealed {
		return chunk.ErrChunkNotSealed
	}

	cursor, err := s.manager.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	defer cursor.Close()

	// Single-pass scan: accumulate positions per source.
	posMap := make(map[chunk.SourceID][]uint64)

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

		posMap[rec.SourceID] = append(posMap[rec.SourceID], ref.Pos)
	}

	// Convert map to sorted slice for deterministic output.
	entries := make([]indexsource.IndexEntry, 0, len(posMap))
	for sid, positions := range posMap {
		entries = append(entries, indexsource.IndexEntry{
			SourceID:  sid,
			Positions: positions,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		a := uuid.UUID(entries[i].SourceID)
		b := uuid.UUID(entries[j].SourceID)
		return a.String() < b.String()
	})

	s.mu.Lock()
	s.indices[chunkID] = entries
	s.mu.Unlock()

	return nil
}

func (s *SourceIndexer) Get(chunkID chunk.ChunkID) ([]indexsource.IndexEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, ok := s.indices[chunkID]
	return entries, ok
}
