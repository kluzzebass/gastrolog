package token

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/kluzzebass/gastrolog/internal/chunk"
	"github.com/kluzzebass/gastrolog/internal/index"
	"github.com/kluzzebass/gastrolog/internal/index/token"
)

// Indexer builds a token index for sealed chunks,
// storing the result in memory.
type Indexer struct {
	manager chunk.ChunkManager
	mu      sync.Mutex
	indices map[chunk.ChunkID][]index.TokenIndexEntry
}

func NewIndexer(manager chunk.ChunkManager) *Indexer {
	return &Indexer{
		manager: manager,
		indices: make(map[chunk.ChunkID][]index.TokenIndexEntry),
	}
}

func (t *Indexer) Name() string {
	return "token"
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

	// Single-pass scan: accumulate positions per token.
	posMap := make(map[string][]uint64)

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

		tokens := token.Tokenize(rec.Raw)
		seen := make(map[string]bool) // dedupe within same record
		for _, tok := range tokens {
			if !seen[tok] {
				seen[tok] = true
				posMap[tok] = append(posMap[tok], ref.Pos)
			}
		}
	}

	// Convert map to sorted slice for deterministic output.
	entries := make([]index.TokenIndexEntry, 0, len(posMap))
	for tok, positions := range posMap {
		entries = append(entries, index.TokenIndexEntry{
			Token:     tok,
			Positions: positions,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Token < entries[j].Token
	})

	t.mu.Lock()
	t.indices[chunkID] = entries
	t.mu.Unlock()

	return nil
}

func (t *Indexer) Get(chunkID chunk.ChunkID) ([]index.TokenIndexEntry, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entries, ok := t.indices[chunkID]
	return entries, ok
}
