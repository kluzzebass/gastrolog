package attr

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
)

// Indexer builds attribute indexes for sealed chunks,
// storing the result in memory. It creates three separate indexes:
//   - Key index: maps attribute keys to record positions
//   - Value index: maps attribute values to record positions
//   - KV index: maps (key, value) pairs to record positions
type Indexer struct {
	manager  chunk.ChunkManager
	mu       sync.Mutex
	keyIndex map[chunk.ChunkID][]index.AttrKeyIndexEntry
	valIndex map[chunk.ChunkID][]index.AttrValueIndexEntry
	kvIndex  map[chunk.ChunkID][]index.AttrKVIndexEntry
}

func NewIndexer(manager chunk.ChunkManager) *Indexer {
	return &Indexer{
		manager:  manager,
		keyIndex: make(map[chunk.ChunkID][]index.AttrKeyIndexEntry),
		valIndex: make(map[chunk.ChunkID][]index.AttrValueIndexEntry),
		kvIndex:  make(map[chunk.ChunkID][]index.AttrKVIndexEntry),
	}
}

func (idx *Indexer) Name() string {
	return "attr"
}

func (idx *Indexer) Build(ctx context.Context, chunkID chunk.ChunkID) error {
	meta, err := idx.manager.Meta(chunkID)
	if err != nil {
		return fmt.Errorf("get chunk meta: %w", err)
	}
	if !meta.Sealed {
		return chunk.ErrChunkNotSealed
	}

	cursor, err := idx.manager.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	defer cursor.Close()

	// Single-pass scan: accumulate positions per key, value, and kv pair.
	keyMap := make(map[string][]uint64)
	valMap := make(map[string][]uint64)
	kvMap := make(map[string][]uint64) // key + "\x00" + value

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

		// Dedupe within record
		seenKeys := make(map[string]struct{})
		seenVals := make(map[string]struct{})
		seenKV := make(map[string]struct{})

		for k, v := range rec.Attrs {
			key := strings.ToLower(k)
			val := strings.ToLower(v)
			kvKey := key + "\x00" + val

			if _, seen := seenKeys[key]; !seen {
				seenKeys[key] = struct{}{}
				keyMap[key] = append(keyMap[key], ref.Pos)
			}
			if _, seen := seenVals[val]; !seen {
				seenVals[val] = struct{}{}
				valMap[val] = append(valMap[val], ref.Pos)
			}
			if _, seen := seenKV[kvKey]; !seen {
				seenKV[kvKey] = struct{}{}
				kvMap[kvKey] = append(kvMap[kvKey], ref.Pos)
			}
		}
	}

	// Convert maps to sorted slices for deterministic output.
	keyEntries := make([]index.AttrKeyIndexEntry, 0, len(keyMap))
	for key, positions := range keyMap {
		keyEntries = append(keyEntries, index.AttrKeyIndexEntry{
			Key:       key,
			Positions: positions,
		})
	}
	slices.SortFunc(keyEntries, func(a, b index.AttrKeyIndexEntry) int {
		return cmp.Compare(a.Key, b.Key)
	})

	valEntries := make([]index.AttrValueIndexEntry, 0, len(valMap))
	for val, positions := range valMap {
		valEntries = append(valEntries, index.AttrValueIndexEntry{
			Value:     val,
			Positions: positions,
		})
	}
	slices.SortFunc(valEntries, func(a, b index.AttrValueIndexEntry) int {
		return cmp.Compare(a.Value, b.Value)
	})

	kvEntries := make([]index.AttrKVIndexEntry, 0, len(kvMap))
	for kvKey, positions := range kvMap {
		key, val := index.SplitKV(kvKey)
		kvEntries = append(kvEntries, index.AttrKVIndexEntry{
			Key:       key,
			Value:     val,
			Positions: positions,
		})
	}
	slices.SortFunc(kvEntries, func(a, b index.AttrKVIndexEntry) int {
		if c := cmp.Compare(a.Key, b.Key); c != 0 {
			return c
		}
		return cmp.Compare(a.Value, b.Value)
	})

	idx.mu.Lock()
	idx.keyIndex[chunkID] = keyEntries
	idx.valIndex[chunkID] = valEntries
	idx.kvIndex[chunkID] = kvEntries
	idx.mu.Unlock()

	return nil
}

func (idx *Indexer) GetKey(chunkID chunk.ChunkID) ([]index.AttrKeyIndexEntry, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	entries, ok := idx.keyIndex[chunkID]
	return entries, ok
}

func (idx *Indexer) GetValue(chunkID chunk.ChunkID) ([]index.AttrValueIndexEntry, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	entries, ok := idx.valIndex[chunkID]
	return entries, ok
}

func (idx *Indexer) GetKV(chunkID chunk.ChunkID) ([]index.AttrKVIndexEntry, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	entries, ok := idx.kvIndex[chunkID]
	return entries, ok
}
