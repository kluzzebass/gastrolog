package attr

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/logging"
)

// Indexer builds attribute indexes for sealed chunks.
// For each chunk, it creates three index files:
//   - _attr_key.idx: maps attribute keys to record positions
//   - _attr_val.idx: maps attribute values to record positions
//   - _attr_kv.idx: maps (key, value) pairs to record positions
//
// The indexer uses a two-pass algorithm:
//   - Pass 1: Count occurrences of each key, value, and (key, value) pair
//   - Pass 2: Build entries with exact-sized position slices
type Indexer struct {
	dir     string
	manager chunk.ChunkManager
	logger  *slog.Logger
}

func NewIndexer(dir string, manager chunk.ChunkManager, logger *slog.Logger) *Indexer {
	return &Indexer{
		dir:     dir,
		manager: manager,
		logger:  logging.Default(logger).With("component", "indexer", "type", "attr"),
	}
}

func (idx *Indexer) Name() string {
	return "attr"
}

type attrCounts struct {
	keyCounts   map[string]uint32
	valueCounts map[string]uint32
	kvCounts    map[string]uint32
	recordCount uint64
}

func (idx *Indexer) Build(ctx context.Context, chunkID chunk.ChunkID) error {
	buildStart := time.Now()

	meta, err := idx.manager.Meta(chunkID)
	if err != nil {
		return fmt.Errorf("get chunk meta: %w", err)
	}
	if !meta.Sealed {
		return chunk.ErrChunkNotSealed
	}

	counts, err := idx.countOccurrences(ctx, chunkID)
	if err != nil {
		return err
	}

	keyEntries, valueEntries, kvEntries := idx.allocateEntries(counts)

	if err := idx.fillPositions(ctx, chunkID, keyEntries, valueEntries, kvEntries); err != nil {
		return err
	}

	idx.sortEntries(keyEntries, valueEntries, kvEntries)

	if err := idx.writeAllIndexes(chunkID, keyEntries, valueEntries, kvEntries); err != nil {
		return err
	}

	idx.logger.Debug("attr index built",
		"chunk", chunkID.String(),
		"records", counts.recordCount,
		"keys", len(keyEntries),
		"values", len(valueEntries),
		"kv_pairs", len(kvEntries),
		"duration", time.Since(buildStart),
	)

	return nil
}

func (idx *Indexer) countOccurrences(ctx context.Context, chunkID chunk.ChunkID) (*attrCounts, error) {
	cursor, err := idx.manager.OpenCursor(chunkID)
	if err != nil {
		return nil, fmt.Errorf("open cursor: %w", err)
	}

	counts := &attrCounts{
		keyCounts:   make(map[string]uint32),
		valueCounts: make(map[string]uint32),
		kvCounts:    make(map[string]uint32),
	}

	for {
		if err := ctx.Err(); err != nil {
			_ = cursor.Close()
			return nil, err
		}

		rec, _, err := cursor.Next()
		if err != nil {
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				break
			}
			_ = cursor.Close()
			return nil, fmt.Errorf("read record: %w", err)
		}
		counts.recordCount++

		seenKeys := make(map[string]struct{})
		seenValues := make(map[string]struct{})
		seenKV := make(map[string]struct{})

		for k, v := range rec.Attrs {
			key := lowercase(k)
			val := lowercase(v)
			kvKey := key + "\x00" + val

			if _, seen := seenKeys[key]; !seen {
				seenKeys[key] = struct{}{}
				counts.keyCounts[key]++
			}
			if _, seen := seenValues[val]; !seen {
				seenValues[val] = struct{}{}
				counts.valueCounts[val]++
			}
			if _, seen := seenKV[kvKey]; !seen {
				seenKV[kvKey] = struct{}{}
				counts.kvCounts[kvKey]++
			}
		}
	}
	_ = cursor.Close()
	return counts, nil
}

func (idx *Indexer) allocateEntries(counts *attrCounts) ([]index.AttrKeyIndexEntry, []index.AttrValueIndexEntry, []index.AttrKVIndexEntry) {
	sortedKeys := make([]string, 0, len(counts.keyCounts))
	for k := range counts.keyCounts {
		sortedKeys = append(sortedKeys, k)
	}
	slices.Sort(sortedKeys)

	sortedValues := make([]string, 0, len(counts.valueCounts))
	for v := range counts.valueCounts {
		sortedValues = append(sortedValues, v)
	}
	slices.Sort(sortedValues)

	sortedKVs := make([]string, 0, len(counts.kvCounts))
	for kv := range counts.kvCounts {
		sortedKVs = append(sortedKVs, kv)
	}
	slices.Sort(sortedKVs)

	keyEntries := make([]index.AttrKeyIndexEntry, len(sortedKeys))
	for i, k := range sortedKeys {
		keyEntries[i] = index.AttrKeyIndexEntry{
			Key:       k,
			Positions: make([]uint64, 0, counts.keyCounts[k]),
		}
	}

	valueEntries := make([]index.AttrValueIndexEntry, len(sortedValues))
	for i, v := range sortedValues {
		valueEntries[i] = index.AttrValueIndexEntry{
			Value:     v,
			Positions: make([]uint64, 0, counts.valueCounts[v]),
		}
	}

	kvEntries := make([]index.AttrKVIndexEntry, len(sortedKVs))
	for i, kv := range sortedKVs {
		key, val := index.SplitKV(kv)
		kvEntries[i] = index.AttrKVIndexEntry{
			Key:       key,
			Value:     val,
			Positions: make([]uint64, 0, counts.kvCounts[kv]),
		}
	}

	return keyEntries, valueEntries, kvEntries
}

func (idx *Indexer) fillPositions(ctx context.Context, chunkID chunk.ChunkID, keyEntries []index.AttrKeyIndexEntry, valueEntries []index.AttrValueIndexEntry, kvEntries []index.AttrKVIndexEntry) error {
	cursor, err := idx.manager.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor pass 2: %w", err)
	}

	keyIdx := make(map[string]int, len(keyEntries))
	for i := range keyEntries {
		keyIdx[keyEntries[i].Key] = i
	}
	valIdx := make(map[string]int, len(valueEntries))
	for i := range valueEntries {
		valIdx[valueEntries[i].Value] = i
	}
	kvIdx := make(map[string]int, len(kvEntries))
	for i := range kvEntries {
		kvIdx[kvEntries[i].Key+"\x00"+kvEntries[i].Value] = i
	}

	for {
		if err := ctx.Err(); err != nil {
			_ = cursor.Close()
			return err
		}

		rec, ref, err := cursor.Next()
		if err != nil {
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				break
			}
			_ = cursor.Close()
			return fmt.Errorf("read record pass 2: %w", err)
		}

		seenKeys := make(map[string]struct{})
		seenValues := make(map[string]struct{})
		seenKV := make(map[string]struct{})

		for k, v := range rec.Attrs {
			key := lowercase(k)
			val := lowercase(v)
			kvKey := key + "\x00" + val

			if _, seen := seenKeys[key]; !seen {
				seenKeys[key] = struct{}{}
				i := keyIdx[key]
				keyEntries[i].Positions = append(keyEntries[i].Positions, ref.Pos)
			}
			if _, seen := seenValues[val]; !seen {
				seenValues[val] = struct{}{}
				i := valIdx[val]
				valueEntries[i].Positions = append(valueEntries[i].Positions, ref.Pos)
			}
			if _, seen := seenKV[kvKey]; !seen {
				seenKV[kvKey] = struct{}{}
				i := kvIdx[kvKey]
				kvEntries[i].Positions = append(kvEntries[i].Positions, ref.Pos)
			}
		}
	}
	_ = cursor.Close()
	return nil
}

func (idx *Indexer) sortEntries(keyEntries []index.AttrKeyIndexEntry, valueEntries []index.AttrValueIndexEntry, kvEntries []index.AttrKVIndexEntry) {
	slices.SortFunc(keyEntries, func(a, b index.AttrKeyIndexEntry) int {
		return cmp.Compare(a.Key, b.Key)
	})
	slices.SortFunc(valueEntries, func(a, b index.AttrValueIndexEntry) int {
		return cmp.Compare(a.Value, b.Value)
	})
	slices.SortFunc(kvEntries, func(a, b index.AttrKVIndexEntry) int {
		if c := cmp.Compare(a.Key, b.Key); c != 0 {
			return c
		}
		return cmp.Compare(a.Value, b.Value)
	})
}

func (idx *Indexer) writeAllIndexes(chunkID chunk.ChunkID, keyEntries []index.AttrKeyIndexEntry, valueEntries []index.AttrValueIndexEntry, kvEntries []index.AttrKVIndexEntry) error {
	chunkDir := filepath.Join(idx.dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o750); err != nil {
		return fmt.Errorf("create index dir: %w", err)
	}
	if err := idx.writeIndex(chunkDir, keyIndexFileName, encodeKeyIndex(keyEntries)); err != nil {
		return fmt.Errorf("write key index: %w", err)
	}
	if err := idx.writeIndex(chunkDir, valueIndexFileName, encodeValueIndex(valueEntries)); err != nil {
		return fmt.Errorf("write value index: %w", err)
	}
	if err := idx.writeIndex(chunkDir, kvIndexFileName, encodeKVIndex(kvEntries)); err != nil {
		return fmt.Errorf("write kv index: %w", err)
	}
	return nil
}

func (idx *Indexer) writeIndex(chunkDir, fileName string, data []byte) error {
	target := filepath.Join(chunkDir, fileName)
	tmpFile, err := os.CreateTemp(chunkDir, fileName+".tmp.*")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpName := tmpFile.Name()

	if err := tmpFile.Chmod(0o644); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpName) //nolint:gosec // G703: tmpName is from os.CreateTemp, not user input
		return fmt.Errorf("chmod: %w", err)
	}

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpName) //nolint:gosec // G703: tmpName is from os.CreateTemp, not user input
		return fmt.Errorf("write: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpName) //nolint:gosec // G703: tmpName is from os.CreateTemp, not user input
		return fmt.Errorf("close: %w", err)
	}

	if err := os.Rename(tmpName, filepath.Clean(target)); err != nil { //nolint:gosec // G703: both paths are from internal index path construction
		_ = os.Remove(tmpName) //nolint:gosec // G703: tmpName is from os.CreateTemp, not user input
		return fmt.Errorf("rename: %w", err)
	}

	return nil
}
