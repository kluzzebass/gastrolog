package attr

import (
	"cmp"
	"context"
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

func (idx *Indexer) Build(ctx context.Context, chunkID chunk.ChunkID) error {
	buildStart := time.Now()

	meta, err := idx.manager.Meta(chunkID)
	if err != nil {
		return fmt.Errorf("get chunk meta: %w", err)
	}
	if !meta.Sealed {
		return chunk.ErrChunkNotSealed
	}

	// Pass 1: Count occurrences
	keyCounts := make(map[string]uint32)
	valueCounts := make(map[string]uint32)
	kvCounts := make(map[string]uint32) // key + "\x00" + value

	var recordCount uint64

	cursor, err := idx.manager.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}

	for {
		if err := ctx.Err(); err != nil {
			cursor.Close()
			return err
		}

		rec, _, err := cursor.Next()
		if err != nil {
			if err == chunk.ErrNoMoreRecords {
				break
			}
			cursor.Close()
			return fmt.Errorf("read record: %w", err)
		}
		recordCount++

		// Dedupe within record
		seenKeys := make(map[string]struct{})
		seenValues := make(map[string]struct{})
		seenKV := make(map[string]struct{})

		for k, v := range rec.Attrs {
			key := lowercase(k)
			val := lowercase(v)
			kvKey := key + "\x00" + val

			if _, seen := seenKeys[key]; !seen {
				seenKeys[key] = struct{}{}
				keyCounts[key]++
			}
			if _, seen := seenValues[val]; !seen {
				seenValues[val] = struct{}{}
				valueCounts[val]++
			}
			if _, seen := seenKV[kvKey]; !seen {
				seenKV[kvKey] = struct{}{}
				kvCounts[kvKey]++
			}
		}
	}
	cursor.Close()

	// Sort keys for deterministic output
	sortedKeys := make([]string, 0, len(keyCounts))
	for k := range keyCounts {
		sortedKeys = append(sortedKeys, k)
	}
	slices.Sort(sortedKeys)

	sortedValues := make([]string, 0, len(valueCounts))
	for v := range valueCounts {
		sortedValues = append(sortedValues, v)
	}
	slices.Sort(sortedValues)

	sortedKVs := make([]string, 0, len(kvCounts))
	for kv := range kvCounts {
		sortedKVs = append(sortedKVs, kv)
	}
	slices.Sort(sortedKVs)

	// Allocate entries with exact sizes
	keyEntries := make([]index.AttrKeyIndexEntry, len(sortedKeys))
	keyWriteIdx := make(map[string]uint32)
	for i, k := range sortedKeys {
		keyEntries[i] = index.AttrKeyIndexEntry{
			Key:       k,
			Positions: make([]uint64, 0, keyCounts[k]),
		}
		keyWriteIdx[k] = uint32(i)
	}

	valueEntries := make([]index.AttrValueIndexEntry, len(sortedValues))
	valueWriteIdx := make(map[string]uint32)
	for i, v := range sortedValues {
		valueEntries[i] = index.AttrValueIndexEntry{
			Value:     v,
			Positions: make([]uint64, 0, valueCounts[v]),
		}
		valueWriteIdx[v] = uint32(i)
	}

	kvEntries := make([]index.AttrKVIndexEntry, len(sortedKVs))
	kvWriteIdx := make(map[string]uint32)
	for i, kv := range sortedKVs {
		parts := splitKV(kv)
		kvEntries[i] = index.AttrKVIndexEntry{
			Key:       parts[0],
			Value:     parts[1],
			Positions: make([]uint64, 0, kvCounts[kv]),
		}
		kvWriteIdx[kv] = uint32(i)
	}

	// Pass 2: Fill positions
	cursor, err = idx.manager.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor pass 2: %w", err)
	}

	for {
		if err := ctx.Err(); err != nil {
			cursor.Close()
			return err
		}

		rec, ref, err := cursor.Next()
		if err != nil {
			if err == chunk.ErrNoMoreRecords {
				break
			}
			cursor.Close()
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
				i := keyWriteIdx[key]
				keyEntries[i].Positions = append(keyEntries[i].Positions, ref.Pos)
			}
			if _, seen := seenValues[val]; !seen {
				seenValues[val] = struct{}{}
				i := valueWriteIdx[val]
				valueEntries[i].Positions = append(valueEntries[i].Positions, ref.Pos)
			}
			if _, seen := seenKV[kvKey]; !seen {
				seenKV[kvKey] = struct{}{}
				i := kvWriteIdx[kvKey]
				kvEntries[i].Positions = append(kvEntries[i].Positions, ref.Pos)
			}
		}
	}
	cursor.Close()

	// Sort entries by key/value for binary search
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

	// Create chunk directory
	chunkDir := filepath.Join(idx.dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return fmt.Errorf("create index dir: %w", err)
	}

	// Write key index
	if err := idx.writeIndex(chunkDir, keyIndexFileName, encodeKeyIndex(keyEntries)); err != nil {
		return fmt.Errorf("write key index: %w", err)
	}

	// Write value index
	if err := idx.writeIndex(chunkDir, valueIndexFileName, encodeValueIndex(valueEntries)); err != nil {
		return fmt.Errorf("write value index: %w", err)
	}

	// Write kv index
	if err := idx.writeIndex(chunkDir, kvIndexFileName, encodeKVIndex(kvEntries)); err != nil {
		return fmt.Errorf("write kv index: %w", err)
	}

	idx.logger.Debug("attr index built",
		"chunk", chunkID.String(),
		"records", recordCount,
		"keys", len(keyEntries),
		"values", len(valueEntries),
		"kv_pairs", len(kvEntries),
		"duration", time.Since(buildStart),
	)

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
		tmpFile.Close()
		os.Remove(tmpName)
		return fmt.Errorf("chmod: %w", err)
	}

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close: %w", err)
	}

	if err := os.Rename(tmpName, target); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename: %w", err)
	}

	return nil
}

func splitKV(kv string) [2]string {
	for i := 0; i < len(kv); i++ {
		if kv[i] == 0 {
			return [2]string{kv[:i], kv[i+1:]}
		}
	}
	return [2]string{kv, ""}
}
