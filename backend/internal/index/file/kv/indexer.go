package kv

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
	"gastrolog/internal/index/inverted"
	"gastrolog/internal/logging"
	"gastrolog/internal/tokenizer"
)

// Default budget and cardinality limits.
const (
	// DefaultKVBudget is the default budget for the KV index in bytes.
	// This controls how much space the (key,value) index can use.
	DefaultKVBudget = 10 * 1024 * 1024 // 10 MB

	// Defensive hard caps (retained even with budgeting)
	MaxUniqueKeys   = 10000
	MaxValuesPerKey = 1000
	MaxTotalEntries = 100000
)

// Config holds configuration for the KV indexer.
type Config struct {
	// KVBudget is the maximum size in bytes for the KV index.
	// Key and Value indexes are built from the same candidates but stored separately.
	// Set to 0 to use DefaultKVBudget.
	KVBudget int64

	// Extractors is the list of KV extractors to run on each record.
	// If empty, defaults to [ExtractKeyValues] for backward compatibility.
	Extractors []tokenizer.KVExtractor
}

// Indexer builds kv indexes for sealed chunks.
// This index is heuristic and non-authoritative.
//
// For each chunk, it creates three index files:
//   - _kv_key.idx: maps keys to record positions
//   - _kv_val.idx: maps values to record positions
//   - _kv_kv.idx: maps (key, value) pairs to record positions
//
// The indexer uses budget-based admission control:
//  1. Collect all candidate (key,value) pairs and their exact position lists
//  2. Compute frequency per (key,value)
//  3. Compute exact encoded size for each (key,value) entry
//  4. Sort entries by descending frequency (tie-breaker: smaller size first)
//  5. Admit entries while total_bytes + entry_bytes <= budget
//  6. Discard remaining entries silently
//
// Key-only indexing remains enabled even when value indexing is budget-exhausted.
// Defensive hard caps are retained as a safety net.
type Indexer struct {
	dir        string
	manager    chunk.ChunkManager
	logger     *slog.Logger
	kvBudget   int64
	extractors []tokenizer.KVExtractor
}

func NewIndexer(dir string, manager chunk.ChunkManager, logger *slog.Logger) *Indexer {
	return NewIndexerWithConfig(dir, manager, logger, Config{})
}

func NewIndexerWithConfig(dir string, manager chunk.ChunkManager, logger *slog.Logger, cfg Config) *Indexer {
	budget := cfg.KVBudget
	if budget <= 0 {
		budget = DefaultKVBudget
	}
	extractors := cfg.Extractors
	if len(extractors) == 0 {
		extractors = []tokenizer.KVExtractor{tokenizer.ExtractKeyValues}
	}
	return &Indexer{
		dir:        dir,
		manager:    manager,
		logger:     logging.Default(logger).With("component", "indexer", "type", "kv"),
		kvBudget:   budget,
		extractors: extractors,
	}
}

func (idx *Indexer) Name() string {
	return "kv"
}

// kvCandidate holds a (key,value) pair candidate for indexing.
type kvCandidate struct {
	key       string
	value     string
	positions []uint64
	frequency uint32 // number of records containing this pair
	cost      int    // exact encoded size in bytes
}

// keyCost calculates the exact encoded size for a key index entry.
func keyCost(key string, posCount int) int {
	// stringLenSize(2) + len(key) + postingOffsetSize(4) + postingCountSize(4) + positions
	return inverted.StringLenSize + len(key) + inverted.PostingOffsetSize + inverted.PostingCountSize + posCount*inverted.PositionSize
}

// valueCost calculates the exact encoded size for a value index entry.
func valueCost(value string, posCount int) int {
	// stringLenSize(2) + len(value) + postingOffsetSize(4) + postingCountSize(4) + positions
	return inverted.StringLenSize + len(value) + inverted.PostingOffsetSize + inverted.PostingCountSize + posCount*inverted.PositionSize
}

// kvCost calculates the exact encoded size for a kv index entry.
func kvCost(key, value string, posCount int) int {
	// stringLenSize(2) + len(key) + stringLenSize(2) + len(value) + postingOffsetSize(4) + postingCountSize(4) + positions
	return inverted.StringLenSize + len(key) + inverted.StringLenSize + len(value) + inverted.PostingOffsetSize + inverted.PostingCountSize + posCount*inverted.PositionSize
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

	// Pass 1: Collect all candidates with counts and positions
	// We collect everything first, then apply budget-based admission.
	keyCandidates := make(map[string]*kvCandidate)   // key -> positions where key appears
	valueCandidates := make(map[string]*kvCandidate) // value -> positions where value appears
	kvCandidates := make(map[string]*kvCandidate)    // "key\x00value" -> positions
	valuesPerKey := make(map[string]map[string]struct{})

	var recordCount uint64
	capped := false
	capReason := ""

	cursor, err := idx.manager.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
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
			return fmt.Errorf("read record: %w", err)
		}
		recordCount++

		if capped {
			continue // Still count records but don't collect more candidates
		}

		// Extract key=value pairs from message using all registered extractors.
		pairs := tokenizer.CombinedExtract(rec.Raw, idx.extractors)

		// Dedupe within record
		seenKeys := make(map[string]struct{})
		seenValues := make(map[string]struct{})
		seenKV := make(map[string]struct{})

		for _, kv := range pairs {
			kvKey := kv.Key + "\x00" + kv.Value

			// Check key hard cap
			if _, ok := keyCandidates[kv.Key]; !ok {
				if len(keyCandidates) >= MaxUniqueKeys {
					capped = true
					capReason = fmt.Sprintf("too many unique keys (limit %d)", MaxUniqueKeys)
					break
				}
				keyCandidates[kv.Key] = &kvCandidate{key: kv.Key}
				valuesPerKey[kv.Key] = make(map[string]struct{})
			}

			// Check values per key hard cap
			if _, ok := valuesPerKey[kv.Key][kv.Value]; !ok {
				if len(valuesPerKey[kv.Key]) >= MaxValuesPerKey {
					capped = true
					capReason = fmt.Sprintf("too many values for key %q (limit %d)", kv.Key, MaxValuesPerKey)
					break
				}
				valuesPerKey[kv.Key][kv.Value] = struct{}{}
			}

			// Check total entries hard cap
			if len(kvCandidates) >= MaxTotalEntries {
				capped = true
				capReason = fmt.Sprintf("too many total entries (limit %d)", MaxTotalEntries)
				break
			}

			// Add to value candidates
			if _, ok := valueCandidates[kv.Value]; !ok {
				valueCandidates[kv.Value] = &kvCandidate{value: kv.Value}
			}

			// Add to kv candidates
			if _, ok := kvCandidates[kvKey]; !ok {
				kvCandidates[kvKey] = &kvCandidate{key: kv.Key, value: kv.Value}
			}

			// Record positions (dedupe within record)
			if _, seen := seenKeys[kv.Key]; !seen {
				seenKeys[kv.Key] = struct{}{}
				keyCandidates[kv.Key].positions = append(keyCandidates[kv.Key].positions, ref.Pos)
				keyCandidates[kv.Key].frequency++
			}
			if _, seen := seenValues[kv.Value]; !seen {
				seenValues[kv.Value] = struct{}{}
				valueCandidates[kv.Value].positions = append(valueCandidates[kv.Value].positions, ref.Pos)
				valueCandidates[kv.Value].frequency++
			}
			if _, seen := seenKV[kvKey]; !seen {
				seenKV[kvKey] = struct{}{}
				kvCandidates[kvKey].positions = append(kvCandidates[kvKey].positions, ref.Pos)
				kvCandidates[kvKey].frequency++
			}
		}
	}
	cursor.Close()

	// If hard caps exceeded, write capped indexes
	if capped {
		idx.logger.Warn("kv index capped due to hard cap",
			"chunk", chunkID.String(),
			"reason", capReason,
		)
		return idx.writeCappedIndexes(chunkID)
	}

	// Compute exact costs for all candidates
	for k, c := range keyCandidates {
		c.cost = keyCost(k, len(c.positions))
	}
	for v, c := range valueCandidates {
		c.cost = valueCost(v, len(c.positions))
	}
	for _, c := range kvCandidates {
		c.cost = kvCost(c.key, c.value, len(c.positions))
	}

	// Apply budget-based admission for KV index
	// Sort by descending frequency, then ascending cost
	kvList := make([]*kvCandidate, 0, len(kvCandidates))
	for _, c := range kvCandidates {
		kvList = append(kvList, c)
	}
	slices.SortFunc(kvList, func(a, b *kvCandidate) int {
		// Descending frequency
		if a.frequency != b.frequency {
			return int(b.frequency) - int(a.frequency)
		}
		// Ascending cost (smaller first)
		return a.cost - b.cost
	})

	// Admit KV entries within budget
	var admittedKV []*kvCandidate
	kvTotalBytes := int64(headerSize) // Start with header overhead
	for _, c := range kvList {
		if kvTotalBytes+int64(c.cost) > idx.kvBudget {
			break
		}
		admittedKV = append(admittedKV, c)
		kvTotalBytes += int64(c.cost)
	}

	// Key index: admit all keys (key-only indexing stays enabled)
	// We still respect the budget, but key index is typically much smaller
	keyList := make([]*kvCandidate, 0, len(keyCandidates))
	for _, c := range keyCandidates {
		keyList = append(keyList, c)
	}
	slices.SortFunc(keyList, func(a, b *kvCandidate) int {
		if a.frequency != b.frequency {
			return int(b.frequency) - int(a.frequency)
		}
		return a.cost - b.cost
	})

	// For key index, we use the same budget but key entries are smaller
	// so typically all keys fit. If they don't, admit by frequency.
	var admittedKeys []*kvCandidate
	keyTotalBytes := int64(headerSize)
	for _, c := range keyList {
		if keyTotalBytes+int64(c.cost) > idx.kvBudget {
			break
		}
		admittedKeys = append(admittedKeys, c)
		keyTotalBytes += int64(c.cost)
	}

	// Value index: same approach
	valueList := make([]*kvCandidate, 0, len(valueCandidates))
	for _, c := range valueCandidates {
		valueList = append(valueList, c)
	}
	slices.SortFunc(valueList, func(a, b *kvCandidate) int {
		if a.frequency != b.frequency {
			return int(b.frequency) - int(a.frequency)
		}
		return a.cost - b.cost
	})

	var admittedValues []*kvCandidate
	valueTotalBytes := int64(headerSize)
	for _, c := range valueList {
		if valueTotalBytes+int64(c.cost) > idx.kvBudget {
			break
		}
		admittedValues = append(admittedValues, c)
		valueTotalBytes += int64(c.cost)
	}

	// Build final entries from admitted candidates
	keyEntries := make([]index.KVKeyIndexEntry, len(admittedKeys))
	for i, c := range admittedKeys {
		keyEntries[i] = index.KVKeyIndexEntry{
			Key:       c.key,
			Positions: c.positions,
		}
	}

	valueEntries := make([]index.KVValueIndexEntry, len(admittedValues))
	for i, c := range admittedValues {
		valueEntries[i] = index.KVValueIndexEntry{
			Value:     c.value,
			Positions: c.positions,
		}
	}

	kvEntries := make([]index.KVIndexEntry, len(admittedKV))
	for i, c := range admittedKV {
		kvEntries[i] = index.KVIndexEntry{
			Key:       c.key,
			Value:     c.value,
			Positions: c.positions,
		}
	}

	// Sort entries by key/value for binary search
	slices.SortFunc(keyEntries, func(a, b index.KVKeyIndexEntry) int {
		return cmp.Compare(a.Key, b.Key)
	})
	slices.SortFunc(valueEntries, func(a, b index.KVValueIndexEntry) int {
		return cmp.Compare(a.Value, b.Value)
	})
	slices.SortFunc(kvEntries, func(a, b index.KVIndexEntry) int {
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
	if err := idx.writeIndex(chunkDir, keyIndexFileName, encodeKeyIndex(keyEntries, index.KVComplete)); err != nil {
		return fmt.Errorf("write key index: %w", err)
	}

	// Write value index
	if err := idx.writeIndex(chunkDir, valueIndexFileName, encodeValueIndex(valueEntries, index.KVComplete)); err != nil {
		return fmt.Errorf("write value index: %w", err)
	}

	// Write kv index
	if err := idx.writeIndex(chunkDir, kvIndexFileName, encodeKVIndex(kvEntries, index.KVComplete)); err != nil {
		return fmt.Errorf("write kv index: %w", err)
	}

	droppedKV := len(kvCandidates) - len(admittedKV)
	droppedKeys := len(keyCandidates) - len(admittedKeys)
	droppedValues := len(valueCandidates) - len(admittedValues)

	idx.logger.Debug("kv index built",
		"chunk", chunkID.String(),
		"records", recordCount,
		"keys", len(keyEntries),
		"values", len(valueEntries),
		"kv_pairs", len(kvEntries),
		"dropped_keys", droppedKeys,
		"dropped_values", droppedValues,
		"dropped_kv", droppedKV,
		"key_bytes", keyTotalBytes,
		"value_bytes", valueTotalBytes,
		"kv_bytes", kvTotalBytes,
		"budget", idx.kvBudget,
		"duration", time.Since(buildStart),
	)

	return nil
}

func (idx *Indexer) writeCappedIndexes(chunkID chunk.ChunkID) error {
	chunkDir := filepath.Join(idx.dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return fmt.Errorf("create index dir: %w", err)
	}

	// Write empty capped key index
	if err := idx.writeIndex(chunkDir, keyIndexFileName, encodeKeyIndex(nil, index.KVCapped)); err != nil {
		return fmt.Errorf("write capped key index: %w", err)
	}

	// Write empty capped value index
	if err := idx.writeIndex(chunkDir, valueIndexFileName, encodeValueIndex(nil, index.KVCapped)); err != nil {
		return fmt.Errorf("write capped value index: %w", err)
	}

	// Write empty capped kv index
	if err := idx.writeIndex(chunkDir, kvIndexFileName, encodeKVIndex(nil, index.KVCapped)); err != nil {
		return fmt.Errorf("write capped kv index: %w", err)
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
