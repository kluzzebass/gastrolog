package kv

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
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

	// Cost calculation constants (matching file format)
	stringLenSize     = 2
	postingOffsetSize = 4
	postingCountSize  = 4
	positionSize      = 4
	headerSize        = 10 // 4 (format header) + 1 (status) + 4 (entry count) + 1 (padding)
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

// Indexer builds kv indexes for sealed chunks,
// storing the result in memory. This index is heuristic and non-authoritative.
//
// For each chunk, it creates three separate indexes:
//   - Key index: maps keys to record positions
//   - Value index: maps values to record positions
//   - KV index: maps (key, value) pairs to record positions
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
	manager    chunk.ChunkManager
	kvBudget   int64
	extractors []tokenizer.KVExtractor
	mu         sync.Mutex
	keyIndex   map[chunk.ChunkID][]index.KVKeyIndexEntry
	valIndex   map[chunk.ChunkID][]index.KVValueIndexEntry
	kvIndex    map[chunk.ChunkID][]index.KVIndexEntry
	status     map[chunk.ChunkID]index.KVIndexStatus
}

func NewIndexer(manager chunk.ChunkManager) *Indexer {
	return NewIndexerWithConfig(manager, Config{})
}

func NewIndexerWithConfig(manager chunk.ChunkManager, cfg Config) *Indexer {
	budget := cfg.KVBudget
	if budget <= 0 {
		budget = DefaultKVBudget
	}
	extractors := cfg.Extractors
	if len(extractors) == 0 {
		extractors = []tokenizer.KVExtractor{tokenizer.ExtractKeyValues}
	}
	return &Indexer{
		manager:    manager,
		kvBudget:   budget,
		extractors: extractors,
		keyIndex:   make(map[chunk.ChunkID][]index.KVKeyIndexEntry),
		valIndex:   make(map[chunk.ChunkID][]index.KVValueIndexEntry),
		kvIndex:    make(map[chunk.ChunkID][]index.KVIndexEntry),
		status:     make(map[chunk.ChunkID]index.KVIndexStatus),
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
	return stringLenSize + len(key) + postingOffsetSize + postingCountSize + posCount*positionSize
}

// valueCost calculates the exact encoded size for a value index entry.
func valueCost(value string, posCount int) int {
	return stringLenSize + len(value) + postingOffsetSize + postingCountSize + posCount*positionSize
}

// kvCost calculates the exact encoded size for a kv index entry.
func kvCost(key, value string, posCount int) int {
	return stringLenSize + len(key) + stringLenSize + len(value) + postingOffsetSize + postingCountSize + posCount*positionSize
}

type kvCollectResult struct {
	keyCandidates   map[string]*kvCandidate
	valueCandidates map[string]*kvCandidate
	kvCandidates    map[string]*kvCandidate
	capped          bool
}

func (idx *Indexer) Build(ctx context.Context, chunkID chunk.ChunkID) error {
	meta, err := idx.manager.Meta(chunkID)
	if err != nil {
		return fmt.Errorf("get chunk meta: %w", err)
	}
	if !meta.Sealed {
		return chunk.ErrChunkNotSealed
	}

	result, err := idx.collectCandidates(ctx, chunkID)
	if err != nil {
		return err
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if result.capped {
		idx.keyIndex[chunkID] = nil
		idx.valIndex[chunkID] = nil
		idx.kvIndex[chunkID] = nil
		idx.status[chunkID] = index.KVCapped
		return nil
	}

	idx.computeCosts(result)
	keyEntries, valueEntries, kvEntries := idx.admitAndBuild(result)

	idx.keyIndex[chunkID] = keyEntries
	idx.valIndex[chunkID] = valueEntries
	idx.kvIndex[chunkID] = kvEntries
	idx.status[chunkID] = index.KVComplete

	return nil
}

func (idx *Indexer) collectCandidates(ctx context.Context, chunkID chunk.ChunkID) (*kvCollectResult, error) {
	cursor, err := idx.manager.OpenCursor(chunkID)
	if err != nil {
		return nil, fmt.Errorf("open cursor: %w", err)
	}
	defer func() { _ = cursor.Close() }()

	result := &kvCollectResult{
		keyCandidates:   make(map[string]*kvCandidate),
		valueCandidates: make(map[string]*kvCandidate),
		kvCandidates:    make(map[string]*kvCandidate),
	}
	valuesPerKey := make(map[string]map[string]struct{})

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		rec, ref, err := cursor.Next()
		if err != nil {
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				break
			}
			return nil, fmt.Errorf("read record: %w", err)
		}

		if result.capped {
			continue
		}

		pairs := tokenizer.CombinedExtract(rec.Raw, idx.extractors)
		processRecordPairs(result, valuesPerKey, pairs, ref.Pos)
	}

	return result, nil
}

func processRecordPairs(result *kvCollectResult, valuesPerKey map[string]map[string]struct{}, pairs []tokenizer.KeyValue, pos uint64) {
	seenKeys := make(map[string]struct{})
	seenValues := make(map[string]struct{})
	seenKV := make(map[string]struct{})

	for _, kv := range pairs {
		kvKey := kv.Key + "\x00" + kv.Value

		if _, ok := result.keyCandidates[kv.Key]; !ok {
			if len(result.keyCandidates) >= MaxUniqueKeys {
				result.capped = true
				return
			}
			result.keyCandidates[kv.Key] = &kvCandidate{key: kv.Key}
			valuesPerKey[kv.Key] = make(map[string]struct{})
		}

		if _, ok := valuesPerKey[kv.Key][kv.Value]; !ok {
			if len(valuesPerKey[kv.Key]) >= MaxValuesPerKey {
				result.capped = true
				return
			}
			valuesPerKey[kv.Key][kv.Value] = struct{}{}
		}

		if len(result.kvCandidates) >= MaxTotalEntries {
			result.capped = true
			return
		}

		if _, ok := result.valueCandidates[kv.Value]; !ok {
			result.valueCandidates[kv.Value] = &kvCandidate{value: kv.Value}
		}
		if _, ok := result.kvCandidates[kvKey]; !ok {
			result.kvCandidates[kvKey] = &kvCandidate{key: kv.Key, value: kv.Value}
		}

		if _, seen := seenKeys[kv.Key]; !seen {
			seenKeys[kv.Key] = struct{}{}
			result.keyCandidates[kv.Key].positions = append(result.keyCandidates[kv.Key].positions, pos)
			result.keyCandidates[kv.Key].frequency++
		}
		if _, seen := seenValues[kv.Value]; !seen {
			seenValues[kv.Value] = struct{}{}
			result.valueCandidates[kv.Value].positions = append(result.valueCandidates[kv.Value].positions, pos)
			result.valueCandidates[kv.Value].frequency++
		}
		if _, seen := seenKV[kvKey]; !seen {
			seenKV[kvKey] = struct{}{}
			result.kvCandidates[kvKey].positions = append(result.kvCandidates[kvKey].positions, pos)
			result.kvCandidates[kvKey].frequency++
		}
	}
}

func (idx *Indexer) computeCosts(result *kvCollectResult) {
	for k, c := range result.keyCandidates {
		c.cost = keyCost(k, len(c.positions))
	}
	for v, c := range result.valueCandidates {
		c.cost = valueCost(v, len(c.positions))
	}
	for _, c := range result.kvCandidates {
		c.cost = kvCost(c.key, c.value, len(c.positions))
	}
}

func sortCandidates(candidates map[string]*kvCandidate) []*kvCandidate {
	list := make([]*kvCandidate, 0, len(candidates))
	for _, c := range candidates {
		list = append(list, c)
	}
	slices.SortFunc(list, func(a, b *kvCandidate) int {
		if a.frequency != b.frequency {
			return int(b.frequency) - int(a.frequency)
		}
		return a.cost - b.cost
	})
	return list
}

func admitWithinBudget(sorted []*kvCandidate, budget int64) []*kvCandidate {
	var admitted []*kvCandidate
	totalBytes := int64(headerSize)
	for _, c := range sorted {
		if totalBytes+int64(c.cost) > budget {
			break
		}
		admitted = append(admitted, c)
		totalBytes += int64(c.cost)
	}
	return admitted
}

func (idx *Indexer) admitAndBuild(result *kvCollectResult) ([]index.KVKeyIndexEntry, []index.KVValueIndexEntry, []index.KVIndexEntry) {
	admittedKeys := admitWithinBudget(sortCandidates(result.keyCandidates), idx.kvBudget)
	admittedValues := admitWithinBudget(sortCandidates(result.valueCandidates), idx.kvBudget)
	admittedKV := admitWithinBudget(sortCandidates(result.kvCandidates), idx.kvBudget)

	keyEntries := make([]index.KVKeyIndexEntry, len(admittedKeys))
	for i, c := range admittedKeys {
		keyEntries[i] = index.KVKeyIndexEntry{Key: c.key, Positions: c.positions}
	}

	valueEntries := make([]index.KVValueIndexEntry, len(admittedValues))
	for i, c := range admittedValues {
		valueEntries[i] = index.KVValueIndexEntry{Value: c.value, Positions: c.positions}
	}

	kvEntries := make([]index.KVIndexEntry, len(admittedKV))
	for i, c := range admittedKV {
		kvEntries[i] = index.KVIndexEntry{Key: c.key, Value: c.value, Positions: c.positions}
	}

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

	return keyEntries, valueEntries, kvEntries
}

func (idx *Indexer) GetKey(chunkID chunk.ChunkID) ([]index.KVKeyIndexEntry, index.KVIndexStatus, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	entries, ok := idx.keyIndex[chunkID]
	if !ok {
		return nil, index.KVComplete, false
	}
	return entries, idx.status[chunkID], true
}

func (idx *Indexer) GetValue(chunkID chunk.ChunkID) ([]index.KVValueIndexEntry, index.KVIndexStatus, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	entries, ok := idx.valIndex[chunkID]
	if !ok {
		return nil, index.KVComplete, false
	}
	return entries, idx.status[chunkID], true
}

func (idx *Indexer) GetKV(chunkID chunk.ChunkID) ([]index.KVIndexEntry, index.KVIndexStatus, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	entries, ok := idx.kvIndex[chunkID]
	if !ok {
		return nil, index.KVComplete, false
	}
	return entries, idx.status[chunkID], true
}

func (idx *Indexer) Delete(chunkID chunk.ChunkID) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	delete(idx.keyIndex, chunkID)
	delete(idx.valIndex, chunkID)
	delete(idx.kvIndex, chunkID)
	delete(idx.status, chunkID)
}
