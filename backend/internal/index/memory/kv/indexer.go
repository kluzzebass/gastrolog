package kv

import (
	"cmp"
	"context"
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
	manager  chunk.ChunkManager
	kvBudget int64
	mu       sync.Mutex
	keyIndex map[chunk.ChunkID][]index.KVKeyIndexEntry
	valIndex map[chunk.ChunkID][]index.KVValueIndexEntry
	kvIndex  map[chunk.ChunkID][]index.KVIndexEntry
	status   map[chunk.ChunkID]index.KVIndexStatus
}

func NewIndexer(manager chunk.ChunkManager) *Indexer {
	return NewIndexerWithConfig(manager, Config{})
}

func NewIndexerWithConfig(manager chunk.ChunkManager, cfg Config) *Indexer {
	budget := cfg.KVBudget
	if budget <= 0 {
		budget = DefaultKVBudget
	}
	return &Indexer{
		manager:  manager,
		kvBudget: budget,
		keyIndex: make(map[chunk.ChunkID][]index.KVKeyIndexEntry),
		valIndex: make(map[chunk.ChunkID][]index.KVValueIndexEntry),
		kvIndex:  make(map[chunk.ChunkID][]index.KVIndexEntry),
		status:   make(map[chunk.ChunkID]index.KVIndexStatus),
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

	// Pass 1: Collect all candidates with counts and positions
	// We collect everything first, then apply budget-based admission.
	keyCandidates := make(map[string]*kvCandidate)   // key -> positions where key appears
	valueCandidates := make(map[string]*kvCandidate) // value -> positions where value appears
	kvCandidates := make(map[string]*kvCandidate)    // "key\x00value" -> positions
	valuesPerKey := make(map[string]map[string]struct{})

	capped := false

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

		if capped {
			continue // Still count records but don't collect more candidates
		}

		// Extract key=value pairs from message
		pairs := tokenizer.ExtractKeyValues(rec.Raw)

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
					break
				}
				keyCandidates[kv.Key] = &kvCandidate{key: kv.Key}
				valuesPerKey[kv.Key] = make(map[string]struct{})
			}

			// Check values per key hard cap
			if _, ok := valuesPerKey[kv.Key][kv.Value]; !ok {
				if len(valuesPerKey[kv.Key]) >= MaxValuesPerKey {
					capped = true
					break
				}
				valuesPerKey[kv.Key][kv.Value] = struct{}{}
			}

			// Check total entries hard cap
			if len(kvCandidates) >= MaxTotalEntries {
				capped = true
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

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// If hard caps exceeded, mark as capped
	if capped {
		idx.keyIndex[chunkID] = nil
		idx.valIndex[chunkID] = nil
		idx.kvIndex[chunkID] = nil
		idx.status[chunkID] = index.KVCapped
		return nil
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
	// so typically all keys fit. If they don't, admit by frequency.
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

	idx.keyIndex[chunkID] = keyEntries
	idx.valIndex[chunkID] = valueEntries
	idx.kvIndex[chunkID] = kvEntries
	idx.status[chunkID] = index.KVComplete

	return nil
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
