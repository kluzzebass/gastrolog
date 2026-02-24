package json

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/tokenizer"
)

// Default budget and cardinality limits.
const (
	DefaultJSONBudget = 10 * 1024 * 1024 // 10 MB
	MaxUniquePaths    = 50000
	MaxTotalPVPairs   = 200000

	// Cost calculation constants (matching file format).
	pvEntrySize  = 4 * 4 // pathID + valueID + blobOffset + count
	positionSize = 4
)

// Config holds configuration for the JSON indexer.
type Config struct {
	Budget int64
}

// Indexer builds a structural JSON index in memory.
type Indexer struct {
	manager chunk.ChunkManager
	budget  int64
	mu      sync.Mutex
	pathIdx map[chunk.ChunkID][]index.JSONPathIndexEntry
	pvIdx   map[chunk.ChunkID][]index.JSONPVIndexEntry
	status  map[chunk.ChunkID]index.JSONIndexStatus
}

func NewIndexer(manager chunk.ChunkManager) *Indexer {
	return NewIndexerWithConfig(manager, Config{})
}

func NewIndexerWithConfig(manager chunk.ChunkManager, cfg Config) *Indexer {
	budget := cfg.Budget
	if budget <= 0 {
		budget = DefaultJSONBudget
	}
	return &Indexer{
		manager: manager,
		budget:  budget,
		pathIdx: make(map[chunk.ChunkID][]index.JSONPathIndexEntry),
		pvIdx:   make(map[chunk.ChunkID][]index.JSONPVIndexEntry),
		status:  make(map[chunk.ChunkID]index.JSONIndexStatus),
	}
}

func (idx *Indexer) Name() string {
	return "json"
}

type pvKey struct {
	pathStr  string
	valueStr string
}

type collectResult struct {
	pathCounts map[string][]uint64
	pvCounts   map[pvKey][]uint64
	capped     bool
}

type collectState struct {
	pathCounts map[string][]uint64
	pvCounts   map[pvKey][]uint64
	capped     bool
	seenPaths  map[string]struct{}
	seenPVs    map[pvKey]struct{}
	pos        uint64
}

func (s *collectState) onPath(pathBytes []byte) {
	if s.capped {
		return
	}
	pathStr := strings.ToLower(string(pathBytes))

	if _, seen := s.seenPaths[pathStr]; seen {
		return
	}
	s.seenPaths[pathStr] = struct{}{}

	if _, exists := s.pathCounts[pathStr]; !exists {
		if len(s.pathCounts) >= MaxUniquePaths {
			s.capped = true
			return
		}
	}
	s.pathCounts[pathStr] = append(s.pathCounts[pathStr], s.pos)
}

func (s *collectState) onPV(pathBytes, valueBytes []byte) {
	if s.capped {
		return
	}
	pathStr := strings.ToLower(string(pathBytes))
	valueStr := string(valueBytes)
	key := pvKey{pathStr, valueStr}

	if _, seen := s.seenPVs[key]; seen {
		return
	}
	s.seenPVs[key] = struct{}{}

	if _, exists := s.pvCounts[key]; !exists {
		if len(s.pvCounts) >= MaxTotalPVPairs {
			s.capped = true
			return
		}
	}
	s.pvCounts[key] = append(s.pvCounts[key], s.pos)
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
		idx.pathIdx[chunkID] = nil
		idx.pvIdx[chunkID] = nil
		idx.status[chunkID] = index.JSONCapped
		return nil
	}

	pathEntries, pvEntries, status := idx.admitAndBuild(result)
	idx.pathIdx[chunkID] = pathEntries
	idx.pvIdx[chunkID] = pvEntries
	idx.status[chunkID] = status

	return nil
}

func (idx *Indexer) collectCandidates(ctx context.Context, chunkID chunk.ChunkID) (*collectResult, error) {
	cursor, err := idx.manager.OpenCursor(chunkID)
	if err != nil {
		return nil, fmt.Errorf("open cursor: %w", err)
	}
	defer func() { _ = cursor.Close() }()

	s := &collectState{
		pathCounts: make(map[string][]uint64),
		pvCounts:   make(map[pvKey][]uint64),
		seenPaths:  make(map[string]struct{}, 32),
		seenPVs:    make(map[pvKey]struct{}, 32),
	}

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

		if s.capped {
			continue
		}

		clear(s.seenPaths)
		clear(s.seenPVs)
		s.pos = ref.Pos
		tokenizer.WalkJSON(rec.Raw, s.onPath, s.onPV)
	}

	return &collectResult{pathCounts: s.pathCounts, pvCounts: s.pvCounts, capped: s.capped}, nil
}

type pvCandidate struct {
	key       pvKey
	positions []uint64
	frequency int
	cost      int
}

func (idx *Indexer) admitAndBuild(result *collectResult) ([]index.JSONPathIndexEntry, []index.JSONPVIndexEntry, index.JSONIndexStatus) {
	pvList := make([]pvCandidate, 0, len(result.pvCounts))
	for k, positions := range result.pvCounts {
		pvList = append(pvList, pvCandidate{
			key:       k,
			positions: positions,
			frequency: len(positions),
			cost:      pvEntrySize + len(positions)*positionSize,
		})
	}

	slices.SortFunc(pvList, func(a, b pvCandidate) int {
		if a.frequency != b.frequency {
			return b.frequency - a.frequency
		}
		return a.cost - b.cost
	})

	var admittedPV []pvCandidate
	pvTotalBytes := int64(0)
	jsonStatus := index.JSONComplete
	for _, c := range pvList {
		if pvTotalBytes+int64(c.cost) > idx.budget {
			jsonStatus = index.JSONCapped
			break
		}
		admittedPV = append(admittedPV, c)
		pvTotalBytes += int64(c.cost)
	}

	pathEntries := make([]index.JSONPathIndexEntry, 0, len(result.pathCounts))
	for path, positions := range result.pathCounts {
		pathEntries = append(pathEntries, index.JSONPathIndexEntry{
			Path:      path,
			Positions: positions,
		})
	}
	slices.SortFunc(pathEntries, func(a, b index.JSONPathIndexEntry) int {
		return cmp.Compare(a.Path, b.Path)
	})

	pvEntries := make([]index.JSONPVIndexEntry, len(admittedPV))
	for i, c := range admittedPV {
		pvEntries[i] = index.JSONPVIndexEntry{
			Path:      c.key.pathStr,
			Value:     c.key.valueStr,
			Positions: c.positions,
		}
	}
	slices.SortFunc(pvEntries, func(a, b index.JSONPVIndexEntry) int {
		if c := cmp.Compare(a.Path, b.Path); c != 0 {
			return c
		}
		return cmp.Compare(a.Value, b.Value)
	})

	return pathEntries, pvEntries, jsonStatus
}

// GetPath returns the path index entries for the given chunk.
func (idx *Indexer) GetPath(chunkID chunk.ChunkID) ([]index.JSONPathIndexEntry, index.JSONIndexStatus, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	entries, ok := idx.pathIdx[chunkID]
	if !ok {
		return nil, index.JSONComplete, false
	}
	return entries, idx.status[chunkID], true
}

// GetPV returns the path-value index entries for the given chunk.
func (idx *Indexer) GetPV(chunkID chunk.ChunkID) ([]index.JSONPVIndexEntry, index.JSONIndexStatus, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	entries, ok := idx.pvIdx[chunkID]
	if !ok {
		return nil, index.JSONComplete, false
	}
	return entries, idx.status[chunkID], true
}

// Delete removes all index data for the given chunk.
func (idx *Indexer) Delete(chunkID chunk.ChunkID) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	delete(idx.pathIdx, chunkID)
	delete(idx.pvIdx, chunkID)
	delete(idx.status, chunkID)
}
