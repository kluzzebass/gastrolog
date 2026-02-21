package json

import (
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/logging"
	"gastrolog/internal/tokenizer"
)

// Default budget and cardinality limits.
const (
	// DefaultJSONBudget is the default budget for the JSON index in bytes.
	// Path-value pairs are admitted within this budget; paths are always admitted.
	DefaultJSONBudget = 10 * 1024 * 1024 // 10 MB

	// Defensive hard caps.
	MaxUniquePaths   = 50000
	MaxTotalPVPairs  = 200000
)

// Config holds configuration for the JSON indexer.
type Config struct {
	// Budget is the maximum size in bytes for the JSON path-value index.
	// Paths are always admitted. Set to 0 to use DefaultJSONBudget.
	Budget int64
}

// Indexer builds a structural JSON index for sealed chunks.
//
// For each chunk, it creates a single index file (_json.idx) containing:
//   - A shared string dictionary for paths and values
//   - Path posting table (path → record positions)
//   - Path-value posting table ((path,value) → record positions)
//   - Concatenated posting blob
//
// The indexer uses budget-based admission for path-value pairs.
// Paths are always admitted (cheap, bounded by schema).
type Indexer struct {
	dir     string
	manager chunk.ChunkManager
	logger  *slog.Logger
	budget  int64
}

func NewIndexer(dir string, manager chunk.ChunkManager, logger *slog.Logger) *Indexer {
	return NewIndexerWithConfig(dir, manager, logger, Config{})
}

func NewIndexerWithConfig(dir string, manager chunk.ChunkManager, logger *slog.Logger, cfg Config) *Indexer {
	budget := cfg.Budget
	if budget <= 0 {
		budget = DefaultJSONBudget
	}
	return &Indexer{
		dir:     dir,
		manager: manager,
		logger:  logging.Default(logger).With("component", "indexer", "type", "json"),
		budget:  budget,
	}
}

func (idx *Indexer) Name() string {
	return "json"
}

// pvKey is the composite key for path-value deduplication.
type pvKey struct {
	pathID  uint32
	valueID uint32
}

// candidate holds a path or path-value candidate with its count.
type candidate struct {
	count uint32
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

	// PASS 1: Count occurrences, intern paths and values.
	pass1Start := time.Now()

	// Shared string interning for paths and values.
	internMap := make(map[string]uint32) // string → dictID
	var dictStrings []string             // dictID → string

	internStr := func(s string) uint32 {
		if id, ok := internMap[s]; ok {
			return id
		}
		id := uint32(len(dictStrings))
		internMap[s] = id
		dictStrings = append(dictStrings, s)
		return id
	}

	pathCounts := make(map[uint32]*candidate)  // dictID → count
	pvCounts := make(map[pvKey]*candidate)      // (pathID, valueID) → count

	var recordCount uint64
	capped := false
	capReason := ""

	cursor, err := idx.manager.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}

	// Per-record dedup sets (reused).
	seenPaths := make(map[uint32]struct{}, 32)
	seenPVs := make(map[pvKey]struct{}, 32)

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

		if capped {
			continue
		}

		// Clear per-record dedup.
		clear(seenPaths)
		clear(seenPVs)

		tokenizer.WalkJSON(rec.Raw, func(pathBytes []byte) {
			if capped {
				return
			}

			pathStr := strings.ToLower(string(pathBytes))
			pathID := internStr(pathStr)

			// Dedup within record.
			if _, seen := seenPaths[pathID]; seen {
				return
			}
			seenPaths[pathID] = struct{}{}

			// Check hard cap.
			if _, exists := pathCounts[pathID]; !exists {
				if len(pathCounts) >= MaxUniquePaths {
					capped = true
					capReason = fmt.Sprintf("too many unique paths (limit %d)", MaxUniquePaths)
					return
				}
				pathCounts[pathID] = &candidate{}
			}
			pathCounts[pathID].count++
		}, func(pathBytes, valueBytes []byte) {
			if capped {
				return
			}

			pathStr := strings.ToLower(string(pathBytes))
			valueStr := string(valueBytes) // already lowercased by walker

			pathID := internStr(pathStr)
			valueID := internStr(valueStr)
			key := pvKey{pathID, valueID}

			// Dedup within record.
			if _, seen := seenPVs[key]; seen {
				return
			}
			seenPVs[key] = struct{}{}

			// Check hard cap.
			if _, exists := pvCounts[key]; !exists {
				if len(pvCounts) >= MaxTotalPVPairs {
					capped = true
					capReason = fmt.Sprintf("too many pv pairs (limit %d)", MaxTotalPVPairs)
					return
				}
				pvCounts[key] = &candidate{}
			}
			pvCounts[key].count++
		})
	}
	cursor.Close()
	pass1Duration := time.Since(pass1Start)

	if capped {
		idx.logger.Warn("json index capped due to hard cap",
			"chunk", chunkID.String(),
			"reason", capReason,
		)
		return idx.writeCappedIndex(chunkID)
	}

	if len(pathCounts) == 0 {
		// No JSON found in this chunk; write an empty complete index.
		return idx.writeEmptyIndex(chunkID)
	}

	// Budget-based admission for path-value pairs.
	// Paths are always admitted.
	type pvCandidate struct {
		key       pvKey
		frequency uint32
		cost      int // positionSize * count
	}

	pvList := make([]pvCandidate, 0, len(pvCounts))
	for k, c := range pvCounts {
		pvList = append(pvList, pvCandidate{
			key:       k,
			frequency: c.count,
			cost:      int(c.count) * positionSize,
		})
	}

	// Sort by descending frequency, ascending cost.
	slices.SortFunc(pvList, func(a, b pvCandidate) int {
		if a.frequency != b.frequency {
			return int(b.frequency) - int(a.frequency)
		}
		return a.cost - b.cost
	})

	// Admit pv entries within budget.
	admittedPV := make(map[pvKey]*candidate)
	pvTotalBytes := int64(0)
	jsonStatus := index.JSONComplete
	for _, c := range pvList {
		entryCost := int64(pvEntrySize + c.cost)
		if pvTotalBytes+entryCost > idx.budget {
			jsonStatus = index.JSONCapped
			break
		}
		admittedPV[c.key] = pvCounts[c.key]
		pvTotalBytes += entryCost
	}

	// Build sorted dictionary. Only include strings that are actually referenced.
	usedStrings := make(map[uint32]struct{})
	for pathID := range pathCounts {
		usedStrings[pathID] = struct{}{}
	}
	for k := range admittedPV {
		usedStrings[k.pathID] = struct{}{}
		usedStrings[k.valueID] = struct{}{}
	}

	type dictEntry struct {
		oldID uint32
		str   string
	}
	var dictEntries []dictEntry
	for id := range usedStrings {
		dictEntries = append(dictEntries, dictEntry{id, dictStrings[id]})
	}
	slices.SortFunc(dictEntries, func(a, b dictEntry) int {
		return cmp.Compare(a.str, b.str)
	})

	// Build new dictionary and ID remapping.
	newDict := make([]string, len(dictEntries))
	remap := make(map[uint32]uint32) // oldID → newID
	for i, e := range dictEntries {
		newDict[i] = e.str
		remap[e.oldID] = uint32(i)
	}

	// Build sorted path table entries.
	type pathBuildEntry struct {
		newDictID uint32
		count     uint32
	}
	pathBuild := make([]pathBuildEntry, 0, len(pathCounts))
	for pathID, c := range pathCounts {
		pathBuild = append(pathBuild, pathBuildEntry{remap[pathID], c.count})
	}
	slices.SortFunc(pathBuild, func(a, b pathBuildEntry) int {
		return cmp.Compare(a.newDictID, b.newDictID)
	})

	// Build sorted pv table entries.
	type pvBuildEntry struct {
		newPathID  uint32
		newValueID uint32
		count      uint32
	}
	pvBuild := make([]pvBuildEntry, 0, len(admittedPV))
	for k, c := range admittedPV {
		pvBuild = append(pvBuild, pvBuildEntry{remap[k.pathID], remap[k.valueID], c.count})
	}
	slices.SortFunc(pvBuild, func(a, b pvBuildEntry) int {
		if c := cmp.Compare(a.newPathID, b.newPathID); c != 0 {
			return c
		}
		return cmp.Compare(a.newValueID, b.newValueID)
	})

	// Compute posting blob layout.
	blobSize := uint32(0)
	pathTable := make([]pathTableEntry, len(pathBuild))
	for i, p := range pathBuild {
		pathTable[i] = pathTableEntry{
			dictID:     p.newDictID,
			blobOffset: blobSize,
			count:      p.count,
		}
		blobSize += p.count * positionSize
	}

	pvTable := make([]pvTableEntry, len(pvBuild))
	for i, pv := range pvBuild {
		pvTable[i] = pvTableEntry{
			pathID:     pv.newPathID,
			valueID:    pv.newValueID,
			blobOffset: blobSize,
			count:      pv.count,
		}
		blobSize += pv.count * positionSize
	}

	// Allocate posting blob and build cursor maps.
	postingBlob := make([]byte, blobSize)

	// Maps for pass 2: track write position within each posting list.
	pathWriteIdx := make(map[uint32]uint32) // newDictID → next write index
	pvWriteIdx := make(map[pvKey]uint32)     // (newPathID, newValueID) → next write index

	// Build reverse lookup: newDictID → path table index
	pathBlobOffset := make(map[uint32]uint32) // newDictID → blob offset
	for _, p := range pathTable {
		pathBlobOffset[p.dictID] = p.blobOffset
	}

	pvBlobOffset := make(map[pvKey]uint32)
	for _, pv := range pvTable {
		pvBlobOffset[pvKey{pv.pathID, pv.valueID}] = pv.blobOffset
	}

	// Build old→new path lookup for pass 2.
	// We need to know which paths and pvs are admitted.
	admittedPathIDs := make(map[uint32]struct{}) // oldID set
	for pathID := range pathCounts {
		admittedPathIDs[pathID] = struct{}{}
	}
	admittedPVKeys := make(map[pvKey]struct{})
	for k := range admittedPV {
		admittedPVKeys[k] = struct{}{}
	}

	// PASS 2: Fill posting positions.
	pass2Start := time.Now()
	cursor2, err := idx.manager.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor pass 2: %w", err)
	}

	seenPaths2 := make(map[uint32]struct{}, 32)
	seenPVs2 := make(map[pvKey]struct{}, 32)

	for {
		if err := ctx.Err(); err != nil {
			cursor2.Close()
			return err
		}

		rec, ref, err := cursor2.Next()
		if err != nil {
			if err == chunk.ErrNoMoreRecords {
				break
			}
			cursor2.Close()
			return fmt.Errorf("read record pass 2: %w", err)
		}

		clear(seenPaths2)
		clear(seenPVs2)

		pos := uint32(ref.Pos)

		tokenizer.WalkJSON(rec.Raw, func(pathBytes []byte) {
			pathStr := strings.ToLower(string(pathBytes))
			oldID, ok := internMap[pathStr]
			if !ok {
				return
			}
			if _, admitted := admittedPathIDs[oldID]; !admitted {
				return
			}
			if _, seen := seenPaths2[oldID]; seen {
				return
			}
			seenPaths2[oldID] = struct{}{}

			newID := remap[oldID]
			blobOff := pathBlobOffset[newID]
			writePos := pathWriteIdx[newID]
			offset := blobOff + writePos*positionSize
			if int(offset)+positionSize <= len(postingBlob) {
				binary.LittleEndian.PutUint32(postingBlob[offset:], pos)
				pathWriteIdx[newID] = writePos + 1
			}
		}, func(pathBytes, valueBytes []byte) {
			pathStr := strings.ToLower(string(pathBytes))
			valueStr := string(valueBytes)
			oldPathID, ok1 := internMap[pathStr]
			oldValueID, ok2 := internMap[valueStr]
			if !ok1 || !ok2 {
				return
			}
			oldKey := pvKey{oldPathID, oldValueID}
			if _, admitted := admittedPVKeys[oldKey]; !admitted {
				return
			}
			if _, seen := seenPVs2[oldKey]; seen {
				return
			}
			seenPVs2[oldKey] = struct{}{}

			newKey := pvKey{remap[oldPathID], remap[oldValueID]}
			blobOff := pvBlobOffset[newKey]
			writePos := pvWriteIdx[newKey]
			offset := blobOff + writePos*positionSize
			if int(offset)+positionSize <= len(postingBlob) {
				binary.LittleEndian.PutUint32(postingBlob[offset:], pos)
				pvWriteIdx[newKey] = writePos + 1
			}
		})
	}
	cursor2.Close()
	pass2Duration := time.Since(pass2Start)

	// Encode and write.
	data := encodeIndex(newDict, pathTable, pvTable, postingBlob, jsonStatus)

	chunkDir := filepath.Join(idx.dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return fmt.Errorf("create index dir: %w", err)
	}

	if err := idx.writeFile(chunkDir, data); err != nil {
		return fmt.Errorf("write json index: %w", err)
	}

	droppedPV := len(pvCounts) - len(admittedPV)
	totalDuration := time.Since(buildStart)

	idx.logger.Debug("json index built",
		"chunk", chunkID.String(),
		"records", recordCount,
		"dict_size", len(newDict),
		"paths", len(pathTable),
		"pv_pairs", len(pvTable),
		"dropped_pv", droppedPV,
		"blob_bytes", len(postingBlob),
		"file_bytes", len(data),
		"status", jsonStatus,
		"pass1", pass1Duration,
		"pass2", pass2Duration,
		"total", totalDuration,
	)

	return nil
}

func (idx *Indexer) writeCappedIndex(chunkID chunk.ChunkID) error {
	data := encodeIndex(nil, nil, nil, nil, index.JSONCapped)
	chunkDir := filepath.Join(idx.dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return fmt.Errorf("create index dir: %w", err)
	}
	return idx.writeFile(chunkDir, data)
}

func (idx *Indexer) writeEmptyIndex(chunkID chunk.ChunkID) error {
	data := encodeIndex(nil, nil, nil, nil, index.JSONComplete)
	chunkDir := filepath.Join(idx.dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return fmt.Errorf("create index dir: %w", err)
	}
	return idx.writeFile(chunkDir, data)
}

func (idx *Indexer) writeFile(chunkDir string, data []byte) error {
	target := filepath.Join(chunkDir, indexFileName)
	tmpFile, err := os.CreateTemp(chunkDir, indexFileName+".tmp.*")
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
