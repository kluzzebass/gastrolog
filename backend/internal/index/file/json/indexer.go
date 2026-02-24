package json

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
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
	MaxUniquePaths  = 50000
	MaxTotalPVPairs = 200000
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
//   - Path posting table (path -> record positions)
//   - Path-value posting table ((path,value) -> record positions)
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

type pass1Result struct {
	internMap   map[string]uint32
	dictStrings []string
	pathCounts  map[uint32]*candidate
	pvCounts    map[pvKey]*candidate
	recordCount uint64
	capped      bool
	capReason   string
	seenPaths   map[uint32]struct{}
	seenPVs     map[pvKey]struct{}
}

func (r *pass1Result) intern(s string) uint32 {
	if id, ok := r.internMap[s]; ok {
		return id
	}
	id := uint32(len(r.dictStrings)) //nolint:gosec // G115: dict size bounded by index budget
	r.internMap[s] = id
	r.dictStrings = append(r.dictStrings, s)
	return id
}

func (r *pass1Result) onPath(pathBytes []byte) {
	if r.capped {
		return
	}
	pathStr := strings.ToLower(string(pathBytes))
	pathID := r.intern(pathStr)

	if _, seen := r.seenPaths[pathID]; seen {
		return
	}
	r.seenPaths[pathID] = struct{}{}

	if _, exists := r.pathCounts[pathID]; !exists {
		if len(r.pathCounts) >= MaxUniquePaths {
			r.capped = true
			r.capReason = fmt.Sprintf("too many unique paths (limit %d)", MaxUniquePaths)
			return
		}
		r.pathCounts[pathID] = &candidate{}
	}
	r.pathCounts[pathID].count++
}

func (r *pass1Result) onPV(pathBytes, valueBytes []byte) {
	if r.capped {
		return
	}
	pathStr := strings.ToLower(string(pathBytes))
	valueStr := string(valueBytes)
	pathID := r.intern(pathStr)
	valueID := r.intern(valueStr)
	key := pvKey{pathID, valueID}

	if _, seen := r.seenPVs[key]; seen {
		return
	}
	r.seenPVs[key] = struct{}{}

	if _, exists := r.pvCounts[key]; !exists {
		if len(r.pvCounts) >= MaxTotalPVPairs {
			r.capped = true
			r.capReason = fmt.Sprintf("too many pv pairs (limit %d)", MaxTotalPVPairs)
			return
		}
		r.pvCounts[key] = &candidate{}
	}
	r.pvCounts[key].count++
}

func (idx *Indexer) pass1(ctx context.Context, chunkID chunk.ChunkID) (*pass1Result, time.Duration, error) {
	pass1Start := time.Now()

	r := &pass1Result{
		internMap:  make(map[string]uint32),
		pathCounts: make(map[uint32]*candidate),
		pvCounts:   make(map[pvKey]*candidate),
		seenPaths:  make(map[uint32]struct{}, 32),
		seenPVs:    make(map[pvKey]struct{}, 32),
	}

	cursor, err := idx.manager.OpenCursor(chunkID)
	if err != nil {
		return nil, 0, fmt.Errorf("open cursor: %w", err)
	}

	for {
		if err := ctx.Err(); err != nil {
			_ = cursor.Close()
			return nil, 0, err
		}

		rec, _, err := cursor.Next()
		if err != nil {
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				break
			}
			_ = cursor.Close()
			return nil, 0, fmt.Errorf("read record: %w", err)
		}
		r.recordCount++

		if r.capped {
			continue
		}

		clear(r.seenPaths)
		clear(r.seenPVs)
		tokenizer.WalkJSON(rec.Raw, r.onPath, r.onPV)
	}
	_ = cursor.Close()

	return r, time.Since(pass1Start), nil
}

type admissionResult struct {
	admittedPV map[pvKey]*candidate
	jsonStatus index.JSONIndexStatus
}

func (idx *Indexer) admitPV(p1 *pass1Result) *admissionResult {
	type pvCandidate struct {
		key       pvKey
		frequency uint32
		cost      int
	}

	pvList := make([]pvCandidate, 0, len(p1.pvCounts))
	for k, c := range p1.pvCounts {
		pvList = append(pvList, pvCandidate{
			key:       k,
			frequency: c.count,
			cost:      int(c.count) * positionSize,
		})
	}

	slices.SortFunc(pvList, func(a, b pvCandidate) int {
		if a.frequency != b.frequency {
			return int(b.frequency) - int(a.frequency)
		}
		return a.cost - b.cost
	})

	admittedPV := make(map[pvKey]*candidate)
	pvTotalBytes := int64(0)
	jsonStatus := index.JSONComplete
	for _, c := range pvList {
		entryCost := int64(pvEntrySize + c.cost)
		if pvTotalBytes+entryCost > idx.budget {
			jsonStatus = index.JSONCapped
			break
		}
		admittedPV[c.key] = p1.pvCounts[c.key]
		pvTotalBytes += entryCost
	}

	return &admissionResult{admittedPV: admittedPV, jsonStatus: jsonStatus}
}

type dictResult struct {
	newDict []string
	remap   map[uint32]uint32
}

func buildDictionary(p1 *pass1Result, admittedPV map[pvKey]*candidate) *dictResult {
	usedStrings := make(map[uint32]struct{})
	for pathID := range p1.pathCounts {
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
		dictEntries = append(dictEntries, dictEntry{id, p1.dictStrings[id]})
	}
	slices.SortFunc(dictEntries, func(a, b dictEntry) int {
		return cmp.Compare(a.str, b.str)
	})

	newDict := make([]string, len(dictEntries))
	remap := make(map[uint32]uint32)
	for i, e := range dictEntries {
		newDict[i] = e.str
		remap[e.oldID] = uint32(i)
	}

	return &dictResult{newDict: newDict, remap: remap}
}

type blobLayout struct {
	pathTable   []pathTableEntry
	pvTable     []pvTableEntry
	postingBlob []byte
	pathBlobOff map[uint32]uint32
	pvBlobOff   map[pvKey]uint32
}

func buildBlobLayout(p1 *pass1Result, admittedPV map[pvKey]*candidate, remap map[uint32]uint32) *blobLayout {
	type pathBuildEntry struct {
		newDictID uint32
		count     uint32
	}
	pathBuild := make([]pathBuildEntry, 0, len(p1.pathCounts))
	for pathID, c := range p1.pathCounts {
		pathBuild = append(pathBuild, pathBuildEntry{remap[pathID], c.count})
	}
	slices.SortFunc(pathBuild, func(a, b pathBuildEntry) int {
		return cmp.Compare(a.newDictID, b.newDictID)
	})

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

	blobSize := uint32(0)
	pathTable := make([]pathTableEntry, len(pathBuild))
	for i, p := range pathBuild {
		pathTable[i] = pathTableEntry{dictID: p.newDictID, blobOffset: blobSize, count: p.count}
		blobSize += p.count * positionSize
	}

	pvTable := make([]pvTableEntry, len(pvBuild))
	for i, pv := range pvBuild {
		pvTable[i] = pvTableEntry{pathID: pv.newPathID, valueID: pv.newValueID, blobOffset: blobSize, count: pv.count}
		blobSize += pv.count * positionSize
	}

	pathBlobOff := make(map[uint32]uint32)
	for _, p := range pathTable {
		pathBlobOff[p.dictID] = p.blobOffset
	}
	pvBlobOff := make(map[pvKey]uint32)
	for _, pv := range pvTable {
		pvBlobOff[pvKey{pv.pathID, pv.valueID}] = pv.blobOffset
	}

	return &blobLayout{
		pathTable:   pathTable,
		pvTable:     pvTable,
		postingBlob: make([]byte, blobSize),
		pathBlobOff: pathBlobOff,
		pvBlobOff:   pvBlobOff,
	}
}

type pass2State struct {
	internMap       map[string]uint32
	admittedPathIDs map[uint32]struct{}
	admittedPVKeys  map[pvKey]struct{}
	remap           map[uint32]uint32
	layout          *blobLayout
	pathWriteIdx    map[uint32]uint32
	pvWriteIdx      map[pvKey]uint32
	seenPaths       map[uint32]struct{}
	seenPVs         map[pvKey]struct{}
	pos             uint32
}

func (s *pass2State) onPath(pathBytes []byte) {
	pathStr := strings.ToLower(string(pathBytes))
	oldID, ok := s.internMap[pathStr]
	if !ok {
		return
	}
	if _, admitted := s.admittedPathIDs[oldID]; !admitted {
		return
	}
	if _, seen := s.seenPaths[oldID]; seen {
		return
	}
	s.seenPaths[oldID] = struct{}{}

	newID := s.remap[oldID]
	blobOff := s.layout.pathBlobOff[newID]
	writePos := s.pathWriteIdx[newID]
	offset := blobOff + writePos*positionSize
	if int(offset)+positionSize <= len(s.layout.postingBlob) {
		binary.LittleEndian.PutUint32(s.layout.postingBlob[offset:], s.pos)
		s.pathWriteIdx[newID] = writePos + 1
	}
}

func (s *pass2State) onPV(pathBytes, valueBytes []byte) {
	pathStr := strings.ToLower(string(pathBytes))
	oldPathID, ok1 := s.internMap[pathStr]
	oldValueID, ok2 := s.internMap[string(valueBytes)]
	if !ok1 || !ok2 {
		return
	}
	oldKey := pvKey{oldPathID, oldValueID}
	if _, admitted := s.admittedPVKeys[oldKey]; !admitted {
		return
	}
	if _, seen := s.seenPVs[oldKey]; seen {
		return
	}
	s.seenPVs[oldKey] = struct{}{}

	newKey := pvKey{s.remap[oldPathID], s.remap[oldValueID]}
	blobOff := s.layout.pvBlobOff[newKey]
	writePos := s.pvWriteIdx[newKey]
	offset := blobOff + writePos*positionSize
	if int(offset)+positionSize <= len(s.layout.postingBlob) {
		binary.LittleEndian.PutUint32(s.layout.postingBlob[offset:], s.pos)
		s.pvWriteIdx[newKey] = writePos + 1
	}
}

func (idx *Indexer) pass2(ctx context.Context, chunkID chunk.ChunkID, p1 *pass1Result, admittedPV map[pvKey]*candidate, remap map[uint32]uint32, layout *blobLayout) (time.Duration, error) {
	pass2Start := time.Now()

	s := &pass2State{
		internMap:    p1.internMap,
		remap:        remap,
		layout:       layout,
		pathWriteIdx: make(map[uint32]uint32),
		pvWriteIdx:   make(map[pvKey]uint32),
		seenPaths:    make(map[uint32]struct{}, 32),
		seenPVs:      make(map[pvKey]struct{}, 32),
	}

	s.admittedPathIDs = make(map[uint32]struct{})
	for pathID := range p1.pathCounts {
		s.admittedPathIDs[pathID] = struct{}{}
	}
	s.admittedPVKeys = make(map[pvKey]struct{})
	for k := range admittedPV {
		s.admittedPVKeys[k] = struct{}{}
	}

	cursor, err := idx.manager.OpenCursor(chunkID)
	if err != nil {
		return 0, fmt.Errorf("open cursor pass 2: %w", err)
	}

	for {
		if err := ctx.Err(); err != nil {
			_ = cursor.Close()
			return 0, err
		}

		rec, ref, err := cursor.Next()
		if err != nil {
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				break
			}
			_ = cursor.Close()
			return 0, fmt.Errorf("read record pass 2: %w", err)
		}

		clear(s.seenPaths)
		clear(s.seenPVs)
		s.pos = uint32(ref.Pos) //nolint:gosec // G115: record positions bounded by chunk record count (< 2^32)
		tokenizer.WalkJSON(rec.Raw, s.onPath, s.onPV)
	}
	_ = cursor.Close()

	return time.Since(pass2Start), nil
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

	p1, pass1Duration, err := idx.pass1(ctx, chunkID)
	if err != nil {
		return err
	}

	if p1.capped {
		idx.logger.Warn("json index capped due to hard cap",
			"chunk", chunkID.String(),
			"reason", p1.capReason,
		)
		return idx.writeCappedIndex(chunkID)
	}

	if len(p1.pathCounts) == 0 {
		return idx.writeEmptyIndex(chunkID)
	}

	ar := idx.admitPV(p1)
	dr := buildDictionary(p1, ar.admittedPV)
	layout := buildBlobLayout(p1, ar.admittedPV, dr.remap)

	pass2Duration, err := idx.pass2(ctx, chunkID, p1, ar.admittedPV, dr.remap, layout)
	if err != nil {
		return err
	}

	data := encodeIndex(dr.newDict, layout.pathTable, layout.pvTable, layout.postingBlob, ar.jsonStatus)

	chunkDir := filepath.Join(idx.dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o750); err != nil {
		return fmt.Errorf("create index dir: %w", err)
	}

	if err := idx.writeFile(chunkDir, data); err != nil {
		return fmt.Errorf("write json index: %w", err)
	}

	idx.logger.Debug("json index built",
		"chunk", chunkID.String(),
		"records", p1.recordCount,
		"dict_size", len(dr.newDict),
		"paths", len(layout.pathTable),
		"pv_pairs", len(layout.pvTable),
		"dropped_pv", len(p1.pvCounts)-len(ar.admittedPV),
		"blob_bytes", len(layout.postingBlob),
		"file_bytes", len(data),
		"status", ar.jsonStatus,
		"pass1", pass1Duration,
		"pass2", pass2Duration,
		"total", time.Since(buildStart),
	)

	return nil
}

func (idx *Indexer) writeCappedIndex(chunkID chunk.ChunkID) error {
	data := encodeIndex(nil, nil, nil, nil, index.JSONCapped)
	chunkDir := filepath.Join(idx.dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o750); err != nil {
		return fmt.Errorf("create index dir: %w", err)
	}
	return idx.writeFile(chunkDir, data)
}

func (idx *Indexer) writeEmptyIndex(chunkID chunk.ChunkID) error {
	data := encodeIndex(nil, nil, nil, nil, index.JSONComplete)
	chunkDir := filepath.Join(idx.dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o750); err != nil {
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
	tmpName := filepath.Clean(tmpFile.Name())

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
