package analyzer

import (
	"errors"
	"slices"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/index/inverted"
)

// Analyzer provides read-only analysis of index health and effectiveness.
type Analyzer struct {
	cm chunk.ChunkManager
	im index.IndexManager
}

// New creates a new index analyzer.
func New(cm chunk.ChunkManager, im index.IndexManager) *Analyzer {
	return &Analyzer{cm: cm, im: im}
}

// AnalyzeChunk analyzes all indexes for a single chunk.
func (a *Analyzer) AnalyzeChunk(chunkID chunk.ChunkID) (*ChunkAnalysis, error) {
	meta, err := a.cm.Meta(chunkID)
	if err != nil {
		return nil, err
	}

	// Get chunk size by counting records
	var chunkRecords int64
	var chunkBytes int64
	cursor, err := a.cm.OpenCursor(chunkID)
	if err != nil {
		return nil, err
	}
	for {
		rec, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			cursor.Close()
			return nil, err
		}
		chunkRecords++
		chunkBytes += int64(len(rec.Raw))
	}
	cursor.Close()

	analysis := &ChunkAnalysis{
		ChunkID:      chunkID,
		ChunkBytes:   chunkBytes,
		ChunkRecords: chunkRecords,
		ChunkStartTS: meta.StartTS,
		ChunkEndTS:   meta.EndTS,
		Sealed:       meta.Sealed,
		AnalyzedAt:   time.Now(),
	}

	// Analyze each index type
	a.analyzeTimeIndex(analysis)
	a.analyzeTokenIndex(analysis)
	a.analyzeAttrKVIndex(analysis)
	a.analyzeKVIndex(analysis)

	return analysis, nil
}

// AnalyzeAll analyzes all chunks and provides aggregate statistics.
func (a *Analyzer) AnalyzeAll() (*AggregateAnalysis, error) {
	chunks, err := a.cm.List()
	if err != nil {
		return nil, err
	}

	agg := &AggregateAnalysis{
		BytesByIndexType:       make(map[IndexType]int64),
		AvgCoverageByIndexType: make(map[IndexType]float64),
		Chunks:                 make([]ChunkAnalysis, 0, len(chunks)),
		AnalyzedAt:             time.Now(),
	}

	// Track coverage sums for averaging
	coverageSums := make(map[IndexType]float64)
	coverageCounts := make(map[IndexType]int64)

	for _, meta := range chunks {
		ca, err := a.AnalyzeChunk(meta.ID)
		if err != nil {
			// Record error but continue
			agg.ChunksWithErrors++
			continue
		}

		agg.ChunksAnalyzed++
		agg.Chunks = append(agg.Chunks, *ca)

		// Aggregate summaries
		hasPartial := false
		hasBudgetExhaustion := false
		hasMissing := false

		for _, s := range ca.Summaries {
			agg.BytesByIndexType[s.IndexType] += s.BytesUsed

			if s.Status == StatusPartial {
				hasPartial = true
				if s.Reason == ReasonBudgetExhausted {
					hasBudgetExhaustion = true
				}
			}
			if s.Status == StatusDisabled {
				hasMissing = true
			}

			// Track coverage for averaging
			if s.Status == StatusEnabled || s.Status == StatusPartial {
				coverageSums[s.IndexType] += s.PercentOfChunk
				coverageCounts[s.IndexType]++
			}
		}

		if hasPartial {
			agg.ChunksWithPartialIndexes++
		}
		if hasBudgetExhaustion {
			agg.ChunksWithBudgetExhaustion++
		}
		if hasMissing {
			agg.ChunksWithMissingIndexes++
		}
	}

	// Compute averages
	for typ, sum := range coverageSums {
		if count := coverageCounts[typ]; count > 0 {
			agg.AvgCoverageByIndexType[typ] = sum / float64(count)
		}
	}

	return agg, nil
}

func (a *Analyzer) analyzeTimeIndex(ca *ChunkAnalysis) {
	idx, err := a.im.OpenTimeIndex(ca.ChunkID)
	if err != nil {
		ca.Summaries = append(ca.Summaries, IndexSummary{
			IndexType: IndexTypeTime,
			Status:    statusFromError(err),
			Error:     errorString(err),
		})
		return
	}

	entries := idx.Entries()
	stats := &TimeIndexStats{
		EntriesCount: int64(len(entries)),
	}

	if len(entries) > 0 {
		stats.EarliestTimestamp = entries[0].Timestamp
		stats.LatestTimestamp = entries[len(entries)-1].Timestamp
	}

	// Estimate index bytes: header (8) + entries * 12 bytes each
	stats.IndexBytes = 8 + int64(len(entries))*12

	// Derived stats
	if len(entries) > 0 && ca.ChunkRecords > 0 {
		stats.AvgRecordsPerSeek = float64(ca.ChunkRecords) / float64(len(entries))
		stats.SamplingIntervalRecords = ca.ChunkRecords / int64(len(entries))

		// Worst case scan: largest gap between consecutive entries
		var maxGap int64
		for i := 1; i < len(entries); i++ {
			gap := int64(entries[i].RecordPos - entries[i-1].RecordPos)
			if gap > maxGap {
				maxGap = gap
			}
		}
		// Also consider gap from last entry to end
		if len(entries) > 0 {
			lastGap := ca.ChunkRecords - int64(entries[len(entries)-1].RecordPos)
			if lastGap > maxGap {
				maxGap = lastGap
			}
		}
		stats.WorstCaseScanRecords = maxGap

		// Time span per entry
		if len(entries) > 1 {
			totalSpan := entries[len(entries)-1].Timestamp.Sub(entries[0].Timestamp)
			stats.TimeSpanPerEntry = totalSpan / time.Duration(len(entries)-1)
		}
	}

	// Red flags
	if ca.ChunkRecords > 0 && stats.WorstCaseScanRecords > ca.ChunkRecords/2 {
		stats.Warnings = append(stats.Warnings, "worst case scan exceeds 50% of chunk")
	}
	if stats.EntriesCount > 0 && stats.EntriesCount < 10 && ca.ChunkRecords > 10000 {
		stats.Warnings = append(stats.Warnings, "very sparse index on large chunk")
	}

	ca.TimeStats = stats
	ca.Summaries = append(ca.Summaries, IndexSummary{
		IndexType:      IndexTypeTime,
		BytesUsed:      stats.IndexBytes,
		PercentOfChunk: safePercent(stats.IndexBytes, ca.ChunkBytes),
		Status:         StatusEnabled,
	})
}

func (a *Analyzer) analyzeTokenIndex(ca *ChunkAnalysis) {
	idx, err := a.im.OpenTokenIndex(ca.ChunkID)
	if err != nil {
		ca.Summaries = append(ca.Summaries, IndexSummary{
			IndexType: IndexTypeToken,
			Status:    statusFromError(err),
			Error:     errorString(err),
		})
		return
	}

	entries := idx.Entries()
	stats := &TokenIndexStats{
		UniqueTokens: int64(len(entries)),
	}

	// Collect frequencies for distribution analysis
	frequencies := make([]int64, 0, len(entries))
	var maxPos uint64

	for _, e := range entries {
		freq := int64(len(e.Positions))
		frequencies = append(frequencies, freq)
		stats.TotalTokenOccurrences += freq

		// Track max position for coverage estimate
		for _, pos := range e.Positions {
			if pos > maxPos {
				maxPos = pos
			}
		}
	}

	// Distribution stats
	if len(frequencies) > 0 {
		slices.Sort(frequencies)
		stats.MaxTokenFrequency = frequencies[len(frequencies)-1]
		stats.P95TokenFrequency = percentile(frequencies, 95)
		stats.P50TokenFrequency = percentile(frequencies, 50)
		stats.AvgPositionsPerToken = float64(stats.TotalTokenOccurrences) / float64(len(entries))
	}

	// Coverage estimate
	stats.RecordsWithTokens = int64(maxPos + 1)
	if ca.ChunkRecords > 0 {
		stats.PercentRecordsIndexed = safePercent(stats.RecordsWithTokens, ca.ChunkRecords) * 100
	}

	// Estimate index bytes: header(8) + string table + posting blob
	// String table: per entry = 2 (len) + avg_token_len + 4 (offset) + 4 (count)
	// Posting blob: total_occurrences * 4
	avgTokenLen := 8 // Reasonable estimate
	stringTableBytes := int64(len(entries)) * int64(2+avgTokenLen+4+4)
	postingBlobBytes := stats.TotalTokenOccurrences * 4
	stats.IndexBytes = 8 + stringTableBytes + postingBlobBytes

	// Top tokens by frequency
	type tokenFreq struct {
		token string
		freq  int64
	}
	topN := make([]tokenFreq, 0, len(entries))
	for _, e := range entries {
		topN = append(topN, tokenFreq{e.Token, int64(len(e.Positions))})
	}
	slices.SortFunc(topN, func(a, b tokenFreq) int {
		return int(b.freq - a.freq) // Descending
	})
	limit := min(10, len(topN))
	for i := 0; i < limit; i++ {
		stats.TopTokensByFrequency = append(stats.TopTokensByFrequency, TokenFrequency{
			Token:     topN[i].token,
			Frequency: topN[i].freq,
		})
	}

	ca.TokenStats = stats
	ca.Summaries = append(ca.Summaries, IndexSummary{
		IndexType:      IndexTypeToken,
		BytesUsed:      stats.IndexBytes,
		PercentOfChunk: safePercent(stats.IndexBytes, ca.ChunkBytes),
		Status:         StatusEnabled,
	})
}

// keyIndexStats holds analysis results for a key-based inverted index.
type keyIndexStats struct {
	uniqueKeys       int64
	totalOccurrences int64
	maxPos           uint64
	topKeys          []TokenFrequency
	indexBytes       int64
}

// analyzeKeyIndex analyzes any key-based inverted index using generics.
func analyzeKeyIndex[T inverted.KeyEntry](entries []T) keyIndexStats {
	stats := keyIndexStats{
		uniqueKeys: int64(len(entries)),
	}

	type keyFreq struct {
		key  string
		freq int64
	}
	topN := make([]keyFreq, 0, len(entries))

	for _, e := range entries {
		positions := e.GetPositions()
		freq := int64(len(positions))
		stats.totalOccurrences += freq
		topN = append(topN, keyFreq{e.GetKey(), freq})

		for _, pos := range positions {
			if pos > stats.maxPos {
				stats.maxPos = pos
			}
		}

		// Estimate bytes: 2 (keyLen) + len(key) + 4 (offset) + 4 (count) + positions*4
		stats.indexBytes += int64(2 + len(e.GetKey()) + 4 + 4 + len(positions)*4)
	}

	// Sort and get top keys
	slices.SortFunc(topN, func(a, b keyFreq) int {
		return int(b.freq - a.freq)
	})
	limit := min(10, len(topN))
	for i := 0; i < limit; i++ {
		stats.topKeys = append(stats.topKeys, TokenFrequency{
			Token:     topN[i].key,
			Frequency: topN[i].freq,
		})
	}

	return stats
}

// valueIndexStats holds analysis results for a value-based inverted index.
type valueIndexStats struct {
	uniqueValues int64
	indexBytes   int64
}

// analyzeValueIndex analyzes any value-based inverted index using generics.
func analyzeValueIndex[T inverted.ValueEntry](entries []T) valueIndexStats {
	stats := valueIndexStats{
		uniqueValues: int64(len(entries)),
	}

	for _, e := range entries {
		positions := e.GetPositions()
		// Estimate bytes: 2 (valLen) + len(val) + 4 (offset) + 4 (count) + positions*4
		stats.indexBytes += int64(2 + len(e.GetValue()) + 4 + 4 + len(positions)*4)
	}

	return stats
}

// kvIndexStats holds analysis results for a key-value inverted index.
type kvIndexStats struct {
	uniquePairs int64
	indexBytes  int64
}

// analyzeKVPairIndex analyzes any key-value pair inverted index using generics.
func analyzeKVPairIndex[T inverted.KVEntry](entries []T) kvIndexStats {
	stats := kvIndexStats{
		uniquePairs: int64(len(entries)),
	}

	for _, e := range entries {
		positions := e.GetPositions()
		// Estimate bytes: 2 (keyLen) + key + 2 (valLen) + val + 4 (offset) + 4 (count) + positions*4
		stats.indexBytes += int64(2 + len(e.GetKey()) + 2 + len(e.GetValue()) + 4 + 4 + len(positions)*4)
	}

	return stats
}

func (a *Analyzer) analyzeAttrKVIndex(ca *ChunkAnalysis) {
	keyIdx, keyErr := a.im.OpenAttrKeyIndex(ca.ChunkID)
	valIdx, valErr := a.im.OpenAttrValueIndex(ca.ChunkID)
	kvIdx, kvErr := a.im.OpenAttrKVIndex(ca.ChunkID)

	// If all three fail, report disabled
	if keyErr != nil && valErr != nil && kvErr != nil {
		ca.Summaries = append(ca.Summaries, IndexSummary{
			IndexType: IndexTypeAttrKV,
			Status:    statusFromError(keyErr),
			Error:     errorString(keyErr),
		})
		return
	}

	stats := &AttrKVIndexStats{}
	var totalBytes int64 = 8 // Header for key index

	// Analyze key index
	if keyErr == nil {
		keyStats := analyzeKeyIndex(keyIdx.Entries())
		stats.UniqueKeys = keyStats.uniqueKeys
		stats.TotalOccurrences = keyStats.totalOccurrences
		stats.RecordsWithAttributes = int64(keyStats.maxPos + 1)
		totalBytes += keyStats.indexBytes

		// Convert top keys
		for _, tf := range keyStats.topKeys {
			stats.TopKeysByOccurrences = append(stats.TopKeysByOccurrences, AttrKeyStats{
				Key:              tf.Token,
				TotalOccurrences: tf.Frequency,
			})
		}
	}

	// Analyze value index
	if valErr == nil {
		valStats := analyzeValueIndex(valIdx.Entries())
		stats.UniqueValues = valStats.uniqueValues
		totalBytes += 8 + valStats.indexBytes // Header + data
	}

	// Analyze KV index
	if kvErr == nil {
		kvStats := analyzeKVPairIndex(kvIdx.Entries())
		stats.UniqueKeyValuePairs = kvStats.uniquePairs
		totalBytes += 8 + kvStats.indexBytes // Header + data
	}

	// Coverage
	if ca.ChunkRecords > 0 {
		stats.PercentRecordsCovered = safePercent(stats.RecordsWithAttributes, ca.ChunkRecords) * 100
	}

	stats.IndexBytes = totalBytes

	ca.AttrKVStats = stats
	ca.Summaries = append(ca.Summaries, IndexSummary{
		IndexType:      IndexTypeAttrKV,
		BytesUsed:      stats.IndexBytes,
		PercentOfChunk: safePercent(stats.IndexBytes, ca.ChunkBytes),
		Status:         StatusEnabled,
	})
}

func (a *Analyzer) analyzeKVIndex(ca *ChunkAnalysis) {
	keyIdx, keyStatus, keyErr := a.im.OpenKVKeyIndex(ca.ChunkID)
	valIdx, valStatus, valErr := a.im.OpenKVValueIndex(ca.ChunkID)
	kvIdx, kvStatus, kvErr := a.im.OpenKVIndex(ca.ChunkID)

	// If all three fail, report disabled
	if keyErr != nil && valErr != nil && kvErr != nil {
		ca.Summaries = append(ca.Summaries, IndexSummary{
			IndexType: IndexTypeKV,
			Status:    statusFromError(keyErr),
			Error:     errorString(keyErr),
		})
		return
	}

	stats := &KVIndexStats{
		KeyStatus:   kvStatusToIndexStatus(keyStatus, keyErr),
		ValueStatus: kvStatusToIndexStatus(valStatus, valErr),
		KVStatus:    kvStatusToIndexStatus(kvStatus, kvErr),
	}
	var totalBytes int64 = 9 // Header + status for key index

	// Analyze key index
	if keyErr == nil {
		keyStats := analyzeKeyIndex(keyIdx.Entries())
		stats.KeysIndexed = keyStats.uniqueKeys
		stats.KeysSeen = keyStats.uniqueKeys // We don't have actual "seen" count
		stats.TotalOccurrences = keyStats.totalOccurrences
		totalBytes += keyStats.indexBytes

		// Convert top keys
		stats.TopKeysByFrequency = keyStats.topKeys
	}

	// Analyze value index
	if valErr == nil {
		valStats := analyzeValueIndex(valIdx.Entries())
		stats.ValuesIndexed = valStats.uniqueValues
		stats.ValuesSeen = valStats.uniqueValues
		totalBytes += 9 + valStats.indexBytes // Header + status + data
	}

	// Analyze KV index
	if kvErr == nil {
		kvStats := analyzeKVPairIndex(kvIdx.Entries())
		stats.PairsIndexed = kvStats.uniquePairs
		stats.PairsSeen = kvStats.uniquePairs
		totalBytes += 9 + kvStats.indexBytes // Header + status + data
	}

	// Check for budget exhaustion
	stats.BudgetExhausted = keyStatus == index.KVCapped ||
		valStatus == index.KVCapped ||
		kvStatus == index.KVCapped

	stats.IndexBytes = totalBytes

	// Determine overall status and reason
	overallStatus := StatusEnabled
	var reason PartialReason
	if stats.BudgetExhausted {
		overallStatus = StatusPartial
		reason = ReasonBudgetExhausted
	}

	ca.KVStats = stats
	ca.Summaries = append(ca.Summaries, IndexSummary{
		IndexType:      IndexTypeKV,
		BytesUsed:      stats.IndexBytes,
		PercentOfChunk: safePercent(stats.IndexBytes, ca.ChunkBytes),
		Status:         overallStatus,
		Reason:         reason,
	})
}

// Helper functions

func statusFromError(err error) IndexStatus {
	if err == nil {
		return StatusEnabled
	}
	if errors.Is(err, index.ErrIndexNotFound) {
		return StatusDisabled
	}
	return StatusError
}

func errorString(err error) string {
	if err == nil || errors.Is(err, index.ErrIndexNotFound) {
		return ""
	}
	return err.Error()
}

func kvStatusToIndexStatus(status index.KVIndexStatus, err error) IndexStatus {
	if err != nil {
		return statusFromError(err)
	}
	if status == index.KVCapped {
		return StatusPartial
	}
	return StatusEnabled
}

func safePercent(part, whole int64) float64 {
	if whole == 0 {
		return 0
	}
	return float64(part) / float64(whole)
}

func percentile(sorted []int64, p int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := (len(sorted) - 1) * p / 100
	return sorted[idx]
}
