// Package analyzer provides read-only analysis of index health and effectiveness.
//
// The analyzer evaluates all index types (token, attr, kv) to answer:
// "Are the indexes worth the bytes and build time they cost?"
//
// It operates on existing index artifacts only and does not modify indexes,
// influence query planning, or affect correctness.
package analyzer

import (
	"time"

	"gastrolog/internal/chunk"
)

// IndexType identifies the type of index being analyzed.
type IndexType string

const (
	IndexTypeToken  IndexType = "token"
	IndexTypeAttrKV IndexType = "attr_kv"
	IndexTypeKV     IndexType = "kv"
	IndexTypeJSON   IndexType = "json"
)

// IndexStatus indicates the health state of an index.
type IndexStatus string

const (
	StatusEnabled  IndexStatus = "enabled"  // Index is complete and functional
	StatusPartial  IndexStatus = "partial"  // Index exists but was truncated/capped
	StatusDisabled IndexStatus = "disabled" // Index doesn't exist or failed to load
	StatusError    IndexStatus = "error"    // Index exists but couldn't be read
)

// PartialReason explains why an index is partial.
type PartialReason string

const (
	ReasonNone            PartialReason = ""
	ReasonBudgetExhausted PartialReason = "budget_exhausted"
	ReasonCapsExceeded    PartialReason = "caps_exceeded"
	ReasonNotApplicable   PartialReason = "not_applicable"
)

// IndexSummary is the high-level health view for any index type.
type IndexSummary struct {
	IndexType      IndexType     `json:"index_type"`
	BytesUsed      int64         `json:"bytes_used"`
	PercentOfChunk float64       `json:"percent_of_chunk"` // Index bytes / chunk raw bytes
	Status         IndexStatus   `json:"status"`
	Reason         PartialReason `json:"reason,omitempty"`
	Error          string        `json:"error,omitempty"` // Error message if status is error
}

// TokenIndexStats holds detailed statistics for a token index.
type TokenIndexStats struct {
	UniqueTokens          int64 `json:"unique_tokens"`
	TotalTokenOccurrences int64 `json:"total_token_occurrences"` // Sum of all posting list lengths
	IndexBytes            int64 `json:"index_bytes"`

	// Distribution stats
	MaxTokenFrequency    int64   `json:"max_token_frequency"`
	P95TokenFrequency    int64   `json:"p95_token_frequency"`
	P50TokenFrequency    int64   `json:"p50_token_frequency"`
	AvgPositionsPerToken float64 `json:"avg_positions_per_token"`

	// Coverage stats
	RecordsWithTokens     int64   `json:"records_with_tokens"` // Approximate (max position + 1)
	PercentRecordsIndexed float64 `json:"percent_records_indexed"`

	// Top contributors
	TopTokensByFrequency []TokenFrequency `json:"top_tokens_by_frequency,omitempty"`
}

// TokenFrequency pairs a token with its occurrence count.
type TokenFrequency struct {
	Token     string `json:"token"`
	Frequency int64  `json:"frequency"`
}

// AttrKVIndexStats holds statistics for authoritative attribute indexes.
type AttrKVIndexStats struct {
	UniqueKeys          int64 `json:"unique_keys"`
	UniqueValues        int64 `json:"unique_values"`
	UniqueKeyValuePairs int64 `json:"unique_key_value_pairs"`
	TotalOccurrences    int64 `json:"total_occurrences"` // Sum of all posting lists

	IndexBytes int64 `json:"index_bytes"` // Combined size of all 3 attr index files

	// Coverage (attributes are always complete, no budget)
	RecordsWithAttributes int64   `json:"records_with_attributes"` // Approximate (max position + 1)
	PercentRecordsCovered float64 `json:"percent_records_covered"`

	// Top keys breakdown
	TopKeysByOccurrences []AttrKeyStats `json:"top_keys_by_occurrences,omitempty"`
}

// AttrKeyStats provides breakdown for a single attribute key.
type AttrKeyStats struct {
	Key              string  `json:"key"`
	UniqueValues     int64   `json:"unique_values"`
	TotalOccurrences int64   `json:"total_occurrences"`
	BytesUsed        int64   `json:"bytes_used"`
	CoveragePercent  float64 `json:"coverage_percent"`
}

// KVIndexStats holds statistics for heuristic message KV indexes.
type KVIndexStats struct {
	// Cardinality
	KeysSeen      int64 `json:"keys_seen"`    // Would be indexed without limits
	KeysIndexed   int64 `json:"keys_indexed"` // Actually indexed
	ValuesSeen    int64 `json:"values_seen"`
	ValuesIndexed int64 `json:"values_indexed"`
	PairsSeen     int64 `json:"pairs_seen"`
	PairsIndexed  int64 `json:"pairs_indexed"`

	// Totals
	TotalOccurrences int64 `json:"total_occurrences"`
	IndexBytes       int64 `json:"index_bytes"` // Combined size of all 3 kv index files

	// Budget info
	BudgetBytes       int64   `json:"budget_bytes"`
	BudgetUsedPercent float64 `json:"budget_used_percent"`
	BudgetExhausted   bool    `json:"budget_exhausted"`

	// Drop stats
	DroppedKeys     int64   `json:"dropped_keys"`
	DroppedValues   int64   `json:"dropped_values"`
	DroppedPairs    int64   `json:"dropped_pairs"`
	DropRatePercent float64 `json:"drop_rate_percent"` // Pairs dropped / pairs seen

	// Status
	KeyStatus   IndexStatus `json:"key_status"`
	ValueStatus IndexStatus `json:"value_status"`
	KVStatus    IndexStatus `json:"kv_status"`

	// Top contributors
	TopKeysByFrequency []TokenFrequency `json:"top_keys_by_frequency,omitempty"`
}

// JSONIndexStats holds statistics for the structural JSON index.
type JSONIndexStats struct {
	UniquePaths     int64 `json:"unique_paths"`
	UniquePVPairs   int64 `json:"unique_pv_pairs"`
	IndexBytes      int64 `json:"index_bytes"`
	BudgetExhausted bool  `json:"budget_exhausted"`

	PathStatus IndexStatus `json:"path_status"`
	PVStatus   IndexStatus `json:"pv_status"`
}

// ChunkAnalysis contains all index analysis for a single chunk.
type ChunkAnalysis struct {
	ChunkID      chunk.ChunkID `json:"chunk_id"`
	ChunkBytes   int64         `json:"chunk_bytes"`   // Raw data size
	ChunkRecords int64         `json:"chunk_records"` // Total records in chunk
	ChunkStartTS time.Time     `json:"chunk_start_ts"`
	ChunkEndTS   time.Time     `json:"chunk_end_ts"`
	Sealed       bool          `json:"sealed"`

	// High-level summaries
	Summaries []IndexSummary `json:"summaries"`

	// Detailed stats per index type
	TokenStats  *TokenIndexStats  `json:"token_stats,omitempty"`
	AttrKVStats *AttrKVIndexStats `json:"attr_kv_stats,omitempty"`
	KVStats     *KVIndexStats     `json:"kv_stats,omitempty"`
	JSONStats   *JSONIndexStats   `json:"json_stats,omitempty"`

	// Analysis timestamp
	AnalyzedAt time.Time `json:"analyzed_at"`
}

// AggregateAnalysis provides cross-chunk statistics.
type AggregateAnalysis struct {
	ChunksAnalyzed int64 `json:"chunks_analyzed"`

	// Bytes by index type
	BytesByIndexType map[IndexType]int64 `json:"bytes_by_index_type"`

	// Coverage averages
	AvgCoverageByIndexType map[IndexType]float64 `json:"avg_coverage_by_index_type"`

	// Problem counts
	ChunksWithPartialIndexes   int64 `json:"chunks_with_partial_indexes"`
	ChunksWithBudgetExhaustion int64 `json:"chunks_with_budget_exhaustion"`
	ChunksWithMissingIndexes   int64 `json:"chunks_with_missing_indexes"`
	ChunksWithErrors           int64 `json:"chunks_with_errors"`

	// Per-chunk results
	Chunks []ChunkAnalysis `json:"chunks,omitempty"`

	// Analysis timestamp
	AnalyzedAt time.Time `json:"analyzed_at"`
}
