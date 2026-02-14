package repl

import (
	"context"
	"iter"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index/analyzer"
	"gastrolog/internal/query"
)

// Client abstracts the backend operations the REPL needs.
// This allows the REPL to work with either a direct orchestrator
// connection (embedded mode) or a remote gRPC connection.
type Client interface {
	// Query operations
	Search(ctx context.Context, store string, q query.Query, resume *query.ResumeToken) (iter.Seq2[chunk.Record, error], func() *query.ResumeToken, error)
	Follow(ctx context.Context, store string, q query.Query) (iter.Seq2[chunk.Record, error], error)
	Explain(ctx context.Context, store string, q query.Query) (*query.QueryPlan, error)

	// Store operations
	ListStores() []StoreInfo
	ChunkManager(store string) ChunkReader
	IndexManager(store string) IndexReader

	// Analysis
	Analyzer(store string) AnalyzerClient

	// Status
	IsRunning() bool
}

// ChunkReader provides read-only access to chunks.
type ChunkReader interface {
	List() ([]chunk.ChunkMeta, error)
	Meta(id chunk.ChunkID) (chunk.ChunkMeta, error)
	Active() *chunk.ChunkMeta
	OpenCursor(id chunk.ChunkID) (chunk.RecordCursor, error)
}

// IndexReader provides read-only access to index information.
type IndexReader interface {
	IndexesComplete(id chunk.ChunkID) (bool, error)
	OpenTokenIndex(id chunk.ChunkID) (TokenIndex, error)
}

// TokenIndex provides read access to a token index.
type TokenIndex interface {
	Entries() []TokenIndexEntry
}

// TokenIndexEntry represents a token and its positions.
type TokenIndexEntry struct {
	Token     string
	Positions []uint64
}

// StoreInfo holds information about a store.
type StoreInfo struct {
	ID          string
	Name        string
	Filter      string
	ChunkCount  int
	RecordCount int64
}

// DisplayName returns the Name if set, otherwise the ID.
func (s StoreInfo) DisplayName() string {
	if s.Name != "" {
		return s.Name
	}
	return s.ID
}

// Stats holds aggregate statistics.
type Stats struct {
	TotalStores  int
	TotalChunks  int
	SealedChunks int
	TotalRecords int64
	OldestRecord time.Time
	NewestRecord time.Time
}

// AnalyzerClient wraps the analyzer for a specific store.
type AnalyzerClient interface {
	AnalyzeChunk(id chunk.ChunkID) (*analyzer.ChunkAnalysis, error)
	AnalyzeAll() (*analyzer.AggregateAnalysis, error)
}
