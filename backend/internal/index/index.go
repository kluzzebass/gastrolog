package index

import (
	"context"
	"errors"
	"time"

	"github.com/kluzzebass/gastrolog/internal/chunk"
)

var ErrIndexNotFound = errors.New("index not found")

type Indexer interface {
	// Name returns a stable identifier for this indexer
	// (e.g. "time", "source", "token").
	Name() string

	// Build builds the index for the given sealed chunk.
	// It is expected to:
	// - open its own cursor
	// - read records
	// - write its own index artifacts
	//
	// Build must be idempotent or overwrite existing artifacts.
	Build(ctx context.Context, chunkID chunk.ChunkID) error
}

// TimeIndexEntry holds a single entry in a time index.
type TimeIndexEntry struct {
	Timestamp time.Time
	RecordPos uint64
}

// SourceIndexEntry holds all record positions for a single source within a chunk.
type SourceIndexEntry struct {
	SourceID  chunk.SourceID
	Positions []uint64
}

// TokenIndexEntry holds all record positions for a single token within a chunk.
type TokenIndexEntry struct {
	Token     string
	Positions []uint64
}

// Index provides read access to a built index of any entry type.
type Index[T any] struct {
	entries []T
}

// NewIndex wraps a slice of entries as an Index.
func NewIndex[T any](entries []T) *Index[T] {
	return &Index[T]{entries: entries}
}

func (idx *Index[T]) Entries() []T {
	return idx.entries
}

type IndexManager interface {
	BuildIndexes(ctx context.Context, chunkID chunk.ChunkID) error
	OpenTimeIndex(chunkID chunk.ChunkID) (*Index[TimeIndexEntry], error)
	OpenSourceIndex(chunkID chunk.ChunkID) (*Index[SourceIndexEntry], error)
	OpenTokenIndex(chunkID chunk.ChunkID) (*Index[TokenIndexEntry], error)
}
