package index

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"gastrolog/internal/chunk"
)

var ErrIndexNotFound = errors.New("index not found")

// ManagerFactory creates an IndexManager from configuration parameters.
// Factories validate required params, apply defaults, and return a fully
// constructed manager or a descriptive error.
// Factories must not start goroutines or perform I/O beyond validation.
//
// The chunkManager parameter is required because indexers need to read
// chunk data to build indexes.
//
// The logger parameter is optional. If nil, the manager disables logging.
// Factories should scope the logger with component-specific attributes.
type ManagerFactory func(params map[string]string, chunkManager chunk.ChunkManager, logger *slog.Logger) (IndexManager, error)

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

// TokenIndexEntry holds all record positions for a single token within a chunk.
type TokenIndexEntry struct {
	Token     string
	Positions []uint64
}

// AttrKeyIndexEntry holds all record positions where a specific attribute key exists.
type AttrKeyIndexEntry struct {
	Key       string
	Positions []uint64
}

func (e AttrKeyIndexEntry) GetKey() string         { return e.Key }
func (e AttrKeyIndexEntry) GetPositions() []uint64 { return e.Positions }

// AttrValueIndexEntry holds all record positions where a specific attribute value exists.
type AttrValueIndexEntry struct {
	Value     string
	Positions []uint64
}

func (e AttrValueIndexEntry) GetValue() string       { return e.Value }
func (e AttrValueIndexEntry) GetPositions() []uint64 { return e.Positions }

// AttrKVIndexEntry holds all record positions where a specific key=value pair exists.
type AttrKVIndexEntry struct {
	Key       string
	Value     string
	Positions []uint64
}

func (e AttrKVIndexEntry) GetKey() string         { return e.Key }
func (e AttrKVIndexEntry) GetValue() string       { return e.Value }
func (e AttrKVIndexEntry) GetPositions() []uint64 { return e.Positions }

// KVKeyIndexEntry holds all record positions where a specific key was extracted
// from log message text. This is a heuristic, non-authoritative index.
type KVKeyIndexEntry struct {
	Key       string
	Positions []uint64
}

func (e KVKeyIndexEntry) GetKey() string         { return e.Key }
func (e KVKeyIndexEntry) GetPositions() []uint64 { return e.Positions }

// KVValueIndexEntry holds all record positions where a specific value was extracted
// from log message text. This is a heuristic, non-authoritative index.
type KVValueIndexEntry struct {
	Value     string
	Positions []uint64
}

func (e KVValueIndexEntry) GetValue() string       { return e.Value }
func (e KVValueIndexEntry) GetPositions() []uint64 { return e.Positions }

// KVIndexEntry holds all record positions where a specific key=value pair
// was extracted from log message text. This is a heuristic, non-authoritative index.
type KVIndexEntry struct {
	Key       string
	Value     string
	Positions []uint64
}

func (e KVIndexEntry) GetKey() string         { return e.Key }
func (e KVIndexEntry) GetValue() string       { return e.Value }
func (e KVIndexEntry) GetPositions() []uint64 { return e.Positions }

// KVIndexStatus indicates whether the kv index is complete or capped.
type KVIndexStatus int

const (
	// KVComplete indicates the index contains all extracted key=value pairs.
	KVComplete KVIndexStatus = iota
	// KVCapped indicates the index was capped due to cardinality limits.
	// Queries must fall back to runtime filtering.
	KVCapped
)

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

// SplitKV splits a combined key-value string (separated by null byte) into key and value.
// Used by attr and kv indexers which store key+"\x00"+value as map keys.
func SplitKV(kv string) (key, value string) {
	for i := 0; i < len(kv); i++ {
		if kv[i] == 0 {
			return kv[:i], kv[i+1:]
		}
	}
	return kv, ""
}

type IndexManager interface {
	BuildIndexes(ctx context.Context, chunkID chunk.ChunkID) error
	DeleteIndexes(chunkID chunk.ChunkID) error
	OpenTokenIndex(chunkID chunk.ChunkID) (*Index[TokenIndexEntry], error)
	OpenAttrKeyIndex(chunkID chunk.ChunkID) (*Index[AttrKeyIndexEntry], error)
	OpenAttrValueIndex(chunkID chunk.ChunkID) (*Index[AttrValueIndexEntry], error)
	OpenAttrKVIndex(chunkID chunk.ChunkID) (*Index[AttrKVIndexEntry], error)

	// OpenKVKeyIndex opens the message key index for the given chunk.
	// Returns the index entries and a status indicating whether the index is complete.
	// If status is KVCapped, the index was truncated due to cardinality limits.
	OpenKVKeyIndex(chunkID chunk.ChunkID) (*Index[KVKeyIndexEntry], KVIndexStatus, error)

	// OpenKVValueIndex opens the message value index for the given chunk.
	// Returns the index entries and a status indicating whether the index is complete.
	// If status is KVCapped, the index was truncated due to cardinality limits.
	OpenKVValueIndex(chunkID chunk.ChunkID) (*Index[KVValueIndexEntry], KVIndexStatus, error)

	// OpenKVIndex opens the kv index for the given chunk.
	// Returns the index entries and a status indicating whether the index is complete.
	// If status is KVCapped, the index was truncated due to cardinality limits
	// and queries must fall back to runtime filtering.
	// Returns ErrIndexNotFound if the index doesn't exist.
	OpenKVIndex(chunkID chunk.ChunkID) (*Index[KVIndexEntry], KVIndexStatus, error)

	// IndexesComplete reports whether all indexes exist for the given chunk.
	// Returns true if all indexes are present, false if any are missing.
	// May clean up orphaned temporary files as a side effect.
	// Note: A capped kv index is still considered "complete" (it exists).
	IndexesComplete(chunkID chunk.ChunkID) (bool, error)

	// FindIngestStartPosition returns the earliest record position with IngestTS >= ts.
	// Returns (pos, true, nil) if found, (0, false, nil) if ts is after all records.
	// Returns ErrIndexNotFound if the ingest index does not exist.
	FindIngestStartPosition(chunkID chunk.ChunkID, ts time.Time) (uint64, bool, error)

	// FindSourceStartPosition returns the earliest record position with SourceTS >= ts.
	// Returns (pos, true, nil) if found, (0, false, nil) if ts is after all records.
	// Returns ErrIndexNotFound if the source index does not exist.
	FindSourceStartPosition(chunkID chunk.ChunkID, ts time.Time) (uint64, bool, error)

	// IndexSizes returns the size in bytes for each index.
	// For file-backed indexes this is the on-disk file size.
	// For in-memory indexes this is an estimate of the data footprint.
	// Missing indexes are omitted from the map.
	IndexSizes(chunkID chunk.ChunkID) map[string]int64
}
