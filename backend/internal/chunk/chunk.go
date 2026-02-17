// Package chunk defines the core abstractions for record storage.
// Records are stored in chunks, which are append-only logs that can be
// sealed and indexed. ChunkManager handles chunk lifecycle, while
// RecordCursor provides bidirectional iteration over records.
package chunk

import (
	"errors"
	"log/slog"
	"time"
)

var (
	ErrNoMoreRecords  = errors.New("no more records")
	ErrChunkNotSealed = errors.New("chunk is not sealed")
	ErrChunkNotFound  = errors.New("chunk not found")
	ErrActiveChunk    = errors.New("cannot delete active chunk")
	ErrMissingWriteTS = errors.New("append preserved requires non-zero WriteTS")
)

// ManagerFactory creates a ChunkManager from configuration parameters.
// Factories validate required params, apply defaults, and return a fully
// constructed manager or a descriptive error.
// Factories must not start goroutines or perform I/O beyond validation.
// The logger parameter provides a store-scoped logger; may be nil.
type ManagerFactory func(params map[string]string, logger *slog.Logger) (ChunkManager, error)

// ChunkManager manages the lifecycle of chunks.
// It handles appending records, sealing chunks, and providing access
// to chunk metadata and cursors.
type ChunkManager interface {
	Append(record Record) (ChunkID, uint64, error)

	// AppendPreserved appends a record using its existing WriteTS instead of
	// assigning a new one. The record's WriteTS must be non-zero.
	// Used by merge/clone operations to preserve original timestamps.
	// The caller is responsible for ensuring WriteTS monotonicity within a chunk
	// (i.e. each call's WriteTS must be >= the previous call's WriteTS).
	AppendPreserved(record Record) (ChunkID, uint64, error)

	Seal() error
	Active() *ChunkMeta
	Meta(id ChunkID) (ChunkMeta, error)
	List() ([]ChunkMeta, error)
	Delete(id ChunkID) error
	OpenCursor(id ChunkID) (RecordCursor, error)

	// FindStartPosition binary searches for the record index at or before the given timestamp.
	// Returns (pos, true, nil) if found, (0, false, nil) if timestamp is before all records.
	// This enables time-based seeking without requiring the time index to be built.
	FindStartPosition(id ChunkID, ts time.Time) (uint64, bool, error)

	// ReadWriteTimestamps reads the WriteTS for each given record position in a chunk.
	// Positions must be valid indices. Returns timestamps in the same order as positions.
	ReadWriteTimestamps(id ChunkID, positions []uint64) ([]time.Time, error)

	// SetRotationPolicy updates the rotation policy for future appends.
	// This takes effect immediately; the next append will use the new policy.
	SetRotationPolicy(policy RotationPolicy)

	// Close releases resources held by the manager (file locks, mmap regions, etc).
	// After Close, the manager must not be used.
	Close() error
}

// ChunkMover extends ChunkManager with filesystem-level chunk movement.
// Not all ChunkManager implementations support this (e.g. memory-based ones
// cannot). Callers should type-assert to check availability.
type ChunkMover interface {
	// Adopt registers a sealed chunk directory already present in the storage dir.
	// The chunk must have valid idx.log metadata and be sealed.
	Adopt(id ChunkID) (ChunkMeta, error)

	// Disown untracks a sealed chunk without deleting its files.
	// The chunk must be sealed and not the active chunk.
	Disown(id ChunkID) error

	// ChunkDir returns the filesystem path for a chunk's directory.
	ChunkDir(id ChunkID) string
}

// ChunkCompressor extends ChunkManager with post-seal compression.
// Not all ChunkManager implementations support this (e.g. memory-based ones
// have nothing to compress). Callers should type-assert to check availability.
type ChunkCompressor interface {
	// CompressChunk compresses the data files of a sealed chunk.
	// No-op if compression is not enabled or the chunk is already compressed.
	CompressChunk(id ChunkID) error

	// SetCompressionEnabled enables or disables compression for future seals.
	// When enabled, uses zstd compression. Safe to call at any time.
	SetCompressionEnabled(enabled bool) error

	// RefreshDiskSizes recomputes Bytes and DiskBytes for a sealed chunk
	// from the actual directory contents. Call after index builds or other
	// operations that add/remove files in the chunk directory.
	RefreshDiskSizes(id ChunkID)
}

// RecordCursor provides bidirectional iteration over records in a chunk.
type RecordCursor interface {
	Next() (Record, RecordRef, error)
	Prev() (Record, RecordRef, error)
	Seek(ref RecordRef) error
	Close() error
}

// MetaStore persists chunk metadata.
type MetaStore interface {
	Save(meta ChunkMeta) error
	Load(id ChunkID) (ChunkMeta, error)
	List() ([]ChunkMeta, error)
}
