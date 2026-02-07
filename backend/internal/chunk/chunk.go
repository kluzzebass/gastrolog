// Package chunk defines the core abstractions for record storage.
// Records are stored in chunks, which are append-only logs that can be
// sealed and indexed. ChunkManager handles chunk lifecycle, while
// RecordCursor provides bidirectional iteration over records.
package chunk

import (
	"errors"
	"time"
)

var (
	ErrNoMoreRecords  = errors.New("no more records")
	ErrChunkNotSealed = errors.New("chunk is not sealed")
	ErrChunkNotFound  = errors.New("chunk not found")
	ErrActiveChunk    = errors.New("cannot delete active chunk")
)

// ManagerFactory creates a ChunkManager from configuration parameters.
// Factories validate required params, apply defaults, and return a fully
// constructed manager or a descriptive error.
// Factories must not start goroutines or perform I/O beyond validation.
type ManagerFactory func(params map[string]string) (ChunkManager, error)

// ChunkManager manages the lifecycle of chunks.
// It handles appending records, sealing chunks, and providing access
// to chunk metadata and cursors.
type ChunkManager interface {
	Append(record Record) (ChunkID, uint64, error)
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
