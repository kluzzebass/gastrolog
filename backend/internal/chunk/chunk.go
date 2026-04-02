// Package chunk defines the core abstractions for record storage.
// Records are stored in chunks, which are append-only logs that can be
// sealed and indexed. ChunkManager handles chunk lifecycle, while
// RecordCursor provides bidirectional iteration over records.
package chunk

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

var (
	ErrNoMoreRecords  = errors.New("no more records")
	ErrChunkNotSealed = errors.New("chunk is not sealed")
	ErrChunkNotFound  = errors.New("chunk not found")
	ErrVaultNotFound  = errors.New("vault not found")
	ErrActiveChunk    = errors.New("cannot delete active chunk")
	ErrChunkArchived = errors.New("chunk is archived and not immediately readable")
	ErrChunkSuspect  = errors.New("chunk blob not found in cloud storage — may be transient")
	ErrNoTSIndex     = errors.New("no TS index available")
)

// TSIndexLoader is an optional interface for chunk managers that can provide
// TS index entries for cloud-backed chunks.
type TSIndexLoader interface {
	LoadIngestEntries(id ChunkID) ([]TSEntry, error)
	LoadSourceEntries(id ChunkID) ([]TSEntry, error)
}

// TSEntry is a (timestamp, position) pair from a timestamp index.
type TSEntry struct {
	TS  int64  // nanoseconds since Unix epoch
	Pos uint32 // record position within chunk
}

// ManagerFactory creates a ChunkManager from configuration parameters.
// Factories validate required params, apply defaults, and return a fully
// constructed manager or a descriptive error.
// Factories must not start goroutines or perform I/O beyond validation.
// The logger parameter provides a vault-scoped logger; may be nil.
type ManagerFactory func(params map[string]string, logger *slog.Logger) (ChunkManager, error)

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

	// FindIngestStartPosition returns the earliest record position with IngestTS >= ts.
	// For active chunks backed by a B+ tree, this does a tree lookup.
	// Returns (0, false, nil) if no matching entry exists or the chunk is sealed.
	FindIngestStartPosition(id ChunkID, ts time.Time) (uint64, bool, error)

	// FindSourceStartPosition returns the earliest record position with SourceTS >= ts.
	// For active chunks backed by a B+ tree, this does a tree lookup.
	// Returns (0, false, nil) if no matching entry exists or the chunk is sealed.
	FindSourceStartPosition(id ChunkID, ts time.Time) (uint64, bool, error)

	// ReadWriteTimestamps reads the WriteTS for each given record position in a chunk.
	// Positions must be valid indices. Returns timestamps in the same order as positions.
	ReadWriteTimestamps(id ChunkID, positions []uint64) ([]time.Time, error)

	// SetRotationPolicy updates the rotation policy for future appends.
	// This takes effect immediately; the next append will use the new policy.
	SetRotationPolicy(policy RotationPolicy)

	// CheckRotation evaluates the current rotation policy against the active
	// chunk state without an incoming record. If the policy triggers (e.g., age
	// exceeded), the active chunk is sealed. Returns the trigger name if rotation
	// occurred, nil otherwise. Safe to call from background sweeps.
	CheckRotation() *string

	// ImportRecords creates a new sealed chunk by consuming records from the
	// iterator, re-stamping each record's WriteTS with a fresh monotonic
	// timestamp. The new chunk is independent of the active chunk; concurrent
	// Append calls are not affected.
	// Returns the metadata of the newly created sealed chunk.
	ImportRecords(next RecordIterator) (ChunkMeta, error)

	// ScanAttrs iterates records in a chunk starting from startPos, calling fn
	// with each record's WriteTS and Attributes. fn returns true to continue,
	// false to stop early. Pass startPos=0 to scan from the beginning.
	// This avoids loading message bodies, enabling lightweight metadata iteration.
	ScanAttrs(id ChunkID, startPos uint64, fn func(writeTS time.Time, attrs Attributes) bool) error

	// SetNextChunkID sets the ID for the next active chunk created by openLocked.
	// Used by secondaries to match the primary's chunk ID during replication.
	// Consumed on next open — subsequent opens revert to NewChunkID().
	SetNextChunkID(id ChunkID)

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
	// No-op if the chunk is already compressed.
	CompressChunk(id ChunkID) error

	// RefreshDiskSizes recomputes Bytes and DiskBytes for a sealed chunk
	// from the actual directory contents. Call after index builds or other
	// operations that add/remove files in the chunk directory.
	RefreshDiskSizes(id ChunkID)
}


// ChunkIndexBuilder builds indexes for a sealed chunk.
// Implementations are injected into the chunk manager at construction time.
// The chunk manager calls Build after compression completes.
type ChunkIndexBuilder interface {
	Build(ctx context.Context, chunkID ChunkID) error
}

// ChunkPostSealProcessor extends ChunkManager with a unified post-seal pipeline.
// The pipeline handles compression, index building, and cloud upload in order.
// Callers should type-assert to check availability.
type ChunkPostSealProcessor interface {
	// PostSealProcess runs the full post-seal pipeline for a sealed chunk.
	// Order: compress → build indexes → refresh sizes → upload to cloud.
	PostSealProcess(ctx context.Context, id ChunkID) error

	// SetIndexBuilders injects index builders into the pipeline.
	// Must be called before PostSealProcess. Passing nil or empty disables
	// index building (e.g., for cloud-backed vaults).
	SetIndexBuilders(builders []ChunkIndexBuilder)

	// HasIndexBuilders reports whether index builders are injected.
	HasIndexBuilders() bool
}

// ChunkArchiver extends ChunkManager with storage-class lifecycle operations
// for cloud-backed chunks. Callers should type-assert to check availability.
type ChunkArchiver interface {
	// ArchiveChunk transitions a cloud-backed sealed chunk to an offline
	// storage class (e.g. "GLACIER", "DEEP_ARCHIVE", "Archive"). After this,
	// the chunk's Archived flag is set and cursor reads return ErrChunkArchived.
	ArchiveChunk(ctx context.Context, id ChunkID, storageClass string) error

	// RestoreChunk initiates retrieval of an archived chunk. On S3 this is
	// async (RestoreObject). Returns nil if already restored or not archived.
	// tier is the restore speed ("Expedited"/"Standard"/"Bulk" for S3,
	// "High"/"Standard" for Azure). days is how long the restored copy stays
	// readable (S3 only).
	RestoreChunk(ctx context.Context, id ChunkID, tier string, days int) error
}

// RecordCursor provides bidirectional iteration over records in a chunk.
type RecordCursor interface {
	Next() (Record, RecordRef, error)
	Prev() (Record, RecordRef, error)
	Seek(ref RecordRef) error
	Close() error
}

// RecordIterator yields records one at a time. Returns ErrNoMoreRecords when
// exhausted. Records are valid at least until the next call. Callers that
// store records beyond the next call must Copy() them.
type RecordIterator func() (Record, error)

// CursorIterator adapts a RecordCursor into a RecordIterator.
func CursorIterator(c RecordCursor) RecordIterator {
	return func() (Record, error) {
		rec, _, err := c.Next()
		return rec, err
	}
}

// MetaStore persists chunk metadata.
type MetaStore interface {
	Save(meta ChunkMeta) error
	Load(id ChunkID) (ChunkMeta, error)
	List() ([]ChunkMeta, error)
}
