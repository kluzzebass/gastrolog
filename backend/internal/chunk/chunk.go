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
	ErrChunkArchived  = errors.New("chunk is archived and not immediately readable")
	ErrChunkSuspect   = errors.New("chunk blob not found in cloud storage — may be transient")
	ErrNoTSIndex      = errors.New("no TS index available")
	// ErrChunkSealed signals that an Append targeted a chunk the
	// cluster (via vault-ctl Raft FSM) considers sealed. Returned by
	// the Manager's append-side gate so the caller can rotate to a
	// fresh active chunk instead of silently extending a chunk the
	// cluster has frozen. See gastrolog-uccg6.
	ErrChunkSealed = errors.New("chunk is sealed; cannot append")
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

	// FindIngestEntryIndex returns the rank (entry index in the IngestTS-
	// sorted index) of the first entry with IngestTS >= ts. Distinct from
	// FindIngestStartPosition which returns the physical record position;
	// the two differ for chunks where physical layout doesn't match
	// IngestTS order (cloud chunks built via ImportRecords). Histogram
	// bucket counting must use rank arithmetic. Returns (0, false, nil)
	// when the chunk has no in-manager TS index (sealed local file
	// chunks — caller falls through to IndexManager.FindIngestEntryIndex).
	// See gastrolog-66b7x.
	FindIngestEntryIndex(id ChunkID, ts time.Time) (uint64, bool, error)

	// ScanActiveIngestTS iterates the active chunk's IngestTS B+ tree in
	// IngestTS-sorted order. The callback receives IngestTS in nanoseconds
	// and returns false to stop early. Returns ErrChunkNotFound if id is
	// not the active chunk. No attr or raw reads — cheap. Used by the
	// histogram counts path on non-monotonic active chunks (tier 2+
	// destinations) where position-as-rank assumptions break.
	// See gastrolog-66b7x.
	ScanActiveIngestTS(id ChunkID, cb func(tsNanos int64) bool) error

	// ScanActiveByIngestTS iterates the active chunk's records in physical
	// (append) order, exposing both IngestTS and Attributes per record.
	// Returns ErrChunkNotFound if id is not the active chunk. Used by the
	// histogram level-breakdown path on non-monotonic active chunks where
	// per-bucket position-based sampling via ScanAttrs is unsafe. See
	// gastrolog-66b7x.
	ScanActiveByIngestTS(id ChunkID, cb func(ingestTS time.Time, attrs Attributes) bool) error

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

	// ImportRecords creates a new sealed chunk with the given ID by consuming
	// records from the iterator, re-stamping each record's WriteTS with a
	// fresh monotonic timestamp. The new chunk is independent of the active
	// chunk; concurrent Append calls are not affected.
	//
	// If id is the zero ChunkID, a new ID is generated. Passing a non-zero id
	// atomically assigns that ID to the imported chunk without going through
	// Manager-wide state — this is the only way to pin an import's ID when
	// concurrent Appends may also be creating chunks. See gastrolog-11rzz.
	ImportRecords(id ChunkID, next RecordIterator) (ChunkMeta, error)

	// ScanAttrs iterates records in a chunk starting from startPos, calling fn
	// with each record's WriteTS and Attributes. fn returns true to continue,
	// false to stop early. Pass startPos=0 to scan from the beginning.
	// This avoids loading message bodies, enabling lightweight metadata iteration.
	ScanAttrs(id ChunkID, startPos uint64, fn func(writeTS time.Time, attrs Attributes) bool) error

	// SetNextChunkID sets the ID for the next active chunk created by openLocked.
	// Used by followers to match the leader's chunk ID during replication.
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

// DirRemover extends ChunkManager with the ability to remove its entire
// data directory. Called after Close() when a tier is deleted so that
// leftover files (.lock, cloud.idx, etc.) and the directory itself are
// cleaned up. Implementations should not attempt any operations after
// RemoveDir returns.
type DirRemover interface {
	RemoveDir() error
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

// ChunkCloudUploader extends ChunkManager with the ability to upload a
// sealed chunk to cloud storage. Used by the cloud backfill path to retry
// uploads that failed when S3 was unreachable. See gastrolog-68fqk.
type ChunkCloudUploader interface {
	UploadToCloud(id ChunkID) error
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

// ChunkBudgetMonitor extends ChunkManager with memory budget awareness.
// The orchestrator checks BudgetExceeded during retention sweeps to force
// early transitions when a memory tier is over budget.
type ChunkBudgetMonitor interface {
	BudgetExceeded() int64 // bytes over budget, 0 = within budget or no budget
}

// ChunkCacheEvictor extends ChunkManager with cache eviction. The orchestrator
// calls EvictCache periodically to enforce size/TTL limits on the warm cache.
type ChunkCacheEvictor interface {
	EvictCache()
}

// CloudChunkInfo carries the metadata needed to register a cloud-backed chunk
// on a follower without streaming any records. All fields come from the tier
// Raft FSM entry (populated by AnnounceSeal + AnnounceUpload on the leader).
type CloudChunkInfo struct {
	WriteStart        time.Time
	WriteEnd          time.Time
	IngestStart       time.Time
	IngestEnd         time.Time
	SourceStart       time.Time
	SourceEnd         time.Time
	RecordCount       int64
	Bytes             int64
	DiskBytes         int64
	IngestIdxOffset   int64
	IngestIdxSize     int64
	SourceIdxOffset   int64
	SourceIdxSize     int64
	NumFrames         int32
	IngestTSMonotonic bool // see ChunkMeta.IngestTSMonotonic
}

// CloudChunkRegistrar extends ChunkManager with the ability to register a
// cloud-backed chunk from metadata alone — no local files, no record streaming.
// Used by follower nodes to adopt chunks from the shared S3 bucket after the
// tier FSM propagates the leader's AnnounceUpload.
type CloudChunkRegistrar interface {
	RegisterCloudChunk(id ChunkID, info CloudChunkInfo) error
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
