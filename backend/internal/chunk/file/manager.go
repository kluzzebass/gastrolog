package file

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/btree"
	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
	"gastrolog/internal/format"
	"gastrolog/internal/logging"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
)

// File names within a chunk directory.
const (
	rawLogFileName   = "raw.log"
	idxLogFileName   = "idx.log"
	attrLogFileName  = "attr.log"
	attrDictFileName = "attr_dict.log"
	ingestBTFileName = "ingest.bt"
	sourceBTFileName = "source.bt"
)

// Per-call timeouts on cloud storage operations that run on the post-seal
// pipeline or hold the chunk-manager mutex. Without these, a slow or
// unresponsive S3 can block the post-seal pipeline indefinitely, causing
// ingest backpressure that cascades up through the ingester. See
// gastrolog-21xs8. The S3 client's own retry logic compounds delays
// rather than capping them, so we need explicit per-call deadlines.
//
// Declared as vars (not consts) so tests can monkey-patch shorter values
// for deterministic timeout-regression tests without burning 60+ seconds
// per test run.
//
// Tunings:
//
//	cloudHeadTimeout    — tiny round-trip, any SLA-compliant S3 hits
//	                      this in well under a second; 10s accommodates
//	                      high-latency remotes with retries
//	cloudUploadTimeout  — chunk blobs are typically <10 MiB; 60s
//	                      handles slow networks (≈150 KiB/s floor)
//	cloudDownloadTimeout — TOC reads are 48 bytes; 10s is generous
//	cloudDeleteTimeout  — simple metadata op; 15s
var (
	cloudHeadTimeout     = 10 * time.Second
	cloudUploadTimeout   = 60 * time.Second
	cloudDownloadTimeout = 10 * time.Second
	cloudDeleteTimeout   = 15 * time.Second
)

var (
	ErrMissingDir      = errors.New("file chunk manager dir is required")
	ErrManagerClosed   = errors.New("manager is closed")
	ErrDirectoryLocked = errors.New("vault directory is locked by another process")
)

type Config struct {
	Dir      string
	FileMode os.FileMode
	Now      func() time.Time

	// RotationPolicy determines when to rotate chunks.
	// If nil, a default policy with 4GB hard limits is used.
	// Use chunk.NewCompositePolicy to combine multiple policies.
	RotationPolicy chunk.RotationPolicy

	// Logger for structured logging. If nil, logging is disabled.
	// The manager scopes this logger with component="chunk-manager".
	Logger *slog.Logger

	// ExpectExisting indicates that this vault is being loaded from config
	// (not freshly created). If the vault directory is missing, a warning
	// is logged about potential data loss.
	ExpectExisting bool

	// CloudStore, when non-nil, enables cloud backing for sealed chunks.
	// After compression, sealed chunks are converted to GLCB format,
	// uploaded to the cloud store, and local files are deleted.
	CloudStore blobstore.Store

	// VaultID is required when CloudStore is set (used for blob key prefix).
	VaultID uuid.UUID

	// CacheDir, when non-empty, enables local caching of cloud GLCB blobs.
	// Cloud chunks are cached after upload and on first read, avoiding
	// range-request round-trips.
	CacheDir string

	// CacheEviction selects the eviction policy: "lru" (default) or "ttl".
	CacheEviction string
	// CacheBudget is the max cache size as a human-readable string (e.g. "1GB", "500MB").
	// Parsed via config.ParseSize. Default 1 GiB.
	CacheBudget string
	// CacheTTL is the max age of cached blobs (TTL mode only). Parsed via config.ParseDuration.
	CacheTTL string

	// CloudReadOnly, when true, enables cloud store reads (cursor, cache)
	// but skips uploads in PostSealProcess. Used by follower nodes that
	// share the leader's S3 bucket for reads without owning the blobs.
	CloudReadOnly bool

	// Announcer, when non-nil, is called after each metadata state change
	// (create, seal, compress, upload, delete) for cluster-wide visibility.
	Announcer chunk.MetadataAnnouncer
}

// Manager manages file-based chunk storage with split raw.log and idx.log files.
//
// File layout per chunk:
//   - raw.log: 4-byte header + concatenated raw log bytes
//   - idx.log: 4-byte header + fixed-size (30-byte) entries
//   - attr.log: 4-byte header + concatenated attribute records
//
// Position semantics: RecordRef.Pos is a record index (0, 1, 2, ...), not a byte offset.
//
// Logging:
//   - Logger is dependency-injected via Config.Logger
//   - Manager owns its scoped logger (component="chunk-manager", type="file")
//   - Logging is intentionally sparse; only lifecycle events are logged
//   - No logging in hot paths (Append, cursor iteration)
type Manager struct {
	mu       sync.Mutex
	cfg      Config
	lockFile *os.File // Exclusive lock on vault directory
	active   *chunkState
	metas    map[chunk.ChunkID]*chunkMeta // In-memory chunk metadata
	closed   bool
	zstdEnc   *zstd.Encoder
	zstdEncMu sync.Mutex // serializes concurrent CompressChunk calls sharing zstdEnc
	cloudIdx      *cloudIndex              // local B+ tree cache of cloud chunk metadata (nil if no cloud store)
	cloudIdxMu    sync.Mutex              // serializes cloudIdx Insert/Delete/Sync (B+ tree is not thread-safe)
	indexBuilders  []chunk.ChunkIndexBuilder // injected post-construction via SetIndexBuilders
	cloudListCache []chunk.ChunkMeta        // cached List() result for cloud chunks; nil = stale
	storageClasses map[chunk.ChunkID]string  // in-memory cache of cloud storage class per chunk
	nextChunkID    *chunk.ChunkID           // if set, used instead of NewChunkID() on next open

	postSealActive sync.Map // chunk.ChunkID → chan struct{} — closed when PostSealProcess finishes
	postSealWg     sync.WaitGroup // tracks in-flight PostSealProcess calls (for Close only)

	// cloudDegraded tracks whether the cloud store is currently unreachable.
	// Set on any failed cloud operation (init, upload, download, list);
	// cleared on any successful one. The orchestrator polls this to raise
	// or clear an operator-visible alert. See gastrolog-68fqk.
	cloudDegraded     atomic.Bool
	cloudDegradedErr  atomic.Value // stores the last cloud error (string) for alert messages

	// pendingAnnouncements accumulates closures that fire metadata announcer
	// calls. The fields are protected by mu. Locked code paths (openLocked,
	// sealLocked, etc.) APPEND closures here instead of calling the announcer
	// directly. The top-level public methods (Append, Seal, CheckRotation)
	// drain this slice via takePendingAnnouncements AFTER releasing mu, then
	// invoke each closure outside the lock.
	//
	// Why: announcer calls go through the tier Raft via the apply forwarder
	// and BLOCK waiting for a Raft commit. The Raft FSM apply path on the
	// same node fires our OnDelete callback which acquires this manager's
	// mu. Holding mu while waiting for Raft creates a circular wait → deadlock.
	// Deferring announces until after the unlock breaks the cycle.
	pendingAnnouncements []func()

	// Logger for this manager instance.
	// Scoped with component="chunk-manager", type="file" at construction time.
	logger *slog.Logger
}

// takePendingAnnouncements drains the queue of deferred announcer calls.
// Must be called with m.mu held; the returned slice can then be invoked
// after releasing the mutex via runPendingAnnouncements.
func (m *Manager) takePendingAnnouncements() []func() {
	if len(m.pendingAnnouncements) == 0 {
		return nil
	}
	out := m.pendingAnnouncements
	m.pendingAnnouncements = nil
	return out
}

// runPendingAnnouncements invokes each deferred announcer closure. Must be
// called WITHOUT m.mu held — the closures issue announcer calls that may
// block on a Raft round-trip whose FSM apply path needs to acquire m.mu.
func runPendingAnnouncements(announces []func()) {
	for _, fn := range announces {
		fn()
	}
}

// chunkMeta holds in-memory metadata derived from idx.log.
// No longer persisted to meta.bin.
type chunkMeta struct {
	id               chunk.ChunkID
	writeStart          time.Time // WriteTS of first record
	writeEnd            time.Time // WriteTS of last record
	recordCount      int64     // Number of records in chunk
	bytes            int64     // Total logical bytes (data + non-data files)
	logicalDataBytes int64     // Logical data bytes only (raw + attr + idx content)
	sealed           bool
	compressed       bool  // true if raw.log/attr.log are compressed
	diskBytes        int64 // actual on-disk size (sum of all files)

	// IngestTS and SourceTS bounds (zero = unknown).
	ingestStart time.Time
	ingestEnd   time.Time
	sourceStart time.Time
	sourceEnd   time.Time

	cloudBacked bool // true = chunk lives in cloud, not on local disk
	archived    bool // true = chunk is in offline storage tier (Glacier, Azure Archive)

	// GLCB TOC: section offsets for embedded TS indexes (0 = none).
	ingestIdxOffset int64
	ingestIdxSize   int64
	sourceIdxOffset int64
	sourceIdxSize   int64

	numFrames int32 // seekable zstd frame count (cloud chunks only)
}

func (m *chunkMeta) toChunkMeta() chunk.ChunkMeta {
	return chunk.ChunkMeta{
		ID:          m.id,
		WriteStart:     m.writeStart,
		WriteEnd:       m.writeEnd,
		RecordCount: m.recordCount,
		Bytes:       m.bytes,
		Sealed:      m.sealed,
		Compressed:  m.compressed || m.cloudBacked, // GLCB blobs are always zstd-compressed
		DiskBytes:   m.diskBytes,
		IngestStart: m.ingestStart,
		IngestEnd:   m.ingestEnd,
		SourceStart: m.sourceStart,
		SourceEnd:   m.sourceEnd,
		CloudBacked: m.cloudBacked,
		Archived:    m.archived,
		NumFrames:   m.numFrames,
	}
}

type chunkState struct {
	meta        *chunkMeta
	rawFile     *os.File
	idxFile     *os.File
	attrFile    *os.File
	dictFile    *os.File
	dict        *chunk.StringDict
	ingestBT    *btree.Tree[int64, uint32] // IngestTS → record position
	sourceBT    *btree.Tree[int64, uint32] // SourceTS → record position
	rawOffset   uint64    // Current write position in raw.log (after header)
	attrOffset  uint64    // Current write position in attr.log (after header)
	recordCount uint64    // Number of records written
	createdAt   time.Time // Wall-clock time when chunk was opened
	writeMu     sync.Mutex     // serializes Phase 2 writes to preserve idx ordering on crash
	inflight    sync.WaitGroup // tracks in-flight Phase 2 writers for safe sealing
}

const lockFileName = ".lock"

func NewManager(cfg Config) (*Manager, error) {
	if cfg.Dir == "" {
		return nil, ErrMissingDir
	}
	cfg.FileMode = cmp.Or(cfg.FileMode, 0o644)
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.RotationPolicy == nil {
		// Default policy: only hard limits (4GB for uint32 offsets)
		cfg.RotationPolicy = chunk.NewHardLimitPolicy(MaxRawLogSize, MaxAttrLogSize)
	}

	// Check if the directory already exists before creating it.
	// If we have to create it, we track that so we can warn about
	// potential data loss (existing vault with missing directory).
	dirExisted := true
	if _, statErr := os.Stat(cfg.Dir); os.IsNotExist(statErr) {
		dirExisted = false
	}

	if err := os.MkdirAll(cfg.Dir, 0o750); err != nil {
		return nil, fmt.Errorf("create vault dir %s: %w", cfg.Dir, err)
	}

	// Acquire exclusive lock on vault directory.
	lockPath := filepath.Join(cfg.Dir, lockFileName)
	lockFile, err := os.OpenFile(filepath.Clean(lockPath), os.O_CREATE|os.O_RDWR, cfg.FileMode)
	if err != nil {
		return nil, fmt.Errorf("open lock file %s: %w", lockPath, err)
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil { //nolint:gosec // G115: uintptr->int is safe on 64-bit
		_ = lockFile.Close()
		return nil, fmt.Errorf("%w: %s", ErrDirectoryLocked, cfg.Dir)
	}

	// Scope logger with component identity.
	logger := logging.Default(cfg.Logger).With("component", "chunk-manager", "type", "file")

	zstdEnc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("create zstd encoder: %w", err)
	}

	manager := &Manager{
		cfg:            cfg,
		lockFile:       lockFile,
		metas:          make(map[chunk.ChunkID]*chunkMeta),
		storageClasses: make(map[chunk.ChunkID]string),
		zstdEnc:        zstdEnc,
		logger:         logger,
	}
	if err := manager.loadExisting(); err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("load existing chunks in %s: %w", cfg.Dir, err)
	}

	// Load cloud-backed chunks if a cloud store is configured.
	if cfg.CloudStore != nil {
		cidx, err := openCloudIndex(cfg.Dir)
		if err != nil {
			_ = lockFile.Close()
			return nil, fmt.Errorf("open cloud index: %w", err)
		}
		manager.cloudIdx = cidx
		if err := manager.loadCloudChunks(); err != nil {
			// S3 may be unreachable at startup (e.g. MinIO not started yet).
			// The cloud index stays empty — the active chunk on local disk
			// works independently. Existing cloud chunks will be discovered
			// on the next reconciliation sweep when S3 comes online. This
			// prevents the entire vault from being permanently skipped on
			// this node. See gastrolog-68fqk.
			logger.Warn("cloud chunk discovery failed, continuing without cloud index",
				"error", err)
			manager.cloudDegraded.Store(true)
		}
	}

	if cfg.CacheDir != "" {
		if err := os.MkdirAll(cfg.CacheDir, 0o750); err != nil {
			_ = lockFile.Close()
			return nil, fmt.Errorf("create cache dir: %w", err)
		}
		logger.Info("warm cache enabled", "cache_dir", cfg.CacheDir)
	}

	if cfg.ExpectExisting && !dirExisted {
		logger.Warn("vault directory was missing and has been recreated empty — if this vault previously held data, it may have been lost",
			"dir", cfg.Dir)
	}

	return manager, nil
}

func (m *Manager) Append(record chunk.Record) (chunk.ChunkID, uint64, error) {
	// ── Phase 1: lock → encode, reserve space ──
	m.mu.Lock()

	if m.closed {
		m.mu.Unlock()
		return chunk.ChunkID{}, 0, ErrManagerClosed
	}

	if m.active == nil {
		if err := m.openLocked(); err != nil {
			m.mu.Unlock()
			return chunk.ChunkID{}, 0, err
		}
	}

	attrBytes, newKeys, err := chunk.EncodeWithDict(record.Attrs, m.active.dict)
	if err != nil {
		m.mu.Unlock()
		return chunk.ChunkID{}, 0, err
	}

	attrBytes, newKeys, err = m.rotateIfNeeded(record, attrBytes, newKeys)
	if err != nil {
		m.mu.Unlock()
		return chunk.ChunkID{}, 0, err
	}

	record.WriteTS = m.cfg.Now()

	// Dict writes stay under lock (small, needs shared dict state).
	if err := m.writeDictEntries(newKeys); err != nil {
		m.mu.Unlock()
		return chunk.ChunkID{}, 0, err
	}

	// Pre-encode idx entry using current offsets (before advancing).
	var idxBuf [IdxEntrySize]byte
	EncodeIdxEntry(IdxEntry{
		SourceTS:   record.SourceTS,
		IngestTS:   record.IngestTS,
		WriteTS:    record.WriteTS,
		RawOffset:  uint32(m.active.rawOffset),  //nolint:gosec // G115: offsets bounded by chunk rotation policy
		RawSize:    uint32(len(record.Raw)),      //nolint:gosec // G115: individual record size bounded by protocol
		AttrOffset: uint32(m.active.attrOffset),  //nolint:gosec // G115: offsets bounded by chunk rotation policy
		AttrSize:   uint16(len(attrBytes)),       //nolint:gosec // G115: attribute size bounded by protocol
		IngestSeq:  record.EventID.IngestSeq,
		IngesterID: record.EventID.IngesterID,
	}, idxBuf[:])

	// Snapshot file handles and compute WriteAt positions.
	active := m.active
	rawPos := int64(format.HeaderSize) + int64(m.active.rawOffset)    //nolint:gosec // G115: bounded
	attrPos := int64(format.HeaderSize) + int64(m.active.attrOffset)  //nolint:gosec // G115: bounded
	idxPos := int64(IdxHeaderSize) + int64(m.active.recordCount)*int64(IdxEntrySize) //nolint:gosec // G115: bounded

	// Reserve space: advance counters while holding the lock.
	recordIndex := m.active.recordCount
	m.updateActiveState(record, uint64(len(record.Raw)), uint64(len(attrBytes)))
	chunkID := m.active.meta.id

	// Track this writer so seal/close can wait for completion.
	active.inflight.Add(1)
	pendingAnnounces := m.takePendingAnnouncements()
	m.mu.Unlock()

	// Fire deferred announcer calls (queued by openLocked / sealLocked
	// during rotation) outside the lock to avoid the FSM-apply-vs-mu
	// deadlock. See pendingAnnouncements field doc for details.
	runPendingAnnouncements(pendingAnnounces)

	// ── Phase 2: I/O without metadata lock ──
	// writeMu serializes disk writes so that records land in reservation
	// order, preserving the crash-safety invariant: idx.log is always a
	// reliable indicator of the last fully-written record.
	defer active.inflight.Done()
	active.writeMu.Lock()
	defer active.writeMu.Unlock()

	if _, err := active.rawFile.WriteAt(record.Raw, rawPos); err != nil {
		return chunk.ChunkID{}, 0, fmt.Errorf("write raw at offset %d: %w", rawPos, err)
	}
	if _, err := active.attrFile.WriteAt(attrBytes, attrPos); err != nil {
		return chunk.ChunkID{}, 0, fmt.Errorf("write attr at offset %d: %w", attrPos, err)
	}
	if _, err := active.idxFile.WriteAt(idxBuf[:], idxPos); err != nil {
		return chunk.ChunkID{}, 0, fmt.Errorf("write idx at offset %d: %w", idxPos, err)
	}

	// Insert into B+ tree indexes for IngestTS/SourceTS seeking.
	recPos := uint32(recordIndex) //nolint:gosec // G115: record index bounded by chunk rotation policy
	if err := active.ingestBT.Insert(record.IngestTS.UnixNano(), recPos); err != nil {
		return chunk.ChunkID{}, 0, fmt.Errorf("insert ingest btree: %w", err)
	}
	if !record.SourceTS.IsZero() {
		if err := active.sourceBT.Insert(record.SourceTS.UnixNano(), recPos); err != nil {
			return chunk.ChunkID{}, 0, fmt.Errorf("insert source btree: %w", err)
		}
	}

	return chunkID, recordIndex, nil
}

func (m *Manager) rotateIfNeeded(record chunk.Record, attrBytes []byte, newKeys []string) ([]byte, []string, error) {
	state := m.activeChunkState()
	trigger := m.cfg.RotationPolicy.ShouldRotate(state, record)
	if trigger == nil {
		return attrBytes, newKeys, nil
	}

	m.logger.Debug("rotating chunk",
		"trigger", *trigger,
		"chunk", state.ChunkID.String(),
		"bytes", state.Bytes,
		"records", state.Records,
		"age", m.cfg.Now().Sub(state.CreatedAt),
	)
	if err := m.sealLocked(); err != nil {
		return nil, nil, err
	}
	if err := m.openLocked(); err != nil {
		return nil, nil, err
	}
	attrBytes, newKeys, err := chunk.EncodeWithDict(record.Attrs, m.active.dict)
	return attrBytes, newKeys, err
}

func writeAll(f *os.File, data []byte) error {
	n, err := f.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

func (m *Manager) writeDictEntries(newKeys []string) error {
	for _, key := range newKeys {
		entry := chunk.EncodeDictEntry(key)
		if err := writeAll(m.active.dictFile, entry); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) updateActiveState(record chunk.Record, rawLen, attrLen uint64) {
	m.active.rawOffset += rawLen
	m.active.attrOffset += attrLen
	m.active.recordCount++
	m.active.meta.recordCount = int64(m.active.recordCount) //nolint:gosec // G115: record count bounded by rotation policy
	dataBytes := int64(m.active.rawOffset + m.active.attrOffset + m.active.recordCount*IdxEntrySize) //nolint:gosec // G115: data bytes bounded by rotation policy
	m.active.meta.logicalDataBytes = dataBytes
	m.active.meta.bytes = dataBytes

	if m.active.meta.writeStart.IsZero() {
		m.active.meta.writeStart = record.WriteTS
	}
	m.active.meta.writeEnd = record.WriteTS

	expandBounds(&m.active.meta.ingestStart, &m.active.meta.ingestEnd, record.IngestTS)
	if !record.SourceTS.IsZero() {
		expandBounds(&m.active.meta.sourceStart, &m.active.meta.sourceEnd, record.SourceTS)
	}
}

func expandBounds(start, end *time.Time, ts time.Time) {
	if start.IsZero() || ts.Before(*start) {
		*start = ts
	}
	if end.IsZero() || ts.After(*end) {
		*end = ts
	}
}

// activeChunkState creates an immutable snapshot of the active chunk's state.
// Must be called with m.mu held.
func (m *Manager) activeChunkState() chunk.ActiveChunkState {
	if m.active == nil {
		return chunk.ActiveChunkState{}
	}

	// Calculate total on-disk bytes: raw + attrs + idx entries.
	// B+ tree indexes are excluded: they are transient (deleted at seal time)
	// and their fixed page overhead would break small size policies.
	totalBytes := m.active.rawOffset + m.active.attrOffset + (m.active.recordCount * IdxEntrySize)

	return chunk.ActiveChunkState{
		ChunkID:     m.active.meta.id,
		WriteStart:     m.active.meta.writeStart,
		LastWriteTS: m.active.meta.writeEnd,
		CreatedAt:   m.active.createdAt,
		Bytes:       totalBytes,
		Records:     m.active.recordCount,
	}
}

func (m *Manager) Seal() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrManagerClosed
	}

	if m.active == nil {
		if err := m.openLocked(); err != nil {
			m.mu.Unlock()
			return err
		}
	}
	err := m.sealLocked()
	pending := m.takePendingAnnouncements()
	m.mu.Unlock()

	// Fire deferred announcer calls outside the lock.
	runPendingAnnouncements(pending)
	return err
}

func (m *Manager) Active() *chunk.ChunkMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil {
		return nil
	}
	meta := m.active.meta.toChunkMeta()
	return &meta
}

func (m *Manager) Meta(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	m.mu.Lock()
	meta := m.lookupMeta(id)
	m.mu.Unlock()
	if meta == nil {
		return chunk.ChunkMeta{}, chunk.ErrChunkNotFound
	}
	return meta.toChunkMeta(), nil
}

// lookupMeta checks the local map first, then the cloud B+ tree index.
// Must be called with m.mu held.
func (m *Manager) lookupMeta(id chunk.ChunkID) *chunkMeta {
	if meta, ok := m.metas[id]; ok {
		return meta
	}
	if m.cloudIdx == nil {
		return nil
	}
	meta, _ := m.cloudIdx.Lookup(id)
	return meta
}

func (m *Manager) List() ([]chunk.ChunkMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Append cloud chunks first so we can deduplicate local metas that
	// are also in the cloud index (e.g. during upload or if adoptCloudBlob
	// hasn't completed yet). The cloud version is authoritative.
	var cloudIDs map[chunk.ChunkID]struct{}
	if m.cloudIdx != nil {
		if m.cloudListCache == nil {
			m.rebuildCloudListCache()
		}
		cloudIDs = make(map[chunk.ChunkID]struct{}, len(m.cloudListCache))
		for i := range m.cloudListCache {
			cloudIDs[m.cloudListCache[i].ID] = struct{}{}
		}
	}

	out := make([]chunk.ChunkMeta, 0, len(m.metas))
	for _, meta := range m.metas {
		if cloudIDs != nil {
			if _, dup := cloudIDs[meta.id]; dup {
				continue // cloud version takes precedence
			}
		}
		out = append(out, meta.toChunkMeta())
	}
	if m.cloudIdx != nil {
		out = append(out, m.cloudListCache...)
	}

	// Sort by WriteStart to ensure consistent ordering.
	slices.SortFunc(out, func(a, b chunk.ChunkMeta) int {
		return a.WriteStart.Compare(b.WriteStart)
	})
	return out, nil
}

func (m *Manager) OpenCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	m.mu.Lock()
	meta := m.lookupMeta(id)
	m.mu.Unlock()
	if meta == nil {
		return nil, chunk.ErrChunkNotFound
	}

	if meta.cloudBacked {
		return m.openCloudCursor(id)
	}

	rawPath := m.rawLogPath(id)
	idxPath := m.idxLogPath(id)
	attrPath := m.attrLogPath(id)
	dictPath := m.dictLogPath(id)

	if meta.sealed {
		return newMmapCursor(id, rawPath, idxPath, attrPath, dictPath)
	}

	return newStdioCursor(id, rawPath, idxPath, attrPath, dictPath)
}

// ScanAttrs iterates all records in a chunk reading only idx.log + attr.log,
// skipping raw.log entirely. This enables O(~88 bytes/record) scans for
// aggregation queries that never inspect message bodies.
func (m *Manager) ScanAttrs(id chunk.ChunkID, startPos uint64, fn func(writeTS time.Time, attrs chunk.Attributes) bool) error {
	m.mu.Lock()
	meta := m.lookupMeta(id)
	if meta == nil {
		m.mu.Unlock()
		return chunk.ErrChunkNotFound
	}

	m.mu.Unlock()

	// Cloud-backed chunks: download and iterate via cursor.
	if meta.cloudBacked {
		return m.scanAttrsCloud(id, startPos, fn)
	}

	idxPath := m.idxLogPath(id)
	attrPath := m.attrLogPath(id)
	dictPath := m.dictLogPath(id)

	if meta.sealed {
		return scanAttrsSealed(idxPath, attrPath, dictPath, startPos, fn)
	}

	// Active chunk: load dict from disk (not the live in-memory dict)
	// to avoid racing with concurrent Append calls.
	return scanAttrsActive(idxPath, attrPath, dictPath, startPos, fn)
}


func (m *Manager) loadExisting() error {
	entries, err := os.ReadDir(m.cfg.Dir)
	if err != nil {
		return fmt.Errorf("read vault dir %s: %w", m.cfg.Dir, err)
	}

	// Collect all unsealed chunks to find the newest one.
	var unsealedIDs []chunk.ChunkID

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		id, err := chunk.ParseChunkID(entry.Name())
		if err != nil {
			// Not a valid chunk ID, skip.
			continue
		}

		// Clean up orphan temp files left by crashed compression or index builds.
		m.cleanOrphanTempFiles(filepath.Join(m.cfg.Dir, entry.Name()))

		meta, err := m.loadChunkMeta(id)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// No idx.log — this directory has no chunk data files.
				// Two cases:
				// 1. Cloud-backed chunk with preserved index files: keep it.
				// 2. Truly empty leftover directory: safe to remove.
				if chunkDirHasFiles(filepath.Join(m.cfg.Dir, entry.Name())) {
					m.logger.Debug("skipping cloud-backed chunk index directory", "chunk", id)
					continue
				}
				m.logger.Info("removing empty leftover chunk directory", "chunk", id)
				_ = os.RemoveAll(filepath.Join(m.cfg.Dir, entry.Name()))
				continue
			}
			return fmt.Errorf("load chunk meta for %s: %w", id, err)
		}

		m.metas[id] = meta

		if !meta.sealed {
			unsealedIDs = append(unsealedIDs, id)
		}
	}

	// If multiple unsealed chunks, seal all but the newest (by ChunkID, which is time-ordered).
	if len(unsealedIDs) > 1 {
		// Sort by ChunkID (UUID v7, time-ordered) - newest last.
		slices.SortFunc(unsealedIDs, func(a, b chunk.ChunkID) int {
			return cmp.Compare(a.String(), b.String())
		})

		// Seal all but the last (newest).
		for _, id := range unsealedIDs[:len(unsealedIDs)-1] {
			m.logger.Info("sealing orphaned active chunk", "chunk", id.String())
			if err := m.sealChunkOnDisk(id); err != nil {
				return fmt.Errorf("seal orphaned chunk %s: %w", id, err)
			}
			m.metas[id].sealed = true
		}

		// Keep only the newest as active candidate.
		unsealedIDs = unsealedIDs[len(unsealedIDs)-1:]
	}

	// Open the single remaining unsealed chunk as active.
	if len(unsealedIDs) == 1 {
		id := unsealedIDs[0]
		if err := m.openActiveChunk(id); err != nil {
			return fmt.Errorf("open active chunk %s: %w", id, err)
		}
	}

	return nil
}

// cleanOrphanTempFiles removes leftover temp files from a chunk directory.
// These can be left behind by crashed compression jobs (.compress-*) or
// index builds (*.tmp.*). Best-effort: errors are logged but not returned.
func (m *Manager) cleanOrphanTempFiles(chunkDir string) {
	entries, err := os.ReadDir(chunkDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".compress-") || strings.Contains(name, ".tmp.") {
			path := filepath.Join(chunkDir, name)
			if err := os.Remove(path); err != nil {
				m.logger.Warn("failed to remove orphan temp file", "path", path, "error", err)
			} else {
				m.logger.Info("removed orphan temp file", "path", path)
			}
		}
	}
}

// sealChunkOnDisk sets the sealed flag in the chunk's file headers without opening it as active.
func (m *Manager) sealChunkOnDisk(id chunk.ChunkID) error {
	rawPath := m.rawLogPath(id)
	idxPath := m.idxLogPath(id)
	attrPath := m.attrLogPath(id)
	dictPath := m.dictLogPath(id)

	// Set sealed flag in raw.log header.
	rawFile, err := os.OpenFile(filepath.Clean(rawPath), os.O_RDWR, m.cfg.FileMode)
	if err != nil {
		return err
	}
	if err := m.setSealedFlag(rawFile); err != nil {
		_ = rawFile.Close()
		return err
	}
	_ = rawFile.Close()

	// Set sealed flag in idx.log header.
	idxFile, err := os.OpenFile(filepath.Clean(idxPath), os.O_RDWR, m.cfg.FileMode)
	if err != nil {
		return err
	}
	if err := m.setSealedFlag(idxFile); err != nil {
		_ = idxFile.Close()
		return err
	}
	_ = idxFile.Close()

	// Set sealed flag in attr.log header.
	attrFile, err := os.OpenFile(filepath.Clean(attrPath), os.O_RDWR, m.cfg.FileMode)
	if err != nil {
		return err
	}
	if err := m.setSealedFlag(attrFile); err != nil {
		_ = attrFile.Close()
		return err
	}
	_ = attrFile.Close()

	// Set sealed flag in attr_dict.log header.
	dictFile, err := os.OpenFile(filepath.Clean(dictPath), os.O_RDWR, m.cfg.FileMode)
	if err != nil {
		return err
	}
	if err := m.setSealedFlag(dictFile); err != nil {
		_ = dictFile.Close()
		return err
	}
	_ = dictFile.Close()

	return nil
}

// openActiveChunk opens an unsealed chunk as the active chunk, with crash recovery.
// chunkFiles holds the four files that make up an active chunk.
// On error paths, closeAll releases all file handles.
type chunkFiles struct {
	raw, idx, attr, dict *os.File
}

func (cf *chunkFiles) closeAll(logger *slog.Logger) {
	for _, f := range []*os.File{cf.raw, cf.idx, cf.attr, cf.dict} {
		if err := f.Close(); err != nil {
			logger.Warn("close chunk file failed", "file", f.Name(), "error", err)
		}
	}
}

// openChunkFiles opens all four chunk files (raw, idx, attr, dict).
// On partial failure, already-opened files are closed before returning.
func (m *Manager) openChunkFiles(id chunk.ChunkID) (*chunkFiles, error) {
	rawFile, err := m.openRawFile(id)
	if err != nil {
		return nil, fmt.Errorf("open raw.log for chunk %s: %w", id, err)
	}
	idxFile, err := m.openIdxFile(id)
	if err != nil {
		_ = rawFile.Close()
		return nil, fmt.Errorf("open idx.log for chunk %s: %w", id, err)
	}
	attrFile, err := m.openAttrFile(id)
	if err != nil {
		_ = rawFile.Close()
		_ = idxFile.Close()
		return nil, fmt.Errorf("open attr.log for chunk %s: %w", id, err)
	}
	dictFile, err := m.openDictFile(id)
	if err != nil {
		_ = rawFile.Close()
		_ = idxFile.Close()
		_ = attrFile.Close()
		return nil, fmt.Errorf("open attr_dict.log for chunk %s: %w", id, err)
	}
	return &chunkFiles{raw: rawFile, idx: idxFile, attr: attrFile, dict: dictFile}, nil
}

// validateAndTruncate reads the idx.log header, computes the record count,
// and truncates raw.log/attr.log if they have crash-orphaned data beyond
// what the index accounts for.
func (m *Manager) validateAndTruncate(id chunk.ChunkID, cf *chunkFiles) (recordCount uint64, rawOffset, attrOffset uint64, createdAt time.Time, err error) {
	// Read idx.log header including createdAt timestamp.
	var headerBuf [IdxHeaderSize]byte
	if _, err = cf.idx.ReadAt(headerBuf[:], 0); err != nil {
		return 0, 0, 0, time.Time{}, fmt.Errorf("read idx.log header for chunk %s: %w", id, err)
	}
	if _, err = format.DecodeAndValidate(headerBuf[:format.HeaderSize], format.TypeIdxLog, IdxLogVersion); err != nil {
		return 0, 0, 0, time.Time{}, fmt.Errorf("invalid idx.log header for chunk %s: %w", id, err)
	}
	createdAtNanos := binary.LittleEndian.Uint64(headerBuf[format.HeaderSize:])
	createdAt = time.Unix(0, int64(createdAtNanos)) //nolint:gosec // G115: nanosecond timestamp fits in int64

	// Compute record count from idx.log file size.
	idxInfo, err := cf.idx.Stat()
	if err != nil {
		return 0, 0, 0, time.Time{}, fmt.Errorf("stat idx.log for chunk %s: %w", id, err)
	}
	recordCount = RecordCount(idxInfo.Size())

	// Compute expected raw.log and attr.log sizes from idx.log.
	// If files have extra data (crash between writes), truncate them.
	var expectedRawSize, expectedAttrSize int64
	if recordCount > 0 {
		lastOffset := IdxFileOffset(recordCount - 1)
		var entryBuf [IdxEntrySize]byte
		if _, err = cf.idx.ReadAt(entryBuf[:], lastOffset); err != nil {
			return 0, 0, 0, time.Time{}, fmt.Errorf("read last idx entry for chunk %s: %w", id, err)
		}
		lastEntry := DecodeIdxEntry(entryBuf[:])
		expectedRawSize = int64(format.HeaderSize) + int64(lastEntry.RawOffset) + int64(lastEntry.RawSize)
		expectedAttrSize = int64(format.HeaderSize) + int64(lastEntry.AttrOffset) + int64(lastEntry.AttrSize)
	} else {
		expectedRawSize = int64(format.HeaderSize)
		expectedAttrSize = int64(format.HeaderSize)
	}

	if err = m.truncateIfNeeded(cf.raw, "raw.log", id, expectedRawSize); err != nil {
		return 0, 0, 0, time.Time{}, err
	}
	if err = m.truncateIfNeeded(cf.attr, "attr.log", id, expectedAttrSize); err != nil {
		return 0, 0, 0, time.Time{}, err
	}

	rawOffset = uint64(expectedRawSize) - uint64(format.HeaderSize)
	attrOffset = uint64(expectedAttrSize) - uint64(format.HeaderSize)
	return recordCount, rawOffset, attrOffset, createdAt, nil
}

// truncateIfNeeded truncates a file to expectedSize if it has crash-orphaned
// data beyond what the index accounts for.
func (m *Manager) truncateIfNeeded(f *os.File, name string, id chunk.ChunkID, expectedSize int64) error {
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s for chunk %s: %w", name, id, err)
	}
	if info.Size() > expectedSize {
		if err := f.Truncate(expectedSize); err != nil {
			return fmt.Errorf("truncate %s for chunk %s: %w", name, id, err)
		}
		m.logger.Info("truncated orphaned "+name+" data",
			"chunk", id.String(),
			"expected", expectedSize,
			"actual", info.Size())
	}
	return nil
}

// loadDictionary reads and decodes the key dictionary from attr_dict.log.
// Uses mmap for the read to avoid heap-allocating the entire file.
func (m *Manager) loadDictionary(id chunk.ChunkID, dictFile *os.File) (*chunk.StringDict, error) {
	dictInfo, err := dictFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat attr_dict.log for chunk %s: %w", id, err)
	}
	fileSize := dictInfo.Size()
	if fileSize <= int64(format.HeaderSize) {
		return chunk.NewStringDict(), nil
	}
	// mmap the entire file read-only; DecodeDictData copies strings so the
	// mapping can be released immediately after decoding.
	data, err := syscall.Mmap(int(dictFile.Fd()), 0, int(fileSize), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115: int64→int safe on 64-bit
	if err != nil {
		return nil, fmt.Errorf("mmap attr_dict.log for chunk %s: %w", id, err)
	}
	dict, decErr := chunk.DecodeDictData(data[format.HeaderSize:])
	if munmapErr := syscall.Munmap(data); munmapErr != nil {
		m.logger.Warn("munmap attr_dict.log failed", "chunk", id.String(), "error", munmapErr)
	}
	if decErr != nil {
		return nil, fmt.Errorf("decode attr_dict.log for chunk %s: %w", id, decErr)
	}
	return dict, nil
}

func (m *Manager) openActiveChunk(id chunk.ChunkID) error {
	meta := m.metas[id]

	cf, err := m.openChunkFiles(id)
	if err != nil {
		return err
	}
	// Close all files on any error; on success, ownership transfers to chunkState.
	success := false
	defer func() {
		if !success {
			cf.closeAll(m.logger)
		}
	}()

	recordCount, rawOffset, attrOffset, createdAt, err := m.validateAndTruncate(id, cf)
	if err != nil {
		return err
	}

	dict, err := m.loadDictionary(id, cf.dict)
	if err != nil {
		return err
	}

	dataBytes := int64(rawOffset + attrOffset + recordCount*IdxEntrySize) //nolint:gosec // G115: data bytes bounded by rotation policy
	meta.logicalDataBytes = dataBytes
	meta.bytes = dataBytes

	ingestBT, sourceBT, err := m.rebuildBTrees(id, cf.idx, recordCount)
	if err != nil {
		return fmt.Errorf("rebuild btrees for chunk %s: %w", id, err)
	}

	m.active = &chunkState{
		meta:        meta,
		rawFile:     cf.raw,
		idxFile:     cf.idx,
		attrFile:    cf.attr,
		dictFile:    cf.dict,
		dict:        dict,
		ingestBT:    ingestBT,
		sourceBT:    sourceBT,
		rawOffset:   rawOffset,
		attrOffset:  attrOffset,
		recordCount: recordCount,
		createdAt:   createdAt,
	}

	success = true
	return nil
}

// rebuildBTrees creates fresh B+ tree indexes from idx.log entries during crash recovery.
// Any stale B+ tree files are removed first.
func (m *Manager) rebuildBTrees(id chunk.ChunkID, idxFile *os.File, recordCount uint64) (*btree.Tree[int64, uint32], *btree.Tree[int64, uint32], error) {
	// Remove stale B+ tree files if they exist from a prior run.
	ingestPath := m.ingestBTPath(id)
	sourcePath := m.sourceBTPath(id)
	_ = os.Remove(ingestPath) //nolint:gosec // G703: path is derived from chunk ID, not user input
	_ = os.Remove(sourcePath) //nolint:gosec // G703: path is derived from chunk ID, not user input

	ingestBT, err := btree.Create(ingestPath, btree.Int64Uint32)
	if err != nil {
		return nil, nil, err
	}
	sourceBT, err := btree.Create(sourcePath, btree.Int64Uint32)
	if err != nil {
		_ = ingestBT.Close()
		return nil, nil, err
	}

	var entryBuf [IdxEntrySize]byte
	for i := range recordCount {
		offset := IdxFileOffset(i)
		if _, err := idxFile.ReadAt(entryBuf[:], offset); err != nil {
			_ = ingestBT.Close()
			_ = sourceBT.Close()
			return nil, nil, fmt.Errorf("read idx entry %d: %w", i, err)
		}
		entry := DecodeIdxEntry(entryBuf[:])
		pos := uint32(i)

		if err := ingestBT.Insert(entry.IngestTS.UnixNano(), pos); err != nil {
			_ = ingestBT.Close()
			_ = sourceBT.Close()
			return nil, nil, err
		}
		if !entry.SourceTS.IsZero() {
			if err := sourceBT.Insert(entry.SourceTS.UnixNano(), pos); err != nil {
				_ = ingestBT.Close()
				_ = sourceBT.Close()
				return nil, nil, err
			}
		}
	}

	if err := ingestBT.Sync(); err != nil {
		_ = ingestBT.Close()
		_ = sourceBT.Close()
		return nil, nil, err
	}
	if err := sourceBT.Sync(); err != nil {
		_ = ingestBT.Close()
		_ = sourceBT.Close()
		return nil, nil, err
	}

	return ingestBT, sourceBT, nil
}

func (m *Manager) loadChunkMeta(id chunk.ChunkID) (*chunkMeta, error) {
	idxPath := m.idxLogPath(id)

	idxFile, err := os.Open(filepath.Clean(idxPath))
	if err != nil {
		return nil, fmt.Errorf("open idx.log for chunk %s: %w", id, err)
	}
	defer func() { _ = idxFile.Close() }()

	var headerBuf [IdxHeaderSize]byte
	if _, err := io.ReadFull(idxFile, headerBuf[:]); err != nil {
		return nil, fmt.Errorf("read idx.log header for chunk %s: %w", id, err)
	}
	header, err := format.DecodeAndValidate(headerBuf[:format.HeaderSize], format.TypeIdxLog, IdxLogVersion)
	if err != nil {
		return nil, fmt.Errorf("invalid idx.log header for chunk %s: %w", id, err)
	}
	sealed := header.Flags&format.FlagSealed != 0

	info, err := idxFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat idx.log for chunk %s: %w", id, err)
	}
	recordCount := RecordCount(info.Size())

	meta := &chunkMeta{
		id:          id,
		recordCount: int64(recordCount), //nolint:gosec // G115: record count fits in int64
		sealed:      sealed,
		compressed:  sealed && m.checkCompressionFlag(id),
	}

	if recordCount == 0 {
		return meta, nil
	}

	firstEntry, lastEntry, err := m.readFirstLastEntries(idxFile, recordCount)
	if err != nil {
		return nil, fmt.Errorf("read first/last entries for chunk %s: %w", id, err)
	}

	meta.writeStart = firstEntry.WriteTS
	meta.writeEnd = lastEntry.WriteTS
	computeIngestBounds(meta, firstEntry, lastEntry)
	computeSourceBounds(meta, firstEntry, lastEntry)

	rawEnd := int64(lastEntry.RawOffset) + int64(lastEntry.RawSize)
	attrEnd := int64(lastEntry.AttrOffset) + int64(lastEntry.AttrSize)
	logicalDataBytes := rawEnd + attrEnd + int64(recordCount)*int64(IdxEntrySize) //nolint:gosec // G115: record count fits in int64
	meta.logicalDataBytes = logicalDataBytes
	meta.bytes = logicalDataBytes

	if sealed {
		meta.bytes = m.computeTotalLogicalBytes(id, logicalDataBytes)
		meta.diskBytes = m.computeDiskBytes(id)
	}

	return meta, nil
}

func (m *Manager) checkCompressionFlag(id chunk.ChunkID) bool {
	rawPath := m.rawLogPath(id)
	rawFile, err := os.Open(filepath.Clean(rawPath))
	if err != nil {
		return false
	}
	defer func() { _ = rawFile.Close() }()

	var rawHeader [format.HeaderSize]byte
	if _, err := io.ReadFull(rawFile, rawHeader[:]); err != nil {
		return false
	}
	h, err := format.Decode(rawHeader[:])
	if err != nil {
		return false
	}
	return h.Flags&format.FlagCompressed != 0
}

func (m *Manager) readFirstLastEntries(idxFile *os.File, recordCount uint64) (IdxEntry, IdxEntry, error) {
	var entryBuf [IdxEntrySize]byte
	if _, err := io.ReadFull(idxFile, entryBuf[:]); err != nil {
		return IdxEntry{}, IdxEntry{}, fmt.Errorf("read first idx entry: %w", err)
	}
	firstEntry := DecodeIdxEntry(entryBuf[:])

	lastOffset := IdxFileOffset(recordCount - 1)
	if _, err := idxFile.Seek(lastOffset, io.SeekStart); err != nil {
		return IdxEntry{}, IdxEntry{}, fmt.Errorf("seek to last idx entry (record %d): %w", recordCount-1, err)
	}
	if _, err := io.ReadFull(idxFile, entryBuf[:]); err != nil {
		return IdxEntry{}, IdxEntry{}, fmt.Errorf("read last idx entry: %w", err)
	}
	lastEntry := DecodeIdxEntry(entryBuf[:])

	return firstEntry, lastEntry, nil
}

func computeIngestBounds(meta *chunkMeta, first, last IdxEntry) {
	if first.IngestTS.Before(last.IngestTS) {
		meta.ingestStart = first.IngestTS
		meta.ingestEnd = last.IngestTS
	} else {
		meta.ingestStart = last.IngestTS
		meta.ingestEnd = first.IngestTS
	}
}

func computeSourceBounds(meta *chunkMeta, first, last IdxEntry) {
	if first.SourceTS.IsZero() && last.SourceTS.IsZero() {
		return
	}
	var minSrc, maxSrc time.Time
	for _, ts := range []time.Time{first.SourceTS, last.SourceTS} {
		if ts.IsZero() {
			continue
		}
		if minSrc.IsZero() || ts.Before(minSrc) {
			minSrc = ts
		}
		if maxSrc.IsZero() || ts.After(maxSrc) {
			maxSrc = ts
		}
	}
	meta.sourceStart = minSrc
	meta.sourceEnd = maxSrc
}

func (m *Manager) SetNextChunkID(id chunk.ChunkID) {
	m.mu.Lock()
	m.nextChunkID = &id
	m.mu.Unlock()
}

func (m *Manager) openLocked() error {
	var id chunk.ChunkID
	if m.nextChunkID != nil {
		id = *m.nextChunkID
		m.nextChunkID = nil
	} else {
		id = chunk.NewChunkID()
	}
	chunkDir := m.chunkDir(id)
	if err := os.MkdirAll(chunkDir, 0o750); err != nil {
		return err
	}

	createdAt := m.cfg.Now()

	// Create and initialize raw.log with header.
	rawFile, err := m.createRawFile(id)
	if err != nil {
		return err
	}

	// Create and initialize idx.log with header + createdAt timestamp.
	idxFile, err := m.createIdxFile(id, createdAt)
	if err != nil {
		_ = rawFile.Close()
		return err
	}

	// Create and initialize attr.log with header.
	attrFile, err := m.createAttrFile(id)
	if err != nil {
		_ = rawFile.Close()
		_ = idxFile.Close()
		return err
	}

	// Create and initialize attr_dict.log with header.
	dictFile, err := m.createDictFile(id)
	if err != nil {
		_ = rawFile.Close()
		_ = idxFile.Close()
		_ = attrFile.Close()
		return err
	}

	// Create B+ tree indexes for IngestTS and SourceTS seeking.
	closeDataFiles := func() {
		_ = rawFile.Close()
		_ = idxFile.Close()
		_ = attrFile.Close()
		_ = dictFile.Close()
	}
	ingestBT, err := btree.Create(m.ingestBTPath(id), btree.Int64Uint32)
	if err != nil {
		closeDataFiles()
		return err
	}
	sourceBT, err := btree.Create(m.sourceBTPath(id), btree.Int64Uint32)
	if err != nil {
		_ = ingestBT.Close()
		closeDataFiles()
		return err
	}

	meta := &chunkMeta{
		id:     id,
		sealed: false,
	}

	m.active = &chunkState{
		meta:        meta,
		rawFile:     rawFile,
		idxFile:     idxFile,
		attrFile:    attrFile,
		dictFile:    dictFile,
		dict:        chunk.NewStringDict(),
		ingestBT:    ingestBT,
		sourceBT:    sourceBT,
		rawOffset:   0, // Data starts after header
		attrOffset:  0, // Data starts after header
		recordCount: 0,
		createdAt:   createdAt,
	}
	m.metas[id] = meta

	if m.cfg.Announcer != nil {
		ann := m.cfg.Announcer
		m.pendingAnnouncements = append(m.pendingAnnouncements, func() {
			ann.AnnounceCreate(id, createdAt, createdAt, createdAt)
		})
	}
	return nil
}

func (m *Manager) createRawFile(id chunk.ChunkID) (*os.File, error) {
	path := m.rawLogPath(id)
	file, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR, m.cfg.FileMode)
	if err != nil {
		return nil, err
	}

	// Write header.
	header := format.Header{
		Type:    format.TypeRawLog,
		Version: RawLogVersion,
		Flags:   0,
	}
	headerBytes := header.Encode()
	if _, err := file.Write(headerBytes[:]); err != nil {
		_ = file.Close()
		return nil, err
	}

	return file, nil
}

func (m *Manager) createIdxFile(id chunk.ChunkID, createdAt time.Time) (*os.File, error) {
	path := m.idxLogPath(id)
	file, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR, m.cfg.FileMode)
	if err != nil {
		return nil, err
	}

	// Write header (4 bytes) + createdAt timestamp (8 bytes).
	var buf [IdxHeaderSize]byte
	header := format.Header{
		Type:    format.TypeIdxLog,
		Version: IdxLogVersion,
		Flags:   0,
	}
	header.EncodeInto(buf[:])
	binary.LittleEndian.PutUint64(buf[format.HeaderSize:], uint64(createdAt.UnixNano()))

	if _, err := file.Write(buf[:]); err != nil {
		_ = file.Close()
		return nil, err
	}

	return file, nil
}

func (m *Manager) createAttrFile(id chunk.ChunkID) (*os.File, error) {
	path := m.attrLogPath(id)
	file, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR, m.cfg.FileMode)
	if err != nil {
		return nil, err
	}

	// Write header.
	header := format.Header{
		Type:    format.TypeAttrLog,
		Version: AttrLogVersion,
		Flags:   0,
	}
	headerBytes := header.Encode()
	if _, err := file.Write(headerBytes[:]); err != nil {
		_ = file.Close()
		return nil, err
	}

	return file, nil
}

func (m *Manager) openRawFile(id chunk.ChunkID) (*os.File, error) {
	path := m.rawLogPath(id)
	return os.OpenFile(filepath.Clean(path), os.O_RDWR, m.cfg.FileMode)
}

func (m *Manager) openIdxFile(id chunk.ChunkID) (*os.File, error) {
	path := m.idxLogPath(id)
	return os.OpenFile(filepath.Clean(path), os.O_RDWR, m.cfg.FileMode)
}

func (m *Manager) openAttrFile(id chunk.ChunkID) (*os.File, error) {
	path := m.attrLogPath(id)
	return os.OpenFile(filepath.Clean(path), os.O_RDWR, m.cfg.FileMode)
}

func (m *Manager) createDictFile(id chunk.ChunkID) (*os.File, error) {
	path := m.dictLogPath(id)
	file, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR, m.cfg.FileMode)
	if err != nil {
		return nil, err
	}

	// Write header.
	header := format.Header{
		Type:    format.TypeAttrDict,
		Version: AttrDictVersion,
		Flags:   0,
	}
	headerBytes := header.Encode()
	if _, err := file.Write(headerBytes[:]); err != nil {
		_ = file.Close()
		return nil, err
	}

	return file, nil
}

func (m *Manager) openDictFile(id chunk.ChunkID) (*os.File, error) {
	path := m.dictLogPath(id)
	return os.OpenFile(filepath.Clean(path), os.O_RDWR|os.O_APPEND, m.cfg.FileMode)
}

func (m *Manager) sealLocked() error {
	if m.active == nil {
		return nil
	}

	// Wait for any in-flight Phase 2 (WriteAt) writers to finish before
	// modifying headers or closing files. Safe to block here: Phase 2 does
	// not hold the mutex, and no new Phase 1 can start while we hold it.
	m.active.inflight.Wait()

	m.active.meta.sealed = true
	id := m.active.meta.id

	// Update sealed flag in all file headers.
	if err := m.setSealedFlag(m.active.rawFile); err != nil {
		return err
	}
	if err := m.setSealedFlag(m.active.idxFile); err != nil {
		return err
	}
	if err := m.setSealedFlag(m.active.attrFile); err != nil {
		return err
	}
	if err := m.setSealedFlag(m.active.dictFile); err != nil {
		return err
	}

	// Close files.
	if err := m.active.rawFile.Close(); err != nil {
		return err
	}
	if err := m.active.idxFile.Close(); err != nil {
		return err
	}
	if err := m.active.attrFile.Close(); err != nil {
		return err
	}
	if err := m.active.dictFile.Close(); err != nil {
		return err
	}

	// Close and remove B+ tree files — sealed chunks use flat indexes.
	if err := m.active.ingestBT.Close(); err != nil {
		return err
	}
	_ = os.Remove(m.ingestBTPath(id))
	if err := m.active.sourceBT.Close(); err != nil {
		return err
	}
	_ = os.Remove(m.sourceBTPath(id))

	// Compute directory-level sizes now that files are closed.
	meta := m.active.meta
	meta.bytes = m.computeTotalLogicalBytes(id, meta.logicalDataBytes)
	meta.diskBytes = m.computeDiskBytes(id)

	if m.cfg.Announcer != nil {
		ann := m.cfg.Announcer
		// Capture the metadata snapshot now (it's mutable; m.active will
		// be cleared below). The closure runs after the caller releases mu.
		writeEnd := meta.writeEnd
		recordCount := meta.recordCount
		bytes := meta.bytes
		ingestEnd := meta.ingestEnd
		sourceEnd := meta.sourceEnd
		m.pendingAnnouncements = append(m.pendingAnnouncements, func() {
			ann.AnnounceSeal(id, writeEnd, recordCount, bytes, ingestEnd, sourceEnd)
		})
	}

	m.active = nil
	return nil
}

func (m *Manager) setSealedFlag(file *os.File) error {
	// Read current flags, OR in FlagSealed, write back.
	var buf [format.HeaderSize]byte
	if _, err := file.ReadAt(buf[:], 0); err != nil {
		return err
	}
	header, err := format.Decode(buf[:])
	if err != nil {
		return err
	}
	header.Flags |= format.FlagSealed
	if _, err := file.Seek(3, io.SeekStart); err != nil {
		return err
	}
	if _, err := file.Write([]byte{header.Flags}); err != nil {
		return err
	}
	return file.Sync()
}

// importFiles holds the four log files needed for a chunk import.
type importFiles struct {
	raw, idx, attr, dict *os.File
	chunkDir             string
}

// cleanup closes all files and removes the chunk directory.
func (f *importFiles) cleanup() {
	_ = f.raw.Close()
	_ = f.idx.Close()
	_ = f.attr.Close()
	_ = f.dict.Close()
	_ = os.RemoveAll(f.chunkDir)
}

// openImportFiles creates the chunk directory and all four log files.
// On failure, any already-created resources are cleaned up.
func (m *Manager) openImportFiles(id chunk.ChunkID, createdAt time.Time) (*importFiles, error) {
	dir := m.chunkDir(id)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}

	rawFile, err := m.createRawFile(id)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	idxFile, err := m.createIdxFile(id, createdAt)
	if err != nil {
		_ = rawFile.Close()
		_ = os.RemoveAll(dir)
		return nil, err
	}
	attrFile, err := m.createAttrFile(id)
	if err != nil {
		_ = rawFile.Close()
		_ = idxFile.Close()
		_ = os.RemoveAll(dir)
		return nil, err
	}
	dictFile, err := m.createDictFile(id)
	if err != nil {
		_ = rawFile.Close()
		_ = idxFile.Close()
		_ = attrFile.Close()
		_ = os.RemoveAll(dir)
		return nil, err
	}

	return &importFiles{
		raw: rawFile, idx: idxFile, attr: attrFile, dict: dictFile,
		chunkDir: dir,
	}, nil
}

// importState tracks per-record offsets during ImportRecords.
type importState struct {
	files      *importFiles
	dict       *chunk.StringDict
	meta       *chunkMeta
	now        func() time.Time
	rawOffset  uint64
	attrOffset uint64
	count      int64
}

// writeRecord writes a single record to the import files and updates offsets/metadata.
func (s *importState) writeRecord(rec chunk.Record) error {
	rec.WriteTS = s.now()

	attrBytes, newKeys, err := chunk.EncodeWithDict(rec.Attrs, s.dict)
	if err != nil {
		return err
	}

	for _, key := range newKeys {
		entry := chunk.EncodeDictEntry(key)
		if err := writeAll(s.files.dict, entry); err != nil {
			return err
		}
	}

	var idxBuf [IdxEntrySize]byte
	EncodeIdxEntry(IdxEntry{
		SourceTS:   rec.SourceTS,
		IngestTS:   rec.IngestTS,
		WriteTS:    rec.WriteTS,
		RawOffset:  uint32(s.rawOffset),  //nolint:gosec // G115: bounded by rotation policy
		RawSize:    uint32(len(rec.Raw)),  //nolint:gosec // G115: bounded by chunk size
		AttrOffset: uint32(s.attrOffset), //nolint:gosec // G115: bounded by rotation policy
		AttrSize:   uint16(len(attrBytes)), //nolint:gosec // G115: bounded by attr encoding
	}, idxBuf[:])

	rawPos := int64(format.HeaderSize) + int64(s.rawOffset)  //nolint:gosec // G115: bounded by rotation policy
	attrPos := int64(format.HeaderSize) + int64(s.attrOffset) //nolint:gosec // G115: bounded by rotation policy
	idxPos := int64(IdxHeaderSize) + s.count*int64(IdxEntrySize)

	if _, err := s.files.raw.WriteAt(rec.Raw, rawPos); err != nil {
		return fmt.Errorf("write raw record %d: %w", s.count, err)
	}
	if _, err := s.files.attr.WriteAt(attrBytes, attrPos); err != nil {
		return fmt.Errorf("write attr record %d: %w", s.count, err)
	}
	if _, err := s.files.idx.WriteAt(idxBuf[:], idxPos); err != nil {
		return fmt.Errorf("write idx record %d: %w", s.count, err)
	}

	s.rawOffset += uint64(len(rec.Raw))
	s.attrOffset += uint64(len(attrBytes))
	s.count++

	if s.meta.writeStart.IsZero() {
		s.meta.writeStart = rec.WriteTS
	}
	s.meta.writeEnd = rec.WriteTS
	expandBounds(&s.meta.ingestStart, &s.meta.ingestEnd, rec.IngestTS)
	if !rec.SourceTS.IsZero() {
		expandBounds(&s.meta.sourceStart, &s.meta.sourceEnd, rec.SourceTS)
	}
	return nil
}

// ImportRecords creates a new sealed chunk by consuming records from the
// iterator, preserving each record's WriteTS. The records are written to a new
// chunk directory separate from the active chunk; concurrent Append calls are
// not affected.
func (m *Manager) ImportRecords(next chunk.RecordIterator) (chunk.ChunkMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return chunk.ChunkMeta{}, ErrManagerClosed
	}

	var id chunk.ChunkID
	if m.nextChunkID != nil {
		id = *m.nextChunkID
		m.nextChunkID = nil
	} else {
		id = chunk.NewChunkID()
	}
	files, err := m.openImportFiles(id, m.cfg.Now())
	if err != nil {
		return chunk.ChunkMeta{}, err
	}

	s := &importState{
		files: files,
		dict:  chunk.NewStringDict(),
		meta:  &chunkMeta{id: id},
		now:   m.cfg.Now,
	}

	for {
		rec, iterErr := next()
		if errors.Is(iterErr, chunk.ErrNoMoreRecords) {
			break
		}
		if iterErr != nil {
			files.cleanup()
			return chunk.ChunkMeta{}, iterErr
		}
		if err := s.writeRecord(rec); err != nil {
			files.cleanup()
			return chunk.ChunkMeta{}, err
		}
	}

	if s.count == 0 {
		files.cleanup()
		return chunk.ChunkMeta{}, nil
	}

	s.meta.recordCount = s.count
	dataBytes := int64(s.rawOffset + s.attrOffset + uint64(s.count)*IdxEntrySize) //nolint:gosec // G115: count is always non-negative
	s.meta.logicalDataBytes = dataBytes

	// Seal the files.
	for _, f := range []*os.File{files.raw, files.idx, files.attr, files.dict} {
		if err := m.setSealedFlag(f); err != nil {
			files.cleanup()
			return chunk.ChunkMeta{}, err
		}
	}

	// Close files.
	for _, f := range []*os.File{files.raw, files.idx, files.attr, files.dict} {
		if err := f.Close(); err != nil {
			_ = os.RemoveAll(files.chunkDir)
			return chunk.ChunkMeta{}, err
		}
	}

	s.meta.sealed = true
	s.meta.bytes = m.computeTotalLogicalBytes(id, s.meta.logicalDataBytes)
	s.meta.diskBytes = m.computeDiskBytes(id)

	m.metas[id] = s.meta
	return s.meta.toChunkMeta(), nil
}

// Close closes the active chunk files without sealing.
// The manager should not be used after Close is called.
// trackCloudResult updates the degraded flag based on a cloud operation result.
// Every cloud operation (upload, download, list, archive, restore) should call
// this after completion. Failed → set degraded + store error. Succeeded → clear.
func (m *Manager) trackCloudResult(err error) {
	if err != nil {
		m.cloudDegraded.Store(true)
		m.cloudDegradedErr.Store(err.Error())
	} else {
		m.cloudDegraded.Store(false)
	}
}

// CloudDegraded returns true if the cloud store is currently unreachable.
// The orchestrator polls this to raise/clear alerts.
func (m *Manager) CloudDegraded() bool {
	return m.cloudDegraded.Load()
}

// CloudDegradedError returns the last cloud error message, or "" if healthy.
func (m *Manager) CloudDegradedError() string {
	if v := m.cloudDegradedErr.Load(); v != nil {
		return v.(string)
	}
	return ""
}

func (m *Manager) Close() error {
	// Mark as closed under the lock so new CompressChunk calls bail out.
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true

	var errs []error

	// Close active chunk files but don't seal (chunk remains active for recovery).
	if m.active != nil {
		errs = append(errs, m.closeActiveFiles()...)
		m.active = nil
	}
	m.mu.Unlock()

	// Wait for in-flight compression to finish before closing the encoder.
	// CompressChunk re-acquires the lock for its metadata update, so we must
	// release first.
	m.postSealWg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Close cloud index.
	if m.cloudIdx != nil {
		if err := m.cloudIdx.Close(); err != nil {
			errs = append(errs, err)
		}
		m.cloudIdx = nil
	}

	// Close zstd encoder.
	if m.zstdEnc != nil {
		if err := m.zstdEnc.Close(); err != nil {
			errs = append(errs, err)
		}
		m.zstdEnc = nil
	}

	// Release directory lock.
	if m.lockFile != nil {
		if err := m.lockFile.Close(); err != nil {
			errs = append(errs, err)
		}
		m.lockFile = nil
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// RemoveDir removes the manager's data directory from disk. Must be called
// after Close() — the manager must not be used afterward. Used when a tier
// is deleted, to clean up leftover files (.lock, cloud.idx) and the tier
// directory itself so removed tiers don't accumulate as orphans.
// See gastrolog-42j4n.
func (m *Manager) RemoveDir() error {
	if !m.closed {
		return errors.New("manager must be closed before RemoveDir")
	}
	return os.RemoveAll(m.cfg.Dir)
}

// closeActiveFiles waits for inflight writers and closes all active chunk resources.
func (m *Manager) closeActiveFiles() []error {
	m.active.inflight.Wait()
	var errs []error
	for _, closer := range []io.Closer{
		m.active.rawFile,
		m.active.idxFile,
		m.active.attrFile,
		m.active.dictFile,
		m.active.ingestBT,
		m.active.sourceBT,
	} {
		if err := closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// Remove B+ tree files — they are transient and only needed while the
	// chunk is active. sealLocked removes them too, but Close() can be
	// called without sealing (e.g. shutdown), leaving orphaned files.
	id := m.active.meta.id
	_ = os.Remove(m.ingestBTPath(id))
	_ = os.Remove(m.sourceBTPath(id))

	return errs
}

// computeDiskBytes sums the on-disk sizes of all files in the chunk directory.
// This includes data files (potentially compressed) and index files.
func (m *Manager) computeDiskBytes(id chunk.ChunkID) int64 {
	entries, err := os.ReadDir(filepath.Join(m.cfg.Dir, id.String()))
	if err != nil {
		return 0
	}
	var total int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total
}

// computeTotalLogicalBytes returns the total logical size of a sealed chunk:
// the logical data size (uncompressed raw + attr + idx content from offsets)
// plus on-disk sizes of all other files (attr_dict, indexes) which aren't
// compressed. This pairs with computeDiskBytes so that uncompressed files
// appear on both sides of the compression ratio and cancel out.
func (m *Manager) computeTotalLogicalBytes(id chunk.ChunkID, logicalDataBytes int64) int64 {
	entries, err := os.ReadDir(filepath.Join(m.cfg.Dir, id.String()))
	if err != nil {
		return logicalDataBytes
	}
	total := logicalDataBytes
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		// Skip the three data files whose logical size is already in logicalDataBytes.
		switch entry.Name() {
		case rawLogFileName, attrLogFileName, idxLogFileName:
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total
}

func (m *Manager) chunkDir(id chunk.ChunkID) string {
	return filepath.Join(m.cfg.Dir, id.String())
}

func (m *Manager) rawLogPath(id chunk.ChunkID) string {
	return filepath.Join(m.chunkDir(id), rawLogFileName)
}

func (m *Manager) idxLogPath(id chunk.ChunkID) string {
	return filepath.Join(m.chunkDir(id), idxLogFileName)
}

func (m *Manager) attrLogPath(id chunk.ChunkID) string {
	return filepath.Join(m.chunkDir(id), attrLogFileName)
}

func (m *Manager) dictLogPath(id chunk.ChunkID) string {
	return filepath.Join(m.chunkDir(id), attrDictFileName)
}

func (m *Manager) ingestBTPath(id chunk.ChunkID) string {
	return filepath.Join(m.chunkDir(id), ingestBTFileName)
}

func (m *Manager) sourceBTPath(id chunk.ChunkID) string {
	return filepath.Join(m.chunkDir(id), sourceBTFileName)
}

// chunkDirHasFiles reports whether a chunk directory contains any regular files.
// Called after cleanOrphanTempFiles, so any remaining files are index files
// belonging to a cloud-backed chunk.
func chunkDirHasFiles(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() {
			return true
		}
	}
	return false
}

// dataFileNames lists the chunk data files that are redundant once a chunk is
// cloud-backed. Index files (*.idx, *.tsidx) are NOT in this list — they are
// preserved locally so queries can filter without downloading from S3.
var dataFileNames = []string{
	rawLogFileName,
	idxLogFileName,
	attrLogFileName,
	attrDictFileName,
	ingestBTFileName,
	sourceBTFileName,
}

// removeLocalDataFiles deletes only the data files from a chunk directory,
// preserving any index files. Returns an error if a file exists but cannot be
// removed; missing files are silently ignored.
func (m *Manager) removeLocalDataFiles(id chunk.ChunkID) error {
	dir := m.chunkDir(id)
	for _, name := range dataFileNames {
		path := filepath.Join(dir, name)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove %s for chunk %s: %w", name, id, err)
		}
	}
	return nil
}

// FindStartPosition binary searches idx.log for the record at or before the given timestamp.
// Uses WriteTS for the search since it's monotonically increasing within a chunk.
func (m *Manager) FindStartPosition(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	m.mu.Lock()
	meta := m.lookupMeta(id)
	m.mu.Unlock()
	if meta == nil {
		return 0, false, chunk.ErrChunkNotFound
	}

	// Cloud-backed chunks have no local idx.log — return (0, false) to
	// fall back to full scan (same behavior as the old cloud manager).
	if meta.cloudBacked {
		return 0, false, nil
	}

	// Quick bounds check using cached time bounds.
	if ts.Before(meta.writeStart) {
		return 0, false, nil // Before all records
	}

	idxPath := m.idxLogPath(id)
	idxFile, err := os.Open(filepath.Clean(idxPath))
	if err != nil {
		return 0, false, fmt.Errorf("open idx.log for chunk %s: %w", id, err)
	}
	defer func() { _ = idxFile.Close() }()

	// Validate header.
	var headerBuf [format.HeaderSize]byte
	if _, err := idxFile.ReadAt(headerBuf[:], 0); err != nil {
		return 0, false, fmt.Errorf("read idx.log header for chunk %s: %w", id, err)
	}
	if _, err := format.DecodeAndValidate(headerBuf[:], format.TypeIdxLog, IdxLogVersion); err != nil {
		return 0, false, fmt.Errorf("invalid idx.log header for chunk %s: %w", id, err)
	}
	info, err := idxFile.Stat()
	if err != nil {
		return 0, false, fmt.Errorf("stat idx.log for chunk %s: %w", id, err)
	}
	recordCount := RecordCount(info.Size())
	if recordCount == 0 {
		return 0, false, nil
	}

	// Binary search for the latest entry with WriteTS <= ts.
	// We're looking for the rightmost position where WriteTS <= ts.
	lo, hi := uint64(0), recordCount
	var entryBuf [IdxEntrySize]byte

	for lo < hi {
		mid := lo + (hi-lo)/2

		offset := IdxFileOffset(mid)
		if _, err := idxFile.ReadAt(entryBuf[:], offset); err != nil {
			return 0, false, fmt.Errorf("read idx entry at pos %d in chunk %s: %w", mid, id, err)
		}
		entry := DecodeIdxEntry(entryBuf[:])

		if entry.WriteTS.After(ts) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}

	// lo is the count of entries with WriteTS <= ts.
	if lo == 0 {
		return 0, false, nil
	}

	return lo - 1, true, nil
}

// FindIngestStartPosition returns the earliest record position with IngestTS >= ts
// for the active chunk. Returns (0, false, nil) for sealed chunks (use the index manager).
func (m *Manager) FindIngestStartPosition(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	m.mu.Lock()
	active := m.active
	meta := m.lookupMeta(id)
	m.mu.Unlock()

	// Active chunk: B+ tree lookup.
	if active != nil && active.meta.id == id {
		it, err := active.ingestBT.FindGE(ts.UnixNano())
		if err != nil {
			return 0, false, fmt.Errorf("btree ingest FindGE: %w", err)
		}
		if !it.Valid() {
			return 0, false, nil
		}
		return uint64(it.Value()), true, nil
	}

	// Cloud-backed chunk with embedded TS index: range-request binary search.
	if meta != nil && meta.cloudBacked && meta.ingestIdxSize > 0 {
		return m.findCloudTSPosition(id, ts, meta.ingestIdxOffset, meta.ingestIdxSize)
	}

	return 0, false, nil
}

// FindSourceStartPosition returns the earliest record position with SourceTS >= ts.
// Supports active chunks (B+ tree) and cloud chunks (embedded TS index via range request).
func (m *Manager) FindSourceStartPosition(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	m.mu.Lock()
	active := m.active
	meta := m.lookupMeta(id)
	m.mu.Unlock()

	// Active chunk: B+ tree lookup.
	if active != nil && active.meta.id == id {
		it, err := active.sourceBT.FindGE(ts.UnixNano())
		if err != nil {
			return 0, false, fmt.Errorf("btree source FindGE: %w", err)
		}
		if !it.Valid() {
			return 0, false, nil
		}
		return uint64(it.Value()), true, nil
	}

	// Cloud-backed chunk with embedded TS index.
	if meta != nil && meta.cloudBacked && meta.sourceIdxSize > 0 {
		return m.findCloudTSPosition(id, ts, meta.sourceIdxOffset, meta.sourceIdxSize)
	}

	return 0, false, nil
}

const tsCacheDir = ".ts-cache"

// LoadIngestEntries reads the locally-cached ingest TS index for a cloud chunk
// and returns all entries sorted by IngestTS. Downloads the index from S3 on
// first access.
func (m *Manager) LoadIngestEntries(id chunk.ChunkID) ([]chunk.TSEntry, error) {
	m.mu.Lock()
	meta := m.lookupMeta(id)
	m.mu.Unlock()
	if meta == nil || !meta.cloudBacked || meta.ingestIdxSize == 0 {
		return nil, chunk.ErrNoTSIndex
	}
	return m.loadTSEntries(id, meta.ingestIdxOffset, meta.ingestIdxSize)
}

// LoadSourceEntries reads the locally-cached source TS index for a cloud chunk.
func (m *Manager) LoadSourceEntries(id chunk.ChunkID) ([]chunk.TSEntry, error) {
	m.mu.Lock()
	meta := m.lookupMeta(id)
	m.mu.Unlock()
	if meta == nil || !meta.cloudBacked || meta.sourceIdxSize == 0 {
		return nil, chunk.ErrNoTSIndex
	}
	return m.loadTSEntries(id, meta.sourceIdxOffset, meta.sourceIdxSize)
}

// loadTSEntries reads a TS index cache file and returns all entries.
// Downloads from S3 on cache miss.
func (m *Manager) loadTSEntries(id chunk.ChunkID, offset, size int64) ([]chunk.TSEntry, error) {
	cachePath := m.tsCachePath(id, offset)

	if _, err := os.Stat(cachePath); err != nil {
		if err := m.downloadTSIndex(id, offset, size, cachePath); err != nil {
			return nil, err
		}
	}

	return readTSEntriesFromFile(cachePath)
}

// readTSEntriesFromFile reads all TS index entries from a local file.
// Each entry is 12 bytes: [tsNano:i64][pos:u32].
func readTSEntriesFromFile(path string) ([]chunk.TSEntry, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	const entrySize = 12
	n := int(info.Size()) / entrySize
	entries := make([]chunk.TSEntry, n)

	var buf [entrySize]byte
	for i := range n {
		if _, err := f.ReadAt(buf[:], int64(i)*entrySize); err != nil {
			return entries[:i], nil // return what we have
		}
		entries[i] = chunk.TSEntry{
			TS:  int64(binary.LittleEndian.Uint64(buf[:8])), //nolint:gosec // G115
			Pos: binary.LittleEndian.Uint32(buf[8:]),
		}
	}
	return entries, nil
}

// findCloudTSPosition looks up the TS index for a cloud chunk via pread-based
// binary search on the local cache file. On cache miss, downloads the TS index
// from S3 and persists it. Subsequent calls do O(log n) pread calls — no heap
// allocation beyond a 12-byte buffer, no mmap, no leak.
func (m *Manager) findCloudTSPosition(id chunk.ChunkID, ts time.Time, offset, size int64) (uint64, bool, error) {
	cachePath := m.tsCachePath(id, offset)

	// Ensure the cache file exists.
	if _, err := os.Stat(cachePath); err != nil {
		if err := m.downloadTSIndex(id, offset, size, cachePath); err != nil {
			return 0, false, err
		}
	}

	// O(log n) binary search via pread — no heap allocation.
	return searchTSCacheFile(cachePath, ts.UnixNano())
}

// downloadTSIndex fetches a TS index section from S3 and writes it to a local
// cache file. Streams directly to disk — no heap buffering.
func (m *Manager) downloadTSIndex(id chunk.ChunkID, offset, size int64, cachePath string) error {
	rc, err := m.cfg.CloudStore.DownloadRange(context.Background(), m.blobKey(id), offset, size)
	if err != nil {
		return fmt.Errorf("download cloud TS index for %s: %w", id, err)
	}
	defer func() { _ = rc.Close() }()

	cacheDir := filepath.Join(m.cfg.Dir, tsCacheDir)
	_ = os.MkdirAll(cacheDir, 0o750)

	f, err := os.CreateTemp(cacheDir, "ts-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file for TS index: %w", err)
	}
	tmpPath := f.Name()

	if _, err := io.Copy(f, rc); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath) //nolint:gosec // G703: tmpPath from os.CreateTemp
		return fmt.Errorf("write cloud TS index for %s: %w", id, err)
	}
	_ = f.Close()

	if err := os.Rename(tmpPath, cachePath); err != nil { //nolint:gosec // G703: tmpPath from os.CreateTemp
		_ = os.Remove(tmpPath) //nolint:gosec // G703: cleanup
		return fmt.Errorf("rename TS cache file: %w", err)
	}
	return nil
}

// searchTSCacheFile does O(log n) binary search on a local TS index cache file
// using pread. Each entry is 12 bytes: [tsNano:i64][pos:u32]. No heap allocation
// beyond a single 12-byte buffer.
func searchTSCacheFile(path string, tsNano int64) (uint64, bool, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return 0, false, err
	}

	const entrySize = 12
	n := int(info.Size()) / entrySize
	if n == 0 {
		return 0, false, nil
	}

	var buf [entrySize]byte
	readEntry := func(i int) (int64, uint32, error) {
		off := int64(i) * entrySize
		if _, err := f.ReadAt(buf[:], off); err != nil {
			return 0, 0, err
		}
		ts := int64(binary.LittleEndian.Uint64(buf[:8])) //nolint:gosec // G115: nanosecond timestamps fit in int64
		pos := binary.LittleEndian.Uint32(buf[8:])
		return ts, pos, nil
	}

	// Quick bounds check.
	lastTS, _, err := readEntry(n - 1)
	if err != nil {
		return 0, false, err
	}
	if tsNano > lastTS {
		return 0, false, nil
	}

	// Binary search: first i where TS[i] >= tsNano.
	lo, hi := 0, n
	for lo < hi {
		mid := lo + (hi-lo)/2
		midTS, _, err := readEntry(mid)
		if err != nil {
			return 0, false, err
		}
		if midTS < tsNano {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	_, pos, err := readEntry(lo)
	if err != nil {
		return 0, false, err
	}
	return uint64(pos), true, nil
}

func (m *Manager) tsCachePath(id chunk.ChunkID, offset int64) string {
	return filepath.Join(m.cfg.Dir, tsCacheDir, fmt.Sprintf("%s.%d", id, offset))
}

// ReadWriteTimestamps reads the WriteTS for each given record position in a chunk.
// Opens idx.log once and reads only the 8-byte WriteTS field for each position.
func (m *Manager) ReadWriteTimestamps(id chunk.ChunkID, positions []uint64) ([]time.Time, error) {
	if len(positions) == 0 {
		return nil, nil
	}

	m.mu.Lock()
	meta := m.lookupMeta(id)
	m.mu.Unlock()
	if meta == nil {
		return nil, chunk.ErrChunkNotFound
	}

	// Cloud-backed chunks: iterate via cursor to collect timestamps.
	if meta.cloudBacked {
		cursor, err := m.openCloudCursor(id)
		if err != nil {
			return nil, err
		}
		defer func() { _ = cursor.Close() }()

		posSet := make(map[uint64]int, len(positions))
		for i, p := range positions {
			posSet[p] = i
		}
		result := make([]time.Time, len(positions))
		var pos uint64
		for {
			rec, _, recErr := cursor.Next()
			if errors.Is(recErr, chunk.ErrNoMoreRecords) {
				break
			}
			if recErr != nil {
				return nil, recErr
			}
			if idx, ok := posSet[pos]; ok {
				result[idx] = rec.WriteTS
			}
			pos++
		}
		return result, nil
	}

	idxPath := m.idxLogPath(id)
	idxFile, err := os.Open(filepath.Clean(idxPath))
	if err != nil {
		return nil, fmt.Errorf("open idx.log for chunk %s: %w", id, err)
	}
	defer func() { _ = idxFile.Close() }()

	results := make([]time.Time, len(positions))
	var buf [8]byte

	for i, pos := range positions {
		offset := int64(IdxHeaderSize) + int64(pos)*int64(IdxEntrySize) + int64(idxWriteTSOffset)
		if _, err := idxFile.ReadAt(buf[:], offset); err != nil {
			return nil, fmt.Errorf("read WriteTS at position %d: %w", pos, err)
		}
		nsec := int64(binary.LittleEndian.Uint64(buf[:])) //nolint:gosec // G115: nanosecond timestamps fit in int64
		results[i] = time.Unix(0, nsec)
	}

	return results, nil
}

// Delete removes a sealed chunk and its data from disk.
// Returns ErrActiveChunk if the chunk is the current active chunk.
// Returns ErrChunkNotFound if the chunk does not exist.
func (m *Manager) Delete(id chunk.ChunkID) error {
	if err := m.deleteInternal(id); err != nil {
		return err
	}
	if m.cfg.Announcer != nil {
		m.cfg.Announcer.AnnounceDelete(id)
	}
	return nil
}

// DeleteSilent removes the chunk's local files and metadata without firing
// the metadata announcer. Used by the tier Raft FSM apply path when a delete
// originating from any node propagates via Raft — re-announcing would create
// an infinite feedback loop.
func (m *Manager) DeleteSilent(id chunk.ChunkID) error {
	return m.deleteInternal(id)
}

func (m *Manager) deleteInternal(id chunk.ChunkID) error {
	m.mu.Lock()

	if m.closed {
		m.mu.Unlock()
		return ErrManagerClosed
	}

	if m.active != nil && m.active.meta.id == id {
		m.mu.Unlock()
		return chunk.ErrActiveChunk
	}

	meta := m.lookupMeta(id)
	if meta == nil {
		m.mu.Unlock()
		return chunk.ErrChunkNotFound
	}

	if meta.cloudBacked { //nolint:nestif // cloud vs local branch is inherently nested
		// Release the lock before the S3 API call — cloud deletes can take
		// hundreds of milliseconds and would block all Appends. Bound the
		// call so an unresponsive S3 can't hold the mu re-acquisition
		// indefinitely. See gastrolog-21xs8.
		key := m.blobKey(id)
		m.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), cloudDeleteTimeout)
		err := m.cfg.CloudStore.Delete(ctx, key)
		cancel()
		m.mu.Lock()
		if err != nil {
			m.mu.Unlock()
			return fmt.Errorf("delete cloud chunk %s: %w", id, err)
		}
		m.removeFromCloudIndex(id)
		// Remove cached GLCB blob (best-effort).
		if m.cfg.CacheDir != "" {
			_ = os.Remove(m.cachePath(id))
		}
	} else {
		dir := m.chunkDir(id)
		// Wait for this chunk's post-seal work to finish — it may be writing
		// temporary files into this chunk's directory.
		m.mu.Unlock()
		if ch, ok := m.postSealActive.Load(id); ok {
			<-ch.(chan struct{})
		}
		m.mu.Lock()
		if err := os.RemoveAll(dir); err != nil {
			m.mu.Unlock()
			return fmt.Errorf("remove chunk dir %s: %w", id, err)
		}
	}

	delete(m.metas, id)          // no-op for cloud chunks (not in metas)
	delete(m.storageClasses, id) // clean up storage class cache
	m.mu.Unlock()
	return nil
}

// CompressChunk compresses raw.log and attr.log for a sealed chunk using zstd.
// Returns nil if the chunk is not found or not sealed. No-op if already compressed.
// Safe to call concurrently with reads (atomic file replacement via rename).
func (m *Manager) CompressChunk(id chunk.ChunkID) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	meta, ok := m.metas[id]
	if !ok {
		m.mu.Unlock()
		return chunk.ErrChunkNotFound
	}
	if !meta.sealed || meta.compressed {
		m.mu.Unlock()
		return nil
	}
	// Mark as compressed BEFORE releasing the lock to prevent a second
	// concurrent CompressChunk from double-compressing the same files.
	// If compression fails below, we clear the flag.
	meta.compressed = true
	rawPath := m.rawLogPath(id)
	attrPath := m.attrLogPath(id)
	mode := m.cfg.FileMode
	m.mu.Unlock()

	// Serialize encoder access — zstd.Encoder is not safe for concurrent use
	// via seekable.NewWriter. Multiple PostSealProcess jobs can run concurrently
	// for different chunks, all sharing m.zstdEnc.
	m.zstdEncMu.Lock()
	if err := compressFile(rawPath, m.zstdEnc, mode); err != nil {
		m.zstdEncMu.Unlock()
		m.rollbackCompressed(id)
		return fmt.Errorf("compress raw.log: %w", err)
	}
	if err := compressFile(attrPath, m.zstdEnc, mode); err != nil {
		m.zstdEncMu.Unlock()
		m.rollbackCompressed(id)
		return fmt.Errorf("compress attr.log: %w", err)
	}
	m.zstdEncMu.Unlock()

	// Update in-memory meta to reflect compressed state and refresh sizes.
	// Capture the announcement data under the lock, but fire AnnounceCompress
	// AFTER releasing — same deadlock-avoidance reasoning as Seal/Append.
	var (
		ann       chunk.MetadataAnnouncer
		diskBytes int64
		announce  bool
	)
	m.mu.Lock()
	if meta := m.metas[id]; meta != nil {
		// meta.compressed already set above; refresh disk sizes.
		meta.diskBytes = m.computeDiskBytes(id)
		if m.cfg.Announcer != nil {
			ann = m.cfg.Announcer
			diskBytes = meta.diskBytes
			announce = true
		}
	}
	m.mu.Unlock()

	if announce {
		ann.AnnounceCompress(id, diskBytes)
	}
	return nil
}

// rollbackCompressed clears the compressed flag if compression fails partway.
func (m *Manager) rollbackCompressed(id chunk.ChunkID) {
	m.mu.Lock()
	if meta := m.metas[id]; meta != nil {
		meta.compressed = false
	}
	m.mu.Unlock()
}

// SetIndexBuilders injects index builders into the post-seal pipeline.
// Must be called before PostSealProcess. Passing nil disables index building.
func (m *Manager) SetIndexBuilders(builders []chunk.ChunkIndexBuilder) {
	m.indexBuilders = builders
}

// HasIndexBuilders reports whether index builders are injected.
func (m *Manager) HasIndexBuilders() bool {
	return len(m.indexBuilders) > 0
}

// PostSealProcess runs the full post-seal pipeline for a sealed chunk:
// compress → build indexes → refresh sizes → upload to cloud.
// Safe to call concurrently — tracked per-chunk for Delete, globally for Close.
func (m *Manager) PostSealProcess(ctx context.Context, id chunk.ChunkID) error {
	// Guard: reject unsealed chunks upfront. Without this, CompressChunk
	// silently no-ops and the index builders fail with ErrChunkNotSealed,
	// producing a spurious WARN on every call. See gastrolog-89k15.
	m.mu.Lock()
	meta, ok := m.metas[id]
	if !ok {
		m.mu.Unlock()
		return chunk.ErrChunkNotFound
	}
	if !meta.sealed {
		m.mu.Unlock()
		return chunk.ErrChunkNotSealed
	}
	m.mu.Unlock()

	done := make(chan struct{})
	m.postSealActive.Store(id, done)
	m.postSealWg.Add(1)
	defer func() {
		close(done)
		m.postSealActive.Delete(id)
		m.postSealWg.Done()
	}()

	// 1. Compress data files.
	if err := m.CompressChunk(id); err != nil {
		return err
	}

	// 2. Build indexes via injected builders.
	for _, builder := range m.indexBuilders {
		if err := builder.Build(ctx, id); err != nil {
			m.logger.Warn("index build failed", "chunk", id, "error", err)
			// Non-fatal: indexes can be rebuilt later.
		}
	}

	// 3. Refresh disk sizes after index files are written.
	if len(m.indexBuilders) > 0 {
		m.RefreshDiskSizes(id)
	}

	// 4. Upload to cloud and delete local if cloud-backed.
	// CloudReadOnly followers skip upload — they adopt the leader's blob
	// via RegisterCloudChunk when the tier Raft FSM propagates the upload.
	if m.cfg.CloudStore != nil && !m.cfg.CloudReadOnly {
		if err := m.uploadToCloud(id); err != nil {
			m.logger.Warn("cloud upload failed, keeping local", "chunk", id, "error", err)
		}
	}

	return nil
}

// RefreshDiskSizes recomputes bytes and diskBytes for a sealed chunk from the
// actual directory contents. Called after index builds add files to the chunk dir.
func (m *Manager) RefreshDiskSizes(id chunk.ChunkID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	meta, ok := m.metas[id]
	if !ok || !meta.sealed || meta.cloudBacked {
		return
	}
	meta.bytes = m.computeTotalLogicalBytes(id, meta.logicalDataBytes)
	meta.diskBytes = m.computeDiskBytes(id)
}

// --- Warm cache helpers ---

// cachePath returns the local cache file path for a cloud chunk's GLCB blob.
func (m *Manager) cachePath(id chunk.ChunkID) string {
	return filepath.Join(m.cfg.CacheDir, id.String()+".glcb")
}

// writeBlobToCache atomically writes a GLCB blob to the cache directory.
// Errors are logged and swallowed — cache writes are best-effort.
func (m *Manager) writeBlobToCache(id chunk.ChunkID, data []byte) {
	if err := os.MkdirAll(m.cfg.CacheDir, 0o750); err != nil {
		m.logger.Warn("cache: failed to create dir", "error", err)
		return
	}
	tmp, err := os.CreateTemp(m.cfg.CacheDir, ".glcb-*.tmp")
	if err != nil {
		m.logger.Warn("cache: failed to create temp file", "error", err)
		return
	}
	tmpPath := filepath.Clean(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath) //nolint:gosec // G703: tmpPath is from os.CreateTemp in CacheDir
		m.logger.Warn("cache: failed to write blob", "chunk", id, "error", err)
		return
	}
	_ = tmp.Close()
	if err := os.Rename(tmpPath, m.cachePath(id)); err != nil { //nolint:gosec // G703: tmpPath is from os.CreateTemp
		_ = os.Remove(tmpPath) //nolint:gosec // G703: tmpPath is from os.CreateTemp
		m.logger.Warn("cache: failed to rename blob", "chunk", id, "error", err)
		return
	}
	m.logger.Debug("cache: wrote GLCB blob", "chunk", id, "bytes", len(data))
}

// openCachedCursor opens a cloud chunk from the local cache directory.
// Returns an error if the cache file doesn't exist or is corrupt.
func (m *Manager) openCachedCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	path := m.cachePath(id)
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err // cache miss
	}
	rd, err := chunkcloud.NewCacheReader(f)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(path) // corrupt — remove so next access re-downloads
		return nil, err
	}
	return chunkcloud.NewSeekableCursor(rd, id), nil
}

// downloadAndCacheCursor downloads the entire GLCB blob from cloud, writes it
// to the cache directory, and opens a local cursor. Blocking on first access.
func (m *Manager) downloadAndCacheCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	rc, err := m.cfg.CloudStore.Download(context.Background(), m.blobKey(id))
	m.trackCloudResult(err)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()

	if err := os.MkdirAll(m.cfg.CacheDir, 0o750); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(m.cfg.CacheDir, ".glcb-*.tmp")
	if err != nil {
		return nil, err
	}
	tmpPath := filepath.Clean(tmp.Name())
	if _, err := io.Copy(tmp, rc); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath) //nolint:gosec // G703: tmpPath is from os.CreateTemp in CacheDir
		return nil, err
	}
	_ = tmp.Close()

	finalPath := m.cachePath(id)
	if err := os.Rename(tmpPath, finalPath); err != nil { //nolint:gosec // G703: tmpPath is from os.CreateTemp
		_ = os.Remove(tmpPath) //nolint:gosec // G703: tmpPath is from os.CreateTemp
		return nil, err
	}

	return m.openCachedCursor(id)
}

// SetRotationPolicy updates the rotation policy for future appends.
// ArchiveChunk transitions a cloud-backed sealed chunk to an offline storage class.
// The blob's storage class is changed via the Archiver interface on the cloud store.
// After this, the chunk's Archived flag is set and cursor reads return ErrChunkArchived.
func (m *Manager) ArchiveChunk(ctx context.Context, id chunk.ChunkID, storageClass string) error {
	m.mu.Lock()
	meta := m.lookupMeta(id)
	if meta == nil {
		m.mu.Unlock()
		return chunk.ErrChunkNotFound
	}
	if !meta.cloudBacked {
		m.mu.Unlock()
		return fmt.Errorf("chunk %s is not cloud-backed, cannot archive", id)
	}
	if meta.archived {
		m.mu.Unlock()
		return nil // already archived
	}
	m.mu.Unlock()

	archiver, ok := m.cfg.CloudStore.(blobstore.Archiver)
	if !ok {
		return errors.New("cloud store does not support archival operations")
	}

	key := m.blobKey(id)
	if err := archiver.Archive(ctx, key, storageClass); err != nil {
		return fmt.Errorf("archive blob %s: %w", key, err)
	}

	m.setArchivedFlag(id, true, storageClass)

	m.logger.Debug("chunk archived",
		"chunk", id.String(), "storageClass", storageClass)
	return nil
}

// RestoreChunk initiates retrieval of an archived chunk from offline storage.
// On completion, the Archived flag is cleared and the chunk becomes readable.
func (m *Manager) RestoreChunk(ctx context.Context, id chunk.ChunkID, tier string, days int) error {
	m.mu.Lock()
	meta := m.lookupMeta(id)
	if meta == nil {
		m.mu.Unlock()
		return chunk.ErrChunkNotFound
	}
	if !meta.archived {
		m.mu.Unlock()
		return nil // not archived, nothing to restore
	}
	m.mu.Unlock()

	archiver, ok := m.cfg.CloudStore.(blobstore.Archiver)
	if !ok {
		return errors.New("cloud store does not support restore operations")
	}

	key := m.blobKey(id)
	if err := archiver.Restore(ctx, key, tier, days); err != nil {
		return fmt.Errorf("restore blob %s: %w", key, err)
	}

	m.setArchivedFlag(id, false, "")

	m.logger.Info("chunk restore initiated", "chunk", id.String())
	return nil
}

// setArchivedFlag updates the archived state for a chunk in both the local
// metas map and the cloud B+ tree index. For cloud-backed chunks, this
// re-inserts the entry with the updated flag.
func (m *Manager) setArchivedFlag(id chunk.ChunkID, archived bool, storageClass string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Track the storage class in the side map (not in the B+ tree — fixed-size value).
	if storageClass != "" {
		m.storageClasses[id] = storageClass
	} else {
		delete(m.storageClasses, id)
	}

	// Local metas (non-cloud chunks or chunks still in both).
	if meta, ok := m.metas[id]; ok {
		meta.archived = archived
	}

	// Cloud B+ tree index — lookup, mutate, re-insert.
	if m.cloudIdx == nil {
		return
	}
	meta, found := m.cloudIdx.Lookup(id)
	if !found {
		return
	}
	meta.archived = archived
	m.cloudIdxMu.Lock()
	if _, err := m.cloudIdx.Delete(id); err != nil {
		m.logger.Warn("cloud index: delete failed", "chunk", id, "error", err)
	}
	if err := m.cloudIdx.Insert(id, meta); err != nil {
		m.logger.Warn("cloud index: insert failed", "chunk", id, "error", err)
	}
	if err := m.cloudIdx.Sync(); err != nil {
		m.logger.Warn("cloud index: sync failed", "error", err)
	}
	m.cloudIdxMu.Unlock()
	m.cloudListCache = nil // invalidate cached List() results
}

func (m *Manager) SetRotationPolicy(policy chunk.RotationPolicy) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg.RotationPolicy = policy
}

// SetAnnouncer injects a metadata announcer for cluster-wide visibility.
// Must be called before any Append/Seal operations. Safe to call with nil
// to disable announcements.
func (m *Manager) SetAnnouncer(a chunk.MetadataAnnouncer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg.Announcer = a
}

func (m *Manager) GetAnnouncer() chunk.MetadataAnnouncer {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg.Announcer
}

func (m *Manager) CheckRotation() *string {
	m.mu.Lock()
	if m.closed || m.active == nil {
		m.mu.Unlock()
		return nil
	}

	state := m.activeChunkState()
	if state.Records == 0 {
		m.mu.Unlock()
		return nil
	}

	var zeroRecord chunk.Record
	trigger := m.cfg.RotationPolicy.ShouldRotate(state, zeroRecord)
	if trigger == nil {
		m.mu.Unlock()
		return nil
	}

	m.logger.Debug("rotating chunk",
		"trigger", *trigger,
		"chunk", state.ChunkID.String(),
		"bytes", state.Bytes,
		"records", state.Records,
		"age", m.cfg.Now().Sub(state.CreatedAt),
	)
	if err := m.sealLocked(); err != nil {
		m.logger.Error("failed to seal chunk during background rotation check",
			"chunk", state.ChunkID.String(), "error", err)
		m.mu.Unlock()
		return nil
	}
	pending := m.takePendingAnnouncements()
	m.mu.Unlock()

	// Fire deferred announcer calls outside the lock.
	runPendingAnnouncements(pending)
	return trigger
}

var _ chunk.ChunkManager = (*Manager)(nil)
var _ chunk.ChunkMover = (*Manager)(nil)
var _ chunk.ChunkCompressor = (*Manager)(nil)

// ChunkDir returns the filesystem path for a chunk's directory.
func (m *Manager) ChunkDir(id chunk.ChunkID) string {
	return m.chunkDir(id)
}

// Disown untracks a sealed chunk without deleting its files.
// The chunk must exist, be sealed, and not be the active chunk.
func (m *Manager) Disown(id chunk.ChunkID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrManagerClosed
	}

	if m.active != nil && m.active.meta.id == id {
		return chunk.ErrActiveChunk
	}

	meta, ok := m.metas[id]
	if !ok {
		return chunk.ErrChunkNotFound
	}
	if !meta.sealed {
		return chunk.ErrChunkNotSealed
	}

	delete(m.metas, id)
	return nil
}

// Adopt registers a sealed chunk directory already present in the storage dir.
// The directory must exist, contain valid idx.log metadata, and the chunk must be sealed.
func (m *Manager) Adopt(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return chunk.ChunkMeta{}, ErrManagerClosed
	}

	// Check if already tracked (local or cloud).
	if m.lookupMeta(id) != nil {
		return chunk.ChunkMeta{}, fmt.Errorf("chunk %s already tracked", id)
	}

	// Verify directory exists.
	dir := m.chunkDir(id)
	if _, err := os.Stat(dir); err != nil {
		return chunk.ChunkMeta{}, fmt.Errorf("chunk directory missing: %w", err)
	}

	meta, err := m.loadChunkMeta(id)
	if err != nil {
		return chunk.ChunkMeta{}, fmt.Errorf("load chunk meta: %w", err)
	}

	if !meta.sealed {
		return chunk.ChunkMeta{}, chunk.ErrChunkNotSealed
	}

	m.metas[id] = meta
	return meta.toChunkMeta(), nil
}

// --- Cloud-backed chunk support ---

// cloudPrefix returns the blob key prefix for this vault's cloud-backed chunks.
// removeFromCloudIndex removes a chunk from the local cloud index, if present.
func (m *Manager) removeFromCloudIndex(id chunk.ChunkID) {
	if m.cloudIdx == nil {
		return
	}
	m.cloudIdxMu.Lock()
	if _, err := m.cloudIdx.Delete(id); err != nil {
		m.logger.Warn("failed to remove from cloud index", "chunk", id, "error", err)
	} else if err := m.cloudIdx.Sync(); err != nil {
		m.logger.Warn("failed to sync cloud index after delete", "chunk", id, "error", err)
	}
	m.cloudIdxMu.Unlock()
	m.cloudListCache = nil // invalidate
	// Clean up cached TS index files.
	cacheDir := filepath.Join(m.cfg.Dir, tsCacheDir)
	matches, _ := filepath.Glob(filepath.Join(cacheDir, id.String()+".*"))
	for _, f := range matches {
		_ = os.Remove(f)
	}
}

// rebuildCloudListCache scans the cloud B+ tree index and caches the result.
// Must be called with m.mu held.
func (m *Manager) rebuildCloudListCache() {
	var cache []chunk.ChunkMeta
	if err := m.cloudIdx.ForEach(func(id chunk.ChunkID, meta *chunkMeta) bool {
		if _, exists := m.metas[id]; !exists {
			cm := meta.toChunkMeta()
			cm.StorageClass = m.storageClasses[id]
			cache = append(cache, cm)
		}
		return true
	}); err != nil {
		m.logger.Warn("cloud index: ForEach failed during cache rebuild", "error", err)
	}
	m.cloudListCache = cache
}

func (m *Manager) cloudPrefix() string {
	return fmt.Sprintf("vault-%s/", m.cfg.VaultID)
}

// blobKey returns the object key for a cloud-backed chunk.
func (m *Manager) blobKey(id chunk.ChunkID) string {
	return m.cloudPrefix() + id.String() + ".glcb"
}

// chunkIDFromBlobKey extracts the ChunkID from a blob key.
func (m *Manager) chunkIDFromBlobKey(key string) (chunk.ChunkID, bool) {
	key = strings.TrimPrefix(key, m.cloudPrefix())
	key = strings.TrimSuffix(key, ".glcb")
	id, err := chunk.ParseChunkID(key)
	if err != nil {
		return chunk.ChunkID{}, false
	}
	return id, true
}

// cloudIdxHas reports whether a chunk is already tracked in the cloud index.
func (m *Manager) cloudIdxHas(id chunk.ChunkID) bool {
	m.cloudIdxMu.Lock()
	_, found := m.cloudIdx.Lookup(id)
	m.cloudIdxMu.Unlock()
	return found
}

// uploadToCloud converts a sealed, compressed chunk to GLCB format, uploads it
// to the cloud store, and deletes the local files. The chunk metadata is
// updated to reflect cloud-backed status.
// SetCloudStore injects (or replaces) the cloud store on a running Manager.
// Used for lazy initialization when S3 was unreachable at construction time
// but becomes available later. Also re-runs cloud chunk discovery if the
// cloud index is empty. Safe for concurrent use. See gastrolog-68fqk.
// SetCloudReadOnly toggles whether this manager uploads to cloud in
// PostSealProcess. Called when tier Raft leadership changes — the new
// leader enables uploads, the old leader disables them. See gastrolog-1s3mf.
func (m *Manager) SetCloudReadOnly(ro bool) {
	m.mu.Lock()
	m.cfg.CloudReadOnly = ro
	m.mu.Unlock()
}

func (m *Manager) SetCloudStore(store blobstore.Store) {
	m.mu.Lock()
	m.cfg.CloudStore = store
	m.mu.Unlock()

	// Try to populate the cloud index now that we have a connection.
	if m.cloudIdx != nil {
		if err := m.loadCloudChunks(); err != nil {
			m.logger.Warn("cloud chunk discovery failed after SetCloudStore", "error", err)
			m.trackCloudResult(err)
		} else {
			m.trackCloudResult(nil)
		}
	}
}

// UploadToCloud uploads a sealed chunk to the configured cloud store.
// Implements chunk.ChunkCloudUploader. Returns an error if the upload fails
// (unlike PostSealProcess which swallows upload errors to avoid blocking
// replication). Used by the cloud backfill path. See gastrolog-68fqk.
func (m *Manager) UploadToCloud(id chunk.ChunkID) error {
	if m.cfg.CloudStore == nil {
		return errors.New("cloud store not configured")
	}

	// Ensure the chunk exists in the tier Raft FSM. Chunks sealed during
	// degraded startup (S3 down, Raft not ready) may be missing from the
	// manifest. Without Create+Seal in the FSM, the subsequent Upload
	// announce has nothing to update. Idempotent — if already present,
	// the FSM ignores the duplicate. See gastrolog-68fqk.
	if m.cfg.Announcer != nil {
		m.mu.Lock()
		meta := m.metas[id]
		m.mu.Unlock()
		if meta != nil && meta.sealed {
			m.cfg.Announcer.AnnounceCreate(id, meta.writeStart, meta.ingestStart, meta.sourceStart)
			m.cfg.Announcer.AnnounceSeal(id, meta.writeEnd, meta.recordCount, meta.bytes, meta.ingestEnd, meta.sourceEnd)
		}
	}

	return m.uploadToCloud(id)
}

func (m *Manager) uploadToCloud(id chunk.ChunkID) error {
	key := m.blobKey(id)

	// If the blob already exists (leader or another replica uploaded first),
	// skip the upload and adopt the existing blob's size. This prevents
	// multiple nodes from overwriting each other's uploads with slightly
	// different compressed output, which causes InvalidRange errors when
	// a node tries to read using its own (now stale) blob size.
	headCtx, headCancel := context.WithTimeout(context.Background(), cloudHeadTimeout)
	existing, err := m.cfg.CloudStore.Head(headCtx, key)
	headCancel()
	if err == nil && existing.Size > 0 {
		m.logger.Debug("cloud blob already exists, skipping upload", "chunk", id, "bytes", existing.Size)
		return m.adoptCloudBlob(id, existing.Size)
	}

	// Open a cursor on the local sealed chunk to read all records.
	cursor, err := m.OpenCursor(id)
	if err != nil {
		return fmt.Errorf("open cursor for cloud upload: %w", err)
	}

	w := chunkcloud.NewWriter(id, m.cfg.VaultID, m.zstdEnc)
	for {
		rec, _, recErr := cursor.Next()
		if errors.Is(recErr, chunk.ErrNoMoreRecords) {
			break
		}
		if recErr != nil {
			_ = cursor.Close()
			return fmt.Errorf("read record for cloud upload: %w", recErr)
		}
		if err := w.Add(rec); err != nil {
			_ = cursor.Close()
			return fmt.Errorf("add record to GLCB writer: %w", err)
		}
	}
	_ = cursor.Close()

	var buf bytes.Buffer
	if _, err := w.WriteTo(&buf); err != nil {
		return fmt.Errorf("compress GLCB: %w", err)
	}

	bm := w.Meta()
	uploadCtx, uploadCancel := context.WithTimeout(context.Background(), cloudUploadTimeout)
	err = m.cfg.CloudStore.Upload(
		uploadCtx,
		key,
		bytes.NewReader(buf.Bytes()),
		chunkcloud.ObjectMetadata(bm),
	)
	uploadCancel()
	m.trackCloudResult(err)
	if err != nil {
		return fmt.Errorf("upload GLCB: %w", err)
	}

	blobSize := int64(buf.Len())

	// Write GLCB blob to local cache (best-effort).
	if m.cfg.CacheDir != "" {
		m.writeBlobToCache(id, buf.Bytes())
	}

	// Delete local data files but keep index files for fast queries.
	if err := m.removeLocalDataFiles(id); err != nil {
		return fmt.Errorf("remove local data files after cloud upload: %w", err)
	}

	// Move metadata from in-memory map to cloud B+ tree index.
	// The chunk is now cloud-only — remove from Go heap.
	toc := w.TOC()
	m.mu.Lock()
	meta := m.metas[id]
	if meta != nil {
		meta.cloudBacked = true
		meta.diskBytes = blobSize
		meta.ingestIdxOffset = toc.IngestIdxOffset
		meta.ingestIdxSize = toc.IngestIdxSize
		meta.sourceIdxOffset = toc.SourceIdxOffset
		meta.sourceIdxSize = toc.SourceIdxSize
		meta.numFrames = w.NumFrames()
		delete(m.metas, id)
	}
	m.mu.Unlock()

	if m.cloudIdx != nil && meta != nil {
		m.cloudIdxMu.Lock()
		if err := m.cloudIdx.Insert(id, meta); err != nil {
			m.logger.Warn("failed to index cloud chunk", "chunk", id, "error", err)
		} else if err := m.cloudIdx.Sync(); err != nil {
			m.logger.Warn("failed to sync cloud index", "chunk", id, "error", err)
		}
		m.cloudIdxMu.Unlock()
		m.mu.Lock()
		m.cloudListCache = nil // invalidate
		m.mu.Unlock()
	}

	if m.cfg.Announcer != nil && meta != nil {
		m.cfg.Announcer.AnnounceUpload(id, meta.diskBytes,
			meta.ingestIdxOffset, meta.ingestIdxSize,
			meta.sourceIdxOffset, meta.sourceIdxSize,
			meta.numFrames)
	}

	m.logger.Debug("chunk uploaded to cloud",
		"chunk", id,
		"bytes", blobSize,
	)
	return nil
}

// adoptCloudBlob transitions a local chunk to cloud-backed using an existing
// S3 blob. Used when another node (typically the leader) already uploaded the
// same chunk. Reads the GLCB header from S3 to get TOC offsets, then deletes
// the local copy.
func (m *Manager) adoptCloudBlob(id chunk.ChunkID, blobSize int64) error {
	key := m.blobKey(id)

	// Read TOC from end of blob to get TS index offsets.
	tocCtx, tocCancel := context.WithTimeout(context.Background(), cloudDownloadTimeout)
	tocBuf, err := m.cfg.CloudStore.DownloadRange(
		tocCtx, key, blobSize-chunkcloud.TOCSize, chunkcloud.TOCSize)
	tocCancel()
	if err != nil {
		return fmt.Errorf("read TOC from existing blob: %w", err)
	}
	defer func() { _ = tocBuf.Close() }()
	tocData := make([]byte, chunkcloud.TOCSize)
	if _, err := io.ReadFull(tocBuf, tocData); err != nil {
		return fmt.Errorf("read TOC bytes: %w", err)
	}
	toc, err := chunkcloud.ParseTOC(tocData)
	if err != nil {
		return fmt.Errorf("parse TOC from existing blob: %w", err)
	}

	// Delete local data files but keep index files for fast queries.
	if err := m.removeLocalDataFiles(id); err != nil {
		return fmt.Errorf("remove local data files after cloud adopt: %w", err)
	}

	m.mu.Lock()
	meta := m.metas[id]
	if meta != nil {
		meta.cloudBacked = true
		meta.compressed = true // GLCB blobs are always zstd-compressed
		meta.diskBytes = blobSize
		meta.ingestIdxOffset = toc.IngestIdxOffset
		meta.ingestIdxSize = toc.IngestIdxSize
		meta.sourceIdxOffset = toc.SourceIdxOffset
		meta.sourceIdxSize = toc.SourceIdxSize
		delete(m.metas, id)
	}
	m.mu.Unlock()

	if m.cloudIdx != nil && meta != nil {
		m.cloudIdxMu.Lock()
		if err := m.cloudIdx.Insert(id, meta); err != nil {
			m.logger.Warn("failed to index adopted cloud chunk", "chunk", id, "error", err)
		} else if err := m.cloudIdx.Sync(); err != nil {
			m.logger.Warn("failed to sync cloud index", "chunk", id, "error", err)
		}
		m.cloudIdxMu.Unlock()
		m.mu.Lock()
		m.cloudListCache = nil
		m.mu.Unlock()
	}

	// Announce to tier Raft so all nodes learn this chunk is cloud-backed.
	// Without this, the FSM overlay keeps returning CloudBacked=false and
	// the backfill re-adopts on every cycle. See gastrolog-68fqk.
	//
	// When meta is nil (chunk already adopted in a prior cycle — no longer
	// in metas), fall back to the cloud index. The FSM requires a Create
	// entry before Upload can set CloudBacked, so announce Create+Seal
	// first (idempotent if already present).
	if m.cfg.Announcer != nil {
		am := meta
		if am == nil && m.cloudIdx != nil {
			m.cloudIdxMu.Lock()
			am, _ = m.cloudIdx.Lookup(id)
			m.cloudIdxMu.Unlock()
		}
		if am != nil {
			m.cfg.Announcer.AnnounceCreate(id, am.writeStart, am.ingestStart, am.sourceStart)
			m.cfg.Announcer.AnnounceSeal(id, am.writeEnd, am.recordCount, am.bytes, am.ingestEnd, am.sourceEnd)
			m.cfg.Announcer.AnnounceUpload(id, blobSize,
				toc.IngestIdxOffset, toc.IngestIdxSize,
				toc.SourceIdxOffset, toc.SourceIdxSize,
				am.numFrames)
		}
	}

	m.logger.Info("chunk adopted from cloud",
		"chunk", id,
		"bytes", blobSize,
	)
	return nil
}

// RegisterCloudChunk registers a cloud-backed chunk from metadata alone,
// without streaming any records or downloading from S3. Creates a cloud
// index entry so the chunk appears in List() and is queryable via
// openCloudCursor. Used by follower nodes when the tier Raft FSM
// propagates the leader's AnnounceUpload.
//
// Idempotent: if the chunk is already registered (in metas or cloudIdx),
// this is a no-op.
func (m *Manager) RegisterCloudChunk(id chunk.ChunkID, info chunk.CloudChunkInfo) error {
	if m.cloudIdx == nil {
		return errors.New("cloud index not available (no cloud store configured)")
	}

	// Check if already known.
	m.mu.Lock()
	if _, ok := m.metas[id]; ok {
		m.mu.Unlock()
		return nil // already local
	}
	m.mu.Unlock()
	if existing, _ := m.cloudIdx.Lookup(id); existing != nil {
		return nil // already in cloud index
	}

	meta := &chunkMeta{
		id:              id,
		writeStart:      info.WriteStart,
		writeEnd:        info.WriteEnd,
		ingestStart:     info.IngestStart,
		ingestEnd:       info.IngestEnd,
		sourceStart:     info.SourceStart,
		sourceEnd:       info.SourceEnd,
		recordCount:     info.RecordCount,
		bytes:           info.Bytes,
		sealed:          true,
		compressed:      true,
		cloudBacked:     true,
		diskBytes:       info.DiskBytes,
		ingestIdxOffset: info.IngestIdxOffset,
		ingestIdxSize:   info.IngestIdxSize,
		sourceIdxOffset: info.SourceIdxOffset,
		sourceIdxSize:   info.SourceIdxSize,
		numFrames:       info.NumFrames,
	}

	m.cloudIdxMu.Lock()
	if err := m.cloudIdx.Insert(id, meta); err != nil {
		m.cloudIdxMu.Unlock()
		return fmt.Errorf("insert cloud chunk %s: %w", id, err)
	}
	if err := m.cloudIdx.Sync(); err != nil {
		m.cloudIdxMu.Unlock()
		return fmt.Errorf("sync cloud index for %s: %w", id, err)
	}
	m.cloudIdxMu.Unlock()

	m.mu.Lock()
	m.cloudListCache = nil
	m.mu.Unlock()

	m.logger.Debug("registered cloud chunk from metadata", "chunk", id, "records", info.RecordCount)
	return nil
}

// scanAttrsCloud iterates a cloud-backed chunk's attributes via cursor.
func (m *Manager) scanAttrsCloud(id chunk.ChunkID, startPos uint64, fn func(writeTS time.Time, attrs chunk.Attributes) bool) error {
	cursor, err := m.openCloudCursor(id)
	if err != nil {
		return err
	}
	defer func() { _ = cursor.Close() }()
	var pos uint64
	for {
		rec, _, recErr := cursor.Next()
		if errors.Is(recErr, chunk.ErrNoMoreRecords) {
			return nil
		}
		if recErr != nil {
			return recErr
		}
		if pos >= startPos {
			if !fn(rec.WriteTS, rec.Attrs) {
				return nil
			}
		}
		pos++
	}
}

// openCloudCursor opens a cloud-backed chunk for random-access record reads
// via range requests. Downloads only the header, dictionary, record index, and
// TOC at init (~few KB). Individual records are fetched on demand — each read
// triggers a range request for the seekable zstd frame containing that record.
//
// This is efficient because cloud cursors are rarely opened: bounded queries
// defer cloud chunks entirely when local chunks can serve the limit. When a
// cloud cursor IS opened, the TS index narrows access to specific positions,
// so only a few frames are fetched rather than the full blob.
func (m *Manager) openCloudCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	m.mu.Lock()
	meta := m.lookupMeta(id)
	m.mu.Unlock()

	if meta == nil {
		return nil, chunk.ErrChunkNotFound
	}

	// Cache hit: open from local GLCB file.
	if m.cfg.CacheDir != "" {
		if cursor, err := m.openCachedCursor(id); err == nil {
			return cursor, nil
		}
	}

	// Cache miss: download entire blob to cache, then open locally.
	if m.cfg.CacheDir != "" {
		if cursor, err := m.downloadAndCacheCursor(id); err == nil {
			return cursor, nil
		} else {
			m.logger.Warn("cache: download-and-cache failed, falling back to range requests",
				"chunk", id, "error", err)
		}
	}

	// Fallback: remote reader with range requests.
	rd, err := chunkcloud.NewRemoteReader(m.cfg.CloudStore, m.blobKey(id), meta.diskBytes)
	if err != nil {
		return nil, fmt.Errorf("open remote reader %s: %w", id, err)
	}

	return chunkcloud.NewRemoteSeekableCursor(rd, id), nil
}

// loadCloudChunks verifies the cloud index is readable and populates it from
// the cloud store if empty. Cloud chunk metadata is NOT loaded into m.metas —
// it stays in the B+ tree and is served on demand via lookupMeta/ForEach.
// After loading, pre-warms the TS index cache so the first query doesn't spike.
func (m *Manager) loadCloudChunks() error {
	var prevCount uint64
	if m.cloudIdx != nil {
		prevCount = m.cloudIdx.Count()
	}
	if err := m.loadCloudChunksFromStore(); err != nil {
		return err
	}
	if m.cloudIdx != nil {
		newCount := m.cloudIdx.Count()
		if newCount > prevCount {
			m.logger.Info("cloud index reconciled with store",
				"previous", prevCount, "current", newCount,
				"added", newCount-prevCount)
		} else {
			m.logger.Info("cloud index ready", "count", newCount)
		}
	}
	m.backfillTSOffsets()
	return nil
}

// backfillTSOffsets reads the GLCB TOC footer for cloud chunks that have zero
// TS index offsets (pre-existing blobs from before the TS index feature).
// Updates the cloud.idx B+ tree so subsequent startups skip this.
func (m *Manager) backfillTSOffsets() {
	if m.cloudIdx == nil || m.cfg.CloudStore == nil {
		return
	}
	var updated int
	if err := m.cloudIdx.ForEach(func(id chunk.ChunkID, meta *chunkMeta) bool {
		if meta.ingestIdxSize > 0 {
			return true // already has offsets
		}
		// Read the last 48 bytes (TOC) from the blob.
		info, err := m.cfg.CloudStore.Head(context.Background(), m.blobKey(id))
		if err != nil || info.Size < chunkcloud.TOCSize {
			return true
		}
		rc, err := m.cfg.CloudStore.DownloadRange(context.Background(), m.blobKey(id), info.Size-chunkcloud.TOCSize, chunkcloud.TOCSize)
		if err != nil {
			return true
		}
		var buf [chunkcloud.TOCSize]byte
		_, err = io.ReadFull(rc, buf[:])
		_ = rc.Close()
		if err != nil {
			return true
		}
		toc, err := chunkcloud.ParseTOC(buf[:])
		if err != nil || toc.IngestIdxOffset == 0 {
			return true // v1 blob without embedded TS indexes
		}
		meta.ingestIdxOffset = toc.IngestIdxOffset
		meta.ingestIdxSize = toc.IngestIdxSize
		meta.sourceIdxOffset = toc.SourceIdxOffset
		meta.sourceIdxSize = toc.SourceIdxSize
		if err := m.cloudIdx.Insert(id, meta); err == nil {
			updated++
		}
		return true
	}); err != nil {
		m.logger.Warn("cloud index: ForEach failed during TS offset backfill", "error", err)
	}
	if updated > 0 {
		if err := m.cloudIdx.Sync(); err != nil {
			m.logger.Warn("cloud index: sync failed after TS offset backfill", "error", err)
		}
		m.cloudListCache = nil // invalidate
		m.logger.Info("backfilled TS index offsets", "updated", updated)
	}
}

// loadCloudChunksFromStore iterates blobs from the cloud store and populates
// the local B+ tree index. Does NOT insert into m.metas.
func (m *Manager) loadCloudChunksFromStore() error {
	var indexed int
	err := m.cfg.CloudStore.List(context.Background(), m.cloudPrefix(), func(blob blobstore.BlobInfo) error { //nolint:contextcheck // long-lived background scan
		id, ok := m.chunkIDFromBlobKey(blob.Key)
		if !ok {
			return nil
		}
		// Don't add to cloud index if the chunk has local data files —
		// let the backfill handle it via UploadToCloud → adoptCloudBlob,
		// which removes local files, updates cloud index, AND announces
		// to the tier Raft FSM. Setting cloudBacked here would make the
		// backfill skip the chunk, leaving the FSM out of date.
		if _, exists := m.metas[id]; exists {
			return nil
		}
		// Skip if already in the cloud index (preserves richer metadata
		// like TS index offsets from previous backfills).
		if m.cloudIdx != nil && m.cloudIdxHas(id) {
			return nil
		}
		cm := chunkcloud.BlobMetaToChunkMeta(id, blob)
		meta := &chunkMeta{
			id:          id,
			writeStart:  cm.WriteStart,
			writeEnd:    cm.WriteEnd,
			recordCount: cm.RecordCount,
			bytes:       cm.Bytes,
			diskBytes:   cm.DiskBytes,
			sealed:      true,
			compressed:  true,
			ingestStart: cm.IngestStart,
			ingestEnd:   cm.IngestEnd,
			sourceStart: cm.SourceStart,
			sourceEnd:   cm.SourceEnd,
			cloudBacked: true,
			archived:    blob.IsArchived(),
		}

		// Cache the storage class for the sweep and UI.
		if blob.StorageClass != "" {
			m.storageClasses[id] = blob.StorageClass
		}

		// Populate the local B+ tree index.
		if m.cloudIdx != nil {
			m.cloudIdxMu.Lock()
			err := m.cloudIdx.Insert(id, meta)
			m.cloudIdxMu.Unlock()
			if err != nil {
				return fmt.Errorf("index cloud chunk %s: %w", id, err)
			}
			indexed++
		}
		return nil
	})
	m.trackCloudResult(err)
	if err != nil {
		return fmt.Errorf("list cloud chunks: %w", err)
	}
	if m.cloudIdx != nil && indexed > 0 {
		m.cloudIdxMu.Lock()
		err = m.cloudIdx.Sync()
		m.cloudIdxMu.Unlock()
		if err != nil {
			return fmt.Errorf("sync cloud index: %w", err)
		}
		m.cloudIdx.EvictClean()
		m.logger.Info("populated cloud index from store", "count", indexed)
	}
	return nil
}
