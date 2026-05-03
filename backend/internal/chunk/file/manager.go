package file

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
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

	// dataGLCBFileName is the canonical sealed-chunk artifact under the
	// chunk redesign (gastrolog-2pw28): a single self-contained, byte-
	// identical-across-replicas, integrity-checked GLCB blob. Sibling
	// files in the chunk directory may exist for node-local artifacts
	// (custom indexes, build-time ledgers) but only data.glcb is the
	// replicable / cloud-uploadable shape.
	dataGLCBFileName    = "data.glcb"
	dataGLCBTmpFileName = "data.glcb.tmp"

	// currentKeyScheme selects the blobKey() derivation function recorded
	// onto every CmdUploadChunk. Only scheme 0 ("vault-<vault>/<chunk>.glcb")
	// exists today; future schemes (date-prefixed, hash-sharded,
	// multi-bucket) get new values without rendering existing FSM entries
	// ambiguous. See gastrolog-grnc3.
	currentKeyScheme uint8 = 0
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
	VaultID glid.GLID

	// CloudServiceID is the FSM-snapshot identifier for the cloud service
	// this Manager is currently wired to. Recorded onto every CmdUploadChunk
	// so a chunk's authoritative store survives a tier reconfiguration that
	// repoints CloudStore. Zero value when CloudStore is nil. See
	// gastrolog-grnc3.
	CloudServiceID glid.GLID

	// CloudReadOnly, when true, enables cloud store reads (cursor, cache)
	// but skips uploads in PostSealProcess. Used by follower nodes that
	// share the leader's S3 bucket for reads without owning the blobs.
	CloudReadOnly bool

	// CacheBudgetBytes is the soft cap on total bytes occupied by warm-cache
	// copies of cloud-backed chunks (<chunkDir>/data.glcb). Zero disables
	// LRU-by-budget eviction. Enforced by EvictCacheLRU which the
	// orchestrator runs on a schedule. See gastrolog-2idw8.
	CacheBudgetBytes uint64

	// CacheTTL is the maximum age (since last OpenCursor) of a warm-cache
	// entry before it gets evicted. Zero disables TTL eviction. Local-only
	// sealed chunks are never affected — their data.glcb is authoritative.
	CacheTTL time.Duration

	// Announcer, when non-nil, is called after each metadata state change
	// (create, seal, compress, upload, delete) for cluster-wide visibility.
	Announcer chunk.MetadataAnnouncer

	// IntegrityVerifier, when non-nil, is consulted on every cold-cache
	// cloud download to verify the GLCB whole-blob digest matches the
	// FSM-recorded value (gastrolog-grnc3). A mismatch causes the
	// downloaded file to be deleted and the read to error — the next
	// retry re-fetches. nil disables verification (acceptable in
	// single-node tests; required in production cluster setups).
	IntegrityVerifier chunk.IntegrityVerifier
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
	mu             sync.Mutex
	cfg            Config
	lockFile       *os.File // Exclusive lock on vault directory
	active         *chunkState
	metas          map[chunk.ChunkID]*chunkMeta // In-memory chunk metadata
	closed         bool
	zstdEnc        *zstd.Encoder
	zstdEncMu      sync.Mutex                // serializes concurrent CompressChunk calls sharing zstdEnc
	cloudIdx       *cloudIndex               // local B+ tree cache of cloud chunk metadata (nil if no cloud store)
	cloudIdxMu     sync.Mutex                // serializes cloudIdx Insert/Delete/Sync (B+ tree is not thread-safe)
	indexBuilders  []chunk.ChunkIndexBuilder // injected post-construction via SetIndexBuilders
	cloudListCache []chunk.ChunkMeta         // cached List() result for cloud chunks; nil = stale
	storageClasses map[chunk.ChunkID]string  // in-memory cache of cloud storage class per chunk
	nextChunkID    *chunk.ChunkID            // if set, used instead of NewChunkID() on next open

	postSealActive sync.Map       // chunk.ChunkID → chan struct{} — closed when PostSealProcess finishes
	postSealWg     sync.WaitGroup // tracks in-flight PostSealProcess calls (for Close only)

	// chunkLocks protects each chunk's file lifetime against concurrent
	// mutation: cursor reads (mmap'd raw.log/idx.log/attr.log regions)
	// take a read lock; compression's atomic rename and retention's
	// os.RemoveAll take the write lock. Without this, an in-flight
	// cursor.Next could SIGBUS when the underlying file is unlinked or
	// renamed out from under its mmap region — the gastrolog-26zu1
	// node-killing crash. The per-chunk granularity preserves
	// concurrency between mutators on different chunks.
	//
	// Lifecycle: created on first chunkLockFor(id) call, removed only
	// after the chunk is fully deleted (deleteInternal). Map entries
	// outlive their chunks for the duration of any in-flight cursor —
	// cleanup happens after the writer holding the lock releases it,
	// at which point no new readers can resolve the lock (meta is gone
	// → ErrChunkNotFound at the meta-lookup step in OpenCursor).
	chunkLocksMu sync.Mutex
	chunkLocks   map[chunk.ChunkID]*sync.RWMutex

	// lastAccess records the most recent OpenCursor time for each
	// cloud-backed chunk's local data.glcb cache. Used by EvictCacheLRU to
	// pick the coldest entries when the cache exceeds its budget. In-memory
	// only — never persisted; eviction signals don't outlive the process,
	// per the chunk-redesign doc's "cache eviction signals are node-local,
	// not FSM state" rule. See gastrolog-2idw8.
	lastAccessMu sync.Mutex
	lastAccess   map[chunk.ChunkID]time.Time

	// cloudDegraded tracks whether the cloud store is currently unreachable.
	// Set on any failed cloud operation (init, upload, download, list);
	// cleared on any successful one. The orchestrator polls this to raise
	// or clear an operator-visible alert. See gastrolog-68fqk.
	cloudDegraded    atomic.Bool
	cloudDegradedErr atomic.Value // stores the last cloud error (string) for alert messages

	// pendingAnnouncements accumulates closures that fire metadata announcer
	// calls. The fields are protected by mu. Locked code paths (openLocked,
	// sealLocked, etc.) APPEND closures here instead of calling the announcer
	// directly. The top-level public methods (Append, Seal, CheckRotation)
	// drain this slice via takePendingAnnouncements AFTER releasing mu, then
	// invoke each closure outside the lock.
	//
	// Why: announcer calls go through vault-ctl Raft via the apply forwarder
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
	writeStart       time.Time // WriteTS of first record
	writeEnd         time.Time // WriteTS of last record
	recordCount      int64     // Number of records in chunk
	bytes            int64     // Total logical bytes (data + non-data files)
	logicalDataBytes int64     // Logical data bytes only (raw + attr + idx content)
	sealed           bool
	diskBytes        int64 // actual on-disk size of data.glcb

	// IngestTS and SourceTS bounds (zero = unknown).
	ingestStart time.Time
	ingestEnd   time.Time
	sourceStart time.Time
	sourceEnd   time.Time

	// IngestTSMonotonic starts true and flips to false the first time an
	// Append delivers a record with IngestTS earlier than the running max.
	// Determined dynamically per chunk — never assumed by tier — because
	// tier 1 ingesters (RELP, Syslog, Fluent, etc.) routinely stamp
	// arbitrary IngestTS values or deliver out of order, and tier 2+
	// destinations may happen to receive records in IngestTS order.
	// See gastrolog-66b7x.
	ingestTSMonotonic bool

	cloudBacked bool // true = chunk lives in cloud, not on local disk
	archived    bool // true = chunk is in offline storage tier (Glacier, Azure Archive)

	// GLCB TOC: section offsets for embedded TS indexes (0 = none).
	ingestIdxOffset int64
	ingestIdxSize   int64
	sourceIdxOffset int64
	sourceIdxSize   int64

	numFrames int32 // seekable zstd frame count (cloud chunks only)

	// rawBytes is the uncompressed record-data size (sum of frame lengths)
	// captured at sealToGLCB time. Distinct from logicalDataBytes which on
	// the legacy multi-file path summed raw + attr + idx — different meaning.
	// Used by uploadToCloud to populate cloud BlobMeta.RawBytes without
	// re-parsing the seekable record body. See gastrolog-24m1t step 7h.
	rawBytes int64

	// blobDigest is the GLCB whole-blob hash from the TOC footer:
	// sha256(header ‖ section_hashes_in_TOC_order ‖ TOC_bytes). Captured
	// at sealToGLCB time so AnnounceUpload can publish it onto the FSM
	// entry without re-reading the local file. Verified on every cache
	// re-fetch (gastrolog-grnc3).
	blobDigest [32]byte
}

func (m *chunkMeta) toChunkMeta() chunk.ChunkMeta {
	return chunk.ChunkMeta{
		ID:          m.id,
		WriteStart:  m.writeStart,
		WriteEnd:    m.writeEnd,
		RecordCount: m.recordCount,
		Bytes:       m.bytes,
		Sealed:      m.sealed,
		DiskBytes:   m.diskBytes,
		IngestStart:       m.ingestStart,
		IngestEnd:         m.ingestEnd,
		SourceStart:       m.sourceStart,
		SourceEnd:         m.sourceEnd,
		IngestTSMonotonic: m.ingestTSMonotonic,
		CloudBacked:       m.cloudBacked,
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
	rawOffset   uint64                     // Current write position in raw.log (after header)
	attrOffset  uint64                     // Current write position in attr.log (after header)
	recordCount uint64                     // Number of records written
	createdAt   time.Time                  // Wall-clock time when chunk was opened
	writeMu     sync.Mutex                 // serializes Phase 2 writes to preserve idx ordering on crash
	writeCond   *sync.Cond                 // orders Phase 2 writes by reserved record index
	nextWrite   uint64                     // next reserved record index allowed to write
	inflight    sync.WaitGroup             // tracks in-flight Phase 2 writers for safe sealing
}

func initWriteOrder(state *chunkState, nextWrite uint64) {
	state.nextWrite = nextWrite
	state.writeCond = sync.NewCond(&state.writeMu)
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
		chunkLocks:     make(map[chunk.ChunkID]*sync.RWMutex),
		lastAccess:     make(map[chunk.ChunkID]time.Time),
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

	if cfg.ExpectExisting && !dirExisted {
		logger.Warn("vault directory was missing and has been recreated empty — if this vault previously held data, it may have been lost",
			"dir", cfg.Dir)
	}

	return manager, nil
}

func (m *Manager) Append(record chunk.Record) (chunk.ChunkID, uint64, error) {
	// ── Phase 1: lock → optional rotate → encode, reserve space ──
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

	// Defense-in-depth (gastrolog-uccg6): refuse to append to an
	// active chunk whose meta is marked sealed. EnsureSealed clears
	// m.active when force-demoting, so this branch should be
	// unreachable in steady state — but if any future path leaves
	// m.active pointing at a sealed chunk (e.g. a partial demote, a
	// race with a concurrent SetSealed call), the rejection is the
	// last line of defense before records silently land on a frozen
	// chunk and never replicate.
	if m.active != nil && m.active.meta.sealed {
		m.mu.Unlock()
		return chunk.ChunkID{}, 0, chunk.ErrChunkSealed
	}

	// Decide rotation before encoding into the current chunk dict.
	// Encoding first can mutate the old chunk's in-memory dict, then rotation
	// seals that chunk without ever persisting those new entries.
	if trigger := m.cfg.RotationPolicy.ShouldRotate(m.activeChunkState(), record); trigger != nil {
		m.logger.Debug("rotating chunk",
			"trigger", *trigger,
			"chunk", m.active.meta.id.String(),
			"bytes", m.active.meta.bytes,
			"records", m.active.recordCount,
			"age", m.cfg.Now().Sub(m.active.createdAt),
		)
		if err := m.sealLocked(); err != nil {
			m.mu.Unlock()
			return chunk.ChunkID{}, 0, err
		}
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
		RawSize:    uint32(len(record.Raw)),     //nolint:gosec // G115: individual record size bounded by protocol
		AttrOffset: uint32(m.active.attrOffset), //nolint:gosec // G115: offsets bounded by chunk rotation policy
		AttrSize:   uint16(len(attrBytes)),      //nolint:gosec // G115: attribute size bounded by protocol
		IngestSeq:  record.EventID.IngestSeq,
		IngesterID: record.EventID.IngesterID,
		NodeID:     record.EventID.NodeID,
	}, idxBuf[:])

	// Snapshot file handles and compute WriteAt positions.
	active := m.active
	rawPos := int64(format.HeaderSize) + int64(m.active.rawOffset)                   //nolint:gosec // G115: bounded
	attrPos := int64(format.HeaderSize) + int64(m.active.attrOffset)                 //nolint:gosec // G115: bounded
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
	for recordIndex != active.nextWrite {
		active.writeCond.Wait()
	}
	defer func() {
		active.nextWrite++
		active.writeCond.Broadcast()
		active.writeMu.Unlock()
	}()

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
	m.active.meta.recordCount = int64(m.active.recordCount)                                          //nolint:gosec // G115: record count bounded by rotation policy
	dataBytes := int64(m.active.rawOffset + m.active.attrOffset + m.active.recordCount*IdxEntrySize) //nolint:gosec // G115: data bytes bounded by rotation policy
	m.active.meta.logicalDataBytes = dataBytes
	m.active.meta.bytes = dataBytes

	if m.active.meta.writeStart.IsZero() {
		m.active.meta.writeStart = record.WriteTS
	}
	m.active.meta.writeEnd = record.WriteTS

	// Track IngestTS monotonicity. The first Append seeds the flag (true)
	// and the running max equals IngestTS. Each subsequent Append flips
	// the flag to false if the record's IngestTS predates the current max
	// (i.e. records are no longer in IngestTS-monotonic order). The flag
	// only flips one direction (true → false). See gastrolog-66b7x.
	if m.active.meta.ingestStart.IsZero() {
		m.active.meta.ingestTSMonotonic = true
	} else if m.active.meta.ingestTSMonotonic && record.IngestTS.Before(m.active.meta.ingestEnd) {
		m.active.meta.ingestTSMonotonic = false
	}
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
		WriteStart:  m.active.meta.writeStart,
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
	defer m.mu.Unlock()
	meta := m.lookupMeta(id)
	if meta == nil {
		return chunk.ChunkMeta{}, chunk.ErrChunkNotFound
	}
	// Snapshot under the lock — toChunkMeta reads fields that
	// CompressChunk/uploadToCloud mutate while holding m.mu.
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
	m.cloudIdxMu.Lock()
	meta, _ := m.cloudIdx.Lookup(id)
	m.cloudIdxMu.Unlock()
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
	// Snapshot the meta flags we need under the lock — reading cloudBacked
	// or sealed after unlocking races with CompressChunk/uploadToCloud which
	// mutate those fields while holding m.mu.
	m.mu.Lock()
	meta := m.lookupMeta(id)
	if meta == nil {
		m.mu.Unlock()
		return nil, chunk.ErrChunkNotFound
	}
	cloudBacked := meta.cloudBacked
	archived := meta.archived
	sealed := meta.sealed
	m.mu.Unlock()

	if cloudBacked {
		// Local data.glcb is the warm cache for cloud-backed chunks: when
		// the leader uploads or any node downloads, the same file lives at
		// <chunkDir>/data.glcb and the read goes through the sealed-local
		// path with no S3 round-trip. Falls back to range-request reads
		// when the local copy was evicted under disk pressure or never
		// existed (follower that hasn't seen this chunk yet). See
		// gastrolog-24m1t step 7j.
		//
		// Archived chunks deliberately bypass the local cache: archive
		// semantics require an explicit Restore before reads, and serving
		// from a stale local copy would let queries silently succeed on
		// data the operator has nominally moved offline. The cloud cursor
		// surfaces ErrChunkArchived from the backing store.
		if !archived && m.hasLocalGLCB(id) {
			if cursor, err := m.openLocalGLCBCursor(id); err == nil {
				m.touchLastAccess(id)
				return cursor, nil
			}
		}
		// Cloud-backed cursors don't touch local mmap regions; the
		// per-chunk lifetime lock is unnecessary for them. Their lifecycle
		// is handled by the cloud index's own concurrency primitives.
		// downloadCloudBlobToChunkDir populates the warm cache as a
		// side-effect; touch lastAccess once the cursor returns so the
		// freshly-cached chunk doesn't immediately register as cold.
		cursor, err := m.openCloudCursor(id)
		if err == nil && m.hasLocalGLCB(id) {
			m.touchLastAccess(id)
		}
		return cursor, err
	}

	// Prefer data.glcb when present. After PostSealProcess (post-seal
	// pipeline stage 7c stage 2a), every sealed chunk has a data.glcb
	// alongside the multi-file artifacts; the GLCB cursor reads through
	// chunkcloud's seekable-zstd path with no per-chunk mmap lock needed
	// (the file is immutable post-rename). Multi-file remains the
	// fallback until step 7c stage 3 deletes that path. See
	// gastrolog-24m1t.
	if sealed && m.hasLocalGLCB(id) {
		if cursor, err := m.openLocalGLCBCursor(id); err == nil {
			return cursor, nil
		}
		// Corrupt or partial data.glcb — fall through to multi-file.
	}

	// Acquire the per-chunk read lock BEFORE opening files. CompressChunk
	// and deleteInternal hold this as a write lock around their atomic
	// rename / os.RemoveAll, so an in-flight cursor's mmap regions can't
	// be invalidated under it. The lock release is wired into the
	// cursor's Close so it tracks the cursor's actual lifetime — the
	// indexer Build pass (gastrolog-26zu1's SIGBUS site) holds its
	// cursor across multiple Next() calls and per-record CPU work, so
	// release-on-Close is the right hook. See gastrolog-26zu1.
	chunkLock := m.chunkLockFor(id)
	chunkLock.RLock()

	// Re-check meta under the read lock — a delete that started before
	// our RLock could have completed during construction. Same for a
	// cloud-upload that flipped cloudBacked false→true between our
	// initial snapshot and the RLock acquisition: if cloudBacked is
	// now true the local files are gone and we MUST route to the
	// cloud cursor instead of constructing an mmap cursor on
	// already-deleted files (gastrolog-2owzp). ErrChunkNotFound is
	// the outcome whether the meta vanished pre- or mid-OpenCursor.
	m.mu.Lock()
	meta = m.lookupMeta(id)
	cloudBackedNow := meta != nil && meta.cloudBacked
	m.mu.Unlock()
	if meta == nil {
		chunkLock.RUnlock()
		return nil, chunk.ErrChunkNotFound
	}
	if cloudBackedNow {
		chunkLock.RUnlock()
		return m.openCloudCursor(id)
	}

	rawPath := m.rawLogPath(id)
	idxPath := m.idxLogPath(id)
	attrPath := m.attrLogPath(id)
	dictPath := m.dictLogPath(id)

	if sealed {
		cursor, err := newMmapCursor(id, rawPath, idxPath, attrPath, dictPath)
		if err != nil {
			chunkLock.RUnlock()
			return nil, err
		}
		cursor.onClose = chunkLock.RUnlock
		return cursor, nil
	}

	cursor, err := newStdioCursor(id, rawPath, idxPath, attrPath, dictPath)
	if err != nil {
		chunkLock.RUnlock()
		return nil, err
	}
	cursor.onClose = chunkLock.RUnlock
	return cursor, nil
}

// chunkLockFor returns the per-chunk RWMutex used to guard the chunk's
// file lifetime against concurrent mutation. Created on first access;
// removed in deleteInternal after the chunk's files are gone. See the
// chunkLocks doc on the Manager struct.
func (m *Manager) chunkLockFor(id chunk.ChunkID) *sync.RWMutex {
	m.chunkLocksMu.Lock()
	defer m.chunkLocksMu.Unlock()
	if l, ok := m.chunkLocks[id]; ok {
		return l
	}
	l := &sync.RWMutex{}
	m.chunkLocks[id] = l
	return l
}

// ScanAttrs iterates all records in a chunk reading only idx.log + attr.log,
// skipping raw.log entirely. This enables O(~88 bytes/record) scans for
// aggregation queries that never inspect message bodies.
//
// Holds the per-chunk read lock for the scan duration so concurrent
// CompressChunk / deleteInternal can't invalidate the mmap regions
// scanAttrsSealed relies on. Same coordination as OpenCursor; see
// gastrolog-26zu1.
func (m *Manager) ScanAttrs(id chunk.ChunkID, startPos uint64, fn func(writeTS time.Time, attrs chunk.Attributes) bool) error {
	// Snapshot meta flags under the lock — reading them after unlocking
	// races with CompressChunk/uploadToCloud which mutate them.
	m.mu.Lock()
	meta := m.lookupMeta(id)
	if meta == nil {
		m.mu.Unlock()
		return chunk.ErrChunkNotFound
	}
	cloudBacked := meta.cloudBacked
	sealed := meta.sealed
	m.mu.Unlock()

	// Cloud-backed chunks: ScanAttrs is "best-effort, no-fetch". Today the
	// only callers are the histogram level-breakdown samplers (timechart
	// per-bucket and non-monotonic chunk-level), and the bucket renders as
	// a hatched "cloud data, breakdown not loaded" ghost when the breakdown
	// is missing — so a network fetch here would burn S3 bandwidth and
	// seconds of wall time to compute a result the user never sees.
	//
	// If the warm cache holds the blob (data.glcb on disk), route through
	// the local GLCB cursor. If not, return nil with zero records scanned;
	// the caller treats sampled==0 as "no breakdown available" and the
	// bucket stays hatched. Callers that genuinely need bytes regardless
	// of cache state should use OpenCursor (which still permits the
	// download path) — not ScanAttrs.
	if cloudBacked {
		if m.hasLocalGLCB(id) {
			return scanAttrsViaGLCB(m, id, startPos, fn)
		}
		return nil
	}

	// Sealed chunks with data.glcb: route through the GLCB cursor so the
	// per-chunk artifact is the source of truth (gastrolog-24m1t step 7c
	// stage 3). Multi-file fallback below stays in place until step 7d
	// retires the multi-file generation entirely.
	if sealed && m.hasLocalGLCB(id) {
		return scanAttrsViaGLCB(m, id, startPos, fn)
	}

	chunkLock := m.chunkLockFor(id)
	chunkLock.RLock()
	defer chunkLock.RUnlock()

	// Re-check meta under the read lock — a delete that started before
	// our RLock could have completed during the snapshot above.
	m.mu.Lock()
	meta = m.lookupMeta(id)
	m.mu.Unlock()
	if meta == nil {
		return chunk.ErrChunkNotFound
	}

	idxPath := m.idxLogPath(id)
	attrPath := m.attrLogPath(id)
	dictPath := m.dictLogPath(id)

	if sealed {
		return scanAttrsSealed(idxPath, attrPath, dictPath, startPos, fn)
	}

	// Active chunk: load dict from disk (not the live in-memory dict)
	// to avoid racing with concurrent Append calls.
	return scanAttrsActive(idxPath, attrPath, dictPath, startPos, fn)
}

// scanAttrsViaGLCB iterates a sealed chunk's records via its data.glcb,
// projecting each one to (writeTS, attrs) for the caller. Used by the
// histogram's level-breakdown path; the chunkcloud cursor decodes full
// records, but per-record CPU is bounded by the seekable-zstd frame
// granularity (256 KB).
func scanAttrsViaGLCB(m *Manager, id chunk.ChunkID, startPos uint64, fn func(writeTS time.Time, attrs chunk.Attributes) bool) error {
	cursor, err := m.openLocalGLCBCursor(id)
	if err != nil {
		return fmt.Errorf("open data.glcb cursor for %s: %w", id, err)
	}
	defer func() { _ = cursor.Close() }()

	if startPos > 0 {
		if err := cursor.Seek(chunk.RecordRef{ChunkID: id, Pos: startPos}); err != nil {
			return fmt.Errorf("seek to %d: %w", startPos, err)
		}
	}
	for {
		rec, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			return nil
		}
		if err != nil {
			return err
		}
		if !fn(rec.WriteTS, rec.Attrs) {
			return nil
		}
	}
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
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("load chunk meta for %s: %w", id, err)
			}
			if recovered := m.recoverChunkWithoutIdxLog(id, entry.Name()); recovered != nil {
				m.metas[id] = recovered
			}
			continue
		}
		m.metas[id] = meta

		if !meta.sealed {
			unsealedIDs = append(unsealedIDs, id)
		}
	}

	// gastrolog-51gme step 8 deleted the "multiple unsealed → seal all
	// but newest" startup heuristic that lived here. Sealed-state
	// projection is now FSM-driven via TierLifecycleReconciler.onSeal +
	// ReconcileFromSnapshot, which call chunk.SealEnsurer.EnsureSealed
	// on this Manager once the tier sub-FSM has loaded. Until that
	// projection runs, multiple unsealed chunks are tolerated; the
	// newest is opened as active and the older ones stay as
	// unsealed-on-disk until the FSM tells us they were sealed.
	if len(unsealedIDs) > 1 {
		slices.SortFunc(unsealedIDs, func(a, b chunk.ChunkID) int {
			return cmp.Compare(a.String(), b.String())
		})
		// Open only the newest as active candidate; the others wait
		// for FSM projection (or for the next post-FSM-restore
		// rotation to seal them via EnsureSealed).
		unsealedIDs = unsealedIDs[len(unsealedIDs)-1:]
	}

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

// EnsureSealed projects the FSM's sealed state onto local chunk files.
// Implements chunk.SealEnsurer. Called from TierLifecycleReconciler's
// onSeal callback when CmdSealChunk applies (steady state) and from
// ReconcileFromSnapshot's projectAllSealedFromFSM walk (recovery
// catchup after Raft snapshot install).
//
// Behavior:
//   - chunk not local        → no-op (this node never had it)
//   - chunk already sealed   → no-op
//   - chunk is local active  → force-demote: close files, remove B+
//     trees, mark sealed=true, clear m.active. Logged at Debug because
//     it fires on every normal seal-rotation on followers (the FSM
//     apply via Raft consistently lands before the leader's
//     record-stream's swap of the follower's active pointer); the
//     event is not operationally interesting on the hot path.
//   - chunk is unsealed local (not active) → set sealed flag in file
//     headers and mark m.metas[id].sealed = true.
//
// Does NOT fire AnnounceSeal (the FSM already has CmdSealChunk
// applied; a second announce would duplicate the Raft entry).
//
// Force-demote-always rationale: a prior design (gastrolog-uccg6-followup)
// split this into "steady-state skip-active" + "recovery force-demote"
// on the theory that the leader's record-stream would swap the
// follower's active pointer in steady state. That assumption is
// topology-dependent — true for ingest tiers fed by continuous appends,
// false for downstream tiers fed only by transitions. The split left
// receipt-protocol delete obligations bouncing off ErrActiveChunk
// forever on transition-fed tiers (gastrolog-2yeht). Always-force-demote
// is the correct invariant for every topology: FSM is authoritative,
// local active must yield. See gastrolog-51gme step 8 / gastrolog-uccg6 /
// gastrolog-2yeht.
func (m *Manager) EnsureSealed(id chunk.ChunkID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrManagerClosed
	}
	meta, ok := m.metas[id]
	if !ok {
		return nil
	}
	if meta.sealed {
		return nil
	}
	if m.active != nil && m.active.meta.id == id {
		// FSM says sealed; local active is the same chunk. Force-demote:
		// close files, mark sealed=true, clear m.active. Subsequent
		// appends will rotate to a fresh active chunk; subsequent delete
		// obligations from the receipt protocol succeed instead of
		// bouncing off ErrActiveChunk.
		m.logger.Debug("EnsureSealed: force-demoting local active to sealed (FSM authoritative)",
			"chunk", id.String())
		return m.sealActiveLocked(false)
	}
	if err := m.sealChunkOnDisk(id); err != nil {
		return err
	}
	meta.sealed = true
	return nil
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
	initWriteOrder(m.active, recordCount)

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

// loadChunkMetaFromGLCB reads a sealed chunk's metadata directly from its
// data.glcb file via the chunkcloud reader. Used at startup for chunks
// that have only data.glcb in the directory (no multi-file artifacts) —
// once stage 3b retires multi-file generation, this becomes the primary
// load path. See gastrolog-24m1t.
//
// Capability-only: not yet wired into loadExisting. The current load
// path still goes through loadChunkMeta (idx.log-based).
func (m *Manager) loadChunkMetaFromGLCB(id chunk.ChunkID) (*chunkMeta, error) {
	path := filepath.Join(m.chunkDir(id), dataGLCBFileName)
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	rd, err := chunkcloud.NewCacheReader(f)
	if err != nil {
		return nil, fmt.Errorf("open data.glcb for %s: %w", id, err)
	}
	defer func() { _ = rd.Close() }()
	bm := rd.Meta()

	return &chunkMeta{
		id:               id,
		recordCount:      int64(bm.RecordCount),
		bytes:            bm.RawBytes,
		sealed:           true,
		writeStart:       bm.WriteStart,
		writeEnd:         bm.WriteEnd,
		ingestStart:      bm.IngestStart,
		ingestEnd:        bm.IngestEnd,
		sourceStart:      bm.SourceStart,
		sourceEnd:        bm.SourceEnd,
		ingestIdxOffset:  bm.IngestIdxOffset,
		ingestIdxSize:    bm.IngestIdxSize,
		sourceIdxOffset:  bm.SourceIdxOffset,
		sourceIdxSize:    bm.SourceIdxSize,
		logicalDataBytes: bm.RawBytes,
	}, nil
}

// recoverChunkWithoutIdxLog handles a chunk directory whose idx.log is
// missing — three cases:
//
//   - data.glcb is present: stage-3b layout (sealed chunk lives as a
//     single GLCB blob); loadChunkMetaFromGLCB reconstructs the meta.
//   - The directory has other files (e.g. an index sidecar): cloud-backed
//     chunk where the cloud blob is the source of truth and only the
//     locally-built TS index files survived. Skip — the cloud-index
//     wiring will re-attach.
//   - The directory is empty: leftover from an aborted seal or a delete
//     race. Remove it.
//
// Returns the loaded chunkMeta (case 1) or nil (cases 2 and 3).
func (m *Manager) recoverChunkWithoutIdxLog(id chunk.ChunkID, entryName string) *chunkMeta {
	if glcbMeta, err := m.loadChunkMetaFromGLCB(id); err == nil {
		return glcbMeta
	}
	dir := filepath.Join(m.cfg.Dir, entryName)
	if chunkDirHasFiles(dir) {
		m.logger.Debug("skipping cloud-backed chunk index directory", "chunk", id)
		return nil
	}
	m.logger.Info("removing empty leftover chunk directory", "chunk", id)
	_ = os.RemoveAll(dir)
	return nil
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
	// IngestTS and SourceTS bounds cannot be derived from first+last
	// physical records on chunks built via ImportRecords (or any chunk
	// where physical write order doesn't match Ingest/Source TS order).
	// Such chunks are produced by tier transitions: streamLocal /
	// StreamAppendToTier preserves each record's IngestTS/SourceTS but
	// appends in source-WriteTS order, so the physical first/last
	// records are not the IngestTS extrema. Scanning all idx entries is
	// O(records) but the idx file is small (12 bytes/entry) and only
	// loaded once per chunk on manager startup. See gastrolog-66b7x.
	if err := scanTSBounds(idxFile, recordCount, meta); err != nil {
		return nil, fmt.Errorf("scan TS bounds for chunk %s: %w", id, err)
	}

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

// scanTSBounds reads every idx entry and records the min/max IngestTS,
// min/max SourceTS, and IngestTS monotonicity into meta. Required for chunks
// built via ImportRecords / streamed Append, where physical record order does
// not match Ingest/Source TS order (so first+last physical entries are not
// the TS extrema). The monotonicity flag is also derived here — true if and
// only if every entry's IngestTS is >= its predecessor in physical order.
// See gastrolog-66b7x.
func scanTSBounds(idxFile *os.File, recordCount uint64, meta *chunkMeta) error {
	if recordCount == 0 {
		return nil
	}
	if _, err := idxFile.Seek(IdxHeaderSize, io.SeekStart); err != nil {
		return fmt.Errorf("seek idx start: %w", err)
	}
	var entryBuf [IdxEntrySize]byte
	var minIngest, maxIngest, minSource, maxSource time.Time
	monotonic := true
	var prevIngest time.Time
	for i := range recordCount {
		if _, err := io.ReadFull(idxFile, entryBuf[:]); err != nil {
			return fmt.Errorf("read idx entry %d: %w", i, err)
		}
		e := DecodeIdxEntry(entryBuf[:])
		if i == 0 || e.IngestTS.Before(minIngest) {
			minIngest = e.IngestTS
		}
		if i == 0 || e.IngestTS.After(maxIngest) {
			maxIngest = e.IngestTS
		}
		if i > 0 && e.IngestTS.Before(prevIngest) {
			monotonic = false
		}
		prevIngest = e.IngestTS
		if !e.SourceTS.IsZero() {
			if minSource.IsZero() || e.SourceTS.Before(minSource) {
				minSource = e.SourceTS
			}
			if maxSource.IsZero() || e.SourceTS.After(maxSource) {
				maxSource = e.SourceTS
			}
		}
	}
	meta.ingestStart = minIngest
	meta.ingestEnd = maxIngest
	meta.sourceStart = minSource
	meta.sourceEnd = maxSource
	meta.ingestTSMonotonic = monotonic
	return nil
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
	initWriteOrder(m.active, 0)
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
	file, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR|os.O_TRUNC, m.cfg.FileMode)
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
	file, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR|os.O_TRUNC, m.cfg.FileMode)
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
	file, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR|os.O_TRUNC, m.cfg.FileMode)
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
	file, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR|os.O_TRUNC, m.cfg.FileMode)
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
	return m.sealActiveLocked(true)
}

// sealActiveLocked seals the current active chunk, closes its files,
// removes its B+ tree files, marks meta.sealed = true, and clears
// m.active. When announce is true, AnnounceSeal is queued for the
// caller to fire post-unlock — the rotation path uses this so the
// rest of the cluster learns about the seal via vault-ctl Raft.
//
// When announce is false, the announcer is NOT called: this path is
// for FSM-driven projection (gastrolog-uccg6 / gastrolog-51gme step 8),
// where the chunk is already sealed in the FSM and proposing
// CmdSealChunk again would duplicate the Raft entry. EnsureSealed
// uses this when projecting an FSM-sealed chunk that happens to be
// the local active pointer — the offline-during-seal restart case
// from the original incident.
//
// Caller must hold m.mu.
func (m *Manager) sealActiveLocked(announce bool) error {
	if m.active == nil {
		return nil
	}

	// Wait for any in-flight Phase 2 (WriteAt) writers to finish before
	// modifying headers or closing files. Safe to block here: Phase 2 does
	// not hold the mutex, and no new Phase 1 can start while we hold it.
	m.active.inflight.Wait()

	id := m.active.meta.id
	m.active.meta.sealed = true

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

	if announce && m.cfg.Announcer != nil {
		ann := m.cfg.Announcer
		// Capture the metadata snapshot now (it's mutable; m.active will
		// be cleared below). The closure runs after the caller releases mu.
		writeEnd := meta.writeEnd
		recordCount := meta.recordCount
		bytes := meta.bytes
		ingestStart := meta.ingestStart
		ingestEnd := meta.ingestEnd
		sourceEnd := meta.sourceEnd
		ingestTSMonotonic := meta.ingestTSMonotonic
		m.pendingAnnouncements = append(m.pendingAnnouncements, func() {
			ann.AnnounceSeal(id, writeEnd, recordCount, bytes, ingestStart, ingestEnd, sourceEnd, ingestTSMonotonic)
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
	m.logger.Debug("chunk-lifecycle: import dir created", "chunk", id.String(), "dir", dir)

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
	// Match append path: stamp local write time when the iterator did not
	// supply one (e.g. ad-hoc imports). Tier replication and other paths
	// set WriteTS from the source chunk — preserve it for leader/follower parity.
	if rec.WriteTS.IsZero() {
		rec.WriteTS = s.now()
	}

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
		RawOffset:  uint32(s.rawOffset),    //nolint:gosec // G115: bounded by rotation policy
		RawSize:    uint32(len(rec.Raw)),   //nolint:gosec // G115: bounded by chunk size
		AttrOffset: uint32(s.attrOffset),   //nolint:gosec // G115: bounded by rotation policy
		AttrSize:   uint16(len(attrBytes)), //nolint:gosec // G115: bounded by attr encoding
		// EventID fields preserve cluster-wide record identity through
		// ImportRecords (cross-node sealed-chunk replication, MoveChunk,
		// catchup paths). Without these, follower-replicated chunks land
		// with zero EventIDs and histogram dedup can't match them
		// against leader-original records → silent double-count. See
		// gastrolog-5qwkw.
		IngestSeq:  rec.EventID.IngestSeq,
		IngesterID: rec.EventID.IngesterID,
		NodeID:     rec.EventID.NodeID,
	}, idxBuf[:])

	rawPos := int64(format.HeaderSize) + int64(s.rawOffset)   //nolint:gosec // G115: bounded by rotation policy
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
	if s.meta.ingestStart.IsZero() {
		s.meta.ingestTSMonotonic = true
	} else if s.meta.ingestTSMonotonic && rec.IngestTS.Before(s.meta.ingestEnd) {
		s.meta.ingestTSMonotonic = false
	}
	expandBounds(&s.meta.ingestStart, &s.meta.ingestEnd, rec.IngestTS)
	if !rec.SourceTS.IsZero() {
		expandBounds(&s.meta.sourceStart, &s.meta.sourceEnd, rec.SourceTS)
	}
	return nil
}

// ImportRecords creates a new sealed chunk with the given ID by consuming
// records from the iterator. A non-zero WriteTS on each record is preserved
// (e.g. tier replication from the leader); if WriteTS is zero, the current
// time from Now is used. Records are written to a new chunk directory
// separate from the active chunk; concurrent Append calls are not affected.
//
// If id is the zero ChunkID, a new ID is generated. Passing the ID directly
// rather than via SetNextChunkID avoids a race where a concurrent Append
// (via openLocked) could consume the pending ID and leave the import to
// allocate a fresh, untracked one — see gastrolog-11rzz.
func (m *Manager) ImportRecords(id chunk.ChunkID, next chunk.RecordIterator) (chunk.ChunkMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return chunk.ChunkMeta{}, ErrManagerClosed
	}

	if id == (chunk.ChunkID{}) {
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
			m.logger.Debug("chunk-lifecycle: import cleanup (iter err)",
				"chunk", id.String(), "error", iterErr)
			return chunk.ChunkMeta{}, iterErr
		}
		if err := s.writeRecord(rec); err != nil {
			files.cleanup()
			m.logger.Debug("chunk-lifecycle: import cleanup (write err)",
				"chunk", id.String(), "error", err)
			return chunk.ChunkMeta{}, err
		}
	}

	if s.count == 0 {
		files.cleanup()
		m.logger.Debug("chunk-lifecycle: import cleanup (zero records)", "chunk", id.String())
		return chunk.ChunkMeta{}, nil
	}

	s.meta.recordCount = s.count
	dataBytes := int64(s.rawOffset + s.attrOffset + uint64(s.count)*IdxEntrySize) //nolint:gosec // G115: count is always non-negative
	s.meta.logicalDataBytes = dataBytes
	// Seal the files.
	for _, f := range []*os.File{files.raw, files.idx, files.attr, files.dict} {
		if err := m.setSealedFlag(f); err != nil {
			files.cleanup()
			m.logger.Debug("chunk-lifecycle: import cleanup (seal flag err)",
				"chunk", id.String(), "error", err)
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
	m.logger.Debug("chunk-lifecycle: import registered in metas",
		"chunk", id.String(), "records", s.count)
	return s.meta.toChunkMeta(), nil
}

// Close closes the active chunk files without sealing.
// The manager should not be used after Close is called.
// trackCloudResult updates the degraded flag based on a cloud operation result.
// Every cloud operation (upload, download, list, archive, restore) should call
// this after completion. Failed → set degraded + store error. Succeeded → clear.
//
// blobstore.ErrBlobNotFound is treated as a logical (non-degraded) outcome:
// the cloud store IS reachable, the blob just isn't there. This happens
// routinely when an FSM-listed chunk's data was never uploaded (or was
// already deleted) and we hit it via search fan-out. Without this carve-out
// every such miss would raise the cloud-unreachable alert (gastrolog-3ukgz).
func (m *Manager) trackCloudResult(err error) {
	if err != nil && !errors.Is(err, blobstore.ErrBlobNotFound) {
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

// ScanActiveIngestTS iterates the active chunk's IngestTS B+ tree, calling
// cb for each entry's IngestTS in IngestTS-sorted order. No attr or raw
// reads. Returns ErrChunkNotFound if id is not the current active chunk.
// See gastrolog-66b7x.
func (m *Manager) ScanActiveIngestTS(id chunk.ChunkID, cb func(tsNanos int64) bool) error {
	m.mu.Lock()
	active := m.active
	m.mu.Unlock()
	if active == nil || active.meta.id != id {
		return chunk.ErrChunkNotFound
	}
	it, err := active.ingestBT.Scan()
	if err != nil {
		return fmt.Errorf("btree scan: %w", err)
	}
	for it.Valid() {
		if !cb(it.Key()) {
			return nil
		}
		it.Next()
	}
	return nil
}

// ScanActiveByIngestTS iterates the active chunk's records in physical order,
// exposing IngestTS + Attributes per record. Single pass over idx + attr; the
// dict is loaded once at the start. Returns ErrChunkNotFound if id is not the
// current active chunk. See gastrolog-66b7x.
func (m *Manager) ScanActiveByIngestTS(id chunk.ChunkID, cb func(ingestTS time.Time, attrs chunk.Attributes) bool) error {
	m.mu.Lock()
	active := m.active
	m.mu.Unlock()
	if active == nil || active.meta.id != id {
		return chunk.ErrChunkNotFound
	}
	idxPath := m.idxLogPath(id)
	attrPath := m.attrLogPath(id)
	dictPath := m.dictLogPath(id)
	return scanIngestAttrsActive(idxPath, attrPath, dictPath, cb)
}

// FindIngestStartPosition returns the earliest record position with IngestTS >= ts
// for the active chunk. Returns (0, false, nil) for sealed chunks (cloud or
// local) — the index manager owns those via OpenIngestMmap, which reads the
// embedded ITSI section out of data.glcb regardless of cloud-backed status
// (the warm cache makes both look identical). See gastrolog-1dg3i.
func (m *Manager) FindIngestStartPosition(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	m.mu.Lock()
	active := m.active
	m.mu.Unlock()

	if active == nil || active.meta.id != id {
		return 0, false, nil
	}
	it, err := active.ingestBT.FindGE(ts.UnixNano())
	if err != nil {
		return 0, false, fmt.Errorf("btree ingest FindGE: %w", err)
	}
	if !it.Valid() {
		return 0, false, nil
	}
	return uint64(it.Value()), true, nil
}

// FindIngestEntryIndex returns the IngestTS-rank of the first entry with
// IngestTS >= ts. For active chunks the B+ tree returns position; that's
// only equal to rank for monotonic chunks (callers gate on
// meta.IngestTSMonotonic). Returns (0, false, nil) for sealed chunks —
// caller falls through to IndexManager.FindIngestEntryIndex which mmaps
// the ITSI section directly from data.glcb (cloud or local — same path
// post-gastrolog-24m1t step 7j warm cache; see gastrolog-1dg3i).
// See gastrolog-66b7x.
func (m *Manager) FindIngestEntryIndex(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	m.mu.Lock()
	active := m.active
	m.mu.Unlock()

	if active == nil || active.meta.id != id {
		return 0, false, nil
	}
	it, err := active.ingestBT.FindGE(ts.UnixNano())
	if err != nil {
		return 0, false, fmt.Errorf("btree ingest FindGE: %w", err)
	}
	if !it.Valid() {
		return 0, false, nil
	}
	// For monotonic active chunks, B+ tree position == rank, so this
	// is correct. For non-monotonic active chunks, the histogram
	// dispatch in query/histogram.go avoids this path entirely.
	return uint64(it.Value()), true, nil
}

// HasLocalContent reports whether the chunk's content is locally readable
// without triggering an S3 fetch. See ChunkManager.HasLocalContent.
func (m *Manager) HasLocalContent(id chunk.ChunkID) bool {
	m.mu.Lock()
	meta := m.lookupMeta(id)
	m.mu.Unlock()
	if meta == nil {
		return false
	}
	if !meta.cloudBacked {
		return true
	}
	// In-tree warm cache (gastrolog-24m1t step 7j): the local data.glcb is
	// the chunk's authoritative cache for cloud-backed reads.
	return m.hasLocalGLCB(id)
}

// HeadCloudBlob probes the chunk's cloud blob via a HEAD request, ignoring
// any local cache. Used by the reconcile sweep to detect blobs deleted
// out-of-band by a lifecycle rule — OpenCursor would silently serve from
// the warm cache and miss the gap. Returns chunk.ErrChunkSuspect for
// missing blobs (the tracker uses that to start the grace window) and
// nil when the blob is present. Implements chunk.CloudBlobChecker.
func (m *Manager) HeadCloudBlob(id chunk.ChunkID) error {
	if m.cfg.CloudStore == nil {
		return errors.New("cloud store not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), cloudHeadTimeout)
	defer cancel()
	info, err := m.cfg.CloudStore.Head(ctx, m.blobKey(id))
	if err != nil {
		if errors.Is(err, blobstore.ErrBlobNotFound) {
			return chunk.ErrChunkSuspect
		}
		return err
	}
	if info.Size == 0 {
		return chunk.ErrChunkSuspect
	}
	return nil
}

// FindSourceStartPosition returns the earliest record position with SourceTS >= ts.
// Active chunks: B+ tree lookup. Sealed (local or cloud-warm-cached): caller
// falls through to IndexManager.FindSourceStartPosition (which mmaps the
// embedded STSI section out of data.glcb). See gastrolog-1dg3i.
func (m *Manager) FindSourceStartPosition(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	m.mu.Lock()
	active := m.active
	m.mu.Unlock()

	if active == nil || active.meta.id != id {
		return 0, false, nil
	}
	it, err := active.sourceBT.FindGE(ts.UnixNano())
	if err != nil {
		return 0, false, fmt.Errorf("btree source FindGE: %w", err)
	}
	if !it.Valid() {
		return 0, false, nil
	}
	return uint64(it.Value()), true, nil
}

// LoadIngestEntries / LoadSourceEntries / .ts-cache plumbing was retired
// in gastrolog-1dg3i: the histogram and search-side TS-ordered scanners
// now read the embedded ITSI/STSI sections directly from data.glcb via
// filetsidx.OpenIngestMmap / OpenSourceMmap (handled by the IndexManager).
// Cloud chunks reach the same path through their warm-cache data.glcb;
// when the warm cache is cold the histogram falls back to FSM-proportional
// distribution rather than fetching the index section from S3. Removed:
// tsCacheDir / tsCachePath / searchTSCacheFile / downloadTSIndex /
// readTSEntriesFromFile / findCloudTSRank / findCloudTSPosition /
// loadTSEntries.

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
// the metadata announcer. Used by the tier FSM apply path when a delete
// originating from any node propagates via Raft — re-announcing would create
// an infinite feedback loop.
func (m *Manager) DeleteSilent(id chunk.ChunkID) error {
	return m.deleteInternal(id)
}

func (m *Manager) deleteInternal(id chunk.ChunkID) error {
	// Wait for any in-flight PostSealProcess on this chunk to finish
	// BEFORE acquiring the per-chunk write lock. PostSealProcess
	// internally calls CompressChunk (which takes chunkLock) and
	// the index Build pass (which opens cursors with chunkLock.RLock).
	// If we held chunkLock here and then waited on postSealActive,
	// CompressChunk's chunkLock.Lock would block on us, the
	// postSealActive channel would never close, and we'd deadlock —
	// observed on TestClusterTransitionBurstNoOrphans.
	if ch, ok := m.postSealActive.Load(id); ok {
		<-ch.(chan struct{})
	}

	// Per-chunk write lock (gastrolog-26zu1): block until in-flight
	// cursor reads on this chunk drain before unlinking files. Without
	// this, an indexer cursor mid-Next() could SIGBUS when os.RemoveAll
	// invalidates its mmap regions.
	chunkLock := m.chunkLockFor(id)
	chunkLock.Lock()
	defer func() {
		chunkLock.Unlock()
		// Clean up the map entry once no readers can resolve it (the
		// chunk's meta is gone after a successful delete, so future
		// OpenCursors fail at meta-lookup before consulting chunkLocks).
		m.chunkLocksMu.Lock()
		delete(m.chunkLocks, id)
		m.chunkLocksMu.Unlock()
	}()

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
		// Chunk is not tracked in metas, but a stray directory may still
		// exist on disk from a partial import that raced with a tombstone.
		// Remove the directory unconditionally — DeleteSilent's contract is
		// "leave no trace of this chunk." Returning ErrChunkNotFound without
		// checking disk leaves orphan directories that fail the cluster-
		// transition invariant (see gastrolog-11rzz).
		dir := m.chunkDir(id)
		m.mu.Unlock()
		if _, statErr := os.Stat(dir); statErr == nil {
			if rmErr := os.RemoveAll(dir); rmErr != nil {
				m.logger.Warn("chunk-lifecycle: orphan dir remove failed",
					"chunk", id.String(), "dir", dir, "error", rmErr)
				return fmt.Errorf("remove orphan chunk dir %s: %w", id, rmErr)
			}
			m.logger.Debug("chunk-lifecycle: removed orphan dir (not in metas)",
				"chunk", id.String(), "dir", dir)
			return nil
		}
		return chunk.ErrChunkNotFound
	}

	if meta.cloudBacked {
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
		// Remove the in-tree warm cache copy of the cloud blob — the
		// chunk dir is the cache post step 7j, so blowing it away on
		// delete keeps disk usage bounded by retention.
		_ = os.RemoveAll(m.chunkDir(id))
	} else {
		dir := m.chunkDir(id)
		// postSealActive was already drained at the top of deleteInternal
		// before we acquired chunkLock — no need to wait again here.
		if err := os.RemoveAll(dir); err != nil {
			m.mu.Unlock()
			m.logger.Warn("chunk-lifecycle: RemoveAll failed",
				"chunk", id.String(), "dir", dir, "error", err)
			return fmt.Errorf("remove chunk dir %s: %w", id, err)
		}
		m.logger.Debug("chunk-lifecycle: removed tracked chunk dir",
			"chunk", id.String(), "dir", dir)
	}

	delete(m.metas, id)          // no-op for cloud chunks (not in metas)
	delete(m.storageClasses, id) // clean up storage class cache
	m.mu.Unlock()
	return nil
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
	//
	// closed check + postSealWg.Add must happen under the same lock Close()
	// uses to set closed=true, otherwise Close's Wait can return on a zero
	// counter between our unlock and Add and let the pipeline continue
	// against a closed Manager.
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrManagerClosed
	}
	meta, ok := m.metas[id]
	if !ok {
		m.mu.Unlock()
		return chunk.ErrChunkNotFound
	}
	if !meta.sealed {
		m.mu.Unlock()
		return chunk.ErrChunkNotSealed
	}
	m.postSealWg.Add(1)
	m.mu.Unlock()

	done := make(chan struct{})
	m.postSealActive.Store(id, done)
	defer func() {
		close(done)
		m.postSealActive.Delete(id)
		m.postSealWg.Done()
	}()

	// 1. Package the sealed chunk as a single data.glcb blob — the
	// canonical sealed-chunk artifact under gastrolog-24m1t. Failure is
	// fatal; there is no longer a multi-file fallback.
	if _, _, err := m.sealToGLCB(id); err != nil {
		return fmt.Errorf("seal chunk %s to GLCB: %w", id, err)
	}

	// 1a. Attach the freshly-computed GLCB section offsets to the FSM
	// manifest entry so the histogram's GLCB section-reader can find
	// the IngestTS index without reading the blob's TOC. Replicates
	// to every node via Raft like the other manifest mutations. See
	// gastrolog-1dg3i.
	if m.cfg.Announcer != nil {
		m.mu.Lock()
		meta := m.lookupMeta(id)
		var (
			ingestOff, ingestSize int64
			sourceOff, sourceSize int64
			numFrames             int32
		)
		if meta != nil {
			ingestOff = meta.ingestIdxOffset
			ingestSize = meta.ingestIdxSize
			sourceOff = meta.sourceIdxOffset
			sourceSize = meta.sourceIdxSize
			numFrames = meta.numFrames
		}
		m.mu.Unlock()
		if meta != nil {
			m.cfg.Announcer.AnnounceAttachOffsets(id, ingestOff, ingestSize, sourceOff, sourceSize, numFrames)
		}
	}

	// 2. Build indexes. Now reads through OpenCursor → GLCB cursor.
	for _, builder := range m.indexBuilders {
		if err := builder.Build(ctx, id); err != nil {
			if isMissingLocalChunkFileError(err) {
				continue
			}
			m.logger.Warn("index build failed", "chunk", id, "error", err)
		}
	}

	// 3. Remove the multi-file artifacts (raw.log, idx.log, attr.log,
	// attr_dict.log, ingest.bt, source.bt). data.glcb is the only
	// sealed artifact from here on.
	if err := m.removeLocalDataFiles(id); err != nil {
		m.logger.Warn("post-seal multi-file cleanup failed; chunk still readable via data.glcb", "chunk", id, "error", err)
	}

	// 4. Refresh disk sizes after index + GLCB files are written.
	if len(m.indexBuilders) > 0 {
		m.RefreshDiskSizes(id)
	}

	// 5. Upload to cloud and delete local if cloud-backed.
	// CloudReadOnly followers skip upload — they adopt the leader's blob
	// via RegisterCloudChunk when the tier FSM propagates the upload.
	if m.cfg.CloudStore != nil && !m.cfg.CloudReadOnly {
		if err := m.uploadToCloud(id); err != nil {
			m.logger.Warn("cloud upload failed, keeping local", "chunk", id, "error", err)
		}
	}

	return nil
}

func isMissingLocalChunkFileError(err error) bool {
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	msg := err.Error()
	if !strings.Contains(msg, "no such file or directory") {
		return false
	}
	return strings.Contains(msg, "open raw.log") ||
		strings.Contains(msg, "open idx.log") ||
		strings.Contains(msg, "open attr.log") ||
		strings.Contains(msg, "open attr_dict")
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

// openLocalGLCBCursor opens a sealed chunk's data.glcb file via the chunkcloud
// reader pipeline. Returns the underlying os error (typically ENOENT) when
// data.glcb is absent so callers can fall back to a remote read.
func (m *Manager) openLocalGLCBCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	path := filepath.Join(m.chunkDir(id), dataGLCBFileName)
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	rd, err := chunkcloud.NewCacheReader(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return chunkcloud.NewSeekableCursor(rd, id), nil
}

// hasLocalGLCB reports whether the chunk directory contains a data.glcb.
// Used by read-path dispatch to prefer the GLCB cursor when available.
func (m *Manager) hasLocalGLCB(id chunk.ChunkID) bool {
	_, err := os.Stat(filepath.Join(m.chunkDir(id), dataGLCBFileName))
	return err == nil
}

// touchLastAccess records that the warm cache for this chunk was just hit
// (or just populated). The lastAccess map is consulted by EvictCacheLRU to
// pick the coldest entries when the cache exceeds its budget. Map is
// in-memory only — never persisted; eviction signals don't outlive the
// process. See gastrolog-2idw8.
func (m *Manager) touchLastAccess(id chunk.ChunkID) {
	m.lastAccessMu.Lock()
	m.lastAccess[id] = m.cfg.Now()
	m.lastAccessMu.Unlock()
}

// EvictCacheLRU deletes warm-cache copies (<chunkDir>/data.glcb) of
// cloud-backed chunks until the total cache size is at or below
// budgetBytes. Coldest-first: chunks with the oldest lastAccess timestamp
// (or no recorded access since startup) are evicted first. Local-only
// sealed chunks are never touched — their data.glcb is the authoritative
// copy, not a cache.
//
// Returns the number of chunks evicted and the total bytes freed.
// gastrolog-2idw8.
func (m *Manager) EvictCacheLRU(budgetBytes uint64) (int, int64) {
	candidates := m.cacheCandidates()
	if len(candidates) == 0 {
		return 0, 0
	}

	var total int64
	for _, c := range candidates {
		total += c.size
	}
	if total <= int64(budgetBytes) { //nolint:gosec // G115: budget round-trip
		return 0, 0
	}

	// Sort coldest-first so the oldest accesses go first. A zero/missing
	// lastAccess (chunk seen on disk but never opened in this process) ranks
	// as oldest possible — those are the most evictable.
	slices.SortFunc(candidates, func(a, b cacheEntry) int {
		return a.lastAccess.Compare(b.lastAccess)
	})

	var evicted int
	var freed int64
	for _, c := range candidates {
		if total <= int64(budgetBytes) { //nolint:gosec // G115
			break
		}
		if err := os.Remove(c.path); err != nil {
			m.logger.Debug("cache eviction: remove failed", "chunk", c.id, "error", err)
			continue
		}
		m.lastAccessMu.Lock()
		delete(m.lastAccess, c.id)
		m.lastAccessMu.Unlock()
		total -= c.size
		freed += c.size
		evicted++
	}
	if evicted > 0 {
		m.logger.Info("cache eviction (LRU)",
			"evicted", evicted, "freed_bytes", freed, "budget", budgetBytes)
	}
	return evicted, freed
}

// EvictCacheTTL deletes warm-cache copies whose lastAccess is older than
// the given TTL. Local-only sealed chunks are never touched. Use alongside
// EvictCacheLRU when the operator wants both age- and size-based eviction.
// gastrolog-2idw8.
func (m *Manager) EvictCacheTTL(ttl time.Duration) (int, int64) {
	if ttl <= 0 {
		return 0, 0
	}
	candidates := m.cacheCandidates()
	cutoff := m.cfg.Now().Add(-ttl)

	var evicted int
	var freed int64
	for _, c := range candidates {
		if c.lastAccess.After(cutoff) {
			continue
		}
		if err := os.Remove(c.path); err != nil {
			m.logger.Debug("cache eviction: remove failed", "chunk", c.id, "error", err)
			continue
		}
		m.lastAccessMu.Lock()
		delete(m.lastAccess, c.id)
		m.lastAccessMu.Unlock()
		freed += c.size
		evicted++
	}
	if evicted > 0 {
		m.logger.Info("cache eviction (TTL)",
			"evicted", evicted, "freed_bytes", freed, "ttl", ttl)
	}
	return evicted, freed
}

// EvictCache runs whichever eviction policies are configured: TTL first
// (drops anything older than CacheTTL), then LRU (caps total cache bytes
// at CacheBudgetBytes). Safe to call at any cadence — eviction is
// node-local and never persisted, so missing a sweep just means the cache
// runs slightly hotter until the next call. Both zero values are no-ops.
func (m *Manager) EvictCache() (int, int64) {
	var totalEvicted int
	var totalFreed int64
	if m.cfg.CacheTTL > 0 {
		ev, fr := m.EvictCacheTTL(m.cfg.CacheTTL)
		totalEvicted += ev
		totalFreed += fr
	}
	if m.cfg.CacheBudgetBytes > 0 {
		ev, fr := m.EvictCacheLRU(m.cfg.CacheBudgetBytes)
		totalEvicted += ev
		totalFreed += fr
	}
	return totalEvicted, totalFreed
}

// cacheEntry is one row in the eviction-candidate set: a chunk whose
// data.glcb is local-but-not-authoritative (cloud-backed).
type cacheEntry struct {
	id         chunk.ChunkID
	path       string
	size       int64
	lastAccess time.Time
}

// cacheCandidates walks the cloud index and returns one entry per
// cloud-backed chunk whose <chunkDir>/data.glcb still exists locally.
// Local-only sealed chunks are NOT eviction candidates — their data.glcb
// is the authoritative copy and removing it would lose data.
func (m *Manager) cacheCandidates() []cacheEntry {
	if m.cloudIdx == nil {
		return nil
	}
	var out []cacheEntry
	m.cloudIdxMu.Lock()
	_ = m.cloudIdx.ForEach(func(id chunk.ChunkID, _ *chunkMeta) bool {
		path := filepath.Join(m.chunkDir(id), dataGLCBFileName)
		info, err := os.Stat(path)
		if err != nil {
			return true
		}
		m.lastAccessMu.Lock()
		la := m.lastAccess[id]
		m.lastAccessMu.Unlock()
		out = append(out, cacheEntry{
			id:         id,
			path:       path,
			size:       info.Size(),
			lastAccess: la,
		})
		return true
	})
	m.cloudIdxMu.Unlock()
	return out
}

// downloadCloudBlobToChunkDir streams the cloud blob for a chunk into
// <chunkDir>/data.glcb atomically and opens a local cursor. The chunk dir
// becomes the warm cache, structurally identical to a freshly-sealed local
// chunk — subsequent reads go through the local-GLCB fast path. See
// gastrolog-24m1t step 7k.
func (m *Manager) downloadCloudBlobToChunkDir(id chunk.ChunkID) (chunk.RecordCursor, error) {
	dir := m.chunkDir(id)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("ensure chunk dir for cache: %w", err)
	}
	tmp, err := os.CreateTemp(dir, dataGLCBFileName+".tmp.*")
	if err != nil {
		return nil, fmt.Errorf("create tmp for cache: %w", err)
	}
	tmpPath := filepath.Clean(tmp.Name())

	rc, err := m.cfg.CloudStore.Download(context.Background(), m.blobKey(id))
	m.trackCloudResult(err)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath) //nolint:gosec // G703: tmpPath from CreateTemp in chunkDir
		return nil, err
	}
	defer func() { _ = rc.Close() }()

	if _, err := io.Copy(tmp, rc); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath) //nolint:gosec // G703
		return nil, err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath) //nolint:gosec // G703
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath) //nolint:gosec // G703
		return nil, err
	}

	// Integrity check (gastrolog-grnc3): before promoting tmp → final,
	// read the TOC footer and verify the recorded BlobDigest matches what
	// the FSM stamped at upload time. A mismatch means the bytes we just
	// pulled are not the bytes the leader uploaded — corruption in flight,
	// a clobbered S3 object, or a retention race. Reject so we don't seed
	// the warm cache with bad bytes that would be re-served forever.
	if err := m.verifyDownloadedBlob(id, tmpPath); err != nil {
		_ = os.Remove(tmpPath) //nolint:gosec // G703
		return nil, err
	}

	finalPath := filepath.Join(dir, dataGLCBFileName)
	if err := os.Rename(tmpPath, finalPath); err != nil { //nolint:gosec // G304: tmpPath is from os.CreateTemp inside chunkDir
		_ = os.Remove(tmpPath) //nolint:gosec // G703
		return nil, err
	}

	return m.openLocalGLCBCursor(id)
}

// verifyDownloadedBlob compares the GLCB whole-blob digest read from the
// just-downloaded tmp file's TOC footer against the FSM-recorded digest
// for this chunk. Returns nil when verification is skipped (no verifier
// configured, or no FSM record yet for this chunk). Returns an error on
// genuine mismatch so the caller can discard the tmp and re-fetch.
func (m *Manager) verifyDownloadedBlob(id chunk.ChunkID, path string) error {
	if m.cfg.IntegrityVerifier == nil {
		return nil
	}
	expected, ok := m.cfg.IntegrityVerifier.ExpectedDigest(id)
	if !ok {
		// No FSM expectation on file (pre-grnc3 entry, or the upload's
		// CmdUploadChunk hasn't applied locally yet). Skip; a later read
		// will re-verify once the FSM catches up.
		return nil
	}
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return fmt.Errorf("open downloaded blob for verify: %w", err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat downloaded blob for verify: %w", err)
	}
	toc, err := chunkcloud.ReadTOC(f, info.Size())
	if err != nil {
		return fmt.Errorf("read TOC for digest verify: %w", err)
	}
	if toc.BlobDigest != expected {
		return fmt.Errorf("downloaded blob digest mismatch for %s: rejecting cache populate (FSM=%x, blob=%x)", id, expected[:8], toc.BlobDigest[:8])
	}
	return nil
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
	m.cloudIdxMu.Lock()
	meta, found := m.cloudIdx.Lookup(id)
	if !found {
		m.cloudIdxMu.Unlock()
		return
	}
	meta.archived = archived
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

// SetIntegrityVerifier injects the verifier consulted on every cold-cache
// cloud download (gastrolog-grnc3). Safe to pass nil to disable
// verification entirely.
func (m *Manager) SetIntegrityVerifier(v chunk.IntegrityVerifier) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg.IntegrityVerifier = v
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
	return m.uploadToCloud(id)
}

// sealToGLCB packages a sealed multi-file chunk into a single
// `<chunkdir>/data.glcb` blob atomically: write to data.glcb.tmp, fsync,
// rename. Same encoding as uploadToCloud (the cloud blob and the local
// sealed file are byte-identical by construction, which is what unlocks
// binary chunk replication later — gastrolog-3o5b4).
//
// Capability-only: not yet wired into the seal pipeline. Subsequent
// commits flip PostSealProcess to call this in place of CompressChunk
// and switch read paths to consume data.glcb. See gastrolog-24m1t.
//
// On success returns the GLCB writer so callers can read TOC offsets
// and NumFrames without a second pass over the file.
func (m *Manager) sealToGLCB(id chunk.ChunkID) (*chunkcloud.Writer, int64, error) {
	m.mu.Lock()
	if m.closed || m.zstdEnc == nil {
		m.mu.Unlock()
		return nil, 0, ErrManagerClosed
	}
	meta, ok := m.metas[id]
	if !ok {
		m.mu.Unlock()
		return nil, 0, chunk.ErrChunkNotFound
	}
	if !meta.sealed {
		m.mu.Unlock()
		return nil, 0, chunk.ErrChunkNotSealed
	}
	m.mu.Unlock()

	cursor, err := m.OpenCursor(id)
	if err != nil {
		return nil, 0, fmt.Errorf("open cursor for GLCB seal: %w", err)
	}

	w := chunkcloud.NewWriter(id, m.cfg.VaultID, m.zstdEnc)
	for {
		rec, _, recErr := cursor.Next()
		if errors.Is(recErr, chunk.ErrNoMoreRecords) {
			break
		}
		if recErr != nil {
			_ = cursor.Close()
			return nil, 0, fmt.Errorf("read record for GLCB seal: %w", recErr)
		}
		if err := w.Add(rec); err != nil {
			_ = cursor.Close()
			return nil, 0, fmt.Errorf("add record to GLCB writer: %w", err)
		}
	}
	_ = cursor.Close()

	dir := m.chunkDir(id)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, 0, fmt.Errorf("ensure chunk dir: %w", err)
	}
	tmpPath := filepath.Join(dir, dataGLCBTmpFileName)
	finalPath := filepath.Join(dir, dataGLCBFileName)

	// Open the tmp with O_EXCL so a stale tmp from a prior aborted seal
	// surfaces as a clear error rather than getting clobbered. The
	// cleanOrphanTempFiles sweep at startup is responsible for tmp removal
	// on crash recovery.
	f, err := os.OpenFile(filepath.Clean(tmpPath), os.O_RDWR|os.O_CREATE|os.O_EXCL, m.cfg.FileMode)
	if err != nil {
		return nil, 0, fmt.Errorf("create %s: %w", dataGLCBTmpFileName, err)
	}
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(tmpPath)
	}

	// Serialize zstd encoder access — chunkcloud.Writer reuses the shared
	// m.zstdEnc, which klauspost/zstd's seekable writer is not safe for
	// concurrent use against. Same rationale as CompressChunk's
	// zstdEncMu serialization.
	m.zstdEncMu.Lock()
	written, werr := w.WriteTo(f)
	m.zstdEncMu.Unlock()
	if werr != nil {
		cleanup()
		return nil, 0, fmt.Errorf("write GLCB: %w", werr)
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return nil, 0, fmt.Errorf("fsync %s: %w", dataGLCBTmpFileName, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, 0, fmt.Errorf("close %s: %w", dataGLCBTmpFileName, err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return nil, 0, fmt.Errorf("rename %s → %s: %w", dataGLCBTmpFileName, dataGLCBFileName, err)
	}

	// Cache the TOC, frame count, blob size, and compressed flag on the
	// chunkMeta so downstream consumers don't have to re-parse the blob
	// tail. data.glcb's record-data section is zstd-compressed by
	// construction, so compressed=true is semantically correct (the same
	// flag CompressChunk used to set in the multi-file pipeline; it
	// drops out entirely in step 7f).
	toc := w.TOC()
	numFrames := w.NumFrames()
	bm := w.Meta()
	m.mu.Lock()
	if meta := m.metas[id]; meta != nil {
		meta.ingestIdxOffset = toc.IngestIdxOffset
		meta.ingestIdxSize = toc.IngestIdxSize
		meta.sourceIdxOffset = toc.SourceIdxOffset
		meta.sourceIdxSize = toc.SourceIdxSize
		meta.numFrames = numFrames
		meta.rawBytes = bm.RawBytes
		meta.blobDigest = toc.BlobDigest
	}
	m.mu.Unlock()

	return w, written, nil
}

func (m *Manager) uploadToCloud(id chunk.ChunkID) error {
	key := m.blobKey(id)

	// Snapshot the BlobMeta + cached TOC under the manager lock. After step
	// 7c the local data.glcb is byte-identical to what NewWriter would emit
	// (sealToGLCB and the cloud writer share the encoder), so streaming the
	// file straight to S3 replaces the legacy cursor → NewWriter → buf →
	// upload dance — saving one full encode pass and keeping the compressed
	// bytes off the heap. See gastrolog-24m1t step 7h.
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrManagerClosed
	}
	meta := m.metas[id]
	if meta == nil {
		m.mu.Unlock()
		return chunk.ErrChunkNotFound
	}
	if !meta.sealed {
		m.mu.Unlock()
		return chunk.ErrChunkNotSealed
	}
	bm := chunkcloud.BlobMeta{
		ChunkID:         id,
		VaultID:         m.cfg.VaultID,
		RecordCount:     uint32(meta.recordCount), //nolint:gosec // G115: sealed chunk record count fits in uint32 by rotation policy
		RawBytes:        meta.rawBytes,
		WriteStart:      meta.writeStart,
		WriteEnd:        meta.writeEnd,
		IngestStart:     meta.ingestStart,
		IngestEnd:       meta.ingestEnd,
		SourceStart:     meta.sourceStart,
		SourceEnd:       meta.sourceEnd,
		IngestIdxOffset: meta.ingestIdxOffset,
		IngestIdxSize:   meta.ingestIdxSize,
		SourceIdxOffset: meta.sourceIdxOffset,
		SourceIdxSize:   meta.sourceIdxSize,
	}
	toc := chunkcloud.BlobTOC{
		IngestIdxOffset: meta.ingestIdxOffset,
		IngestIdxSize:   meta.ingestIdxSize,
		SourceIdxOffset: meta.sourceIdxOffset,
		SourceIdxSize:   meta.sourceIdxSize,
	}
	numFrames := meta.numFrames
	m.mu.Unlock()

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

	glcbPath := filepath.Join(m.chunkDir(id), dataGLCBFileName)

	statInfo, err := os.Stat(filepath.Clean(glcbPath))
	if err != nil {
		return fmt.Errorf("stat data.glcb: %w", err)
	}
	blobSize := statInfo.Size()

	uploadFile, err := os.Open(filepath.Clean(glcbPath))
	if err != nil {
		return fmt.Errorf("open data.glcb for upload: %w", err)
	}
	uploadCtx, uploadCancel := context.WithTimeout(context.Background(), cloudUploadTimeout)
	err = m.cfg.CloudStore.Upload(
		uploadCtx,
		key,
		uploadFile,
		chunkcloud.ObjectMetadata(bm),
	)
	uploadCancel()
	_ = uploadFile.Close()
	m.trackCloudResult(err)
	if err != nil {
		return fmt.Errorf("upload GLCB: %w", err)
	}

	// No separate CacheDir mirror — the local data.glcb that we just
	// uploaded stays in <chunkDir> and IS the warm cache (step 7k).

	// Take the per-chunk write lock around the FSM announce, the file
	// removal, AND the metadata transition. Cursors in flight on this
	// chunk must drain before mmap regions are invalidated. Without
	// this, an indexer Build pass running concurrently with
	// backfillCloudUploads SIGBUSes inside DecodeIdxEntry when its
	// idx.log mmap region is removed mid-Next(). See gastrolog-2owzp.
	chunkLock := m.chunkLockFor(id)
	chunkLock.Lock()
	defer chunkLock.Unlock()

	// FSM-first: announce the upload before any local mutation. Applier.Apply
	// blocks on quorum + local FSM apply, so once this returns the FSM is
	// authoritative for "this chunk is cloud-backed". Readers landing between
	// the announce and the file removal still see consistent state — the FSM
	// CloudBacked flag flips first, then the local files go. The previous
	// order had a window where files were gone but the FSM still said local,
	// producing the L>>count / gap / dip artifacts in histogram.
	// See gastrolog-35l6a.
	if m.cfg.Announcer != nil {
		m.cfg.Announcer.AnnounceUpload(id, blobSize,
			toc.IngestIdxOffset, toc.IngestIdxSize,
			toc.SourceIdxOffset, toc.SourceIdxSize,
			numFrames,
			meta.blobDigest, m.cfg.CloudServiceID, currentKeyScheme)
	}

	// Delete the multi-file data artifacts (raw.log/idx.log/etc.) — they
	// are redundant with data.glcb and remain only on chunks sealed before
	// step 7c stage 3b landed. Keep the local data.glcb itself: post step
	// 7j it is the warm cache for the now-cloud-backed chunk, read
	// transparently via OpenCursor's local-GLCB fast path. Eviction under
	// disk pressure can delete it later; the cloud blob is authoritative.
	if err := m.removeLocalDataFiles(id); err != nil {
		return fmt.Errorf("remove local data files after cloud upload: %w", err)
	}

	// Move metadata from in-memory map to cloud B+ tree index.
	// The chunk is now cloud-only — remove from Go heap.
	m.mu.Lock()
	meta = m.metas[id]
	if meta != nil {
		meta.cloudBacked = true
		meta.diskBytes = blobSize
		meta.ingestIdxOffset = toc.IngestIdxOffset
		meta.ingestIdxSize = toc.IngestIdxSize
		meta.sourceIdxOffset = toc.SourceIdxOffset
		meta.sourceIdxSize = toc.SourceIdxSize
		meta.numFrames = numFrames
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

	m.logger.Debug("chunk uploaded to cloud",
		"chunk", id,
		"bytes", blobSize,
	)
	return nil
}

// adoptCloudBlob transitions a local chunk to cloud-backed using an existing
// S3 blob. Used when another node (typically the leader) already uploaded the
// same chunk while we were preparing our own upload. The local data.glcb is
// byte-identical to the cloud blob by construction (sealToGLCB and the cloud
// writer share the encoder, and the input record set is the same), so the
// TOC cached on chunkMeta during sealToGLCB matches what's in S3 — no range
// request needed to read it back. See gastrolog-24m1t step 7i.
func (m *Manager) adoptCloudBlob(id chunk.ChunkID, blobSize int64) error {
	// Per-chunk write lock around the FSM announce, the disk transition,
	// and the meta mutation; same rationale as uploadToCloud
	// (gastrolog-2owzp). adoptCloudBlob fires when another node beat us
	// to the upload — we still transition local files to cloud-only state,
	// which is the same mutation any in-flight cursor needs to drain
	// against.
	chunkLock := m.chunkLockFor(id)
	chunkLock.Lock()
	defer chunkLock.Unlock()

	// Snapshot the cached TOC + numFrames from the local sealed chunk. If
	// the chunk was already adopted in a prior cycle (no longer in m.metas),
	// fall back to the cloud index entry.
	m.mu.Lock()
	am := m.metas[id]
	m.mu.Unlock()
	if am == nil && m.cloudIdx != nil {
		m.cloudIdxMu.Lock()
		am, _ = m.cloudIdx.Lookup(id)
		m.cloudIdxMu.Unlock()
	}
	if am == nil {
		return fmt.Errorf("adopt cloud blob: no local meta for %s", id)
	}
	ingestIdxOff := am.ingestIdxOffset
	ingestIdxSize := am.ingestIdxSize
	sourceIdxOff := am.sourceIdxOffset
	sourceIdxSize := am.sourceIdxSize
	numFrames := am.numFrames
	blobDigest := am.blobDigest

	// FSM-first: announce before any local mutation. Applier.Apply blocks on
	// quorum + local FSM apply, so once the announce returns the FSM is
	// authoritative for "this chunk is cloud-backed". Without this, the FSM
	// overlay keeps returning CloudBacked=false and the backfill re-adopts on
	// every cycle (gastrolog-68fqk); readers landing between removeLocalDataFiles
	// and the announce would have observed "FSM says local, files gone",
	// producing histogram artifacts (gastrolog-35l6a).
	if m.cfg.Announcer != nil {
		m.cfg.Announcer.AnnounceUpload(id, blobSize,
			ingestIdxOff, ingestIdxSize,
			sourceIdxOff, sourceIdxSize,
			numFrames,
			blobDigest, m.cfg.CloudServiceID, currentKeyScheme)
	}

	// Delete the multi-file data artifacts (raw.log/idx.log/etc.) — same
	// rationale as uploadToCloud: data.glcb itself stays as the warm cache
	// for the now-cloud-backed chunk (gastrolog-24m1t step 7j).
	if err := m.removeLocalDataFiles(id); err != nil {
		return fmt.Errorf("remove local data files after cloud adopt: %w", err)
	}

	m.mu.Lock()
	meta := m.metas[id]
	if meta != nil {
		meta.cloudBacked = true
		meta.diskBytes = blobSize
		meta.ingestIdxOffset = ingestIdxOff
		meta.ingestIdxSize = ingestIdxSize
		meta.sourceIdxOffset = sourceIdxOff
		meta.sourceIdxSize = sourceIdxSize
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

	m.logger.Debug("chunk adopted from cloud",
		"chunk", id,
		"bytes", blobSize,
	)
	return nil
}

// RegisterCloudChunk registers a cloud-backed chunk from metadata alone,
// without streaming any records or downloading from S3. Creates a cloud
// index entry so the chunk appears in List() and is queryable via
// openCloudCursor. Used by follower nodes when the tier FSM
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
	m.cloudIdxMu.Lock()
	existing, _ := m.cloudIdx.Lookup(id)
	m.cloudIdxMu.Unlock()
	if existing != nil {
		return nil // already in cloud index
	}

	meta := &chunkMeta{
		id:                id,
		writeStart:        info.WriteStart,
		writeEnd:          info.WriteEnd,
		ingestStart:       info.IngestStart,
		ingestEnd:         info.IngestEnd,
		sourceStart:       info.SourceStart,
		sourceEnd:         info.SourceEnd,
		ingestTSMonotonic: info.IngestTSMonotonic,
		recordCount:       info.RecordCount,
		bytes:             info.Bytes,
		sealed:            true,
		cloudBacked:       true,
		diskBytes:         info.DiskBytes,
		ingestIdxOffset:   info.IngestIdxOffset,
		ingestIdxSize:     info.IngestIdxSize,
		sourceIdxOffset:   info.SourceIdxOffset,
		sourceIdxSize:     info.SourceIdxSize,
		numFrames:         info.NumFrames,
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

// openCloudCursor opens a cloud-backed chunk for record reads when the
// in-tree warm cache (<chunkDir>/data.glcb) is absent. The fast path lives
// in OpenCursor — by the time we get here either the cache was evicted or
// the chunk was never sealed locally (follower that adopted via FSM).
//
// Strategy: try a one-shot full download into the chunk dir first so the
// next read goes through the local-GLCB fast path. On download failure
// fall back to range-request reads via NewRemoteReader. The remote reader
// pulls header/dict/index/TOC at init (~few KB) and fetches individual
// frames on demand, so it stays cheap when the per-query touch count is
// low (notably histograms that read the TS index only).
func (m *Manager) openCloudCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	m.mu.Lock()
	meta := m.lookupMeta(id)
	m.mu.Unlock()

	if meta == nil {
		return nil, chunk.ErrChunkNotFound
	}

	if cursor, err := m.downloadCloudBlobToChunkDir(id); err == nil {
		return cursor, nil
	} else {
		// Debug, not Warn: this fires on every cloud cursor that races
		// with retention-driven blob deletion. The range-request
		// fallback below propagates the real error to callers that
		// genuinely need it; raising WARN here would flood operator
		// logs during every retention sweep. See gastrolog-2c96i.
		m.logger.Debug("cache: cloud blob download failed, falling back to range requests",
			"chunk", id, "error", err)
	}

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
		// Drop local m.metas entries for chunks that the cloud index also
		// holds — post step 7j the local data.glcb sticks around as warm
		// cache after upload, but the authoritative meta (with archived /
		// numFrames / TOC offsets) is the cloud index entry. Without this
		// reconciliation a restart resurrects a stale local-sealed meta
		// that masks the cloud-recorded archived flag. The data.glcb file
		// itself stays put — OpenCursor's local-GLCB fast path picks it
		// up via hasLocalGLCB. See gastrolog-24m1t step 7j.
		m.dropLocalMetaForCloudChunks()
	}
	m.backfillTSOffsets()
	return nil
}

// dropLocalMetaForCloudChunks removes m.metas entries for any chunk also
// present in the cloud index. The on-disk data.glcb is preserved as warm
// cache; only the duplicated in-memory meta goes.
func (m *Manager) dropLocalMetaForCloudChunks() {
	if m.cloudIdx == nil {
		return
	}
	m.cloudIdxMu.Lock()
	cloudIDs := make([]chunk.ChunkID, 0, m.cloudIdx.Count())
	_ = m.cloudIdx.ForEach(func(id chunk.ChunkID, _ *chunkMeta) bool {
		cloudIDs = append(cloudIDs, id)
		return true
	})
	m.cloudIdxMu.Unlock()

	m.mu.Lock()
	for _, id := range cloudIDs {
		delete(m.metas, id)
	}
	m.mu.Unlock()
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
		// Pull the TOC from the tail of the blob.
		info, err := m.cfg.CloudStore.Head(context.Background(), m.blobKey(id))
		if err != nil || info.Size < int64(chunkcloud.TOCFooterSize) {
			return true
		}
		toc, err := chunkcloud.DownloadTOC(context.Background(), m.cfg.CloudStore, m.blobKey(id), info.Size)
		if err != nil || toc.IngestIdxOffset == 0 {
			return true
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
		// Skip if already in the cloud index (preserves richer metadata
		// like TS index offsets from previous backfills).
		if m.cloudIdx != nil && m.cloudIdxHas(id) {
			return nil
		}
		// If we also have a local entry, the local data.glcb is now the
		// warm cache for an already-uploaded chunk. Drop the local meta
		// in favor of the authoritative cloud entry that's about to be
		// inserted into cloudIdx — the local data.glcb file stays in
		// place as cache and will be picked up by OpenCursor's local-GLCB
		// fast path. See gastrolog-24m1t step 7j.
		delete(m.metas, id)
		cm := chunkcloud.BlobMetaToChunkMeta(id, blob)
		meta := &chunkMeta{
			id:          id,
			writeStart:  cm.WriteStart,
			writeEnd:    cm.WriteEnd,
			recordCount: cm.RecordCount,
			bytes:       cm.Bytes,
			diskBytes:   cm.DiskBytes,
			sealed:      true,
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
