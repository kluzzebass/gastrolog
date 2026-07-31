// Package raftwal provides a shared write-ahead log for multiple hashicorp/raft
// groups. Instead of each group writing to its own boltdb (with independent
// fsync per write), all groups append to a single WAL file. Writes are batched
// and fsynced together, amortizing the disk I/O cost across all groups.
//
// Each group gets a GroupStore handle that implements raft.LogStore and
// raft.StableStore. Reads are served from an in-memory index; writes go
// through the shared WAL with coalesced fsync.
//
// The WAL is segmented: when a segment exceeds the target size, a new segment
// is started. After DeleteRange batches, when at least CompactionMinSegments
// files exist, the WAL may compact: rewrite live state into new segments,
// fsync, then remove older segment files (replay-safe).
package raftwal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	hraft "github.com/hashicorp/raft"
)

// entryType tags each WAL record so the reader knows how to interpret it.
type entryType byte

const (
	entryLog          entryType = 1 // raft.Log entry
	entryStableSet    entryType = 2 // StableStore.Set(key, val)
	entryStableUint64 entryType = 3 // StableStore.SetUint64(key, val)
	entryDeleteRange  entryType = 4 // LogStore.DeleteRange(min, max)
	entryGroupReg     entryType = 5 // group name → numeric ID registration
	entryLogBatch     entryType = 6 // atomic batch of raft.Log entries (one StoreLogs call)
)

const (
	// segmentTargetSize is the target size for a WAL segment before rotation.
	segmentTargetSize = 64 * 1024 * 1024 // 64 MB

	// walFilePrefix is the prefix for WAL segment files.
	walFilePrefix = "wal-"

	// walFileSuffix is the suffix for WAL segment files.
	walFileSuffix = ".log"

	// syncBatchWindow is how long the writer waits to collect more writes
	// before fsyncing. A short window (1ms) amortizes fsync across groups
	// while keeping latency low.
	syncBatchWindow = 1 * time.Millisecond

	// headerSize is groupID (4) + entryType (1) + payload length (4) + CRC (4).
	headerSize = 13

	// logCacheBudgetBytes is the default per-group heap budget for recently
	// appended log payloads (the recent window). Entries beyond the budget
	// stay indexed in memory but are read back from WAL segment files on
	// demand, bounding heap by the budget instead of by log length.
	logCacheBudgetBytes = 1 << 20 // 1 MiB

	// logBatchCountSize and logBatchEntryLenSize describe the entryLogBatch
	// payload layout: [count:4] then per entry [len:4][encoded raft.Log].
	logBatchCountSize    = 4
	logBatchEntryLenSize = 4

	// cacheFIFOCompactMin is the minimum consumed-prefix length before the
	// recent-window eviction queue reclaims its backing array.
	cacheFIFOCompactMin = 1024
)

var (
	ErrNotFound  = errors.New("not found")
	errWALClosed = errors.New("wal closed")
	crc32Table   = crc32.MakeTable(crc32.Castagnoli)
)

// Config holds tunable parameters for the WAL.
type Config struct {
	// SegmentTargetSize is the target size for a WAL segment before rotation.
	// Default: 64MB.
	SegmentTargetSize int64

	// SyncBatchWindow is how long the writer waits to collect more writes
	// before fsyncing. Default: 1ms.
	SyncBatchWindow time.Duration

	// CompactionMinSegments is the minimum number of WAL segment files required
	// before automatic compaction is attempted after DeleteRange writes.
	// Default: 2.
	CompactionMinSegments int

	// LogCacheBudgetBytes is the per-group heap budget for recently appended
	// log entry payloads (the recent window). GetLog serves entries within
	// the window from memory; older entries are read from WAL segment files.
	// This bounds heap by the budget rather than by log length — memory over
	// throughput. Default: 1MiB.
	LogCacheBudgetBytes int64

	// SegmentSync, if non-nil, is called instead of (*os.File).Sync on the
	// active WAL segment after a batch is written (and during compaction).
	// Used by tests for deterministic fsync failure injection. Production code
	// must leave this nil.
	SegmentSync func(*os.File) error

	// SegmentPreallocate, if non-nil, is called instead of the platform
	// preallocation syscall when reserving segment blocks. Used by tests for
	// deterministic reservation failure injection. Production code must
	// leave this nil.
	SegmentPreallocate func(*os.File, int64) error

	// OnReserveState, if non-nil, is invoked when the WAL's space reserve is
	// lost (preallocation failed — the WAL runs on ordinary allocation and a
	// full volume can panic Raft) or restored. Invoked from the batch-writer
	// goroutine: must not block. err is nil when lost is false.
	OnReserveState func(lost bool, err error)
}

func (c Config) withDefaults() Config {
	if c.SegmentTargetSize <= 0 {
		c.SegmentTargetSize = segmentTargetSize
	}
	if c.SyncBatchWindow <= 0 {
		c.SyncBatchWindow = syncBatchWindow
	}
	if c.CompactionMinSegments <= 0 {
		c.CompactionMinSegments = 2
	}
	if c.LogCacheBudgetBytes <= 0 {
		c.LogCacheBudgetBytes = logCacheBudgetBytes
	}
	return c
}

// CompactionStats captures the most recent automatic WAL compaction result.
type CompactionStats struct {
	ReclaimedSegments int
	ReclaimedBytes    int64
	RetainedSegments  int
	RetainedBytes     int64
}

// WAL is the shared write-ahead log. Create one per node; all Raft
// groups on that node share it.
type WAL struct {
	// stateMu protects in-memory group state (groups, groupIDs, nextGID,
	// lastCompaction). Disk writes and fsync run in batchWriter without
	// holding stateMu so concurrent reads are not blocked on I/O.
	stateMu  sync.RWMutex
	dir      string
	cfg      Config
	groups   map[uint32]*groupState // groupID → state
	groupIDs map[string]uint32      // group name → numeric ID
	nextGID  uint32

	// Active segment.
	seg     *os.File
	segPath string
	segSize int64
	segSeq  int

	// Space reserve: every segment is preallocated to its full target size
	// (physical blocks only — logical size still marks end of data for
	// replay), and the NEXT segment is created fully reserved before it is
	// needed, so rotation at crisis time allocates nothing. A
	// Raft term bump is ~30 bytes; one reserved segment is effectively
	// unlimited election-storm runway on a full volume. sparePath is the
	// already-reserved next segment ("" when the reserve is lost) — consumed
	// on promotion so darwin's physical-EOF preallocation never doubles.
	sparePath   string
	reserveLost atomic.Bool

	// Segment read handles for serving log payloads beyond the recent
	// window. readersMu guards the map itself; handles
	// for compacted segments are closed only under stateMu.Lock, which is
	// mutually exclusive with GetLog readers (they hold stateMu.RLock
	// across location lookup + pread), so a handle is never closed while a
	// read is in flight on it.
	readersMu sync.Mutex
	readers   map[int]*os.File

	// Batch writer: collects writes and fsyncs once per batch.
	writeCh chan writeOp
	syncCh  chan chan error // request a sync, get back the result
	done    chan struct{}

	// Append-latency instrumentation: caller-observed submit latency —
	// queue wait + write + batch fsync — which is exactly what a Raft
	// StoreLogs call experiences. The shared batch writer
	// serializes ALL groups, so one slow fsync inflates every group's
	// latency; these counters make that visible in NodeStats instead of
	// discoverable only by pprof during an incident.
	appendCount    atomic.Uint64
	appendNanos    atomic.Uint64
	appendMaxNanos atomic.Uint64 // max since the last AppendLatencyStats read
	wg             sync.WaitGroup

	lastCompaction CompactionStats
}

// logLoc locates one encoded raft.Log payload inside a WAL segment file.
type logLoc struct {
	seg    int   // segment sequence number
	off    int64 // file offset of the encoded payload
	length int   // encoded payload length
}

// groupState holds per-group in-memory state.
type groupState struct {
	// Log index: raft log index → durable location in the WAL segments.
	// The index itself stays in memory for every live entry; payloads live
	// on disk and are cached only within the recent window below.
	logs       map[uint64]logLoc
	firstIndex uint64
	lastIndex  uint64

	// Recent window: encoded payloads of recently appended entries,
	// bounded by Config.LogCacheBudgetBytes. GetLog
	// beyond the window reads the payload back from the segment file.
	cache      map[uint64][]byte
	cacheBytes int64
	cacheQueue cacheFIFO

	// Stable store: small key-value pairs (CurrentTerm, LastVotedFor).
	stable map[string][]byte
}

func newGroupState() *groupState {
	return &groupState{
		logs:   make(map[uint64]logLoc),
		cache:  make(map[uint64][]byte),
		stable: make(map[string][]byte),
	}
}

// cacheFIFO tracks cached log indices in insertion order so eviction drops
// the oldest cached payload first. Stale entries (already dropped by
// DeleteRange or overwrite) are skipped lazily on pop.
type cacheFIFO struct {
	items []uint64
	head  int
}

func (q *cacheFIFO) push(v uint64) { q.items = append(q.items, v) }

func (q *cacheFIFO) peek() (uint64, bool) {
	if q.head >= len(q.items) {
		return 0, false
	}
	return q.items[q.head], true
}

func (q *cacheFIFO) pop() (uint64, bool) {
	if q.head >= len(q.items) {
		return 0, false
	}
	v := q.items[q.head]
	q.head++
	switch {
	case q.head == len(q.items):
		q.items = q.items[:0]
		q.head = 0
	case q.head >= cacheFIFOCompactMin && q.head*2 >= len(q.items):
		n := copy(q.items, q.items[q.head:])
		q.items = q.items[:n]
		q.head = 0
	}
	return v, true
}

// writeOp is a single write submitted to the batch writer.
type writeOp struct {
	groupID uint32
	typ     entryType
	payload []byte
	done    chan error
}

// Open opens or creates a WAL in the given directory.
// Pass a zero Config for defaults (64MB segments, 1ms batch window).
func Open(dir string, cfgs ...Config) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("raftwal: mkdir: %w", err)
	}

	var cfg Config
	if len(cfgs) > 0 {
		cfg = cfgs[0]
	}
	cfg = cfg.withDefaults()

	w := &WAL{
		dir:      dir,
		cfg:      cfg,
		groups:   make(map[uint32]*groupState),
		groupIDs: make(map[string]uint32),
		nextGID:  1,
		readers:  make(map[int]*os.File),
		writeCh:  make(chan writeOp, 4096),
		syncCh:   make(chan chan error, 64),
		done:     make(chan struct{}),
	}

	// Replay existing segments to rebuild in-memory state.
	if err := w.replay(); err != nil {
		return nil, fmt.Errorf("raftwal: replay: %w", err)
	}

	// Open a new segment for writing.
	if err := w.rotateSegment(); err != nil {
		return nil, fmt.Errorf("raftwal: open segment: %w", err)
	}

	// Start the batch writer goroutine.
	w.wg.Add(1)
	go w.batchWriter()

	return w, nil
}

// GroupStore returns a handle for the named group that implements
// raft.LogStore and raft.StableStore.
func (w *WAL) GroupStore(name string) *GroupStore {
	w.stateMu.Lock()

	gid, ok := w.groupIDs[name]
	needsReg := false
	if !ok {
		gid = w.nextGID
		w.nextGID++
		w.groupIDs[name] = gid
		if _, exists := w.groups[gid]; !exists {
			w.groups[gid] = newGroupState()
		}
		needsReg = true
	}
	w.stateMu.Unlock()

	// Persist the name→ID mapping outside the lock (submit acquires it).
	if needsReg {
		_ = w.submit(writeOp{
			groupID: gid,
			typ:     entryGroupReg,
			payload: []byte(name),
		})
	}

	return &GroupStore{wal: w, groupID: gid}
}

// LastCompactionStats returns statistics from the most recent automatic
// compaction run. If no compaction has run yet, fields are zero.
func (w *WAL) LastCompactionStats() CompactionStats {
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()
	return w.lastCompaction
}

// Close flushes pending writes and closes the WAL. Safe to call multiple times.
func (w *WAL) Close() error {
	w.stateMu.Lock()
	select {
	case <-w.done:
		w.stateMu.Unlock()
		return nil // already closed
	default:
		close(w.done)
	}
	w.stateMu.Unlock()
	w.wg.Wait()
	// Drain any ops that were enqueued but never processed.
	for {
		select {
		case op := <-w.writeCh:
			if op.done != nil {
				op.done <- errWALClosed
			}
		default:
			goto drained
		}
	}
drained:
	// Close read handles under stateMu.Lock — the same exclusion compaction
	// uses — so no in-flight GetLog is mid-pread on a handle being closed.
	// (A GetLog issued after Close still works: segmentReader reopens.)
	w.stateMu.Lock()
	w.closeSegmentReadersUpTo(w.segSeq)
	w.stateMu.Unlock()
	if w.seg != nil {
		return w.seg.Close()
	}
	return nil
}

// submit sends a write to the batch writer and waits for the fsync.
func (w *WAL) submit(op writeOp) error {
	// Check done first — after Close(), no new ops are accepted.
	select {
	case <-w.done:
		return errWALClosed
	default:
	}
	start := time.Now()
	op.done = make(chan error, 1)
	select {
	case w.writeCh <- op:
	case <-w.done:
		return errWALClosed
	}
	err := <-op.done
	w.observeAppendLatency(time.Since(start))
	return err
}

func (w *WAL) observeAppendLatency(d time.Duration) {
	ns := uint64(d.Nanoseconds()) //nolint:gosec // durations are non-negative
	w.appendCount.Add(1)
	w.appendNanos.Add(ns)
	for {
		cur := w.appendMaxNanos.Load()
		if ns <= cur || w.appendMaxNanos.CompareAndSwap(cur, ns) {
			return
		}
	}
}

// AppendTotals returns the cumulative submit count and total latency. Pure
// read — safe for snapshot paths between stats ticks.
func (w *WAL) AppendTotals() (count, totalNanos uint64) {
	return w.appendCount.Load(), w.appendNanos.Load()
}

// TakeMaxAppendLatency returns the maximum single-submit latency observed
// since the previous call and resets it ("max since last stats tick"). Call
// only from the ticking stats path, never from snapshot reads, or the tick's
// max gets consumed early.
func (w *WAL) TakeMaxAppendLatency() (maxNanos uint64) {
	return w.appendMaxNanos.Swap(0)
}

// batchWriter is the single goroutine that writes to the WAL file.
// It collects writes from writeCh, appends them to the segment, and
// fsyncs once per batch.
func (w *WAL) batchWriter() {
	defer w.wg.Done()

	var batch []writeOp
	timer := time.NewTimer(w.cfg.SyncBatchWindow)
	defer timer.Stop()

	for {
		// Wait for the first write or shutdown.
		select {
		case op := <-w.writeCh:
			batch = append(batch, op)
		case <-w.done:
			return
		}

		// Drain any more writes that arrived in the batch window.
		timer.Reset(w.cfg.SyncBatchWindow)
	drain:
		for {
			select {
			case op := <-w.writeCh:
				batch = append(batch, op)
			case <-timer.C:
				break drain
			case <-w.done:
				// Flush what we have before exiting.
				w.flushBatch(batch)
				return
			}
		}

		w.flushBatch(batch)
		batch = batch[:0]
	}
}

// syncActiveSegment persists the active segment; SegmentSync overrides when set.
func (w *WAL) syncActiveSegment() error {
	if w.seg == nil {
		return nil
	}
	if w.cfg.SegmentSync != nil {
		return w.cfg.SegmentSync(w.seg)
	}
	return w.seg.Sync()
}

// appliedRecord is a successfully appended writeOp plus where its payload
// landed, so the in-memory index can reference the durable bytes.
type appliedRecord struct {
	op         writeOp
	seg        int   // segment sequence the record was written to
	payloadOff int64 // file offset of the record payload
}

// flushBatch writes all ops to the segment, fsyncs once, and notifies callers.
// Segment I/O and fsync run without stateMu so reads can proceed concurrently.
func (w *WAL) flushBatch(batch []writeOp) {
	if len(batch) == 0 {
		return
	}

	applied, writeErr, sawDeleteRange := w.appendBatchToSegment(batch)

	if len(applied) > 0 {
		w.stateMu.Lock()
		for _, rec := range applied {
			w.applyToMemory(rec.op.groupID, rec.op.typ, rec.op.payload, rec.seg, rec.payloadOff)
		}
		w.stateMu.Unlock()
	}

	// Single fsync for the entire batch — no stateMu held.
	syncErr := w.syncActiveSegment()

	if syncErr == nil && writeErr == nil && sawDeleteRange {
		// Best effort: compaction must never affect caller-visible success
		// for already-fsynced writes. Run before notifying waiters so
		// LastCompactionStats is stable when DeleteRange returns.
		_ = w.compactSegments()
	}

	w.notifyBatchWaiters(batch, syncErr)
}

func (w *WAL) appendBatchToSegment(batch []writeOp) (applied []appliedRecord, writeErr error, sawDeleteRange bool) {
	for i := range batch {
		if writeErr != nil {
			if batch[i].done != nil {
				batch[i].done <- writeErr
			}
			continue
		}
		// Rotate before writing if this entry would push the segment
		// past the target size. This keeps segments bounded and ensures
		// large payloads start on a fresh segment.
		entrySize := int64(headerSize + len(batch[i].payload))
		if w.segSize > 0 && w.segSize+entrySize > w.cfg.SegmentTargetSize {
			if err := w.rotateSegment(); err != nil {
				writeErr = err
				if batch[i].done != nil {
					batch[i].done <- err
				}
				continue
			}
		}
		rec := appliedRecord{op: batch[i], seg: w.segSeq, payloadOff: w.segSize + headerSize}
		if err := w.appendEntry(batch[i].groupID, batch[i].typ, batch[i].payload); err != nil {
			writeErr = err
			if batch[i].done != nil {
				batch[i].done <- err
			}
			continue
		}
		applied = append(applied, rec)
		if batch[i].typ == entryDeleteRange {
			sawDeleteRange = true
		}
	}
	return applied, writeErr, sawDeleteRange
}

func (w *WAL) notifyBatchWaiters(batch []writeOp, syncErr error) {
	for i := range batch {
		if batch[i].done != nil {
			select {
			case batch[i].done <- syncErr:
			default:
				// Already sent an error above.
			}
		}
	}
}

// appendEntry writes a single WAL entry to the current segment.
// Must be called from batchWriter only.
func (w *WAL) appendEntry(groupID uint32, typ entryType, payload []byte) error {
	// Format: [groupID:4][type:1][length:4][payload:N][crc32:4]
	hdr := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(hdr[0:4], groupID)
	hdr[4] = byte(typ)
	binary.LittleEndian.PutUint32(hdr[5:9], uint32(len(payload))) //nolint:gosec // bounded by available memory
	crc := crc32.Checksum(payload, crc32Table)
	binary.LittleEndian.PutUint32(hdr[9:13], crc)

	if _, err := w.seg.Write(hdr); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if _, err := w.seg.Write(payload); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	w.segSize += int64(headerSize + len(payload))
	return nil
}

// applyToMemory updates the in-memory index for a group. seg and payloadOff
// locate the record payload in its segment file so log entries can be read
// back from disk once they leave the recent window.
// Must be called with w.stateMu held for writing.
func (w *WAL) applyToMemory(groupID uint32, typ entryType, payload []byte, seg int, payloadOff int64) {
	gs := w.groups[groupID]
	if gs == nil {
		gs = newGroupState()
		w.groups[groupID] = gs
	}

	switch typ {
	case entryLog:
		gs.applyLogEntry(payload, logLoc{seg: seg, off: payloadOff, length: len(payload)}, w.cfg.LogCacheBudgetBytes)

	case entryLogBatch:
		forEachBatchEntry(payload, func(off int, enc []byte) {
			gs.applyLogEntry(enc, logLoc{seg: seg, off: payloadOff + int64(off), length: len(enc)}, w.cfg.LogCacheBudgetBytes)
		})

	case entryStableSet:
		key, val := decodeStableSet(payload)
		gs.stable[key] = val

	case entryStableUint64:
		key, val := decodeStableUint64(payload)
		// Store as 8-byte big-endian for GetUint64 compatibility.
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, val)
		gs.stable[key] = buf

	case entryDeleteRange:
		gs.applyDeleteRange(payload)

	case entryGroupReg:
		name := string(payload)
		w.groupIDs[name] = groupID
		if groupID >= w.nextGID {
			w.nextGID = groupID + 1
		}
	}
}

// segmentPath returns the on-disk path for segment sequence seq.
func (w *WAL) segmentPath(seq int) string {
	return filepath.Join(w.dir, fmt.Sprintf("%s%06d%s", walFilePrefix, seq, walFileSuffix))
}

// segmentReader returns a read-only handle for segment seq, opening it
// lazily. Callers must hold stateMu (read or write): compaction closes
// superseded handles only under stateMu.Lock.
func (w *WAL) segmentReader(seq int) (*os.File, error) {
	w.readersMu.Lock()
	defer w.readersMu.Unlock()
	if w.readers == nil {
		w.readers = make(map[int]*os.File)
	}
	if f, ok := w.readers[seq]; ok {
		return f, nil
	}
	f, err := os.Open(w.segmentPath(seq))
	if err != nil {
		return nil, err
	}
	w.readers[seq] = f
	return f, nil
}

// closeSegmentReadersUpTo closes cached read handles for segments <= maxSeq.
// Must be called with stateMu.Lock held so no GetLog is mid-pread on them.
func (w *WAL) closeSegmentReadersUpTo(maxSeq int) {
	w.readersMu.Lock()
	defer w.readersMu.Unlock()
	for seq, f := range w.readers {
		if seq <= maxSeq {
			_ = f.Close()
			delete(w.readers, seq)
		}
	}
}

// readPayload reads an encoded log payload back from its WAL segment.
// Callers must hold stateMu (read or write) — see segmentReader — except the
// batchWriter during compaction, when group state is frozen (the batchWriter
// is the only mutator and it is busy compacting).
func (w *WAL) readPayload(loc logLoc) ([]byte, error) {
	f, err := w.segmentReader(loc.seg)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, loc.length)
	if _, err := f.ReadAt(buf, loc.off); err != nil {
		return nil, err
	}
	return buf, nil
}

// rotateSegment closes the current segment and opens a new one.
// Must be called from batchWriter only.
func (w *WAL) rotateSegment() error {
	if w.seg != nil {
		if err := w.seg.Close(); err != nil {
			return fmt.Errorf("close segment: %w", err)
		}
	}

	w.segSeq++
	w.segPath = w.segmentPath(w.segSeq)
	promotedSpare := w.segPath == w.sparePath
	f, err := os.OpenFile(w.segPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644) //nolint:gosec // G304: path is constructed internally
	if err != nil {
		return fmt.Errorf("open segment %s: %w", w.segPath, err)
	}
	w.seg = f
	w.segSize = 0
	w.reconcileReserve(promotedSpare)
	return nil
}

// reconcileReserve restores the space-reserve invariant after a rotation:
// the active segment is preallocated to its full target size (skipped when
// it IS the consumed spare — already reserved; re-reserving would double the
// claim on darwin) and the next segment exists fully reserved. Reservation
// failure is DEGRADED, never fatal: the WAL keeps appending on ordinary
// allocation, and the transition is surfaced via OnReserveState so the
// operator learns the ENOSPC immunity is gone while there is still runway.
func (w *WAL) reconcileReserve(promotedSpare bool) {
	w.sparePath = ""
	var err error
	if !promotedSpare {
		err = w.preallocateSegment(w.seg)
	}
	if err == nil {
		err = w.ensureSpare()
	}
	lost := err != nil
	if w.reserveLost.Swap(lost) != lost && w.cfg.OnReserveState != nil {
		w.cfg.OnReserveState(lost, err)
	}
}

// ensureSpare creates the next segment file fully reserved so the coming
// rotation allocates nothing. Records the path in sparePath on success.
func (w *WAL) ensureSpare() error {
	sparePath := w.segmentPath(w.segSeq + 1)
	f, err := os.OpenFile(sparePath, os.O_CREATE|os.O_RDWR, 0o644) //nolint:gosec // G304: path is constructed internally
	if err != nil {
		return fmt.Errorf("create spare segment: %w", err)
	}
	err = w.preallocateSegment(f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("reserve spare segment %s: %w", sparePath, err)
	}
	w.sparePath = sparePath
	return nil
}

// preallocateSegment reserves the segment's full target size;
// SegmentPreallocate overrides when set.
func (w *WAL) preallocateSegment(f *os.File) error {
	if w.cfg.SegmentPreallocate != nil {
		return w.cfg.SegmentPreallocate(f, w.cfg.SegmentTargetSize)
	}
	return preallocate(f, w.cfg.SegmentTargetSize)
}

// ReserveLost reports whether the WAL's space reserve is currently lost
// (a preallocation failed and has not yet been restored by a later rotation).
func (w *WAL) ReserveLost() bool {
	return w.reserveLost.Load()
}

// replay reads all existing WAL segments and rebuilds in-memory state.
func (w *WAL) replay() error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	segments := make([]segmentInfo, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, walFilePrefix) || !strings.HasSuffix(name, walFileSuffix) {
			continue
		}
		seq, ok := parseSegmentSeq(name)
		if !ok {
			continue
		}
		path := filepath.Join(w.dir, name)
		info, err := e.Info()
		if err != nil {
			return err
		}
		if info.Size() == 0 {
			// A logically-empty segment is the previous run's reserved
			// spare (or a never-written rotation). It has nothing to
			// replay, but its preallocated blocks pin reserve space —
			// remove it so restarts don't accumulate orphaned reserves.
			_ = os.Remove(path)
			continue
		}
		segments = append(segments, segmentInfo{
			path: path,
			seq:  seq,
		})
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].seq < segments[j].seq })

	for _, seg := range segments {
		if err := w.replaySegment(seg.path, seg.seq); err != nil {
			return fmt.Errorf("replay %s: %w", seg.path, err)
		}
		// Track the highest segment sequence number.
		if seg.seq > w.segSeq {
			w.segSeq = seg.seq
		}
	}
	return nil
}

type segmentInfo struct {
	path string
	seq  int
	size int64
}

func parseSegmentSeq(name string) (int, bool) {
	seqPart := strings.TrimPrefix(name, walFilePrefix)
	seqPart = strings.TrimSuffix(seqPart, walFileSuffix)
	var seq int
	if _, err := fmt.Sscanf(seqPart, "%d", &seq); err != nil {
		return 0, false
	}
	return seq, true
}

func (w *WAL) listSegments() ([]segmentInfo, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	segments := make([]segmentInfo, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, walFilePrefix) || !strings.HasSuffix(name, walFileSuffix) {
			continue
		}
		seq, ok := parseSegmentSeq(name)
		if !ok {
			continue
		}
		path := filepath.Join(w.dir, name)
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		segments = append(segments, segmentInfo{
			path: path,
			seq:  seq,
			size: info.Size(),
		})
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].seq < segments[j].seq })
	return segments, nil
}

func (w *WAL) compactSegments() error {
	segments, err := w.listSegments()
	if err != nil {
		return err
	}
	if len(segments) < w.cfg.CompactionMinSegments {
		return nil
	}

	// Capture segment horizon to reclaim only pre-compaction files.
	oldMaxSeq := w.segSeq
	if oldMaxSeq <= 0 {
		return nil
	}

	if w.seg != nil {
		if err := w.syncActiveSegment(); err != nil {
			return err
		}
	}
	if err := w.rotateSegment(); err != nil {
		return err
	}

	snap := w.collectCompactionSnapshot()
	newLocs, err := w.writeCompactionSnapshot(snap)
	if err != nil {
		return err
	}
	if err := w.syncActiveSegment(); err != nil {
		return err
	}

	// Swap log locations to the fsynced compacted copies, remove the
	// superseded files, and close their read handles — all under
	// stateMu.Lock so no GetLog can pread a segment being removed.
	w.stateMu.Lock()
	for gid, locs := range newLocs {
		gs := w.groups[gid]
		if gs == nil {
			continue
		}
		for idx, loc := range locs {
			if _, ok := gs.logs[idx]; ok {
				gs.logs[idx] = loc
			}
		}
	}
	var reclaimedSegments int
	var reclaimedBytes int64
	var removeErr error
	for _, seg := range segments {
		if seg.seq > oldMaxSeq {
			continue
		}
		if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
			removeErr = err
			break
		}
		reclaimedSegments++
		reclaimedBytes += seg.size
	}
	w.closeSegmentReadersUpTo(oldMaxSeq)
	w.stateMu.Unlock()
	if removeErr != nil {
		// Locations already point at the compacted copies (durable), so a
		// leftover old file is only wasted space; replay tolerates it.
		return removeErr
	}

	remaining, err := w.listSegments()
	if err != nil {
		return err
	}
	var retainedBytes int64
	for _, seg := range remaining {
		retainedBytes += seg.size
	}
	w.stateMu.Lock()
	w.lastCompaction = CompactionStats{
		ReclaimedSegments: reclaimedSegments,
		ReclaimedBytes:    reclaimedBytes,
		RetainedSegments:  len(remaining),
		RetainedBytes:     retainedBytes,
	}
	w.stateMu.Unlock()
	return nil
}

type snapshotRecord struct {
	groupID uint32
	typ     entryType
	payload []byte
}

// compactionLogRef identifies one live log entry to carry across compaction.
// The payload is NOT materialized up front: it is either referenced from the
// recent window (cached) or streamed back from its old segment when written,
// so compaction heap stays bounded regardless of log length.
type compactionLogRef struct {
	groupID uint32
	index   uint64
	loc     logLoc
	cached  []byte // payload when inside the recent window; nil → read from loc
}

// compactionSnapshot is the live state captured for a compaction run.
type compactionSnapshot struct {
	records []snapshotRecord   // group registrations + stable keys (small)
	logs    []compactionLogRef // live log entries
}

// collectCompactionSnapshot captures live WAL state under stateMu for
// compaction. Group state is frozen for the whole compaction run — the
// batchWriter is the only mutator and it is the one compacting — so the
// captured locations and cached payload references stay valid until the
// location swap.
func (w *WAL) collectCompactionSnapshot() compactionSnapshot {
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()

	type groupRef struct {
		name string
		id   uint32
	}
	refs := make([]groupRef, 0, len(w.groupIDs))
	for name, id := range w.groupIDs {
		refs = append(refs, groupRef{name: name, id: id})
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].id == refs[j].id {
			return refs[i].name < refs[j].name
		}
		return refs[i].id < refs[j].id
	})

	var snap compactionSnapshot
	for _, ref := range refs {
		payload := append([]byte(nil), []byte(ref.name)...)
		snap.records = append(snap.records, snapshotRecord{
			groupID: ref.id,
			typ:     entryGroupReg,
			payload: payload,
		})
	}

	groupIDs := make([]uint32, 0, len(w.groups))
	for gid := range w.groups {
		groupIDs = append(groupIDs, gid)
	}
	slices.Sort(groupIDs)

	for _, gid := range groupIDs {
		gs := w.groups[gid]
		if gs == nil {
			continue
		}

		stableKeys := make([]string, 0, len(gs.stable))
		for k := range gs.stable {
			stableKeys = append(stableKeys, k)
		}
		sort.Strings(stableKeys)
		for _, key := range stableKeys {
			val := gs.stable[key]
			payload := encodeStableSet(key, val)
			snap.records = append(snap.records, snapshotRecord{
				groupID: gid,
				typ:     entryStableSet,
				payload: payload,
			})
		}

		logIndexes := make([]uint64, 0, len(gs.logs))
		for idx := range gs.logs {
			logIndexes = append(logIndexes, idx)
		}
		slices.Sort(logIndexes)
		for _, idx := range logIndexes {
			ref := compactionLogRef{groupID: gid, index: idx, loc: gs.logs[idx]}
			if enc, ok := gs.cache[idx]; ok {
				ref.cached = enc
			}
			snap.logs = append(snap.logs, ref)
		}
	}
	return snap
}

// writeCompactionSnapshot appends the captured state to the fresh segment and
// returns the new location of every carried log entry, keyed by group.
func (w *WAL) writeCompactionSnapshot(snap compactionSnapshot) (map[uint32]map[uint64]logLoc, error) {
	for _, rec := range snap.records {
		if _, err := w.appendCompactedEntry(rec.groupID, rec.typ, rec.payload); err != nil {
			return nil, err
		}
	}
	newLocs := make(map[uint32]map[uint64]logLoc)
	for _, ref := range snap.logs {
		payload := ref.cached
		if payload == nil {
			var err error
			payload, err = w.readPayload(ref.loc)
			if err != nil {
				return nil, err
			}
		}
		loc, err := w.appendCompactedEntry(ref.groupID, entryLog, payload)
		if err != nil {
			return nil, err
		}
		locs := newLocs[ref.groupID]
		if locs == nil {
			locs = make(map[uint64]logLoc)
			newLocs[ref.groupID] = locs
		}
		locs[ref.index] = loc
	}
	return newLocs, nil
}

func (w *WAL) appendCompactedEntry(groupID uint32, typ entryType, payload []byte) (logLoc, error) {
	entrySize := int64(headerSize + len(payload))
	if w.segSize > 0 && w.segSize+entrySize > w.cfg.SegmentTargetSize {
		if err := w.rotateSegment(); err != nil {
			return logLoc{}, err
		}
	}
	loc := logLoc{seg: w.segSeq, off: w.segSize + headerSize, length: len(payload)}
	return loc, w.appendEntry(groupID, typ, payload)
}

// replaySegment reads a single WAL segment file and applies entries to memory.
// Streams the file to avoid loading 64MB segments into heap; tracks each
// record's payload offset so the log index can serve payloads from disk.
func (w *WAL) replaySegment(path string, seq int) error {
	f, err := os.Open(path) //nolint:gosec // G304: path constructed internally
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		return err
	}
	fileSize := fi.Size()

	hdr := make([]byte, headerSize)
	var off int64
	for {
		if _, err := io.ReadFull(f, hdr); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil // clean EOF or truncated header at end
			}
			return err
		}

		groupID := binary.LittleEndian.Uint32(hdr[0:4])
		typ := entryType(hdr[4])
		length := int(binary.LittleEndian.Uint32(hdr[5:9]))
		storedCRC := binary.LittleEndian.Uint32(hdr[9:13])

		if typ == 0 {
			// Valid entry types start at 1. A zero type means a zeroed
			// region — a preallocated tail or torn write. CRC32 of an
			// empty payload is 0, so a zero header would otherwise pass
			// the checksum and replay would walk the whole zero region.
			return nil
		}

		payloadOff := off + headerSize
		if int64(length) > fileSize-payloadOff {
			// A claimed payload length past EOF is a torn or corrupted
			// header. Stop BEFORE allocating: a garbage header can claim
			// up to 4GB and the blind allocation would blow the heap for
			// bytes that cannot exist.
			return nil
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			return nil // truncated payload — stop replay
		}

		if crc32.Checksum(payload, crc32Table) != storedCRC {
			return nil // corrupted entry — stop replay
		}

		w.applyToMemory(groupID, typ, payload, seq, payloadOff)
		off = payloadOff + int64(length)
	}
}

// applyLogEntry indexes one encoded raft.Log at its durable location and
// admits the payload to the recent window (bounded by budget bytes).
// Must be called with w.stateMu held for writing.
func (gs *groupState) applyLogEntry(payload []byte, loc logLoc, budget int64) {
	var log hraft.Log
	if err := decodelog(payload, &log); err != nil {
		return
	}
	gs.logs[log.Index] = loc
	if gs.firstIndex == 0 || log.Index < gs.firstIndex {
		gs.firstIndex = log.Index
	}
	if log.Index > gs.lastIndex {
		gs.lastIndex = log.Index
	}
	gs.cacheStore(log.Index, payload, budget)
}

// cacheStore admits a payload to the recent window and evicts the oldest
// cached payloads until the window fits the budget again.
func (gs *groupState) cacheStore(index uint64, payload []byte, budget int64) {
	if int64(len(payload)) > budget {
		// An oversized entry is never cached — admitting it would evict the
		// entire window for a single payload. GetLog serves it from disk.
		gs.cacheDrop(index)
		return
	}
	if old, ok := gs.cache[index]; ok {
		gs.cacheBytes -= int64(len(old))
	}
	gs.cache[index] = payload
	gs.cacheBytes += int64(len(payload))
	gs.cacheQueue.push(index)
	gs.cacheEvict(budget)
}

func (gs *groupState) cacheEvict(budget int64) {
	for gs.cacheBytes > budget {
		idx, ok := gs.cacheQueue.pop()
		if !ok {
			return
		}
		gs.cacheDrop(idx)
	}
	// Prune stale queue heads (indices already dropped by DeleteRange or
	// superseded by overwrite) so the queue tracks the live window.
	for {
		idx, ok := gs.cacheQueue.peek()
		if !ok {
			return
		}
		if _, cached := gs.cache[idx]; cached {
			return
		}
		_, _ = gs.cacheQueue.pop()
	}
}

func (gs *groupState) cacheDrop(index uint64) {
	if old, ok := gs.cache[index]; ok {
		gs.cacheBytes -= int64(len(old))
		delete(gs.cache, index)
	}
}

func (gs *groupState) applyDeleteRange(payload []byte) {
	lo, hi := decodeDeleteRange(payload)
	if hi < lo {
		return
	}
	for i := lo; i <= hi; i++ {
		delete(gs.logs, i)
		gs.cacheDrop(i)
	}
	// Match hashicorp/raft InmemStore.DeleteRange bound updates so suffix
	// truncation (AppendEntries conflict) does not erase the surviving prefix
	// or poison GetLog for indices that still exist.
	if lo <= gs.firstIndex {
		gs.firstIndex = hi + 1
	}
	if hi >= gs.lastIndex {
		gs.lastIndex = lo - 1
	}
	if gs.firstIndex > gs.lastIndex {
		gs.firstIndex = 0
		gs.lastIndex = 0
	}
}

// --- Encoding helpers ---

func encodelog(log *hraft.Log) []byte {
	// Simple encoding: [index:8][term:8][type:1][data:N][extensions:N]
	// Extensions length is prefixed with 4 bytes.
	extLen := len(log.Extensions)
	buf := make([]byte, 8+8+1+4+len(log.Data)+4+extLen)
	binary.LittleEndian.PutUint64(buf[0:8], log.Index)
	binary.LittleEndian.PutUint64(buf[8:16], log.Term)
	buf[16] = byte(log.Type)
	binary.LittleEndian.PutUint32(buf[17:21], uint32(len(log.Data))) //nolint:gosec // bounded by available memory
	copy(buf[21:21+len(log.Data)], log.Data)
	off := 21 + len(log.Data)
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(extLen)) //nolint:gosec // bounded by available memory
	copy(buf[off+4:], log.Extensions)
	return buf
}

func decodelog(data []byte, log *hraft.Log) error {
	if len(data) < 21 {
		return errors.New("short log entry")
	}
	log.Index = binary.LittleEndian.Uint64(data[0:8])
	log.Term = binary.LittleEndian.Uint64(data[8:16])
	log.Type = hraft.LogType(data[16])
	dataLen := int(binary.LittleEndian.Uint32(data[17:21]))
	if len(data) < 21+dataLen+4 {
		return errors.New("truncated log data")
	}
	log.Data = make([]byte, dataLen)
	copy(log.Data, data[21:21+dataLen])
	off := 21 + dataLen
	extLen := int(binary.LittleEndian.Uint32(data[off : off+4]))
	// Always assign Extensions (nil when absent): callers may reuse the same
	// hraft.Log across GetLog calls, and a stale Extensions slice from a
	// previous decode must not leak into this entry.
	log.Extensions = nil
	if extLen > 0 && off+4+extLen <= len(data) {
		log.Extensions = make([]byte, extLen)
		copy(log.Extensions, data[off+4:off+4+extLen])
	}
	return nil
}

// encodeLogBatch encodes a StoreLogs batch as ONE WAL record payload:
// [count:4] then per entry [len:4][encoded raft.Log]. The record header's
// single CRC covers the whole batch, so replay applies a batch all-or-nothing
// — a torn tail can never surface a half-applied batch.
func encodeLogBatch(logs []*hraft.Log) []byte {
	encs := make([][]byte, len(logs))
	size := logBatchCountSize
	for i, lg := range logs {
		encs[i] = encodelog(lg)
		size += logBatchEntryLenSize + len(encs[i])
	}
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[0:logBatchCountSize], uint32(len(logs))) //nolint:gosec // bounded by available memory
	off := logBatchCountSize
	for _, enc := range encs {
		binary.LittleEndian.PutUint32(buf[off:off+logBatchEntryLenSize], uint32(len(enc))) //nolint:gosec // bounded by available memory
		off += logBatchEntryLenSize
		off += copy(buf[off:], enc)
	}
	return buf
}

// forEachBatchEntry walks an entryLogBatch payload, invoking fn with each
// sub-entry's offset within the payload and its encoded bytes. Stops silently
// on a bounds violation — malformed payloads cannot pass the record CRC, so
// this is belt-and-braces like decodelog's length checks.
func forEachBatchEntry(payload []byte, fn func(off int, enc []byte)) {
	if len(payload) < logBatchCountSize {
		return
	}
	count := int(binary.LittleEndian.Uint32(payload[0:logBatchCountSize]))
	off := logBatchCountSize
	for range count {
		if off+logBatchEntryLenSize > len(payload) {
			return
		}
		n := int(binary.LittleEndian.Uint32(payload[off : off+logBatchEntryLenSize]))
		off += logBatchEntryLenSize
		if off+n > len(payload) {
			return
		}
		fn(off, payload[off:off+n])
		off += n
	}
}

func encodeStableSet(key string, val []byte) []byte {
	buf := make([]byte, 2+len(key)+len(val))
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(key))) //nolint:gosec // keys are short strings
	copy(buf[2:2+len(key)], key)
	copy(buf[2+len(key):], val)
	return buf
}

func decodeStableSet(data []byte) (string, []byte) {
	if len(data) < 2 {
		return "", nil
	}
	keyLen := int(binary.LittleEndian.Uint16(data[0:2]))
	if len(data) < 2+keyLen {
		return "", nil
	}
	key := string(data[2 : 2+keyLen])
	val := make([]byte, len(data)-2-keyLen)
	copy(val, data[2+keyLen:])
	return key, val
}

func encodeStableUint64(key string, val uint64) []byte {
	buf := make([]byte, 2+len(key)+8)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(key))) //nolint:gosec // keys are short strings
	copy(buf[2:2+len(key)], key)
	binary.LittleEndian.PutUint64(buf[2+len(key):], val)
	return buf
}

func decodeStableUint64(data []byte) (string, uint64) {
	if len(data) < 2 {
		return "", 0
	}
	keyLen := int(binary.LittleEndian.Uint16(data[0:2]))
	if len(data) < 2+keyLen+8 {
		return "", 0
	}
	key := string(data[2 : 2+keyLen])
	val := binary.LittleEndian.Uint64(data[2+keyLen:])
	return key, val
}

func encodeDeleteRange(lo, hi uint64) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], lo)
	binary.LittleEndian.PutUint64(buf[8:16], hi)
	return buf
}

func decodeDeleteRange(data []byte) (uint64, uint64) {
	if len(data) < 16 {
		return 0, 0
	}
	return binary.LittleEndian.Uint64(data[0:8]), binary.LittleEndian.Uint64(data[8:16])
}
