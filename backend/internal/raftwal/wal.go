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
// is started. Old segments are removed once all groups have advanced past them
// (via DeleteRange).
package raftwal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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
	entryGroupReg    entryType = 5 // group name → numeric ID registration
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
)

var (
	ErrNotFound   = errors.New("not found")
	ErrCompacted  = errors.New("log entry compacted")
	errWALClosed  = errors.New("wal closed")
	crc32Table    = crc32.MakeTable(crc32.Castagnoli)
)

// Config holds tunable parameters for the WAL.
type Config struct {
	// SegmentTargetSize is the target size for a WAL segment before rotation.
	// Default: 64MB.
	SegmentTargetSize int64

	// SyncBatchWindow is how long the writer waits to collect more writes
	// before fsyncing. Default: 1ms.
	SyncBatchWindow time.Duration
}

func (c Config) withDefaults() Config {
	if c.SegmentTargetSize <= 0 {
		c.SegmentTargetSize = segmentTargetSize
	}
	if c.SyncBatchWindow <= 0 {
		c.SyncBatchWindow = syncBatchWindow
	}
	return c
}

// WAL is the shared write-ahead log. Create one per node; all Raft
// groups on that node share it.
type WAL struct {
	mu       sync.Mutex
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

	// Batch writer: collects writes and fsyncs once per batch.
	writeCh chan writeOp
	syncCh  chan chan error // request a sync, get back the result
	done    chan struct{}
	wg      sync.WaitGroup
}

// groupState holds per-group in-memory state.
type groupState struct {
	// Log index: maps raft log index → WAL position for reads.
	// Only the most recent segment's entries are indexed; older
	// entries were already read by raft during startup.
	logs       map[uint64][]byte // index → serialized raft.Log
	firstIndex uint64
	lastIndex  uint64

	// Stable store: small key-value pairs (CurrentTerm, LastVotedFor).
	stable map[string][]byte

	// DeleteRange tracking: the lowest index that's been deleted.
	// Reads below this return ErrCompacted.
	deletedThrough uint64
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
	w.mu.Lock()

	gid, ok := w.groupIDs[name]
	needsReg := false
	if !ok {
		gid = w.nextGID
		w.nextGID++
		w.groupIDs[name] = gid
		if _, exists := w.groups[gid]; !exists {
			w.groups[gid] = &groupState{
				logs:   make(map[uint64][]byte),
				stable: make(map[string][]byte),
			}
		}
		needsReg = true
	}
	w.mu.Unlock()

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

// Close flushes pending writes and closes the WAL.
func (w *WAL) Close() error {
	close(w.done)
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
	op.done = make(chan error, 1)
	select {
	case w.writeCh <- op:
	case <-w.done:
		return errWALClosed
	}
	return <-op.done
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

// flushBatch writes all ops to the segment, fsyncs once, and notifies callers.
func (w *WAL) flushBatch(batch []writeOp) {
	if len(batch) == 0 {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	var writeErr error

	for i := range batch {
		if writeErr != nil {
			batch[i].done <- writeErr
			continue
		}
		// Rotate before writing if this entry would push the segment
		// past the target size. This keeps segments bounded and ensures
		// large payloads start on a fresh segment.
		entrySize := int64(headerSize + len(batch[i].payload))
		if w.segSize > 0 && w.segSize+entrySize > w.cfg.SegmentTargetSize {
			if err := w.rotateSegment(); err != nil {
				writeErr = err
				batch[i].done <- err
				continue
			}
		}
		if err := w.appendEntry(batch[i].groupID, batch[i].typ, batch[i].payload); err != nil {
			writeErr = err
			batch[i].done <- err
			continue
		}
		// Apply to in-memory state.
		w.applyToMemory(batch[i].groupID, batch[i].typ, batch[i].payload)
	}

	// Single fsync for the entire batch.
	syncErr := w.seg.Sync()

	// Notify all callers.
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
// Must be called with w.mu held.
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

// applyToMemory updates the in-memory index for a group.
// Must be called with w.mu held.
func (w *WAL) applyToMemory(groupID uint32, typ entryType, payload []byte) {
	gs := w.groups[groupID]
	if gs == nil {
		gs = &groupState{
			logs:   make(map[uint64][]byte),
			stable: make(map[string][]byte),
		}
		w.groups[groupID] = gs
	}

	switch typ {
	case entryLog:
		var log hraft.Log
		if err := decodelog(payload, &log); err != nil {
			return
		}
		gs.logs[log.Index] = payload
		if gs.firstIndex == 0 || log.Index < gs.firstIndex {
			gs.firstIndex = log.Index
		}
		if log.Index > gs.lastIndex {
			gs.lastIndex = log.Index
		}

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

// rotateSegment closes the current segment and opens a new one.
// Must be called with w.mu held.
func (w *WAL) rotateSegment() error {
	if w.seg != nil {
		if err := w.seg.Close(); err != nil {
			return fmt.Errorf("close segment: %w", err)
		}
	}

	w.segSeq++
	w.segPath = filepath.Join(w.dir, fmt.Sprintf("%s%06d%s", walFilePrefix, w.segSeq, walFileSuffix))
	f, err := os.OpenFile(w.segPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644) //nolint:gosec // G304: path is constructed internally
	if err != nil {
		return fmt.Errorf("open segment %s: %w", w.segPath, err)
	}
	w.seg = f
	w.segSize = 0
	return nil
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

	var segments []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), walFilePrefix) && strings.HasSuffix(e.Name(), walFileSuffix) {
			segments = append(segments, filepath.Join(w.dir, e.Name()))
		}
	}
	sort.Strings(segments) // lexicographic = chronological with zero-padded seq

	for _, seg := range segments {
		if err := w.replaySegment(seg); err != nil {
			return fmt.Errorf("replay %s: %w", seg, err)
		}
		// Track the highest segment sequence number.
		name := filepath.Base(seg)
		name = strings.TrimPrefix(name, walFilePrefix)
		name = strings.TrimSuffix(name, walFileSuffix)
		var seq int
		if _, err := fmt.Sscanf(name, "%d", &seq); err == nil && seq > w.segSeq {
			w.segSeq = seq
		}
	}
	return nil
}

// replaySegment reads a single WAL segment file and applies entries to memory.
// Streams the file with a buffered reader to avoid loading 64MB segments into heap.
func (w *WAL) replaySegment(path string) error {
	f, err := os.Open(path) //nolint:gosec // G304: path constructed internally
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	hdr := make([]byte, headerSize)
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

		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			return nil // truncated payload — stop replay
		}

		if crc32.Checksum(payload, crc32Table) != storedCRC {
			return nil // corrupted entry — stop replay
		}

		w.applyToMemory(groupID, typ, payload)
	}
}

func (gs *groupState) applyDeleteRange(payload []byte) {
	lo, hi := decodeDeleteRange(payload)
	for i := lo; i <= hi; i++ {
		delete(gs.logs, i)
	}
	if hi >= gs.deletedThrough {
		gs.deletedThrough = hi
	}
	if gs.deletedThrough >= gs.firstIndex {
		gs.firstIndex = gs.deletedThrough + 1
		if gs.firstIndex > gs.lastIndex {
			gs.firstIndex = 0
			gs.lastIndex = 0
		}
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
	if extLen > 0 && off+4+extLen <= len(data) {
		log.Extensions = make([]byte, extLen)
		copy(log.Extensions, data[off+4:off+4+extLen])
	}
	return nil
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
