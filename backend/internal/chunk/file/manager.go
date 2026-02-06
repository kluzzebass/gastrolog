package file

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
	"gastrolog/internal/logging"
)

// File names within a chunk directory.
const (
	rawLogFileName  = "raw.log"
	idxLogFileName  = "idx.log"
	attrLogFileName = "attr.log"
)

var (
	ErrMissingDir      = errors.New("file chunk manager dir is required")
	ErrManagerClosed   = errors.New("manager is closed")
	ErrDirectoryLocked = errors.New("data directory is locked by another process")
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
	lockFile *os.File // Exclusive lock on data directory
	active   *chunkState
	metas    map[chunk.ChunkID]*chunkMeta // In-memory chunk metadata
	closed   bool

	// Logger for this manager instance.
	// Scoped with component="chunk-manager", type="file" at construction time.
	logger *slog.Logger
}

// chunkMeta holds in-memory metadata derived from idx.log.
// No longer persisted to meta.bin.
type chunkMeta struct {
	id          chunk.ChunkID
	startTS     time.Time // WriteTS of first record
	endTS       time.Time // WriteTS of last record
	recordCount int64     // Number of records in chunk
	sealed      bool
}

func (m *chunkMeta) toChunkMeta() chunk.ChunkMeta {
	return chunk.ChunkMeta{
		ID:          m.id,
		StartTS:     m.startTS,
		EndTS:       m.endTS,
		RecordCount: m.recordCount,
		Sealed:      m.sealed,
	}
}

type chunkState struct {
	meta        *chunkMeta
	rawFile     *os.File
	idxFile     *os.File
	attrFile    *os.File
	rawOffset   uint64    // Current write position in raw.log (after header)
	attrOffset  uint64    // Current write position in attr.log (after header)
	recordCount uint64    // Number of records written
	createdAt   time.Time // Wall-clock time when chunk was opened
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

	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}

	// Acquire exclusive lock on data directory.
	lockPath := filepath.Join(cfg.Dir, lockFileName)
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, cfg.FileMode)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lockFile.Close()
		return nil, fmt.Errorf("%w: %s", ErrDirectoryLocked, cfg.Dir)
	}

	// Scope logger with component identity.
	logger := logging.Default(cfg.Logger).With("component", "chunk-manager", "type", "file")

	manager := &Manager{
		cfg:      cfg,
		lockFile: lockFile,
		metas:    make(map[chunk.ChunkID]*chunkMeta),
		logger:   logger,
	}
	if err := manager.loadExisting(); err != nil {
		lockFile.Close()
		return nil, err
	}

	return manager, nil
}

func (m *Manager) Append(record chunk.Record) (chunk.ChunkID, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return chunk.ChunkID{}, 0, ErrManagerClosed
	}

	// Encode attributes upfront to check size limits and for policy decisions.
	attrBytes, err := record.Attrs.Encode()
	if err != nil {
		return chunk.ChunkID{}, 0, err
	}

	// Ensure we have an active chunk.
	if m.active == nil {
		if err := m.openLocked(); err != nil {
			return chunk.ChunkID{}, 0, err
		}
	}

	// Build state snapshot for rotation decision.
	state := m.activeChunkState()

	// Check rotation policy before append.
	if m.cfg.RotationPolicy.ShouldRotate(state, record) {
		m.logger.Info("rotating chunk",
			"chunk", state.ChunkID.String(),
			"bytes", state.Bytes,
			"records", state.Records,
			"age", m.cfg.Now().Sub(state.CreatedAt),
		)
		if err := m.sealLocked(); err != nil {
			return chunk.ChunkID{}, 0, err
		}
		if err := m.openLocked(); err != nil {
			return chunk.ChunkID{}, 0, err
		}
	}

	// WriteTS is assigned by the chunk manager, not the caller.
	// Monotonic by construction since writes are mutex-serialized.
	record.WriteTS = m.cfg.Now()

	rawLen := uint64(len(record.Raw))
	attrLen := uint64(len(attrBytes))

	// Write raw data to raw.log.
	n, err := m.active.rawFile.Write(record.Raw)
	if err != nil {
		return chunk.ChunkID{}, 0, err
	}
	if n != len(record.Raw) {
		return chunk.ChunkID{}, 0, io.ErrShortWrite
	}

	// Write attributes to attr.log.
	n, err = m.active.attrFile.Write(attrBytes)
	if err != nil {
		return chunk.ChunkID{}, 0, err
	}
	if n != len(attrBytes) {
		return chunk.ChunkID{}, 0, io.ErrShortWrite
	}

	// Build and write idx.log entry.
	entry := IdxEntry{
		SourceTS:   record.SourceTS,
		IngestTS:   record.IngestTS,
		WriteTS:    record.WriteTS,
		RawOffset:  uint32(m.active.rawOffset),
		RawSize:    uint32(rawLen),
		AttrOffset: uint32(m.active.attrOffset),
		AttrSize:   uint16(attrLen),
	}
	var entryBuf [IdxEntrySize]byte
	EncodeIdxEntry(entry, entryBuf[:])
	n, err = m.active.idxFile.Write(entryBuf[:])
	if err != nil {
		return chunk.ChunkID{}, 0, err
	}
	if n != IdxEntrySize {
		return chunk.ChunkID{}, 0, io.ErrShortWrite
	}

	// Update in-memory state.
	recordIndex := m.active.recordCount
	m.active.rawOffset += rawLen
	m.active.attrOffset += attrLen
	m.active.recordCount++
	m.active.meta.recordCount = int64(m.active.recordCount)

	// Update time bounds.
	if m.active.meta.startTS.IsZero() {
		m.active.meta.startTS = record.WriteTS
	}
	m.active.meta.endTS = record.WriteTS

	return m.active.meta.id, recordIndex, nil
}

// activeChunkState creates an immutable snapshot of the active chunk's state.
// Must be called with m.mu held.
func (m *Manager) activeChunkState() chunk.ActiveChunkState {
	if m.active == nil {
		return chunk.ActiveChunkState{}
	}

	// Calculate total on-disk bytes: raw + attrs + idx entries
	// (not counting headers, which are fixed overhead)
	totalBytes := m.active.rawOffset + m.active.attrOffset + (m.active.recordCount * IdxEntrySize)

	return chunk.ActiveChunkState{
		ChunkID:     m.active.meta.id,
		StartTS:     m.active.meta.startTS,
		LastWriteTS: m.active.meta.endTS,
		CreatedAt:   m.active.createdAt,
		Bytes:       totalBytes,
		Records:     m.active.recordCount,
	}
}

func (m *Manager) Seal() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrManagerClosed
	}

	if m.active == nil {
		if err := m.openLocked(); err != nil {
			return err
		}
	}
	return m.sealLocked()
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
	meta, ok := m.metas[id]
	m.mu.Unlock()
	if !ok {
		return chunk.ChunkMeta{}, chunk.ErrChunkNotFound
	}
	return meta.toChunkMeta(), nil
}

func (m *Manager) List() ([]chunk.ChunkMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]chunk.ChunkMeta, 0, len(m.metas))
	for _, meta := range m.metas {
		out = append(out, meta.toChunkMeta())
	}
	// Sort by StartTS to ensure consistent ordering.
	slices.SortFunc(out, func(a, b chunk.ChunkMeta) int {
		return a.StartTS.Compare(b.StartTS)
	})
	return out, nil
}

func (m *Manager) OpenCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	m.mu.Lock()
	meta, ok := m.metas[id]
	m.mu.Unlock()
	if !ok {
		return nil, chunk.ErrChunkNotFound
	}

	rawPath := m.rawLogPath(id)
	idxPath := m.idxLogPath(id)
	attrPath := m.attrLogPath(id)

	if meta.sealed {
		return newMmapCursor(id, rawPath, idxPath, attrPath)
	}

	return newStdioCursor(id, rawPath, idxPath, attrPath)
}

func (m *Manager) loadExisting() error {
	entries, err := os.ReadDir(m.cfg.Dir)
	if err != nil {
		return err
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

		meta, err := m.loadChunkMeta(id)
		if err != nil {
			return err
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
				return err
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
			return err
		}
	}

	return nil
}

// sealChunkOnDisk sets the sealed flag in the chunk's file headers without opening it as active.
func (m *Manager) sealChunkOnDisk(id chunk.ChunkID) error {
	rawPath := m.rawLogPath(id)
	idxPath := m.idxLogPath(id)
	attrPath := m.attrLogPath(id)

	// Set sealed flag in raw.log header.
	rawFile, err := os.OpenFile(rawPath, os.O_RDWR, m.cfg.FileMode)
	if err != nil {
		return err
	}
	if err := m.setSealedFlag(rawFile); err != nil {
		rawFile.Close()
		return err
	}
	rawFile.Close()

	// Set sealed flag in idx.log header.
	idxFile, err := os.OpenFile(idxPath, os.O_RDWR, m.cfg.FileMode)
	if err != nil {
		return err
	}
	if err := m.setSealedFlag(idxFile); err != nil {
		idxFile.Close()
		return err
	}
	idxFile.Close()

	// Set sealed flag in attr.log header.
	attrFile, err := os.OpenFile(attrPath, os.O_RDWR, m.cfg.FileMode)
	if err != nil {
		return err
	}
	if err := m.setSealedFlag(attrFile); err != nil {
		attrFile.Close()
		return err
	}
	attrFile.Close()

	return nil
}

// openActiveChunk opens an unsealed chunk as the active chunk, with crash recovery.
func (m *Manager) openActiveChunk(id chunk.ChunkID) error {
	meta := m.metas[id]

	rawFile, err := m.openRawFile(id)
	if err != nil {
		return err
	}
	idxFile, err := m.openIdxFile(id)
	if err != nil {
		rawFile.Close()
		return err
	}
	attrFile, err := m.openAttrFile(id)
	if err != nil {
		rawFile.Close()
		idxFile.Close()
		return err
	}

	closeAll := func() {
		rawFile.Close()
		idxFile.Close()
		attrFile.Close()
	}

	// Read idx.log header including createdAt timestamp.
	var headerBuf [IdxHeaderSize]byte
	if _, err := idxFile.ReadAt(headerBuf[:], 0); err != nil {
		closeAll()
		return err
	}
	createdAtMicros := binary.LittleEndian.Uint64(headerBuf[format.HeaderSize:])
	createdAt := time.UnixMicro(int64(createdAtMicros))

	// Compute record count from idx.log file size.
	idxInfo, err := idxFile.Stat()
	if err != nil {
		closeAll()
		return err
	}
	recordCount := RecordCount(idxInfo.Size())

	// Compute expected raw.log and attr.log sizes from idx.log.
	// If files have extra data (crash between writes), truncate them.
	var expectedRawSize, expectedAttrSize int64
	if recordCount > 0 {
		// Read last idx.log entry to get expected end positions.
		lastOffset := IdxFileOffset(recordCount - 1)
		var entryBuf [IdxEntrySize]byte
		if _, err := idxFile.ReadAt(entryBuf[:], lastOffset); err != nil {
			closeAll()
			return err
		}
		lastEntry := DecodeIdxEntry(entryBuf[:])
		expectedRawSize = int64(format.HeaderSize) + int64(lastEntry.RawOffset) + int64(lastEntry.RawSize)
		expectedAttrSize = int64(format.HeaderSize) + int64(lastEntry.AttrOffset) + int64(lastEntry.AttrSize)
	} else {
		expectedRawSize = int64(format.HeaderSize)
		expectedAttrSize = int64(format.HeaderSize)
	}

	// Truncate raw.log if needed.
	rawInfo, err := rawFile.Stat()
	if err != nil {
		closeAll()
		return err
	}
	if rawInfo.Size() > expectedRawSize {
		if err := rawFile.Truncate(expectedRawSize); err != nil {
			closeAll()
			return err
		}
		m.logger.Info("truncated orphaned raw.log data",
			"chunk", id.String(),
			"expected", expectedRawSize,
			"actual", rawInfo.Size())
	}

	// Truncate attr.log if needed.
	attrInfo, err := attrFile.Stat()
	if err != nil {
		closeAll()
		return err
	}
	if attrInfo.Size() > expectedAttrSize {
		if err := attrFile.Truncate(expectedAttrSize); err != nil {
			closeAll()
			return err
		}
		m.logger.Info("truncated orphaned attr.log data",
			"chunk", id.String(),
			"expected", expectedAttrSize,
			"actual", attrInfo.Size())
	}

	rawOffset := uint64(expectedRawSize) - uint64(format.HeaderSize)
	attrOffset := uint64(expectedAttrSize) - uint64(format.HeaderSize)

	m.active = &chunkState{
		meta:        meta,
		rawFile:     rawFile,
		idxFile:     idxFile,
		attrFile:    attrFile,
		rawOffset:   rawOffset,
		attrOffset:  attrOffset,
		recordCount: recordCount,
		createdAt:   createdAt,
	}

	return nil
}

// loadChunkMeta derives metadata from idx.log.
func (m *Manager) loadChunkMeta(id chunk.ChunkID) (*chunkMeta, error) {
	idxPath := m.idxLogPath(id)

	idxFile, err := os.Open(idxPath)
	if err != nil {
		return nil, err
	}
	defer idxFile.Close()

	// Read and validate header.
	var headerBuf [IdxHeaderSize]byte
	if _, err := io.ReadFull(idxFile, headerBuf[:]); err != nil {
		return nil, err
	}
	header, err := format.DecodeAndValidate(headerBuf[:format.HeaderSize], format.TypeIdxLog, IdxLogVersion)
	if err != nil {
		return nil, err
	}
	sealed := header.Flags&format.FlagSealed != 0

	info, err := idxFile.Stat()
	if err != nil {
		return nil, err
	}
	recordCount := RecordCount(info.Size())

	meta := &chunkMeta{
		id:          id,
		recordCount: int64(recordCount),
		sealed:      sealed,
	}

	if recordCount == 0 {
		return meta, nil
	}

	// Read first entry for startTS (already positioned after header from ReadFull above).
	var entryBuf [IdxEntrySize]byte
	if _, err := io.ReadFull(idxFile, entryBuf[:]); err != nil {
		return nil, err
	}
	firstEntry := DecodeIdxEntry(entryBuf[:])
	meta.startTS = firstEntry.WriteTS

	// Read last entry for endTS.
	lastOffset := IdxFileOffset(recordCount - 1)
	if _, err := idxFile.Seek(lastOffset, io.SeekStart); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(idxFile, entryBuf[:]); err != nil {
		return nil, err
	}
	lastEntry := DecodeIdxEntry(entryBuf[:])
	meta.endTS = lastEntry.WriteTS

	return meta, nil
}

func (m *Manager) openLocked() error {
	id := chunk.NewChunkID()
	chunkDir := m.chunkDir(id)
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
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
		rawFile.Close()
		return err
	}

	// Create and initialize attr.log with header.
	attrFile, err := m.createAttrFile(id)
	if err != nil {
		rawFile.Close()
		idxFile.Close()
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
		rawOffset:   0, // Data starts after header
		attrOffset:  0, // Data starts after header
		recordCount: 0,
		createdAt:   createdAt,
	}
	m.metas[id] = meta
	return nil
}

func (m *Manager) createRawFile(id chunk.ChunkID) (*os.File, error) {
	path := m.rawLogPath(id)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, m.cfg.FileMode)
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
		file.Close()
		return nil, err
	}

	return file, nil
}

func (m *Manager) createIdxFile(id chunk.ChunkID, createdAt time.Time) (*os.File, error) {
	path := m.idxLogPath(id)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, m.cfg.FileMode)
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
	binary.LittleEndian.PutUint64(buf[format.HeaderSize:], uint64(createdAt.UnixMicro()))

	if _, err := file.Write(buf[:]); err != nil {
		file.Close()
		return nil, err
	}

	return file, nil
}

func (m *Manager) createAttrFile(id chunk.ChunkID) (*os.File, error) {
	path := m.attrLogPath(id)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, m.cfg.FileMode)
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
		file.Close()
		return nil, err
	}

	return file, nil
}

func (m *Manager) openRawFile(id chunk.ChunkID) (*os.File, error) {
	path := m.rawLogPath(id)
	return os.OpenFile(path, os.O_RDWR|os.O_APPEND, m.cfg.FileMode)
}

func (m *Manager) openIdxFile(id chunk.ChunkID) (*os.File, error) {
	path := m.idxLogPath(id)
	return os.OpenFile(path, os.O_RDWR|os.O_APPEND, m.cfg.FileMode)
}

func (m *Manager) openAttrFile(id chunk.ChunkID) (*os.File, error) {
	path := m.attrLogPath(id)
	return os.OpenFile(path, os.O_RDWR|os.O_APPEND, m.cfg.FileMode)
}

func (m *Manager) sealLocked() error {
	if m.active == nil {
		return nil
	}

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

	m.active = nil
	return nil
}

func (m *Manager) setSealedFlag(file *os.File) error {
	// Seek to flags byte (offset 3 in header).
	if _, err := file.Seek(3, io.SeekStart); err != nil {
		return err
	}
	if _, err := file.Write([]byte{format.FlagSealed}); err != nil {
		return err
	}
	return file.Sync()
}

// Close closes the active chunk files without sealing.
// The manager should not be used after Close is called.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true

	var errs []error

	// Close active chunk files but don't seal (chunk remains active for recovery).
	if m.active != nil {
		if err := m.active.rawFile.Close(); err != nil {
			errs = append(errs, err)
		}
		if err := m.active.idxFile.Close(); err != nil {
			errs = append(errs, err)
		}
		if err := m.active.attrFile.Close(); err != nil {
			errs = append(errs, err)
		}
		m.active = nil
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

// FindStartPosition binary searches idx.log for the record at or before the given timestamp.
// Uses WriteTS for the search since it's monotonically increasing within a chunk.
func (m *Manager) FindStartPosition(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	m.mu.Lock()
	meta, ok := m.metas[id]
	m.mu.Unlock()
	if !ok {
		return 0, false, chunk.ErrChunkNotFound
	}

	// Quick bounds check using cached time bounds.
	if ts.Before(meta.startTS) {
		return 0, false, nil // Before all records
	}

	idxPath := m.idxLogPath(id)
	idxFile, err := os.Open(idxPath)
	if err != nil {
		return 0, false, err
	}
	defer idxFile.Close()

	info, err := idxFile.Stat()
	if err != nil {
		return 0, false, err
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
			return 0, false, err
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

// ReadWriteTimestamps reads the WriteTS for each given record position in a chunk.
// Opens idx.log once and reads only the 8-byte WriteTS field for each position.
func (m *Manager) ReadWriteTimestamps(id chunk.ChunkID, positions []uint64) ([]time.Time, error) {
	if len(positions) == 0 {
		return nil, nil
	}

	m.mu.Lock()
	_, ok := m.metas[id]
	m.mu.Unlock()
	if !ok {
		return nil, chunk.ErrChunkNotFound
	}

	idxPath := m.idxLogPath(id)
	idxFile, err := os.Open(idxPath)
	if err != nil {
		return nil, err
	}
	defer idxFile.Close()

	results := make([]time.Time, len(positions))
	var buf [8]byte

	for i, pos := range positions {
		offset := int64(IdxHeaderSize) + int64(pos)*int64(IdxEntrySize) + int64(idxWriteTSOffset)
		if _, err := idxFile.ReadAt(buf[:], offset); err != nil {
			return nil, fmt.Errorf("read WriteTS at position %d: %w", pos, err)
		}
		usec := int64(binary.LittleEndian.Uint64(buf[:]))
		results[i] = time.UnixMicro(usec)
	}

	return results, nil
}

// SetRotationPolicy updates the rotation policy for future appends.
func (m *Manager) SetRotationPolicy(policy chunk.RotationPolicy) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg.RotationPolicy = policy
}

var _ chunk.ChunkManager = (*Manager)(nil)
