package file

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
	"gastrolog/internal/logging"
)

// File names within a chunk directory.
const (
	rawLogFileName = "raw.log"
	idxLogFileName = "idx.log"
	// sourcesFileName is declared in sources.go
)

var (
	ErrMissingDir           = errors.New("file chunk manager dir is required")
	ErrMultipleActiveChunks = errors.New("multiple active chunks found")
	ErrManagerClosed        = errors.New("manager is closed")
)

type Config struct {
	Dir           string
	MaxChunkBytes int64 // Soft limit for raw.log size (0 = no soft limit)
	FileMode      os.FileMode
	Now           func() time.Time

	// Logger for structured logging. If nil, logging is disabled.
	// The manager scopes this logger with component="chunk-manager".
	Logger *slog.Logger
}

// Manager manages file-based chunk storage with split raw.log and idx.log files.
//
// File layout per chunk:
//   - raw.log: 4-byte header + concatenated raw log bytes
//   - idx.log: 4-byte header + fixed-size (28-byte) entries
//   - sources.bin: sourceID to localID mappings
//
// Position semantics: RecordRef.Pos is a record index (0, 1, 2, ...), not a byte offset.
//
// Logging:
//   - Logger is dependency-injected via Config.Logger
//   - Manager owns its scoped logger (component="chunk-manager", type="file")
//   - Logging is intentionally sparse; only lifecycle events are logged
//   - No logging in hot paths (Append, cursor iteration)
type Manager struct {
	mu      sync.Mutex
	cfg     Config
	active  *chunkState
	metas   map[chunk.ChunkID]*chunkMeta // In-memory chunk metadata
	sources map[chunk.ChunkID]*SourceMap
	closed  bool

	// Logger for this manager instance.
	// Scoped with component="chunk-manager", type="file" at construction time.
	logger *slog.Logger
}

// chunkMeta holds in-memory metadata derived from idx.log.
// No longer persisted to meta.bin.
type chunkMeta struct {
	id      chunk.ChunkID
	startTS time.Time // WriteTS of first record
	endTS   time.Time // WriteTS of last record
	sealed  bool
}

func (m *chunkMeta) toChunkMeta() chunk.ChunkMeta {
	return chunk.ChunkMeta{
		ID:      m.id,
		StartTS: m.startTS,
		EndTS:   m.endTS,
		Sealed:  m.sealed,
		// Size is no longer tracked; callers can stat files if needed.
	}
}

type chunkState struct {
	meta        *chunkMeta
	rawFile     *os.File
	idxFile     *os.File
	sources     *SourceMap
	rawOffset   uint64 // Current write position in raw.log (after header)
	recordCount uint64 // Number of records written
}

func NewManager(cfg Config) (*Manager, error) {
	if cfg.Dir == "" {
		return nil, ErrMissingDir
	}
	if cfg.FileMode == 0 {
		cfg.FileMode = 0o644
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}

	// Scope logger with component identity.
	logger := logging.Default(cfg.Logger).With("component", "chunk-manager", "type", "file")

	manager := &Manager{
		cfg:     cfg,
		metas:   make(map[chunk.ChunkID]*chunkMeta),
		sources: make(map[chunk.ChunkID]*SourceMap),
		logger:  logger,
	}
	if err := manager.loadExisting(); err != nil {
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

	rawLen := uint64(len(record.Raw))

	if m.active == nil || m.shouldRotate(rawLen) {
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

	localID, _, err := m.active.sources.GetOrAssign(record.SourceID)
	if err != nil {
		return chunk.ChunkID{}, 0, err
	}

	// Write raw data to raw.log.
	n, err := m.active.rawFile.Write(record.Raw)
	if err != nil {
		return chunk.ChunkID{}, 0, err
	}
	if n != len(record.Raw) {
		return chunk.ChunkID{}, 0, io.ErrShortWrite
	}

	// Build and write idx.log entry.
	entry := IdxEntry{
		IngestTS:      record.IngestTS,
		WriteTS:       record.WriteTS,
		SourceLocalID: localID,
		RawOffset:     uint32(m.active.rawOffset),
		RawSize:       uint32(rawLen),
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
	m.active.recordCount++

	// Update time bounds.
	if m.active.meta.startTS.IsZero() {
		m.active.meta.startTS = record.WriteTS
	}
	m.active.meta.endTS = record.WriteTS

	return m.active.meta.id, recordIndex, nil
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
	return out, nil
}

func (m *Manager) OpenCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	m.mu.Lock()
	meta, ok := m.metas[id]
	sourceMap := m.sources[id]
	m.mu.Unlock()
	if !ok {
		return nil, chunk.ErrChunkNotFound
	}
	if sourceMap == nil {
		var err error
		sourceMap, err = m.loadSourceMap(id)
		if err != nil {
			return nil, err
		}
	}

	rawPath := m.rawLogPath(id)
	idxPath := m.idxLogPath(id)

	if meta.sealed {
		return newMmapCursor(id, rawPath, idxPath, sourceMap.Resolve)
	}

	// Active chunk: use standard I/O reader.
	return newStdioCursor(id, rawPath, idxPath, sourceMap.Resolve)
}

func (m *Manager) loadExisting() error {
	entries, err := os.ReadDir(m.cfg.Dir)
	if err != nil {
		return err
	}

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

		sourceMap, err := m.loadSourceMap(id)
		if err != nil {
			return err
		}

		if !meta.sealed {
			if m.active != nil {
				return ErrMultipleActiveChunks
			}

			rawFile, err := m.openRawFile(id)
			if err != nil {
				return err
			}
			idxFile, err := m.openIdxFile(id)
			if err != nil {
				rawFile.Close()
				return err
			}

			// Compute rawOffset from raw.log file size.
			rawInfo, err := rawFile.Stat()
			if err != nil {
				rawFile.Close()
				idxFile.Close()
				return err
			}
			rawOffset := uint64(rawInfo.Size()) - uint64(format.HeaderSize)

			// Compute record count from idx.log file size.
			idxInfo, err := idxFile.Stat()
			if err != nil {
				rawFile.Close()
				idxFile.Close()
				return err
			}
			recordCount := RecordCount(idxInfo.Size())

			m.active = &chunkState{
				meta:        meta,
				rawFile:     rawFile,
				idxFile:     idxFile,
				sources:     sourceMap,
				rawOffset:   rawOffset,
				recordCount: recordCount,
			}
		}
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
	var headerBuf [format.HeaderSize]byte
	if _, err := io.ReadFull(idxFile, headerBuf[:]); err != nil {
		return nil, err
	}
	header, err := format.DecodeAndValidate(headerBuf[:], format.TypeIdxLog, IdxLogVersion)
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
		id:     id,
		sealed: sealed,
	}

	if recordCount == 0 {
		return meta, nil
	}

	// Read first entry for startTS.
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

func (m *Manager) shouldRotate(rawLen uint64) bool {
	if m.active == nil {
		return false
	}

	newRawOffset := m.active.rawOffset + rawLen

	// Hard limit: raw.log must stay under 4GB (uint32 max for rawOffset field).
	if newRawOffset > MaxRawLogSize {
		return true
	}

	// Soft limit: user-configurable max chunk size.
	if m.cfg.MaxChunkBytes > 0 && newRawOffset > uint64(m.cfg.MaxChunkBytes) {
		return true
	}

	return false
}

func (m *Manager) openLocked() error {
	id := chunk.NewChunkID()
	chunkDir := m.chunkDir(id)
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return err
	}

	// Create and initialize raw.log with header.
	rawFile, err := m.createRawFile(id)
	if err != nil {
		return err
	}

	// Create and initialize idx.log with header.
	idxFile, err := m.createIdxFile(id)
	if err != nil {
		rawFile.Close()
		return err
	}

	sourceMap := m.sourceMap(id)
	meta := &chunkMeta{
		id:     id,
		sealed: false,
	}

	m.active = &chunkState{
		meta:        meta,
		rawFile:     rawFile,
		idxFile:     idxFile,
		sources:     sourceMap,
		rawOffset:   0, // Data starts after header
		recordCount: 0,
	}
	m.metas[id] = meta
	m.sources[id] = sourceMap
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

func (m *Manager) createIdxFile(id chunk.ChunkID) (*os.File, error) {
	path := m.idxLogPath(id)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, m.cfg.FileMode)
	if err != nil {
		return nil, err
	}

	// Write header.
	header := format.Header{
		Type:    format.TypeIdxLog,
		Version: IdxLogVersion,
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

func (m *Manager) sealLocked() error {
	if m.active == nil {
		return nil
	}

	m.active.meta.sealed = true

	// Update sealed flag in both file headers.
	if err := m.setSealedFlag(m.active.rawFile); err != nil {
		return err
	}
	if err := m.setSealedFlag(m.active.idxFile); err != nil {
		return err
	}

	// Close files.
	if err := m.active.rawFile.Close(); err != nil {
		return err
	}
	if err := m.active.idxFile.Close(); err != nil {
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

	if m.active == nil {
		return nil
	}

	// Close files but don't seal (chunk remains active for recovery).
	var errs []error
	if err := m.active.rawFile.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := m.active.idxFile.Close(); err != nil {
		errs = append(errs, err)
	}

	m.active = nil

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

func (m *Manager) sourceMap(id chunk.ChunkID) *SourceMap {
	if sourceMap, ok := m.sources[id]; ok {
		return sourceMap
	}
	sourceMap := NewSourceMap(m.chunkDir(id), m.cfg.FileMode)
	m.sources[id] = sourceMap
	return sourceMap
}

func (m *Manager) loadSourceMap(id chunk.ChunkID) (*SourceMap, error) {
	m.mu.Lock()
	sourceMap, ok := m.sources[id]
	if !ok {
		sourceMap = NewSourceMap(m.chunkDir(id), m.cfg.FileMode)
		m.sources[id] = sourceMap
	}
	m.mu.Unlock()
	if err := sourceMap.Load(); err != nil {
		return nil, err
	}
	return sourceMap, nil
}

var _ chunk.ChunkManager = (*Manager)(nil)
