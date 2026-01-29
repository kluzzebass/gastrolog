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
	"gastrolog/internal/logging"
)

var ErrMissingDir = errors.New("file chunk manager dir is required")
var ErrMultipleActiveChunks = errors.New("multiple active chunks found")
var ErrManagerClosed = errors.New("manager is closed")

// Default meta flush interval.
const DefaultMetaFlushInterval = 5 * time.Second

type Config struct {
	Dir           string
	MaxChunkBytes int64
	FileMode      os.FileMode
	Now           func() time.Time
	MetaStore     chunk.MetaStore

	// MetaFlushInterval controls how often dirty metadata is flushed to disk.
	// Zero means use DefaultMetaFlushInterval. Negative disables background
	// flushing (meta is only written on Seal/Close).
	MetaFlushInterval time.Duration

	// Logger for structured logging. If nil, logging is disabled.
	// The manager scopes this logger with component="chunk-manager".
	Logger *slog.Logger
}

// Manager manages file-based chunk storage.
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
	metas   map[chunk.ChunkID]chunk.ChunkMeta
	sources map[chunk.ChunkID]*SourceMap
	closed  bool

	// Async meta flush.
	metaDirty   bool
	flushStopCh chan struct{}
	flushWg     sync.WaitGroup

	// Logger for this manager instance.
	// Scoped with component="chunk-manager", type="file" at construction time.
	logger *slog.Logger
}

type chunkState struct {
	meta    chunk.ChunkMeta
	file    *os.File
	sources *SourceMap
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
	if cfg.MetaStore == nil {
		cfg.MetaStore = NewMetaStore(cfg.Dir, cfg.FileMode)
	}
	if cfg.MetaFlushInterval == 0 {
		cfg.MetaFlushInterval = DefaultMetaFlushInterval
	}

	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}

	// Scope logger with component identity.
	logger := logging.Default(cfg.Logger).With("component", "chunk-manager", "type", "file")

	manager := &Manager{
		cfg:         cfg,
		metas:       make(map[chunk.ChunkID]chunk.ChunkMeta),
		sources:     make(map[chunk.ChunkID]*SourceMap),
		flushStopCh: make(chan struct{}),
		logger:      logger,
	}
	if err := manager.loadExisting(); err != nil {
		return nil, err
	}

	// Start background flush goroutine if interval is positive.
	if cfg.MetaFlushInterval > 0 {
		manager.flushWg.Add(1)
		go manager.flushLoop()
	}

	return manager, nil
}

// flushLoop periodically flushes dirty metadata in the background.
func (m *Manager) flushLoop() {
	defer m.flushWg.Done()

	ticker := time.NewTicker(m.cfg.MetaFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.flushStopCh:
			return
		case <-ticker.C:
			m.mu.Lock()
			if m.metaDirty && m.active != nil {
				// Best effort - ignore errors in background flush.
				// Final flush on Close() will report errors.
				_ = m.cfg.MetaStore.Save(m.active.meta)
				m.metaDirty = false
			}
			m.mu.Unlock()
		}
	}
}

func (m *Manager) Append(record chunk.Record) (chunk.ChunkID, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return chunk.ChunkID{}, 0, ErrManagerClosed
	}

	recordSize, err := RecordSize(len(record.Raw))
	if err != nil {
		return chunk.ChunkID{}, 0, err
	}

	if m.active == nil || m.shouldRotate(int64(recordSize)) {
		if err := m.sealLocked(); err != nil {
			return chunk.ChunkID{}, 0, err
		}
		if err := m.openLocked(); err != nil {
			return chunk.ChunkID{}, 0, err
		}
	}

	// WriteTS is assigned by the chunk manager, not the caller.
	// Monotonic by construction since writes are mutex-serialized.
	// Uses m.cfg.Now (defaults to time.Now) so tests can inject a fake clock.
	record.WriteTS = m.cfg.Now()

	localID, _, err := m.active.sources.GetOrAssign(record.SourceID)
	if err != nil {
		return chunk.ChunkID{}, 0, err
	}
	offset, size, err := appendRecord(m.active.file, record, localID)
	if err != nil {
		return chunk.ChunkID{}, 0, err
	}
	m.updateMetaLocked(record.WriteTS, offset, size)
	m.metas[m.active.meta.ID] = m.active.meta
	m.metaDirty = true

	// Meta flush happens asynchronously in flushLoop, not here.

	return m.active.meta.ID, offset, nil
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
	meta := m.active.meta
	return &meta
}

func (m *Manager) Meta(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	m.mu.Lock()
	meta, ok := m.metas[id]
	m.mu.Unlock()
	if !ok {
		return chunk.ChunkMeta{}, chunk.ErrChunkNotFound
	}
	return meta, nil
}

func (m *Manager) List() ([]chunk.ChunkMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]chunk.ChunkMeta, 0, len(m.metas))
	for _, meta := range m.metas {
		out = append(out, meta)
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

	recordsPath := m.recordsPath(id)
	if meta.Sealed {
		info, err := os.Stat(recordsPath)
		if err != nil {
			return nil, err
		}
		if info.Size() == 0 {
			r, err := OpenReader(recordsPath)
			if err != nil {
				return nil, err
			}
			return newRecordReader(r, sourceMap.Resolve, id, 0), nil
		}
		r, err := OpenMmapReader(recordsPath)
		if err != nil {
			return nil, err
		}
		return newRecordReader(r, sourceMap.Resolve, id, int64(len(r.data))), nil
	}

	r, err := OpenReader(recordsPath)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(recordsPath)
	if err != nil {
		r.Close()
		return nil, err
	}
	return newRecordReader(r, sourceMap.Resolve, id, info.Size()), nil
}

func (m *Manager) loadExisting() error {
	metas, err := m.cfg.MetaStore.List()
	if err != nil {
		return err
	}
	for _, meta := range metas {
		m.metas[meta.ID] = meta
		sourceMap, err := m.loadSourceMap(meta.ID)
		if err != nil {
			return err
		}
		if !meta.Sealed {
			if m.active != nil {
				return ErrMultipleActiveChunks
			}
			file, err := m.openFile(meta.ID)
			if err != nil {
				return err
			}
			m.active = &chunkState{meta: meta, file: file, sources: sourceMap}
		}
	}
	return nil
}

func (m *Manager) shouldRotate(nextSize int64) bool {
	if m.active == nil || m.cfg.MaxChunkBytes <= 0 {
		return false
	}
	return m.active.meta.Size+nextSize > m.cfg.MaxChunkBytes
}

func (m *Manager) openLocked() error {
	id := chunk.NewChunkID()
	chunkDir := m.chunkDir(id)
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return err
	}
	file, err := m.openFile(id)
	if err != nil {
		return err
	}
	sourceMap := m.sourceMap(id)
	meta := chunk.ChunkMeta{
		ID:     id,
		Sealed: false,
	}
	// Write initial meta synchronously - this is rare (only on new chunk).
	if err := m.cfg.MetaStore.Save(meta); err != nil {
		_ = file.Close()
		return err
	}
	m.active = &chunkState{
		meta:    meta,
		file:    file,
		sources: sourceMap,
	}
	m.metas[id] = meta
	m.sources[id] = sourceMap
	return nil
}

func (m *Manager) openFile(id chunk.ChunkID) (*os.File, error) {
	recordsPath := m.recordsPath(id)
	return os.OpenFile(recordsPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, m.cfg.FileMode)
}

func (m *Manager) sealLocked() error {
	if m.active == nil {
		return nil
	}
	m.active.meta.Sealed = true
	m.metas[m.active.meta.ID] = m.active.meta

	// Always flush meta synchronously on seal.
	if err := m.cfg.MetaStore.Save(m.active.meta); err != nil {
		return err
	}
	m.metaDirty = false

	if err := m.active.file.Close(); err != nil {
		return err
	}
	m.active = nil
	return nil
}

func (m *Manager) updateMetaLocked(ts time.Time, offset uint64, size uint32) {
	if m.active.meta.StartTS.IsZero() {
		m.active.meta.StartTS = ts
	}
	m.active.meta.EndTS = ts
	m.active.meta.Size = int64(offset) + int64(size)
}

// Flush persists any dirty metadata to disk immediately.
// Call this before graceful shutdown to minimize data loss.
func (m *Manager) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.metaDirty || m.active == nil {
		return nil
	}

	if err := m.cfg.MetaStore.Save(m.active.meta); err != nil {
		return err
	}
	m.metaDirty = false
	return nil
}

// Close stops the background flush goroutine, flushes any dirty metadata,
// and closes the active chunk file. The manager should not be used after
// Close is called.
func (m *Manager) Close() error {
	// Stop background flush goroutine first.
	close(m.flushStopCh)
	m.flushWg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true

	if m.active == nil {
		return nil
	}

	// Flush dirty meta.
	if m.metaDirty {
		if err := m.cfg.MetaStore.Save(m.active.meta); err != nil {
			return err
		}
		m.metaDirty = false
	}

	// Close file but don't seal (chunk remains active for recovery).
	if err := m.active.file.Close(); err != nil {
		return err
	}
	m.active.file = nil
	return nil
}

func (m *Manager) chunkDir(id chunk.ChunkID) string {
	return filepath.Join(m.cfg.Dir, id.String())
}

func (m *Manager) recordsPath(id chunk.ChunkID) string {
	return filepath.Join(m.chunkDir(id), recordsFileName)
}

func appendRecord(file *os.File, record chunk.Record, localID uint32) (uint64, uint32, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, 0, err
	}
	offset := uint64(info.Size())
	buf, err := EncodeRecord(record, localID)
	if err != nil {
		return offset, 0, err
	}
	n, err := file.Write(buf)
	if err != nil {
		return offset, 0, err
	}
	if n != len(buf) {
		return offset, 0, io.ErrShortWrite
	}
	return offset, uint32(len(buf)), nil
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
