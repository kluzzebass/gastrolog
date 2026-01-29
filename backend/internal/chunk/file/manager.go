package file

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gastrolog/internal/chunk"
)

var ErrMissingDir = errors.New("file chunk manager dir is required")
var ErrMultipleActiveChunks = errors.New("multiple active chunks found")

// Default meta flush interval.
const DefaultMetaFlushInterval = 5 * time.Second

type Config struct {
	Dir           string
	MaxChunkBytes int64
	FileMode      os.FileMode
	Now           func() time.Time
	MetaStore     chunk.MetaStore

	// MetaFlushInterval controls how often dirty metadata is flushed to disk.
	// Zero means use DefaultMetaFlushInterval. Negative means flush on every write.
	MetaFlushInterval time.Duration
}

type Manager struct {
	mu      sync.Mutex
	cfg     Config
	active  *chunkState
	metas   map[chunk.ChunkID]chunk.ChunkMeta
	sources map[chunk.ChunkID]*SourceMap

	// Meta flush state.
	metaDirty     bool
	lastMetaFlush time.Time
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

	manager := &Manager{
		cfg:     cfg,
		metas:   make(map[chunk.ChunkID]chunk.ChunkMeta),
		sources: make(map[chunk.ChunkID]*SourceMap),
	}
	if err := manager.loadExisting(); err != nil {
		return nil, err
	}
	return manager, nil
}

func (m *Manager) Append(record chunk.Record) (chunk.ChunkID, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

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

	// Flush meta periodically or on every write if interval is negative.
	if err := m.maybeFlushMetaLocked(record.WriteTS); err != nil {
		return chunk.ChunkID{}, 0, err
	}

	return m.active.meta.ID, offset, nil
}

func (m *Manager) Seal() error {
	m.mu.Lock()
	defer m.mu.Unlock()
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

	// Always flush meta on seal.
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

// maybeFlushMetaLocked flushes metadata if dirty and the flush interval has elapsed.
// If MetaFlushInterval is negative, flushes on every call (legacy behavior).
// The writeTS parameter is used to avoid an extra clock call.
func (m *Manager) maybeFlushMetaLocked(writeTS time.Time) error {
	if !m.metaDirty || m.active == nil {
		return nil
	}

	// Negative interval means flush every write.
	if m.cfg.MetaFlushInterval < 0 {
		if err := m.cfg.MetaStore.Save(m.active.meta); err != nil {
			return err
		}
		m.metaDirty = false
		m.lastMetaFlush = writeTS
		return nil
	}

	// Check if enough time has passed since last flush.
	if writeTS.Sub(m.lastMetaFlush) >= m.cfg.MetaFlushInterval {
		if err := m.cfg.MetaStore.Save(m.active.meta); err != nil {
			return err
		}
		m.metaDirty = false
		m.lastMetaFlush = writeTS
	}

	return nil
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
	m.lastMetaFlush = m.cfg.Now()
	return nil
}

// Close flushes any dirty metadata and closes the active chunk file.
// The manager should not be used after Close is called.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

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
