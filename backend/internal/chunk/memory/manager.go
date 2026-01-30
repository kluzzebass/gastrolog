package memory

import (
	"log/slog"
	"sync"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/logging"
)

type Config struct {
	MaxChunkBytes int64
	Now           func() time.Time
	MetaStore     chunk.MetaStore

	// Logger for structured logging. If nil, logging is disabled.
	// The manager scopes this logger with component="chunk-manager".
	Logger *slog.Logger
}

// Manager manages in-memory chunk storage.
//
// Logging:
//   - Logger is dependency-injected via Config.Logger
//   - Manager owns its scoped logger (component="chunk-manager", type="memory")
//   - Logging is intentionally sparse; only lifecycle events are logged
//   - No logging in hot paths (Append, cursor iteration)
type Manager struct {
	mu     sync.Mutex
	cfg    Config
	active *chunkState
	chunks []*chunkState

	// Logger for this manager instance.
	// Scoped with component="chunk-manager", type="memory" at construction time.
	logger *slog.Logger
}

type chunkState struct {
	meta    chunk.ChunkMeta
	records []chunk.Record
	size    int64
}

func NewManager(cfg Config) (*Manager, error) {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MetaStore == nil {
		cfg.MetaStore = NewMetaStore()
	}

	// Scope logger with component identity.
	logger := logging.Default(cfg.Logger).With("component", "chunk-manager", "type", "memory")

	manager := &Manager{
		cfg:    cfg,
		chunks: make([]*chunkState, 0),
		logger: logger,
	}
	return manager, nil
}

func (m *Manager) Append(record chunk.Record) (chunk.ChunkID, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.active == nil || m.shouldRotate(1) {
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

	offset := uint64(len(m.active.records))
	m.active.records = append(m.active.records, record)
	m.active.size = int64(len(m.active.records))
	m.updateMetaLocked(record.WriteTS, m.active.size)

	if err := m.cfg.MetaStore.Save(m.active.meta); err != nil {
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
	state := m.findChunkLocked(id)
	m.mu.Unlock()
	if state == nil {
		return chunk.ChunkMeta{}, chunk.ErrChunkNotFound
	}
	return state.meta, nil
}

func (m *Manager) List() ([]chunk.ChunkMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]chunk.ChunkMeta, 0, len(m.chunks))
	for _, state := range m.chunks {
		out = append(out, state.meta)
	}
	return out, nil
}

func (m *Manager) OpenCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	m.mu.Lock()
	state := m.findChunkLocked(id)
	m.mu.Unlock()
	if state == nil {
		return nil, chunk.ErrChunkNotFound
	}
	return newRecordReader(state.records, id), nil
}

func (m *Manager) shouldRotate(nextSize int64) bool {
	if m.active == nil || m.cfg.MaxChunkBytes <= 0 {
		return false
	}
	return m.active.meta.Size+nextSize > m.cfg.MaxChunkBytes
}

func (m *Manager) openLocked() error {
	id := chunk.NewChunkID()
	meta := chunk.ChunkMeta{ID: id}
	if err := m.cfg.MetaStore.Save(meta); err != nil {
		return err
	}
	m.active = &chunkState{
		meta:    meta,
		records: nil,
		size:    0,
	}
	m.chunks = append(m.chunks, m.active)
	return nil
}

func (m *Manager) sealLocked() error {
	if m.active == nil {
		return nil
	}
	m.active.meta.Sealed = true
	if err := m.cfg.MetaStore.Save(m.active.meta); err != nil {
		return err
	}
	m.active = nil
	return nil
}

func (m *Manager) updateMetaLocked(ts time.Time, size int64) {
	if m.active.meta.StartTS.IsZero() {
		m.active.meta.StartTS = ts
	}
	m.active.meta.EndTS = ts
	m.active.meta.Size = size
}

func (m *Manager) findChunkLocked(id chunk.ChunkID) *chunkState {
	for _, state := range m.chunks {
		if state.meta.ID == id {
			return state
		}
	}
	return nil
}

// FindStartPosition binary searches for the record at or before the given timestamp.
// Uses WriteTS for the search since it's monotonically increasing within a chunk.
func (m *Manager) FindStartPosition(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	m.mu.Lock()
	state := m.findChunkLocked(id)
	m.mu.Unlock()
	if state == nil {
		return 0, false, chunk.ErrChunkNotFound
	}

	if len(state.records) == 0 {
		return 0, false, nil
	}

	// Quick bounds check.
	if ts.Before(state.records[0].WriteTS) {
		return 0, false, nil
	}

	// Binary search for the latest record with WriteTS <= ts.
	lo, hi := 0, len(state.records)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if state.records[mid].WriteTS.After(ts) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}

	if lo == 0 {
		return 0, false, nil
	}

	return uint64(lo - 1), true, nil
}

var _ chunk.ChunkManager = (*Manager)(nil)
