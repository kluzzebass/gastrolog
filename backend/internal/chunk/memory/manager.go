package memory

import (
	"sync"
	"time"

	"github.com/kluzzebass/gastrolog/internal/chunk"
)

type Config struct {
	MaxChunkBytes int64
	Now           func() time.Time
	MetaStore     chunk.MetaStore
}

type Manager struct {
	mu     sync.Mutex
	cfg    Config
	active *chunkState
	chunks []*chunkState
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
	manager := &Manager{
		cfg:    cfg,
		chunks: make([]*chunkState, 0),
	}
	return manager, nil
}

func (m *Manager) Append(record chunk.Record) (chunk.ChunkID, int64, error) {
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

	offset := int64(len(m.active.records))
	m.active.records = append(m.active.records, record)
	m.active.size = int64(len(m.active.records))
	m.updateMetaLocked(record.IngestTS, m.active.size)

	if err := m.cfg.MetaStore.Save(m.active.meta); err != nil {
		return chunk.ChunkID{}, 0, err
	}

	return m.active.meta.ID, offset, nil
}

func (m *Manager) Seal() error {
	m.mu.Lock()
	defer m.mu.Unlock()
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

func (m *Manager) List() ([]chunk.ChunkMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]chunk.ChunkMeta, 0, len(m.chunks))
	for _, state := range m.chunks {
		out = append(out, state.meta)
	}
	return out, nil
}

func (m *Manager) OpenReader(id chunk.ChunkID) (chunk.RecordReader, error) {
	m.mu.Lock()
	state := m.findChunkLocked(id)
	m.mu.Unlock()
	if state == nil {
		return nil, chunk.ErrChunkNotFound
	}
	if !state.meta.Sealed {
		return nil, chunk.ErrChunkNotSealed
	}
	return newRecordReader(state.records), nil
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
	micros := ts.UnixMicro()
	if ts.IsZero() {
		micros = 0
	}
	if m.active.meta.StartTS == 0 {
		m.active.meta.StartTS = micros
	}
	m.active.meta.EndTS = micros
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

var _ chunk.ChunkManager = (*Manager)(nil)
