package memory

import (
	"sync"

	"gastrolog/internal/chunk"
)

type MetaStore struct {
	mu    sync.Mutex
	metas map[chunk.ChunkID]chunk.ChunkMeta
}

func NewMetaStore() *MetaStore {
	return &MetaStore{
		metas: make(map[chunk.ChunkID]chunk.ChunkMeta),
	}
}

func (s *MetaStore) Save(meta chunk.ChunkMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metas[meta.ID] = meta
	return nil
}

func (s *MetaStore) Load(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, ok := s.metas[id]
	if !ok {
		return chunk.ChunkMeta{}, chunk.ErrChunkNotFound
	}
	return meta, nil
}

func (s *MetaStore) List() ([]chunk.ChunkMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]chunk.ChunkMeta, 0, len(s.metas))
	for _, meta := range s.metas {
		out = append(out, meta)
	}
	return out, nil
}

var _ chunk.MetaStore = (*MetaStore)(nil)
