package memory

import (
	"context"

	"github.com/kluzzebass/gastrolog/internal/chunk"
	"github.com/kluzzebass/gastrolog/internal/index"
)

type IndexStore[T any] interface {
	Get(chunkID chunk.ChunkID) ([]T, bool)
}

type Manager struct {
	indexers    []index.Indexer
	timeStore   IndexStore[index.TimeIndexEntry]
	sourceStore IndexStore[index.SourceIndexEntry]
	builder     *index.BuildHelper
}

func NewManager(indexers []index.Indexer, timeStore IndexStore[index.TimeIndexEntry], sourceStore IndexStore[index.SourceIndexEntry]) *Manager {
	return &Manager{
		indexers:    indexers,
		timeStore:   timeStore,
		sourceStore: sourceStore,
		builder:     index.NewBuildHelper(),
	}
}

func (m *Manager) BuildIndexes(ctx context.Context, chunkID chunk.ChunkID) error {
	return m.builder.Build(ctx, chunkID, m.indexers)
}

func (m *Manager) OpenTimeIndex(chunkID chunk.ChunkID) (*index.Index[index.TimeIndexEntry], error) {
	entries, ok := m.timeStore.Get(chunkID)
	if !ok {
		return nil, index.ErrIndexNotFound
	}
	return index.NewIndex(entries), nil
}

func (m *Manager) OpenSourceIndex(chunkID chunk.ChunkID) (*index.Index[index.SourceIndexEntry], error) {
	entries, ok := m.sourceStore.Get(chunkID)
	if !ok {
		return nil, index.ErrIndexNotFound
	}
	return index.NewIndex(entries), nil
}
