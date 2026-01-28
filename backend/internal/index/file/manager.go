package file

import (
	"context"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	filesource "gastrolog/internal/index/file/source"
	filetime "gastrolog/internal/index/file/time"
	filetoken "gastrolog/internal/index/file/token"
)

type Manager struct {
	dir      string
	indexers []index.Indexer
	builder  *index.BuildHelper
}

func NewManager(dir string, indexers []index.Indexer) *Manager {
	return &Manager{
		dir:      dir,
		indexers: indexers,
		builder:  index.NewBuildHelper(),
	}
}

func (m *Manager) BuildIndexes(ctx context.Context, chunkID chunk.ChunkID) error {
	return m.builder.Build(ctx, chunkID, m.indexers)
}

func (m *Manager) OpenTimeIndex(chunkID chunk.ChunkID) (*index.Index[index.TimeIndexEntry], error) {
	entries, err := filetime.LoadIndex(m.dir, chunkID)
	if err != nil {
		return nil, fmt.Errorf("open time index: %w", err)
	}
	return index.NewIndex(entries), nil
}

func (m *Manager) OpenSourceIndex(chunkID chunk.ChunkID) (*index.Index[index.SourceIndexEntry], error) {
	entries, err := filesource.LoadIndex(m.dir, chunkID)
	if err != nil {
		return nil, fmt.Errorf("open source index: %w", err)
	}
	return index.NewIndex(entries), nil
}

func (m *Manager) OpenTokenIndex(chunkID chunk.ChunkID) (*index.Index[index.TokenIndexEntry], error) {
	entries, err := filetoken.LoadIndex(m.dir, chunkID)
	if err != nil {
		return nil, fmt.Errorf("open token index: %w", err)
	}
	return index.NewIndex(entries), nil
}
