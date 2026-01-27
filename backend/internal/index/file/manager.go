package file

import (
	"context"
	"fmt"

	"github.com/kluzzebass/gastrolog/internal/chunk"
	"github.com/kluzzebass/gastrolog/internal/index"
	filesource "github.com/kluzzebass/gastrolog/internal/index/file/source"
	filetime "github.com/kluzzebass/gastrolog/internal/index/file/time"
)

type Manager struct {
	dir      string
	indexers []index.Indexer
}

func NewManager(dir string, indexers []index.Indexer) *Manager {
	return &Manager{
		dir:      dir,
		indexers: indexers,
	}
}

func (m *Manager) BuildIndexes(ctx context.Context, chunkID chunk.ChunkID) error {
	for _, idx := range m.indexers {
		if err := idx.Build(ctx, chunkID); err != nil {
			return fmt.Errorf("build %s index: %w", idx.Name(), err)
		}
	}
	return nil
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
