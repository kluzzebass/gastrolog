package index

import (
	"context"

	"gastrolog/internal/chunk"
)

// BuilderAdapter wraps a set of indexers into a chunk.ChunkIndexBuilder.
// It reuses BuildHelper for call deduplication and parallel fan-out.
type BuilderAdapter struct {
	helper   *BuildHelper
	indexers []Indexer
}

// NewBuilderAdapter creates a ChunkIndexBuilder from a set of indexers.
func NewBuilderAdapter(indexers []Indexer) *BuilderAdapter {
	return &BuilderAdapter{
		helper:   NewBuildHelper(),
		indexers: indexers,
	}
}

// Build implements chunk.ChunkIndexBuilder.
func (a *BuilderAdapter) Build(ctx context.Context, chunkID chunk.ChunkID) error {
	return a.helper.Build(ctx, chunkID, a.indexers)
}
