package memory

import (
	"context"
	"log/slog"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/logging"
)

type IndexStore[T any] interface {
	Get(chunkID chunk.ChunkID) ([]T, bool)
}

// Manager manages in-memory index storage.
//
// Logging:
//   - Logger is dependency-injected via NewManager
//   - Manager owns its scoped logger (component="index-manager", type="memory")
//   - Logging is intentionally sparse; only lifecycle events are logged
//   - No logging in hot paths (index lookups)
type Manager struct {
	indexers    []index.Indexer
	timeStore   IndexStore[index.TimeIndexEntry]
	sourceStore IndexStore[index.SourceIndexEntry]
	tokenStore  IndexStore[index.TokenIndexEntry]
	builder     *index.BuildHelper

	// Logger for this manager instance.
	// Scoped with component="index-manager", type="memory" at construction time.
	logger *slog.Logger
}

// NewManager creates an in-memory index manager.
// If logger is nil, logging is disabled.
func NewManager(
	indexers []index.Indexer,
	timeStore IndexStore[index.TimeIndexEntry],
	sourceStore IndexStore[index.SourceIndexEntry],
	tokenStore IndexStore[index.TokenIndexEntry],
	logger *slog.Logger,
) *Manager {
	return &Manager{
		indexers:    indexers,
		timeStore:   timeStore,
		sourceStore: sourceStore,
		tokenStore:  tokenStore,
		builder:     index.NewBuildHelper(),
		logger:      logging.Default(logger).With("component", "index-manager", "type", "memory"),
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

func (m *Manager) OpenTokenIndex(chunkID chunk.ChunkID) (*index.Index[index.TokenIndexEntry], error) {
	if m.tokenStore == nil {
		return nil, index.ErrIndexNotFound
	}
	entries, ok := m.tokenStore.Get(chunkID)
	if !ok {
		return nil, index.ErrIndexNotFound
	}
	return index.NewIndex(entries), nil
}
