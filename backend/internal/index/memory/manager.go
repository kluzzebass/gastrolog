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

// AttrIndexStore provides access to all three attribute index types.
type AttrIndexStore interface {
	GetKey(chunkID chunk.ChunkID) ([]index.AttrKeyIndexEntry, bool)
	GetValue(chunkID chunk.ChunkID) ([]index.AttrValueIndexEntry, bool)
	GetKV(chunkID chunk.ChunkID) ([]index.AttrKVIndexEntry, bool)
}

// Manager manages in-memory index storage.
//
// Logging:
//   - Logger is dependency-injected via NewManager
//   - Manager owns its scoped logger (component="index-manager", type="memory")
//   - Logging is intentionally sparse; only lifecycle events are logged
//   - No logging in hot paths (index lookups)
type Manager struct {
	indexers   []index.Indexer
	timeStore  IndexStore[index.TimeIndexEntry]
	tokenStore IndexStore[index.TokenIndexEntry]
	attrStore  AttrIndexStore
	builder    *index.BuildHelper

	// Logger for this manager instance.
	// Scoped with component="index-manager", type="memory" at construction time.
	logger *slog.Logger
}

// NewManager creates an in-memory index manager.
// If logger is nil, logging is disabled.
func NewManager(
	indexers []index.Indexer,
	timeStore IndexStore[index.TimeIndexEntry],
	tokenStore IndexStore[index.TokenIndexEntry],
	attrStore AttrIndexStore,
	logger *slog.Logger,
) *Manager {
	return &Manager{
		indexers:   indexers,
		timeStore:  timeStore,
		tokenStore: tokenStore,
		attrStore:  attrStore,
		builder:    index.NewBuildHelper(),
		logger:     logging.Default(logger).With("component", "index-manager", "type", "memory"),
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

func (m *Manager) OpenAttrKeyIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrKeyIndexEntry], error) {
	if m.attrStore == nil {
		return nil, index.ErrIndexNotFound
	}
	entries, ok := m.attrStore.GetKey(chunkID)
	if !ok {
		return nil, index.ErrIndexNotFound
	}
	return index.NewIndex(entries), nil
}

func (m *Manager) OpenAttrValueIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrValueIndexEntry], error) {
	if m.attrStore == nil {
		return nil, index.ErrIndexNotFound
	}
	entries, ok := m.attrStore.GetValue(chunkID)
	if !ok {
		return nil, index.ErrIndexNotFound
	}
	return index.NewIndex(entries), nil
}

func (m *Manager) OpenAttrKVIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrKVIndexEntry], error) {
	if m.attrStore == nil {
		return nil, index.ErrIndexNotFound
	}
	entries, ok := m.attrStore.GetKV(chunkID)
	if !ok {
		return nil, index.ErrIndexNotFound
	}
	return index.NewIndex(entries), nil
}

// IndexesComplete reports whether all indexes exist for the given chunk.
// For in-memory indexes, this checks if all stores have entries for the chunk.
func (m *Manager) IndexesComplete(chunkID chunk.ChunkID) (bool, error) {
	if _, ok := m.timeStore.Get(chunkID); !ok {
		return false, nil
	}
	if m.tokenStore != nil {
		if _, ok := m.tokenStore.Get(chunkID); !ok {
			return false, nil
		}
	}
	if m.attrStore != nil {
		if _, ok := m.attrStore.GetKey(chunkID); !ok {
			return false, nil
		}
		if _, ok := m.attrStore.GetValue(chunkID); !ok {
			return false, nil
		}
		if _, ok := m.attrStore.GetKV(chunkID); !ok {
			return false, nil
		}
	}
	return true, nil
}
