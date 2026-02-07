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
	Delete(chunkID chunk.ChunkID)
}

// AttrIndexStore provides access to all three attribute index types.
type AttrIndexStore interface {
	GetKey(chunkID chunk.ChunkID) ([]index.AttrKeyIndexEntry, bool)
	GetValue(chunkID chunk.ChunkID) ([]index.AttrValueIndexEntry, bool)
	GetKV(chunkID chunk.ChunkID) ([]index.AttrKVIndexEntry, bool)
	Delete(chunkID chunk.ChunkID)
}

// KVIndexStore provides access to all three kv index types.
type KVIndexStore interface {
	GetKey(chunkID chunk.ChunkID) ([]index.KVKeyIndexEntry, index.KVIndexStatus, bool)
	GetValue(chunkID chunk.ChunkID) ([]index.KVValueIndexEntry, index.KVIndexStatus, bool)
	GetKV(chunkID chunk.ChunkID) ([]index.KVIndexEntry, index.KVIndexStatus, bool)
	Delete(chunkID chunk.ChunkID)
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
	tokenStore IndexStore[index.TokenIndexEntry]
	attrStore  AttrIndexStore
	kvStore    KVIndexStore
	builder    *index.BuildHelper

	// Logger for this manager instance.
	// Scoped with component="index-manager", type="memory" at construction time.
	logger *slog.Logger
}

// NewManager creates an in-memory index manager.
// If logger is nil, logging is disabled.
func NewManager(
	indexers []index.Indexer,
	tokenStore IndexStore[index.TokenIndexEntry],
	attrStore AttrIndexStore,
	kvStore KVIndexStore,
	logger *slog.Logger,
) *Manager {
	return &Manager{
		indexers:   indexers,
		tokenStore: tokenStore,
		attrStore:  attrStore,
		kvStore:    kvStore,
		builder:    index.NewBuildHelper(),
		logger:     logging.Default(logger).With("component", "index-manager", "type", "memory"),
	}
}

func (m *Manager) BuildIndexes(ctx context.Context, chunkID chunk.ChunkID) error {
	return m.builder.Build(ctx, chunkID, m.indexers)
}

// DeleteIndexes removes all index data for the given chunk from memory stores.
func (m *Manager) DeleteIndexes(chunkID chunk.ChunkID) error {
	if m.tokenStore != nil {
		m.tokenStore.Delete(chunkID)
	}
	if m.attrStore != nil {
		m.attrStore.Delete(chunkID)
	}
	if m.kvStore != nil {
		m.kvStore.Delete(chunkID)
	}
	return nil
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

func (m *Manager) OpenKVKeyIndex(chunkID chunk.ChunkID) (*index.Index[index.KVKeyIndexEntry], index.KVIndexStatus, error) {
	if m.kvStore == nil {
		return nil, index.KVComplete, index.ErrIndexNotFound
	}
	entries, status, ok := m.kvStore.GetKey(chunkID)
	if !ok {
		return nil, index.KVComplete, index.ErrIndexNotFound
	}
	return index.NewIndex(entries), status, nil
}

func (m *Manager) OpenKVValueIndex(chunkID chunk.ChunkID) (*index.Index[index.KVValueIndexEntry], index.KVIndexStatus, error) {
	if m.kvStore == nil {
		return nil, index.KVComplete, index.ErrIndexNotFound
	}
	entries, status, ok := m.kvStore.GetValue(chunkID)
	if !ok {
		return nil, index.KVComplete, index.ErrIndexNotFound
	}
	return index.NewIndex(entries), status, nil
}

func (m *Manager) OpenKVIndex(chunkID chunk.ChunkID) (*index.Index[index.KVIndexEntry], index.KVIndexStatus, error) {
	if m.kvStore == nil {
		return nil, index.KVComplete, index.ErrIndexNotFound
	}
	entries, status, ok := m.kvStore.GetKV(chunkID)
	if !ok {
		return nil, index.KVComplete, index.ErrIndexNotFound
	}
	return index.NewIndex(entries), status, nil
}

// IndexesComplete reports whether all indexes exist for the given chunk.
// For in-memory indexes, this checks if all stores have entries for the chunk.
func (m *Manager) IndexesComplete(chunkID chunk.ChunkID) (bool, error) {
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
	if m.kvStore != nil {
		if _, _, ok := m.kvStore.GetKey(chunkID); !ok {
			return false, nil
		}
		if _, _, ok := m.kvStore.GetValue(chunkID); !ok {
			return false, nil
		}
		if _, _, ok := m.kvStore.GetKV(chunkID); !ok {
			return false, nil
		}
	}
	return true, nil
}
