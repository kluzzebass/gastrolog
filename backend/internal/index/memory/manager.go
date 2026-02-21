package memory

import (
	"context"
	"log/slog"
	"time"

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

// JSONIndexStore provides access to JSON path and path-value indexes.
type JSONIndexStore interface {
	GetPath(chunkID chunk.ChunkID) ([]index.JSONPathIndexEntry, index.JSONIndexStatus, bool)
	GetPV(chunkID chunk.ChunkID) ([]index.JSONPVIndexEntry, index.JSONIndexStatus, bool)
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
	jsonStore  JSONIndexStore
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

// NewManagerWithJSON creates an in-memory index manager with JSON index support.
func NewManagerWithJSON(
	indexers []index.Indexer,
	tokenStore IndexStore[index.TokenIndexEntry],
	attrStore AttrIndexStore,
	kvStore KVIndexStore,
	jsonStore JSONIndexStore,
	logger *slog.Logger,
) *Manager {
	return &Manager{
		indexers:   indexers,
		tokenStore: tokenStore,
		attrStore:  attrStore,
		kvStore:    kvStore,
		jsonStore:  jsonStore,
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
	if m.jsonStore != nil {
		m.jsonStore.Delete(chunkID)
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

func (m *Manager) OpenJSONPathIndex(chunkID chunk.ChunkID) (*index.Index[index.JSONPathIndexEntry], index.JSONIndexStatus, error) {
	if m.jsonStore == nil {
		return nil, index.JSONComplete, index.ErrIndexNotFound
	}
	entries, status, ok := m.jsonStore.GetPath(chunkID)
	if !ok {
		return nil, index.JSONComplete, index.ErrIndexNotFound
	}
	return index.NewIndex(entries), status, nil
}

func (m *Manager) OpenJSONPVIndex(chunkID chunk.ChunkID) (*index.Index[index.JSONPVIndexEntry], index.JSONIndexStatus, error) {
	if m.jsonStore == nil {
		return nil, index.JSONComplete, index.ErrIndexNotFound
	}
	entries, status, ok := m.jsonStore.GetPV(chunkID)
	if !ok {
		return nil, index.JSONComplete, index.ErrIndexNotFound
	}
	return index.NewIndex(entries), status, nil
}

// FindIngestStartPosition returns ErrIndexNotFound; memory index manager does not
// maintain ingest timestamp indexes.
func (m *Manager) FindIngestStartPosition(chunkID chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	return 0, false, index.ErrIndexNotFound
}

// FindSourceStartPosition returns ErrIndexNotFound; memory index manager does not
// maintain source timestamp indexes.
func (m *Manager) FindSourceStartPosition(chunkID chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	return 0, false, index.ErrIndexNotFound
}

// IndexSizes estimates the in-memory data footprint for each index.
func (m *Manager) IndexSizes(chunkID chunk.ChunkID) map[string]int64 {
	sizes := make(map[string]int64)

	if m.tokenStore != nil {
		if entries, ok := m.tokenStore.Get(chunkID); ok {
			var s int64
			for _, e := range entries {
				s += int64(len(e.Token)) + int64(len(e.Positions))*8
			}
			sizes["token"] = s
		}
	}
	if m.attrStore != nil {
		if entries, ok := m.attrStore.GetKey(chunkID); ok {
			var s int64
			for _, e := range entries {
				s += int64(len(e.Key)) + int64(len(e.Positions))*8
			}
			sizes["attr_key"] = s
		}
		if entries, ok := m.attrStore.GetValue(chunkID); ok {
			var s int64
			for _, e := range entries {
				s += int64(len(e.Value)) + int64(len(e.Positions))*8
			}
			sizes["attr_val"] = s
		}
		if entries, ok := m.attrStore.GetKV(chunkID); ok {
			var s int64
			for _, e := range entries {
				s += int64(len(e.Key)) + int64(len(e.Value)) + int64(len(e.Positions))*8
			}
			sizes["attr_kv"] = s
		}
	}
	if m.kvStore != nil {
		if entries, _, ok := m.kvStore.GetKey(chunkID); ok {
			var s int64
			for _, e := range entries {
				s += int64(len(e.Key)) + int64(len(e.Positions))*8
			}
			sizes["kv_key"] = s
		}
		if entries, _, ok := m.kvStore.GetValue(chunkID); ok {
			var s int64
			for _, e := range entries {
				s += int64(len(e.Value)) + int64(len(e.Positions))*8
			}
			sizes["kv_val"] = s
		}
		if entries, _, ok := m.kvStore.GetKV(chunkID); ok {
			var s int64
			for _, e := range entries {
				s += int64(len(e.Key)) + int64(len(e.Value)) + int64(len(e.Positions))*8
			}
			sizes["kv_kv"] = s
		}
	}
	if m.jsonStore != nil {
		var s int64
		if entries, _, ok := m.jsonStore.GetPath(chunkID); ok {
			for _, e := range entries {
				s += int64(len(e.Path)) + int64(len(e.Positions))*8
			}
		}
		if entries, _, ok := m.jsonStore.GetPV(chunkID); ok {
			for _, e := range entries {
				s += int64(len(e.Path)) + int64(len(e.Value)) + int64(len(e.Positions))*8
			}
		}
		if s > 0 {
			sizes["json"] = s
		}
	}

	return sizes
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
	if m.jsonStore != nil {
		if _, _, ok := m.jsonStore.GetPath(chunkID); !ok {
			return false, nil
		}
	}
	return true, nil
}
