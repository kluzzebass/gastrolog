package orchestrator

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/index/analyzer"
)

// IndexInfo describes a single index for a chunk.
type IndexInfo struct {
	Name       string
	Exists     bool
	EntryCount int64
	SizeBytes  int64
}

// ChunkIndexReport aggregates chunk seal status and per-index info.
type ChunkIndexReport struct {
	Sealed  bool
	Indexes []IndexInfo
}

// storeManagers looks up both managers for a store under RLock.
// Returns ErrStoreNotFound if the store doesn't exist.
func (o *Orchestrator) storeManagers(storeID uuid.UUID) (chunk.ChunkManager, index.IndexManager, error) {
	o.mu.RLock()
	s := o.stores[storeID]
	o.mu.RUnlock()
	if s == nil {
		return nil, nil, fmt.Errorf("%w: %s", ErrStoreNotFound, storeID)
	}
	return s.Chunks, s.Indexes, nil
}

// chunkManager looks up the chunk manager for a store under RLock.
func (o *Orchestrator) chunkManager(storeID uuid.UUID) (chunk.ChunkManager, error) {
	o.mu.RLock()
	s := o.stores[storeID]
	o.mu.RUnlock()
	if s == nil {
		return nil, fmt.Errorf("%w: %s", ErrStoreNotFound, storeID)
	}
	return s.Chunks, nil
}

// indexManager looks up the index manager for a store under RLock.
func (o *Orchestrator) indexManager(storeID uuid.UUID) (index.IndexManager, error) {
	o.mu.RLock()
	s := o.stores[storeID]
	o.mu.RUnlock()
	if s == nil {
		return nil, fmt.Errorf("%w: %s", ErrStoreNotFound, storeID)
	}
	return s.Indexes, nil
}

// --- Chunk read ---

// ListChunkMetas returns all chunk metadata for a store.
func (o *Orchestrator) ListChunkMetas(storeID uuid.UUID) ([]chunk.ChunkMeta, error) {
	cm, err := o.chunkManager(storeID)
	if err != nil {
		return nil, err
	}
	return cm.List()
}

// GetChunkMeta returns metadata for a specific chunk.
func (o *Orchestrator) GetChunkMeta(storeID uuid.UUID, chunkID chunk.ChunkID) (chunk.ChunkMeta, error) {
	cm, err := o.chunkManager(storeID)
	if err != nil {
		return chunk.ChunkMeta{}, err
	}
	return cm.Meta(chunkID)
}

// OpenCursor opens a record cursor for the given chunk.
func (o *Orchestrator) OpenCursor(storeID uuid.UUID, chunkID chunk.ChunkID) (chunk.RecordCursor, error) {
	cm, err := o.chunkManager(storeID)
	if err != nil {
		return nil, err
	}
	return cm.OpenCursor(chunkID)
}

// StoreExists returns true if a store with the given ID is registered.
func (o *Orchestrator) StoreExists(storeID uuid.UUID) bool {
	o.mu.RLock()
	s := o.stores[storeID]
	o.mu.RUnlock()
	return s != nil
}

// --- Chunk write ---

// Append appends a record to the store's active chunk.
func (o *Orchestrator) Append(storeID uuid.UUID, rec chunk.Record) (chunk.ChunkID, uint64, error) {
	cm, err := o.chunkManager(storeID)
	if err != nil {
		return chunk.ChunkID{}, 0, err
	}
	return cm.Append(rec)
}

// SealActive seals the active chunk if it has records. No-op if empty or no active chunk.
func (o *Orchestrator) SealActive(storeID uuid.UUID) error {
	cm, err := o.chunkManager(storeID)
	if err != nil {
		return err
	}
	if active := cm.Active(); active != nil && active.RecordCount > 0 {
		return cm.Seal()
	}
	return nil
}

// --- Index ops ---

// IndexSizes returns the size in bytes for each index of a chunk.
func (o *Orchestrator) IndexSizes(storeID uuid.UUID, chunkID chunk.ChunkID) (map[string]int64, error) {
	im, err := o.indexManager(storeID)
	if err != nil {
		return nil, err
	}
	return im.IndexSizes(chunkID), nil
}

// IndexesComplete reports whether all indexes exist for the given chunk.
func (o *Orchestrator) IndexesComplete(storeID uuid.UUID, chunkID chunk.ChunkID) (bool, error) {
	im, err := o.indexManager(storeID)
	if err != nil {
		return false, err
	}
	return im.IndexesComplete(chunkID)
}

// BuildIndexes builds all indexes for a sealed chunk.
func (o *Orchestrator) BuildIndexes(ctx context.Context, storeID uuid.UUID, chunkID chunk.ChunkID) error {
	im, err := o.indexManager(storeID)
	if err != nil {
		return err
	}
	return im.BuildIndexes(ctx, chunkID)
}

// DeleteIndexes removes all indexes for a chunk.
func (o *Orchestrator) DeleteIndexes(storeID uuid.UUID, chunkID chunk.ChunkID) error {
	im, err := o.indexManager(storeID)
	if err != nil {
		return err
	}
	return im.DeleteIndexes(chunkID)
}

// --- Composite ---

// ChunkIndexInfos returns seal status and per-index info for a chunk.
func (o *Orchestrator) ChunkIndexInfos(storeID uuid.UUID, chunkID chunk.ChunkID) (*ChunkIndexReport, error) {
	cm, im, err := o.storeManagers(storeID)
	if err != nil {
		return nil, err
	}

	meta, err := cm.Meta(chunkID)
	if err != nil {
		return nil, err
	}

	sizes := im.IndexSizes(chunkID)

	report := &ChunkIndexReport{
		Sealed:  meta.Sealed,
		Indexes: make([]IndexInfo, 0, 7),
	}

	// Token index
	if idx, err := im.OpenTokenIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "token", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["token"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "token"})
	}

	// Attr key index
	if idx, err := im.OpenAttrKeyIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "attr_key", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["attr_key"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "attr_key"})
	}

	// Attr value index
	if idx, err := im.OpenAttrValueIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "attr_val", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["attr_val"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "attr_val"})
	}

	// Attr kv index
	if idx, err := im.OpenAttrKVIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "attr_kv", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["attr_kv"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "attr_kv"})
	}

	// KV key index
	if idx, _, err := im.OpenKVKeyIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "kv_key", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["kv_key"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "kv_key"})
	}

	// KV value index
	if idx, _, err := im.OpenKVValueIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "kv_val", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["kv_val"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "kv_val"})
	}

	// KV combined index
	if idx, _, err := im.OpenKVIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "kv_kv", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["kv_kv"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "kv_kv"})
	}

	return report, nil
}

// NewAnalyzer creates an index analyzer for the given store.
func (o *Orchestrator) NewAnalyzer(storeID uuid.UUID) (*analyzer.Analyzer, error) {
	cm, im, err := o.storeManagers(storeID)
	if err != nil {
		return nil, err
	}
	return analyzer.New(cm, im), nil
}

// SupportsChunkMove returns true if both stores support filesystem-level chunk moves.
func (o *Orchestrator) SupportsChunkMove(srcID, dstID uuid.UUID) bool {
	o.mu.RLock()
	srcStore := o.stores[srcID]
	dstStore := o.stores[dstID]
	o.mu.RUnlock()
	if srcStore == nil || dstStore == nil {
		return false
	}
	_, srcOK := srcStore.Chunks.(chunk.ChunkMover)
	_, dstOK := dstStore.Chunks.(chunk.ChunkMover)
	return srcOK && dstOK
}
