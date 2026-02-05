package query

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
)

// StoreRegistry provides access to all stores' chunk and index managers.
// This abstraction allows the query engine to search across multiple stores.
type StoreRegistry interface {
	// ListStores returns all registered store IDs.
	ListStores() []string

	// ChunkManager returns the chunk manager for the given store.
	// Returns nil if the store doesn't exist.
	ChunkManager(storeID string) chunk.ChunkManager

	// IndexManager returns the index manager for the given store.
	// Returns nil if the store doesn't exist.
	IndexManager(storeID string) index.IndexManager
}

// singleStoreRegistry wraps a single chunk/index manager pair as a StoreRegistry.
// This allows backward compatibility with code that creates an Engine for one store.
type singleStoreRegistry struct {
	storeID string
	chunks  chunk.ChunkManager
	indexes index.IndexManager
}

func (r *singleStoreRegistry) ListStores() []string {
	return []string{r.storeID}
}

func (r *singleStoreRegistry) ChunkManager(storeID string) chunk.ChunkManager {
	if storeID == r.storeID {
		return r.chunks
	}
	return nil
}

func (r *singleStoreRegistry) IndexManager(storeID string) index.IndexManager {
	if storeID == r.storeID {
		return r.indexes
	}
	return nil
}
