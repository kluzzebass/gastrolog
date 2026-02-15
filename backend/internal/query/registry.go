package query

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"

	"github.com/google/uuid"
)

// StoreRegistry provides access to all stores' chunk and index managers.
// This abstraction allows the query engine to search across multiple stores.
type StoreRegistry interface {
	// ListStores returns all registered store IDs.
	ListStores() []uuid.UUID

	// ChunkManager returns the chunk manager for the given store.
	// Returns nil if the store doesn't exist.
	ChunkManager(storeID uuid.UUID) chunk.ChunkManager

	// IndexManager returns the index manager for the given store.
	// Returns nil if the store doesn't exist.
	IndexManager(storeID uuid.UUID) index.IndexManager
}
