package query

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"

	"github.com/google/uuid"
)

// VaultRegistry provides access to all vaults' chunk and index managers.
// This abstraction allows the query engine to search across multiple vaults.
type VaultRegistry interface {
	// ListVaults returns all registered vault IDs.
	ListVaults() []uuid.UUID

	// ChunkManager returns the chunk manager for the given vault.
	// Returns nil if the vault doesn't exist.
	ChunkManager(vaultID uuid.UUID) chunk.ChunkManager

	// IndexManager returns the index manager for the given vault.
	// Returns nil if the vault doesn't exist.
	IndexManager(vaultID uuid.UUID) index.IndexManager
}
