package query

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
)

// VaultRegistry provides access to all vaults' chunk and index managers.
// This abstraction allows the query engine to search across multiple vaults.
type VaultRegistry interface {
	// ListVaults returns all registered vault IDs.
	ListVaults() []glid.GLID

	// ChunkManager returns the chunk manager for the given vault.
	// Returns nil if the vault doesn't exist.
	ChunkManager(vaultID glid.GLID) chunk.ChunkManager

	// IndexManager returns the index manager for the given vault.
	// Returns nil if the vault doesn't exist.
	IndexManager(vaultID glid.GLID) index.IndexManager
}
