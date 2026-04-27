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

	// TransitionStreamedChunks returns the set of chunk IDs in the given
	// vault that have been streamed to the next tier but not yet expired
	// (i.e. the tier FSM has CmdTransitionStreamed applied for them, and
	// retention is awaiting the destination receipt before deleting the
	// source). The histogram and other count-based aggregations skip
	// these chunks: their records have already been counted in the
	// destination tier, so counting them at the source too is the
	// transition-window double-count that gastrolog-4xusf tracks.
	//
	// Returns an empty (or nil) map when the vault has no streamed chunks
	// or when the registry's source-of-truth for this state isn't
	// available (e.g. unit-test registries that don't model transitions).
	TransitionStreamedChunks(vaultID glid.GLID) map[chunk.ChunkID]bool
}
