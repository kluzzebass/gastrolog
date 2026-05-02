// Package manifest defines the cluster-wide read surfaces over the per-tier
// chunk manifests held by tier sub-FSMs. It is the home for any interface
// that exposes vaults' runtime metadata to consumers above
// internal/chunk and internal/index but below internal/orchestrator —
// principally the query engine, retention, and any future caller that
// needs an FSM-grounded view of "what chunks does this vault have, and
// what does the FSM say about each one."
//
// See docs/chunk_redesign.md for the FSM-as-source-of-truth rule and
// docs/ubiquitous_language.md for the Manifest definition.
package manifest

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	tierfsm "gastrolog/internal/tier/raftfsm"
)

// VaultRegistry provides access to all vaults' chunk and index managers and
// to the per-vault transition-streamed set. Implemented by the orchestrator;
// consumed by the query engine and other callers that need to fan out across
// vaults.
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

// Reader exposes the FSM-projected view of a vault's chunk manifest. Every
// caller that needs sealed-chunk metadata routes through this interface
// instead of reaching into chunk.Manager.metas / cloudIdx / chunkMeta —
// the FSM is the source of truth, and Reader is the only sanctioned read
// path. The active chunk is *not* covered here (its running maxima live in
// chunk.Manager and don't round-trip through Raft); callers ask the chunk
// manager directly for that.
//
// See docs/chunk_redesign.md for the rule and the active-chunk exception.
type Reader interface {
	// SealedEntries returns the manifest entries for every sealed chunk in
	// the given vault, regardless of tier. The returned slice is a snapshot;
	// callers may mutate or sort it.
	//
	// Returns nil if the vault is unknown.
	SealedEntries(vaultID glid.GLID) []tierfsm.ManifestEntry

	// Entry returns the manifest entry for one chunk in a vault. The bool
	// is false if the chunk is unknown, sealed=false (active chunks are
	// not part of the manifest read surface), or the vault is unknown.
	Entry(vaultID glid.GLID, chunkID chunk.ChunkID) (tierfsm.ManifestEntry, bool)
}
