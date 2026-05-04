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
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	"gastrolog/internal/vaultraft/tierfsm"
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

	// Reader returns the manifest Reader for FSM-projected sealed-chunk
	// metadata. Memory-mode and test registries can return a projecting
	// fallback (NewProjectingReader) when no FSM is wired.
	Reader() Reader

	// IndexReader returns the FSM-grounded IngestTS-rank lookup interface.
	// Returns nil when the registry's tier instances aren't wired to a
	// chunk/index manager (e.g. a metadata-only test registry); callers
	// should treat nil as "no index access" and fall through to other
	// strategies (FSM-based proportional distribution).
	IndexReader() IndexReader
}

// Reader exposes the FSM-projected view of chunk manifests. Every caller
// that needs sealed-chunk metadata routes through this interface instead
// of reaching into chunk.Manager.metas / cloudIdx / chunkMeta — the FSM
// is the source of truth, and Reader is the only sanctioned read path.
// The active chunk is *not* covered here (its running maxima live in
// chunk.Manager and don't round-trip through Raft); callers ask the
// chunk manager directly for that.
//
// See docs/chunk_redesign.md for the rule and the active-chunk exception.
type Reader interface {
	// Entry returns the manifest entry for the chunk with this ID. The
	// bool is false if no manifest holds the chunk, or if the chunk is
	// active (active chunks are not part of the manifest read surface —
	// see chunk.Manager for those). ChunkIDs are globally unique GLIDs,
	// so no vault qualifier is needed.
	Entry(chunkID chunk.ChunkID) (tierfsm.ManifestEntry, bool)

	// EntriesForVault returns the manifest entries for every sealed chunk
	// in the given vault, regardless of tier. The returned slice is a
	// snapshot; callers may mutate or sort it.
	//
	// Returns nil if the vault is unknown.
	EntriesForVault(vaultID glid.GLID) []tierfsm.ManifestEntry
}

// IndexReader is the FSM-grounded read path for the IngestTS rank index
// stored inside each sealed chunk's GLCB blob. Separate from Reader
// (metadata-only) because index lookup involves file I/O — keeping the
// interfaces narrow lets test mocks for metadata stay simple. Composes
// with Reader: typical implementations look up an Entry via Reader, then
// use Entry.IngestIdxOffset/Size to read the section from a chunk-local
// byte stream.
//
// The histogram and other rank-arithmetic consumers route through this
// instead of reaching into chunk.Manager.FindIngestEntryIndex /
// index.Manager.FindIngestEntryIndex directly. The implementation is
// responsible for dispatching to the right tier's chunk Manager and
// using FSM-derived offsets — never trusting projected meta when the
// FSM has the authoritative offsets.
type IndexReader interface {
	// FindIngestRank returns the rank of the first IngestTS-sorted entry
	// with TS >= ts in the given chunk's IngestTS index. ok=false when the
	// chunk's index isn't locally resolvable (uncached cloud chunk, missing
	// GLCB, or FSM unaware of chunk).
	FindIngestRank(chunkID chunk.ChunkID, ts time.Time) (rank uint64, ok bool)

	// FindIngestPos returns the physical record position (in append order)
	// for the same query. Equal to rank for monotonic chunks, divergent for
	// non-monotonic chunks built via ImportRecords. Used by cursor
	// positioning, not bucket counting.
	FindIngestPos(chunkID chunk.ChunkID, ts time.Time) (pos uint64, ok bool)
}

// IndexReaderProvider is the subset of VaultRegistry needed by query-side
// callers that want an IndexReader. Letting them depend on this narrow
// surface keeps test mocks small.
type IndexReaderProvider interface {
	IndexReader() IndexReader
}
