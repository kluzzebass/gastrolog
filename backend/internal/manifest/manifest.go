// Package manifest defines the cluster-wide read surfaces over the per-vault
// chunk manifests held by instance sub-FSMs. It is the home for any interface
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
	"gastrolog/internal/vaultraft/vaultctlfsm"
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

	// Reader returns the manifest Reader for FSM-projected sealed-chunk
	// metadata. Memory-mode and test registries can return a projecting
	// fallback (NewProjectingReader) when no FSM is wired.
	Reader() Reader

	// IndexReader returns the FSM-grounded IngestTS-rank lookup interface.
	// Returns nil when the registry's vault instances aren't wired to a
	// chunk/index manager (e.g. a metadata-only test registry); callers
	// should treat nil as "no index access" and fall through to other
	// strategies (FSM-based proportional distribution).
	IndexReader() IndexReader
}

// SearchChunkLister supplies the chunk metadata set for query and histogram
// discovery. When implemented by the registry, the query engine includes
// pipeline active and sealing chunks (manifest-backed, no GLCB yet) in
// addition to sealed manifest entries. File/memory vaults also append the
// chunk-manager active head (m.active). Reader() remains sealed-only for
// retention and integrity surfaces.
type SearchChunkLister interface {
	SearchChunkMetas(vaultID glid.GLID) []chunk.ChunkMeta
}

// PipelineChunkOpener opens a record cursor for pipeline active or sealing
// chunks that have no registered GLCB yet (manifest segment-span reads).
type PipelineChunkOpener interface {
	OpenPipelineChunkCursor(vaultID glid.GLID, chunkID chunk.ChunkID) (chunk.RecordCursor, error)
}

// PipelineIngestScanner scans IngestTS from pipeline active/sealing chunks
// via the open-chunk manifest (segment spans / partial GLCB).
type PipelineIngestScanner interface {
	ScanPipelineChunkIngestTS(vaultID glid.GLID, chunkID chunk.ChunkID, cb func(tsNanos int64) bool) error
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
	Entry(chunkID chunk.ChunkID) (vaultctlfsm.ManifestEntry, bool)

	// EntriesForVault returns the manifest entries for every sealed chunk
	// in the given vault. The returned slice is a snapshot; callers may
	// mutate or sort it.
	//
	// Returns nil if the vault is unknown.
	EntriesForVault(vaultID glid.GLID) []vaultctlfsm.ManifestEntry
}

// IndexReader is the read path for the IngestTS rank index stored inside
// each sealed chunk's GLCB blob. Separate from Reader (metadata-only)
// because index lookup involves file I/O — keeping the interfaces narrow
// lets test mocks for metadata stay simple.
//
// Section-offset authority: for LOCAL reads, the blob's own embedded TOC
// is authoritative — implementations read sections via the mmap'd GLCB
// TOC, not via Entry.IngestIdxOffset/Size. The FSM-replicated offsets are
// a replication/verification copy (snapshot restore, digest checks), not
// the local read path; there is exactly one authoritative source per
// access mode, never two synced copies (see CLAUDE.md "Single Source of
// Truth"). Whether rank/pos reads later collapse onto one FSM-grounded
// section reader is a question for the GLCB codec abstraction, not for
// this interface.
//
// The histogram and other rank-arithmetic consumers route through this
// instead of reaching into chunk.Manager.FindIngestEntryIndex /
// index.Manager.FindIngestEntryIndex directly; the implementation is
// responsible for dispatching to the right vault's chunk manager.
type IndexReader interface {
	// FindIngestRank returns the rank of the first IngestTS-sorted entry
	// with TS >= ts in the given chunk's IngestTS index. ok=false when THIS
	// lookup isn't locally resolvable (uncached cloud-backed chunk, missing
	// GLCB, or FSM unaware of chunk). Resolvability is per timestamp, not
	// per chunk: implementations may answer boundary timestamps exactly
	// from FSM-replicated index metadata (rank 0 strictly before a sealed
	// monotonic chunk's IngestStart) while interior timestamps of the same
	// chunk stay unresolvable without local ITSI bytes. Consumers doing
	// rank arithmetic must check ok on every lookup and fall back (FSM
	// proportional distribution) on the first miss.
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
