package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// VaultInstance is the node-local materialization of a VaultConfig.
// VaultConfig (in Raft config) is the logical definition; VaultInstance is the
// physical runtime: chunk manager + index manager + query engine.
//
// A single node may host multiple VaultInstances for the same vault when
// same-node replication is active (different file storages). Each instance
// has a unique StorageID and its own chunk manager pointing to a different
// directory.
type VaultInstance struct {
	VaultID         glid.GLID // identity of the owning vault
	StorageID       string    // the file storage ID this instance uses (empty for memory/JSONL vaults)
	Type            string
	Chunks          chunk.ChunkManager
	Indexes         index.IndexManager
	Query           *query.Engine
	IsFollower      bool                       // true if this node is a follower for this instance
	LeaderNodeID    string                     // the leader node's ID (empty if this IS the leader)
	FollowerTargets []system.ReplicationTarget // per-storage targets (populated on leader only)

	// The three raft-backed callback facets below are embedded, so their
	// fields promote onto VaultInstance (e.g. t.IsTombstoned, t.HasRaftLeader
	// still resolve). They are wired atomically as a group by
	// applyRaftCallbacks; every field is nil together for memory-mode vaults
	// (no FSM, no replication). Grouping the ~15 former loose callbacks into
	// named facets makes the "last-writer-wins" wiring concern typed: the
	// wiring sets three cohesive dependencies instead of fifteen.
	RaftLeadershipFacet
	RaftApplyFacet
	ManifestReadFacet

	// Reconciler owns chunk-lifecycle execution for this vault instance:
	// FSM-apply event handlers (seal, retention-pending, transition-streamed,
	// transition-received, request-delete, ack-delete, finalize-delete) plus
	// the canonical deleteChunk entry point. All cluster-wide deletes route
	// through here. Nil for memory-mode vaults (no FSM, no replication).
	Reconciler *VaultLifecycleReconciler
}

// RaftLeadershipFacet groups the vault control-plane Raft leadership queries.
// All fields are nil when no Raft group exists (single-node / memory mode).
type RaftLeadershipFacet struct {
	// HasRaftLeader returns true if the vault control-plane Raft group has an elected leader (cluster mode).
	// Nil when no Raft group exists (single-node / memory mode).
	HasRaftLeader func() bool

	// IsRaftLeader returns true if THIS node is the vault ctl Raft leader (cluster mode).
	// Nil when no Raft group exists (single-node / memory mode — always leader).
	IsRaftLeader func() bool
}

// RaftApplyFacet groups the replicated-metadata mutations proposed through the
// vault control-plane Raft group. All fields are nil when no Raft group exists.
type RaftApplyFacet struct {
	// ApplyRaftRetentionPending marks a chunk as retention-pending in replicated metadata.
	ApplyRaftRetentionPending func(id chunk.ChunkID) error

	// ApplyRaftAckChunkHolders / ApplyRaftRevokeChunkHolders commit this
	// node's chunk holder receipts — bytes-earned residency (batched).
	ApplyRaftAckChunkHolders    func(ids []chunk.ChunkID, nodeID string) error
	ApplyRaftRevokeChunkHolders func(ids []chunk.ChunkID, nodeID string) error

	// ApplyRaftRequestDelete proposes the receipt-based delete protocol's
	// opening command (CmdRequestDelete). The FSM adds a pendingDeletes entry
	// keyed by chunk ID with the given reason and expectedFrom set; every
	// node in expectedFrom owes a CmdAckDelete after deleting its local
	// copy, and the leader proposes CmdFinalizeDelete once expectedFrom is
	// empty. Nil when no Raft group exists.
	ApplyRaftRequestDelete func(id chunk.ChunkID, reason string, expectedFrom []string) error

	// ApplyRaftAckDelete proposes a node's ack of a pending delete obligation.
	// Idempotent: duplicate / unknown-node acks are no-ops. Nil when no Raft
	// group exists.
	ApplyRaftAckDelete func(id chunk.ChunkID, nodeID string) error

	// ApplyRaftFinalizeDelete proposes the leader's finalization of a pending
	// delete. Removes the pendingDeletes entry; the entry-removal already
	// happened in the FSM applyFinalizeDelete handler, so this is purely the
	// distributed-commit signal. Nil when no Raft group exists.
	ApplyRaftFinalizeDelete func(id chunk.ChunkID) error

	// ApplyRaftPruneNode proposes removal of a node from every pendingDeletes
	// entry's ExpectedFrom set on this instance sub-FSM. Used by the leader's
	// membership-change handler after RemoveServer succeeds: a decommissioned
	// node's outstanding ack obligations would otherwise pin pendingDeletes
	// entries forever. Nil when no Raft group exists.
	ApplyRaftPruneNode func(nodeID string) error
}

// ManifestReadFacet groups the read-only queries against the vault control-plane
// FSM manifest. All fields are nil for memory-mode vaults (no FSM); callers fall
// back to the chunk manager in that case.
type ManifestReadFacet struct {
	// ListManifest returns all chunk IDs in the vault-ctl FSM view — the authoritative
	// set of chunks that should exist. Nil when no Raft group exists.
	ListManifest func() []chunk.ChunkID

	// ManifestEntries returns every chunk's full manifest entry for this
	// instance (sealed and active alike — callers filter on Sealed when they
	// want only sealed chunks, e.g. the manifest.Reader implementation
	// honoring the active-chunk exception). Nil for memory-mode vaults
	// (no FSM); the orchestrator falls back to the chunk manager in
	// that case.
	ManifestEntries func() []vaultctlfsm.ManifestEntry

	// ManifestEntry returns the manifest entry for one chunk on this instance,
	// or false if this instance doesn't hold the chunk. Nil for memory-mode
	// instances; the orchestrator falls back to the chunk manager.
	ManifestEntry func(id chunk.ChunkID) (vaultctlfsm.ManifestEntry, bool)

	// ListRetentionPending returns chunk IDs with RetentionPending=true in the FSM.
	ListRetentionPending func() []chunk.ChunkID

	// IsTombstoned returns true if the given chunk ID has been deleted from
	// this instance's replicated FSM and is still within the tombstone retention
	// window. Used to reject stale replication commands (ImportSealed,
	// Append, Seal) that race with retention — without this check, a late
	// ImportSealed RPC could recreate a chunk the cluster already deleted,
	// producing a "ghost" chunk on the follower. Nil when no Raft group
	// exists.
	IsTombstoned func(id chunk.ChunkID) bool

	// IsFSMReady returns true after the vault-ctl FSM has applied at least one log
	// entry or restored from a snapshot. Before that, the manifest is incomplete
	// and must not be used for reconciliation decisions.
	IsFSMReady func() bool
}

// applyRaftCallbacks wires raft-backed metadata operations from a
// vaultRaftCallbacks into the three cohesive facets as a single group, so a
// re-wire replaces each facet atomically rather than fifteen loose fields.
func (t *VaultInstance) applyRaftCallbacks(cb vaultRaftCallbacks) {
	t.RaftLeadershipFacet = RaftLeadershipFacet{
		HasRaftLeader: cb.hasLeader,
		IsRaftLeader:  cb.isLeader,
	}
	t.RaftApplyFacet = RaftApplyFacet{
		ApplyRaftRetentionPending:   cb.applyRetPending,
		ApplyRaftAckChunkHolders:    cb.applyAckChunkHolders,
		ApplyRaftRevokeChunkHolders: cb.applyRevokeChunkHolders,
		ApplyRaftRequestDelete:      cb.applyRequestDelete,
		ApplyRaftAckDelete:          cb.applyAckDelete,
		ApplyRaftFinalizeDelete:     cb.applyFinalizeDelete,
		ApplyRaftPruneNode:          cb.applyPruneNode,
	}
	t.ManifestReadFacet = ManifestReadFacet{
		ListManifest:         cb.listChunks,
		ManifestEntries:      cb.manifestEntries,
		ManifestEntry:        cb.manifestEntry,
		ListRetentionPending: cb.listRetPending,
		IsTombstoned:         cb.isTombstoned,
		IsFSMReady:           cb.isFSMReady,
	}
}

// IsLeader returns true if this node is the leader for this instance.
func (t *VaultInstance) IsLeader() bool { return !t.IsFollower }

// ShouldForwardToFollowers returns true if this leader instance has replication targets.
func (t *VaultInstance) ShouldForwardToFollowers() bool {
	return t.IsLeader() && len(t.FollowerTargets) > 0
}
