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

	// HasRaftLeader returns true if the vault control-plane Raft group has an elected leader (cluster mode).
	// Nil when no Raft group exists (single-node / memory mode).
	HasRaftLeader func() bool

	// IsRaftLeader returns true if THIS node is the vault ctl Raft leader (cluster mode).
	// Nil when no Raft group exists (single-node / memory mode — always leader).
	IsRaftLeader func() bool

	// ApplyRaftRetentionPending marks a chunk as retention-pending in replicated metadata.
	ApplyRaftRetentionPending func(id chunk.ChunkID) error

	// ListRetentionPending returns chunk IDs with RetentionPending=true in the FSM.
	ListRetentionPending func() []chunk.ChunkID

	// IsTombstoned returns true if the given chunk ID has been deleted from
	// this instance's replicated FSM and is still within the tombstone retention
	// window. Used to reject stale replication commands (ImportSealed,
	// Append, Seal) that race with retention — without this check, a late
	// ImportSealed RPC could recreate a chunk the cluster already deleted,
	// producing a "ghost" chunk on the follower. See gastrolog-11rzz.
	// Nil when no Raft group exists.
	IsTombstoned func(id chunk.ChunkID) bool

	// ApplyRaftRequestDelete proposes the receipt-based delete protocol's
	// opening command (CmdRequestDelete). The FSM adds a pendingDeletes entry
	// keyed by chunk ID with the given reason and expectedFrom set; every
	// node in expectedFrom owes a CmdAckDelete after deleting its local
	// copy, and the leader proposes CmdFinalizeDelete once expectedFrom is
	// empty. Nil when no Raft group exists. See gastrolog-51gme.
	ApplyRaftRequestDelete func(id chunk.ChunkID, reason string, expectedFrom []string) error

	// ApplyRaftAckDelete proposes a node's ack of a pending delete obligation.
	// Idempotent: duplicate / unknown-node acks are no-ops. Nil when no Raft
	// group exists. See gastrolog-51gme.
	ApplyRaftAckDelete func(id chunk.ChunkID, nodeID string) error

	// ApplyRaftFinalizeDelete proposes the leader's finalization of a pending
	// delete. Removes the pendingDeletes entry; the entry-removal already
	// happened in the FSM applyFinalizeDelete handler, so this is purely the
	// distributed-commit signal. Nil when no Raft group exists. See
	// gastrolog-51gme.
	ApplyRaftFinalizeDelete func(id chunk.ChunkID) error

	// ApplyRaftPruneNode proposes removal of a node from every pendingDeletes
	// entry's ExpectedFrom set on this instance sub-FSM. Used by the leader's
	// membership-change handler after RemoveServer succeeds: a decommissioned
	// node's outstanding ack obligations would otherwise pin pendingDeletes
	// entries forever. Nil when no Raft group exists. See gastrolog-51gme step 10.
	ApplyRaftPruneNode func(nodeID string) error

	// Reconciler owns chunk-lifecycle execution for this vault instance:
	// FSM-apply event handlers (seal, retention-pending, transition-streamed,
	// transition-received, request-delete, ack-delete, finalize-delete) plus
	// the canonical deleteChunk entry point. All cluster-wide deletes route
	// through here over gastrolog-51gme steps 4-8. Nil for memory-mode vaults
	// (no FSM, no replication).
	Reconciler *VaultLifecycleReconciler

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

	// ChunkPlacement returns the per-chunk Receiving/Holding placement
	// from the vault-ctl FSM (gastrolog-nd6sz). Nil when no Raft group
	// exists; nil-returning closure when the chunk has no placement
	// entry (LeaderDriven chunks pre-fan-out have no placement at
	// all, while FanOut chunks always have one). Used by appendRecord
	// to dispatch FanOut chunks to fanOutAppend rather than the
	// legacy forwardToFollowers path.
	ChunkPlacement func(id chunk.ChunkID) *vaultctlfsm.ChunkPlacement

	// IsFSMReady returns true after the vault-ctl FSM has applied at least one log
	// entry or restored from a snapshot. Before that, the manifest is incomplete
	// and must not be used for reconciliation decisions.
	IsFSMReady func() bool

	// OverlayFromFSM returns a copy of the given chunk meta with cluster-wide
	// fields (CloudBacked, Archived) sourced from replicated metadata
	// FSM instead of the local chunk manager. The local chunk manager only
	// reflects this node's view, which is wrong for those fields on follower
	// nodes: followers strip sealed_backing from their chunk-manager params
	// (see reconfig_vaults.go), so their CloudStore is nil and their local
	// CloudBacked is permanently false even when the cluster has uploaded
	// the chunk to S3. The FSM has the authoritative cluster-wide truth via
	// the replicated CmdUploadChunk / CmdArchiveChunk commands, so we
	// override from there. See gastrolog-asg4l.
	//
	// Nil when no Raft group exists (single-node / memory mode), in which
	// case the local chunk manager view is already authoritative.
	OverlayFromFSM func(chunk.ChunkMeta) chunk.ChunkMeta

	// ChunkResidency returns the node IDs that currently hold a chunk's
	// bytes, computed authoritatively from the FSM (placement set minus
	// in-flight delete-acks). Used by the WatchChunks event-relay path to
	// stamp authoritative replica info on outbound events so clients
	// don't have to reconstruct it from per-node event accumulation,
	// which drifts on leadership transfer and active-chunk catchup.
	// See gastrolog-66vmg.
	//
	// Nil for memory-mode vaults (no FSM); callers fall back to omitting
	// the replica info, which the client treats as "preserve existing"
	// in mergeMeta.
	ChunkResidency func(id chunk.ChunkID, placementNodeIDs []string) []string
}

// applyRaftCallbacks wires raft-backed metadata operations from a vaultRaftCallbacks.
func (t *VaultInstance) applyRaftCallbacks(cb vaultRaftCallbacks) {
	t.HasRaftLeader = cb.hasLeader
	t.IsRaftLeader = cb.isLeader
	t.ApplyRaftRequestDelete = cb.applyRequestDelete
	t.ApplyRaftAckDelete = cb.applyAckDelete
	t.ApplyRaftFinalizeDelete = cb.applyFinalizeDelete
	t.ApplyRaftPruneNode = cb.applyPruneNode
	t.ListManifest = cb.listChunks
	t.ApplyRaftRetentionPending = cb.applyRetPending
	t.ListRetentionPending = cb.listRetPending
	t.IsTombstoned = cb.isTombstoned
	t.IsFSMReady = cb.isFSMReady
	t.OverlayFromFSM = cb.overlayFromFSM
	t.ChunkResidency = cb.chunkResidency
	t.ManifestEntries = cb.manifestEntries
	t.ManifestEntry = cb.manifestEntry
	t.ChunkPlacement = cb.chunkPlacement
}

// IsLeader returns true if this node is the leader for this instance.
func (t *VaultInstance) IsLeader() bool { return !t.IsFollower }

// ShouldForwardToFollowers returns true if this leader instance has replication targets.
func (t *VaultInstance) ShouldForwardToFollowers() bool {
	return t.IsLeader() && len(t.FollowerTargets) > 0
}
