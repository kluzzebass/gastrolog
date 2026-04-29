package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
)

// TierInstance is the node-local materialization of a TierConfig.
// TierConfig (in Raft config) is the logical definition.
// TierInstance is the physical runtime: chunk manager + index manager + query engine.
//
// A single node may host multiple TierInstances for the same tier when
// same-node replication is active (different file storages). Each instance
// has a unique StorageID and its own chunk manager pointing to a different
// directory.
type TierInstance struct {
	TierID          glid.GLID
	StorageID       string // the file storage ID this instance uses (empty for memory/JSONL tiers)
	Type            string
	Chunks          chunk.ChunkManager
	Indexes         index.IndexManager
	Query           *query.Engine
	IsFollower      bool                       // true if this node is a follower for this tier
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

	// ApplyRaftTransitionStreamed marks a chunk as streamed to the next tier
	// in replicated metadata, so it won't be deleted until the destination confirms
	// replication. See gastrolog-4913n.
	ApplyRaftTransitionStreamed func(id chunk.ChunkID) error

	// ListTransitionStreamed returns chunk IDs with TransitionStreamed=true in the FSM.
	ListTransitionStreamed func() []chunk.ChunkID

	// ApplyRaftTransitionReceived writes a receipt confirming that records
	// from the given source chunk have been imported into this tier. Called
	// by the source tier after streaming completes. The Raft commit gives
	// majority-durable confirmation. See gastrolog-4913n.
	ApplyRaftTransitionReceived func(sourceChunkID chunk.ChunkID) error

	// HasTransitionReceipt returns true if the FSM has a receipt for the
	// given source chunk ID, meaning this tier has durably received its records.
	HasTransitionReceipt func(sourceChunkID chunk.ChunkID) bool

	// IsTombstoned returns true if the given chunk ID has been deleted from
	// this tier's replicated FSM and is still within the tombstone retention
	// window. Used to reject stale replication commands (ImportSealed,
	// Append, Seal) that race with retention — without this check, a late
	// ImportSealed RPC could recreate a chunk the cluster already deleted,
	// producing a "ghost" chunk on the follower. See gastrolog-11rzz.
	// Nil when no Raft group exists.
	IsTombstoned func(id chunk.ChunkID) bool

	// ApplyRaftDelete applies CmdDeleteChunk through replicated metadata and blocks
	// until committed. Returns an error if not leader or timeout. Nil when no
	// Raft group exists.
	ApplyRaftDelete func(id chunk.ChunkID) error

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

	// Reconciler owns chunk-lifecycle execution for this tier instance:
	// FSM-apply event handlers (seal, retention-pending, transition-streamed,
	// transition-received, request-delete, ack-delete, finalize-delete) plus
	// the canonical deleteChunk entry point. All cluster-wide deletes route
	// through here over gastrolog-51gme steps 4-8. Nil for memory-mode tiers
	// (no FSM, no replication).
	Reconciler *TierLifecycleReconciler

	// ListManifest returns all chunk IDs in the tier FSM view — the authoritative
	// set of chunks that should exist. Nil when no Raft group exists.
	ListManifest func() []chunk.ChunkID

	// IsFSMReady returns true after the tier FSM has applied at least one log
	// entry or restored from a snapshot. Before that, the manifest is incomplete
	// and must not be used for reconciliation decisions.
	IsFSMReady func() bool

	// OverlayFromFSM returns a copy of the given chunk meta with cluster-wide
	// fields (CloudBacked, Archived, NumFrames) sourced from replicated metadata
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
}

// applyRaftCallbacks wires raft-backed metadata operations from a tierRaftCallbacks.
func (t *TierInstance) applyRaftCallbacks(cb tierRaftCallbacks) {
	t.HasRaftLeader = cb.hasLeader
	t.IsRaftLeader = cb.isLeader
	t.ApplyRaftDelete = cb.applyDelete
	t.ApplyRaftRequestDelete = cb.applyRequestDelete
	t.ApplyRaftAckDelete = cb.applyAckDelete
	t.ApplyRaftFinalizeDelete = cb.applyFinalizeDelete
	t.ListManifest = cb.listChunks
	t.ApplyRaftRetentionPending = cb.applyRetPending
	t.ListRetentionPending = cb.listRetPending
	t.ApplyRaftTransitionStreamed = cb.applyTransitionStreamed
	t.ListTransitionStreamed = cb.listTransitionStreamed
	t.ApplyRaftTransitionReceived = cb.applyTransitionReceived
	t.HasTransitionReceipt = cb.hasTransitionReceipt
	t.IsTombstoned = cb.isTombstoned
	t.IsFSMReady = cb.isFSMReady
	t.OverlayFromFSM = cb.overlayFromFSM
}

// IsLeader returns true if this node is the leader for this tier.
func (t *TierInstance) IsLeader() bool { return !t.IsFollower }

// ShouldForwardToFollowers returns true if this leader tier has replication targets.
func (t *TierInstance) ShouldForwardToFollowers() bool {
	return t.IsLeader() && len(t.FollowerTargets) > 0
}
