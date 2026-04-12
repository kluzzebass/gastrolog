package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/config"
	"gastrolog/internal/index"
	"gastrolog/internal/query"

	"github.com/google/uuid"
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
	TierID           uuid.UUID
	StorageID        string // the file storage ID this instance uses (empty for memory/JSONL tiers)
	Type             string
	Chunks           chunk.ChunkManager
	Indexes          index.IndexManager
	Query            *query.Engine
	IsFollower      bool                     // true if this node is a config-placed follower (used at build time and as bootstrap fallback)
	FollowerTargets []config.ReplicationTarget // per-storage replication targets

	// HasRaftLeader returns true if the tier's Raft group has an elected leader.
	// Nil when no Raft group exists (single-node / memory mode).
	HasRaftLeader func() bool

	// IsRaftLeader returns true if THIS node is the Raft leader for this tier.
	// Nil when no Raft group exists (single-node / memory mode — always leader).
	IsRaftLeader func() bool

	// ApplyRaftRetentionPending marks a chunk as retention-pending in the tier Raft.
	ApplyRaftRetentionPending func(id chunk.ChunkID) error

	// ListRetentionPending returns chunk IDs with RetentionPending=true in the FSM.
	ListRetentionPending func() []chunk.ChunkID

	// ApplyRaftTransitionStreamed marks a chunk as streamed to the next tier
	// in the tier Raft, so it won't be deleted until the destination confirms
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

	// ApplyRaftDelete applies CmdDeleteChunk to the tier Raft group and blocks
	// until committed. Returns an error if not leader or timeout. Nil when no
	// Raft group exists.
	ApplyRaftDelete func(id chunk.ChunkID) error

	// ListManifest returns all chunk IDs in the tier Raft FSM — the authoritative
	// set of chunks that should exist. Nil when no Raft group exists.
	ListManifest func() []chunk.ChunkID

	// IsFSMReady returns true after the tier FSM has applied at least one log
	// entry or restored from a snapshot. Before that, the manifest is incomplete
	// and must not be used for reconciliation decisions.
	IsFSMReady func() bool

	// OverlayFromFSM returns a copy of the given chunk meta with cluster-wide
	// fields (CloudBacked, Archived, NumFrames) sourced from the tier Raft
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
	t.ListManifest = cb.listChunks
	t.ApplyRaftRetentionPending = cb.applyRetPending
	t.ListRetentionPending = cb.listRetPending
	t.ApplyRaftTransitionStreamed = cb.applyTransitionStreamed
	t.ListTransitionStreamed = cb.listTransitionStreamed
	t.ApplyRaftTransitionReceived = cb.applyTransitionReceived
	t.HasTransitionReceipt = cb.hasTransitionReceipt
	t.IsFSMReady = cb.isFSMReady
	t.OverlayFromFSM = cb.overlayFromFSM
}

// IsLeader returns true if this node is the operational leader for this tier.
// Derives from the tier Raft group when available. During bootstrap (Raft
// exists but no leader elected yet) and in single-node/memory mode (no Raft
// group), falls back to config placement (!IsFollower). See gastrolog-1s3mf.
func (t *TierInstance) IsLeader() bool {
	if t.IsRaftLeader != nil {
		if t.HasRaftLeader != nil && !t.HasRaftLeader() {
			// Bootstrap: Raft group exists but no leader elected yet.
			// Fall back to config placement so the system can start up.
			return !t.IsFollower
		}
		return t.IsRaftLeader()
	}
	// No Raft group (single-node / memory mode).
	return !t.IsFollower
}

// IsPrimaryInstance returns true if this is the config-placed primary
// instance (not a follower replica). Used for query deduplication and
// instance selection — "which local copy is canonical" — NOT for gating
// background operations (use IsLeader() for that). See gastrolog-1s3mf.
func (t *TierInstance) IsPrimaryInstance() bool {
	return !t.IsFollower
}

// ShouldForwardToFollowers returns true if this leader tier has replication targets.
func (t *TierInstance) ShouldForwardToFollowers() bool {
	return t.IsLeader() && len(t.FollowerTargets) > 0
}
