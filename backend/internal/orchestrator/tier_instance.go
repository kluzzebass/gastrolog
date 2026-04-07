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
	IsFollower      bool                     // true if this node is a follower for this tier
	LeaderNodeID    string                   // the leader node's ID (empty if this IS the leader)
	FollowerTargets []config.ReplicationTarget // per-storage targets (populated on leader only)

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
}

// applyRaftCallbacks wires raft-backed metadata operations from a tierRaftCallbacks.
func (t *TierInstance) applyRaftCallbacks(cb tierRaftCallbacks) {
	t.HasRaftLeader = cb.hasLeader
	t.IsRaftLeader = cb.isLeader
	t.ApplyRaftDelete = cb.applyDelete
	t.ListManifest = cb.listChunks
	t.ApplyRaftRetentionPending = cb.applyRetPending
	t.ListRetentionPending = cb.listRetPending
	t.IsFSMReady = cb.isFSMReady
}

// IsLeader returns true if this node is the leader for this tier.
func (t *TierInstance) IsLeader() bool { return !t.IsFollower }

// ShouldForwardToFollowers returns true if this leader tier has replication targets.
func (t *TierInstance) ShouldForwardToFollowers() bool {
	return t.IsLeader() && len(t.FollowerTargets) > 0
}
