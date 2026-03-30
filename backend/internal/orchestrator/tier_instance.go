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
	IsSecondary      bool                     // true if this node is a secondary for this tier
	PrimaryNodeID    string                   // the primary node's ID (empty if this IS the primary)
	SecondaryTargets []config.ReplicationTarget // per-storage targets (populated on primary only)

	// HasRaftLeader returns true if the tier's Raft group has an elected leader.
	// Nil when no Raft group exists (single-node / memory mode).
	HasRaftLeader func() bool

	// IsRaftLeader returns true if THIS node is the Raft leader for this tier.
	// Nil when no Raft group exists (single-node / memory mode — always leader).
	IsRaftLeader func() bool

	// ApplyRaftDelete applies CmdDeleteChunk to the tier Raft group and blocks
	// until committed. Returns an error if not leader or timeout. Nil when no
	// Raft group exists.
	ApplyRaftDelete func(id chunk.ChunkID) error

	// ListManifest returns all chunk IDs in the tier Raft FSM — the authoritative
	// set of chunks that should exist. Nil when no Raft group exists.
	ListManifest func() []chunk.ChunkID
}

// IsPrimary returns true if this node is the primary for this tier.
func (t *TierInstance) IsPrimary() bool { return !t.IsSecondary }

// ShouldForwardToSecondaries returns true if this primary tier has replication targets.
func (t *TierInstance) ShouldForwardToSecondaries() bool {
	return t.IsPrimary() && len(t.SecondaryTargets) > 0
}
