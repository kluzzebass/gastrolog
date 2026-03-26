package orchestrator

import (
	"sync"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

// TierInstance is the node-local materialization of a TierConfig.
// TierConfig (in Raft config) is the logical definition.
// TierInstance is the physical runtime: chunk manager + index manager + query engine.
type TierInstance struct {
	TierID           uuid.UUID
	Type             string
	Chunks           chunk.ChunkManager
	Indexes          index.IndexManager
	Query            *query.Engine
	IsSecondary      bool     // true if this node is a secondary for this tier
	PrimaryNodeID    string   // the primary node's ID (empty if this IS the primary)
	SecondaryNodeIDs []string // secondary nodes (populated on primary, empty on secondaries)

	// durabilityBuf holds records forwarded from the primary for active-chunk
	// durability. Invisible to search, ListChunks, and the inspector.
	// Cleared when sealed-chunk replication delivers the canonical version.
	// Only populated on secondaries.
	durabilityMu  sync.Mutex
	durabilityBuf []chunk.Record
}

// BufferRecord appends a record to the durability buffer (secondary only).
func (t *TierInstance) BufferRecord(rec chunk.Record) {
	t.durabilityMu.Lock()
	t.durabilityBuf = append(t.durabilityBuf, rec)
	t.durabilityMu.Unlock()
}

// ClearDurabilityBuffer discards all buffered records.
// Called when sealed-chunk replication delivers the canonical version.
func (t *TierInstance) ClearDurabilityBuffer() {
	t.durabilityMu.Lock()
	t.durabilityBuf = nil
	t.durabilityMu.Unlock()
}

// DurabilityBufferLen returns the number of buffered records.
func (t *TierInstance) DurabilityBufferLen() int {
	t.durabilityMu.Lock()
	defer t.durabilityMu.Unlock()
	return len(t.durabilityBuf)
}
