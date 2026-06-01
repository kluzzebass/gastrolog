// Package ingestion holds V3 ingestion-phase building blocks. These types are
// developed and tested in isolation before any orchestrator wiring.
package ingestion

import (
	"sync/atomic"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// Minter assigns cluster-unique EventIDs for one ingester instance on one node.
// Constructed when the ingester starts; Mint is called immediately when a
// record is received, fetched, or generated — IngestTS is the mint time.
type Minter struct {
	ingesterID glid.GLID
	nodeID     glid.GLID
	seq        atomic.Uint32
}

// NewMinter returns a minter bound to a single (ingester, node) pair.
func NewMinter(ingesterID, nodeID glid.GLID) *Minter {
	return &Minter{
		ingesterID: ingesterID,
		nodeID:     nodeID,
	}
}

// Mint returns the next EventID. IngestTS is captured at call time (UTC).
func (m *Minter) Mint() chunk.EventID {
	seq := m.seq.Add(1) - 1
	return chunk.EventID{
		IngesterID: m.ingesterID,
		NodeID:     m.nodeID,
		IngestTS:   time.Now().UTC(),
		IngestSeq:  seq,
	}
}
