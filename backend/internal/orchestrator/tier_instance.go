package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

// TierInstance is the node-local materialization of a TierConfig.
// TierConfig (in Raft config) is the logical definition.
// TierInstance is the physical runtime: chunk manager + index manager + query engine.
type TierInstance struct {
	TierID  uuid.UUID
	Type    string
	Chunks  chunk.ChunkManager
	Indexes index.IndexManager
	Query   *query.Engine
}
