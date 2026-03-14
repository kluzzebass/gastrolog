package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

// Vault bundles the chunk manager, index manager, and query engine for a single vault.
// The invariant that every vault ID has all three is now structurally enforced.
type Vault struct {
	ID      uuid.UUID
	Name    string
	Type    string
	Chunks  chunk.ChunkManager
	Indexes index.IndexManager
	Query   *query.Engine
	Enabled bool
}

// NewVault creates a Vault from its components.
func NewVault(id uuid.UUID, cm chunk.ChunkManager, im index.IndexManager, qe *query.Engine) *Vault {
	return &Vault{
		ID:      id,
		Chunks:  cm,
		Indexes: im,
		Query:   qe,
		Enabled: true,
	}
}
