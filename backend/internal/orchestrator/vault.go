package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

// Vault bundles tier instances for a single vault.
// The active tier (Tiers[0]) provides the chunk manager, index manager, and
// query engine used by the orchestrator for ingest and search.
type Vault struct {
	ID      uuid.UUID
	Name    string
	Enabled bool
	Tiers   []*TierInstance
}

// NewVault creates a Vault with the given tier instances.
func NewVault(id uuid.UUID, tiers ...*TierInstance) *Vault {
	return &Vault{
		ID:      id,
		Enabled: true,
		Tiers:   tiers,
	}
}

// ActiveTier returns the first (hot) tier.
func (v *Vault) ActiveTier() *TierInstance { return v.Tiers[0] }

// ChunkManager returns the chunk manager from the active tier.
func (v *Vault) ChunkManager() chunk.ChunkManager { return v.Tiers[0].Chunks }

// IndexManager returns the index manager from the active tier.
func (v *Vault) IndexManager() index.IndexManager { return v.Tiers[0].Indexes }

// QueryEngine returns the query engine from the active tier.
func (v *Vault) QueryEngine() *query.Engine { return v.Tiers[0].Query }

// Type returns the storage type of the active tier.
func (v *Vault) Type() string { return v.Tiers[0].Type }

// NewVaultFromComponents creates a Vault from raw components (chunk manager,
// index manager, query engine). This wraps the components in a single
// TierInstance with type "memory". Intended for test code.
func NewVaultFromComponents(id uuid.UUID, cm chunk.ChunkManager, im index.IndexManager, qe *query.Engine) *Vault {
	return NewVault(id, &TierInstance{
		TierID:  id, // reuse vault ID as tier ID for simplicity
		Type:    "memory",
		Chunks:  cm,
		Indexes: im,
		Query:   qe,
	})
}

// Close closes all tier instances.
func (v *Vault) Close() error {
	var firstErr error
	for _, t := range v.Tiers {
		if err := t.Chunks.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
