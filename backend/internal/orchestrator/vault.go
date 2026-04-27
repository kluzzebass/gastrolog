package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	"gastrolog/internal/query"
)

// Vault bundles tier instances for a single vault.
// Ingest uses ActiveTierChunkManager / ActiveTierIndexManager (Tiers[0]).
// Chunk-scoped operations must resolve the owning tier via the orchestrator,
// not assume Tiers[0].
type Vault struct {
	ID             glid.GLID
	Name           string
	Enabled        bool
	Tiers          []*TierInstance
	multiTierQuery *query.Engine // lazy; created on first QueryEngine() call for multi-tier vaults
}

// NewVault creates a Vault with the given tier instances.
func NewVault(id glid.GLID, tiers ...*TierInstance) *Vault {
	return &Vault{
		ID:      id,
		Enabled: true,
		Tiers:   tiers,
	}
}

// ActiveTier returns the first (hot) tier, or nil if the vault has no tiers yet.
func (v *Vault) ActiveTier() *TierInstance {
	if len(v.Tiers) == 0 {
		return nil
	}
	return v.Tiers[0]
}

// ActiveTierChunkManager returns the chunk manager for the active (ingest) tier
// (Tiers[0]), or nil if the vault has no tiers.
func (v *Vault) ActiveTierChunkManager() chunk.ChunkManager {
	if len(v.Tiers) == 0 {
		return nil
	}
	return v.Tiers[0].Chunks
}

// ActiveTierIndexManager returns the index manager for the active tier, or nil
// if the vault has no tiers.
func (v *Vault) ActiveTierIndexManager() index.IndexManager {
	if len(v.Tiers) == 0 {
		return nil
	}
	return v.Tiers[0].Indexes
}

// QueryEngine returns a query engine that searches ALL local tiers.
// For single-tier vaults, this is the tier's own engine.
// For multi-tier vaults, this uses a tier registry to fan out.
// Returns nil if the vault has no tiers yet.
func (v *Vault) QueryEngine() *query.Engine {
	if len(v.Tiers) == 0 {
		return nil
	}
	if len(v.Tiers) == 1 {
		return v.Tiers[0].Query
	}
	if v.multiTierQuery == nil {
		v.multiTierQuery = query.NewWithRegistry(&tierRegistry{tiers: v.Tiers}, nil)
	}
	return v.multiTierQuery
}

// Type returns the storage type of the active tier, or "" if no tiers.
func (v *Vault) Type() string {
	if len(v.Tiers) == 0 {
		return ""
	}
	return v.Tiers[0].Type
}

// NewVaultFromComponents creates a Vault from raw components (chunk manager,
// index manager, query engine). This wraps the components in a single
// TierInstance with type "memory". Intended for test code.
func NewVaultFromComponents(id glid.GLID, cm chunk.ChunkManager, im index.IndexManager, qe *query.Engine) *Vault {
	return NewVault(id, &TierInstance{
		TierID:  id, // reuse vault ID as tier ID for simplicity
		Type:    "memory",
		Chunks:  cm,
		Indexes: im,
		Query:   qe,
	})
}

// tierRegistry adapts a vault's tiers to the query.VaultRegistry interface,
// allowing the query engine to fan out across all tiers as if they were vaults.
type tierRegistry struct {
	tiers []*TierInstance
}

func (r *tierRegistry) ListVaults() []glid.GLID {
	ids := make([]glid.GLID, len(r.tiers))
	for i, t := range r.tiers {
		ids[i] = t.TierID
	}
	return ids
}

func (r *tierRegistry) ChunkManager(id glid.GLID) chunk.ChunkManager {
	for _, t := range r.tiers {
		if t.TierID == id {
			return t.Chunks
		}
	}
	return nil
}

func (r *tierRegistry) IndexManager(id glid.GLID) index.IndexManager {
	for _, t := range r.tiers {
		if t.TierID == id {
			return t.Indexes
		}
	}
	return nil
}

// TransitionStreamedChunks returns the set of chunk IDs on the given
// tier that have been streamed to the next tier but not yet expired.
// The histogram and other count-based aggregations skip these so the
// records aren't counted twice — once at the source tier and once at
// the destination — during the receipt-confirmation window. See
// gastrolog-4xusf.
func (r *tierRegistry) TransitionStreamedChunks(id glid.GLID) map[chunk.ChunkID]bool {
	for _, t := range r.tiers {
		if t.TierID != id || t.ListTransitionStreamed == nil {
			continue
		}
		ids := t.ListTransitionStreamed()
		if len(ids) == 0 {
			return nil
		}
		out := make(map[chunk.ChunkID]bool, len(ids))
		for _, cid := range ids {
			out[cid] = true
		}
		return out
	}
	return nil
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
