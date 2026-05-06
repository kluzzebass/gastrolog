package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	"gastrolog/internal/query"
)

// Vault is the node-local materialization of a VaultConfig: identity plus
// the single VaultInstance that owns this node's chunk + index + query
// state. Phase 2 (gastrolog-3iy5l) collapsed the per-vault tier slice —
// every vault now owns exactly one instance — so callers reach through
// Instance directly rather than iterating a slice.
type Vault struct {
	ID          glid.GLID
	Name        string
	Enabled     bool
	StorageType string // mirrored from VaultConfig.Type
	Instance    *VaultInstance
}

// NewVault creates a Vault with a single instance.
func NewVault(id glid.GLID, instance *VaultInstance) *Vault {
	return &Vault{
		ID:       id,
		Enabled:  true,
		Instance: instance,
	}
}

// ChunkManager returns the vault's chunk manager, or nil if no instance
// is registered yet (initial registration, before reconcile finishes).
func (v *Vault) ChunkManager() chunk.ChunkManager {
	if v.Instance == nil {
		return nil
	}
	return v.Instance.Chunks
}

// IndexManager returns the vault's index manager, or nil if no instance
// is registered yet.
func (v *Vault) IndexManager() index.IndexManager {
	if v.Instance == nil {
		return nil
	}
	return v.Instance.Indexes
}

// QueryEngine returns the vault's query engine, or nil if no instance
// is registered yet.
func (v *Vault) QueryEngine() *query.Engine {
	if v.Instance == nil {
		return nil
	}
	return v.Instance.Query
}

// Type returns the storage type of the vault, falling back to the
// instance's own type for legacy callers that constructed Vault without
// StorageType set (notably NewVaultFromComponents and other test paths).
func (v *Vault) Type() string {
	if v.StorageType != "" {
		return v.StorageType
	}
	if v.Instance == nil {
		return ""
	}
	return v.Instance.Type
}

// NewVaultFromComponents creates a Vault from raw components (chunk manager,
// index manager, query engine), wrapping them in a single VaultInstance
// of type "memory". Intended for test code.
func NewVaultFromComponents(id glid.GLID, cm chunk.ChunkManager, im index.IndexManager, qe *query.Engine) *Vault {
	return NewVault(id, &VaultInstance{
		VaultID: id,
		Type:    "memory",
		Chunks:  cm,
		Indexes: im,
		Query:   qe,
	})
}

// Close closes the vault's instance.
func (v *Vault) Close() error {
	if v.Instance == nil || v.Instance.Chunks == nil {
		return nil
	}
	return v.Instance.Chunks.Close()
}
