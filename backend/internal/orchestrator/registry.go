package orchestrator

import (
	"bytes"
	"fmt"
	"slices"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

// RegisterVault adds a vault to the registry.
func (o *Orchestrator) RegisterVault(vault *Vault) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.vaults[vault.ID] = vault
}

// RegisterDigester appends a digester to the processing pipeline.
// Digesters run in registration order on each message before storage.
// Must be called before Start().
func (o *Orchestrator) RegisterDigester(d Digester) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.digesters = append(o.digesters, d)
}

// RegisterIngester adds a ingester to the registry.
// Must be called before Start().
func (o *Orchestrator) RegisterIngester(id uuid.UUID, name, ingType string, r Ingester) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ingesters[id] = r
	o.ingesterMeta[id] = ingesterInfo{Name: name, Type: ingType}
	if o.ingesterStats[id] == nil {
		o.ingesterStats[id] = &IngesterStats{}
	}
}

// SetFilterSet sets the filter set for attribute-based message filtering.
// Must be called before Start() or Ingest().
// If not set, messages are sent to all vaults (legacy fan-out behavior).
func (o *Orchestrator) SetFilterSet(fs *FilterSet) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.filterSet = fs
}

// UnregisterIngester removes a ingester from the registry.
// Must be called before Start() or after Stop().
func (o *Orchestrator) UnregisterIngester(id uuid.UUID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.ingesters, id)
	delete(o.ingesterMeta, id)
}

// ChunkManager returns the chunk manager registered under the given key.
// Returns nil if not found.
func (o *Orchestrator) ChunkManager(key uuid.UUID) chunk.ChunkManager {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if s := o.vaults[key]; s != nil {
		return s.ChunkManager()
	}
	return nil
}

// IndexManager returns the index manager registered under the given key.
// Returns nil if not found.
func (o *Orchestrator) IndexManager(key uuid.UUID) index.IndexManager {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if s := o.vaults[key]; s != nil {
		return s.IndexManager()
	}
	return nil
}

// TriggerIngester triggers a one-shot emission on a Triggerable ingester.
// Returns an error if the ingester is not found or doesn't support triggering.
func (o *Orchestrator) TriggerIngester(id uuid.UUID) error {
	o.mu.RLock()
	ing, ok := o.ingesters[id]
	o.mu.RUnlock()
	if !ok {
		return fmt.Errorf("ingester not found: %s", id)
	}
	trig, ok := ing.(Triggerable)
	if !ok {
		return fmt.Errorf("ingester %s does not support triggering", id)
	}
	trig.Trigger()
	return nil
}

// IsRunning returns true if the orchestrator is running.
func (o *Orchestrator) IsRunning() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.running
}

// ListVaults returns all registered vault IDs in deterministic order.
func (o *Orchestrator) ListVaults() []uuid.UUID {
	o.mu.RLock()
	defer o.mu.RUnlock()
	keys := make([]uuid.UUID, 0, len(o.vaults))
	for k := range o.vaults {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
	return keys
}

// LocalPrimaryTierIDs returns the set of tier IDs where this node is the
// primary. Used by search fan-out: secondary tiers are NOT included because
// they may be incomplete (missing active chunk, replication lag). The search
// always fans out to the primary for authoritative results.
func (o *Orchestrator) LocalPrimaryTierIDs() map[uuid.UUID]bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	ids := make(map[uuid.UUID]bool)
	for _, v := range o.vaults {
		for _, t := range v.Tiers {
			if !t.IsSecondary {
				ids[t.TierID] = true
			}
		}
	}
	return ids
}

// HasLocalQueryEngine returns true if the vault has at least one tier with
// a query engine on this node (i.e., actual searchable data, not just a
// routing entry). Used by search fan-out to decide local vs. remote.
func (o *Orchestrator) HasLocalQueryEngine(vaultID uuid.UUID) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	v := o.vaults[vaultID]
	if v == nil {
		return false
	}
	return len(v.Tiers) > 0
}

// ListIngesters returns all registered ingester IDs in deterministic order.
func (o *Orchestrator) ListIngesters() []uuid.UUID {
	o.mu.RLock()
	defer o.mu.RUnlock()
	keys := make([]uuid.UUID, 0, len(o.ingesters))
	for k := range o.ingesters {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
	return keys
}

// QueryEngine returns the query engine registered under the given key.
// Returns nil if not found.
func (o *Orchestrator) QueryEngine(key uuid.UUID) *query.Engine {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if s := o.vaults[key]; s != nil {
		return s.QueryEngine()
	}
	return nil
}

// MultiVaultQueryEngine returns a query engine that searches across all vaults.
// Vault predicates in queries (e.g., "vault_id=<uuid>") filter which vaults are searched.
func (o *Orchestrator) MultiVaultQueryEngine() *query.Engine {
	return query.NewWithRegistry(o, o.logger)
}

// PrimaryTierQueryEngine returns a query engine that only searches primary
// tiers (not secondary replicas). Used by ForwardSearch handlers to avoid
// double-counting when the requesting node already searches its own secondaries.
func (o *Orchestrator) PrimaryTierQueryEngine() *query.Engine {
	return query.NewWithRegistry(&primaryTierRegistry{o: o}, o.logger)
}

// primaryTierRegistry provides a flat view of all primary tiers across all
// vaults. Each tier is a searchable unit keyed by its tier ID.
type primaryTierRegistry struct {
	o *Orchestrator
}

func (r *primaryTierRegistry) ListVaults() []uuid.UUID {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	var ids []uuid.UUID
	for _, v := range r.o.vaults {
		for _, t := range v.Tiers {
			if !t.IsSecondary {
				ids = append(ids, t.TierID)
			}
		}
	}
	return ids
}

func (r *primaryTierRegistry) ChunkManager(key uuid.UUID) chunk.ChunkManager {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	for _, v := range r.o.vaults {
		for _, t := range v.Tiers {
			if t.TierID == key && !t.IsSecondary {
				return t.Chunks
			}
		}
	}
	return nil
}

func (r *primaryTierRegistry) IndexManager(key uuid.UUID) index.IndexManager {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	for _, v := range r.o.vaults {
		for _, t := range v.Tiers {
			if t.TierID == key && !t.IsSecondary {
				return t.Indexes
			}
		}
	}
	return nil
}

func (r *primaryTierRegistry) QueryEngine(_ uuid.UUID) *query.Engine { return nil }

// PrimaryTierQueryEngineForVault returns a query engine scoped to primary
// tiers of a single vault. Used by ForwardSearch — the vault is already
// selected, no vault_id= filtering needed.
func (o *Orchestrator) PrimaryTierQueryEngineForVault(vaultID uuid.UUID) *query.Engine {
	o.mu.RLock()
	defer o.mu.RUnlock()
	v := o.vaults[vaultID]
	if v == nil {
		return nil
	}
	var primary []*TierInstance
	for _, t := range v.Tiers {
		if !t.IsSecondary {
			primary = append(primary, t)
		}
	}
	if len(primary) == 0 {
		return nil
	}
	return query.NewWithRegistry(&tierRegistry{tiers: primary}, o.logger)
}
