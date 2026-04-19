package orchestrator

import (
	"bytes"
	"fmt"
	"gastrolog/internal/glid"
	"slices"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/query"
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

// RegisterIngester adds an ingester to the registry.
// Must be called before Start().
func (o *Orchestrator) RegisterIngester(id glid.GLID, name, ingType string, r Ingester) {
	o.registerIngester(id, name, ingType, false, r)
}

func (o *Orchestrator) registerIngester(id glid.GLID, name, ingType string, passive bool, r Ingester) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ingesters[id] = r
	o.ingesterMeta[id] = ingesterInfo{Name: name, Type: ingType, Passive: passive}
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
func (o *Orchestrator) UnregisterIngester(id glid.GLID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.ingesters, id)
	delete(o.ingesterMeta, id)
}

// ChunkManager returns the chunk manager registered under the given key.
// Returns nil if not found.
func (o *Orchestrator) ChunkManager(key glid.GLID) chunk.ChunkManager {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if s := o.vaults[key]; s != nil {
		return s.ChunkManager()
	}
	return nil
}

// IndexManager returns the index manager registered under the given key.
// Returns nil if not found.
func (o *Orchestrator) IndexManager(key glid.GLID) index.IndexManager {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if s := o.vaults[key]; s != nil {
		return s.IndexManager()
	}
	return nil
}

// TriggerIngester triggers a one-shot emission on a Triggerable ingester.
// Returns an error if the ingester is not found or doesn't support triggering.
func (o *Orchestrator) TriggerIngester(id glid.GLID) error {
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
func (o *Orchestrator) ListVaults() []glid.GLID {
	o.mu.RLock()
	defer o.mu.RUnlock()
	keys := make([]glid.GLID, 0, len(o.vaults))
	for k := range o.vaults {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b glid.GLID) int { return bytes.Compare(a[:], b[:]) })
	return keys
}

// LocalPrimaryTierIDs returns the set of tier IDs where this node is the
// leader. Used by search fan-out: follower tiers are NOT included because
// they may be incomplete (missing active chunk, replication lag). The search
// always fans out to the leader for authoritative results.
func (o *Orchestrator) LocalPrimaryTierIDs() map[glid.GLID]bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	ids := make(map[glid.GLID]bool)
	for _, v := range o.vaults {
		for _, t := range v.Tiers {
			if !t.IsFollower {
				ids[t.TierID] = true
			}
		}
	}
	return ids
}

// HasLocalQueryEngine returns true if the vault has at least one tier with
// a query engine on this node (i.e., actual searchable data, not just a
// routing entry). Used by search fan-out to decide local vs. remote.
func (o *Orchestrator) HasLocalQueryEngine(vaultID glid.GLID) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	v := o.vaults[vaultID]
	if v == nil {
		return false
	}
	for _, t := range v.Tiers {
		if t.Query != nil {
			return true
		}
	}
	return false
}

// ListIngesters returns all registered ingester IDs in deterministic order.
func (o *Orchestrator) ListIngesters() []glid.GLID {
	o.mu.RLock()
	defer o.mu.RUnlock()
	keys := make([]glid.GLID, 0, len(o.ingesters))
	for k := range o.ingesters {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b glid.GLID) int { return bytes.Compare(a[:], b[:]) })
	return keys
}

// QueryEngine returns the query engine registered under the given key.
// Returns nil if not found.
func (o *Orchestrator) QueryEngine(key glid.GLID) *query.Engine {
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

// PrimaryTierQueryEngine returns a query engine that only searches leader
// tiers (not follower replicas). Used by ForwardSearch handlers to avoid
// double-counting when the requesting node already searches its own followers.
func (o *Orchestrator) PrimaryTierQueryEngine() *query.Engine {
	return query.NewWithRegistry(&primaryTierRegistry{o: o}, o.logger)
}

// primaryTierRegistry provides a flat view of all leader tiers across all
// vaults. Each tier is a searchable unit keyed by its tier ID.
type primaryTierRegistry struct {
	o *Orchestrator
}

func (r *primaryTierRegistry) ListVaults() []glid.GLID {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	var ids []glid.GLID
	for _, v := range r.o.vaults {
		for _, t := range v.Tiers {
			if !t.IsFollower && t.Query != nil {
				ids = append(ids, t.TierID)
			}
		}
	}
	return ids
}

func (r *primaryTierRegistry) ChunkManager(key glid.GLID) chunk.ChunkManager {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	for _, v := range r.o.vaults {
		for _, t := range v.Tiers {
			if t.TierID == key && !t.IsFollower && t.Query != nil {
				return t.Chunks
			}
		}
	}
	return nil
}

func (r *primaryTierRegistry) IndexManager(key glid.GLID) index.IndexManager {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	for _, v := range r.o.vaults {
		for _, t := range v.Tiers {
			if t.TierID == key && !t.IsFollower && t.Query != nil {
				return t.Indexes
			}
		}
	}
	return nil
}

func (r *primaryTierRegistry) QueryEngine(_ glid.GLID) *query.Engine { return nil }

// PrimaryTierQueryEngineForVault returns a query engine scoped to leader
// tiers of a single vault. Used by ForwardSearch — the vault is already
// selected, no vault_id= filtering needed.
func (o *Orchestrator) PrimaryTierQueryEngineForVault(vaultID glid.GLID) *query.Engine {
	o.mu.RLock()
	defer o.mu.RUnlock()
	v := o.vaults[vaultID]
	if v == nil {
		return nil
	}
	var primary []*TierInstance
	for _, t := range v.Tiers {
		if !t.IsFollower && t.Query != nil {
			primary = append(primary, t)
		}
	}
	if len(primary) == 0 {
		return nil
	}
	return query.NewWithRegistry(&tierRegistry{tiers: primary}, o.logger)
}
