package orchestrator

import (
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
		return s.Chunks
	}
	return nil
}

// IndexManager returns the index manager registered under the given key.
// Returns nil if not found.
func (o *Orchestrator) IndexManager(key uuid.UUID) index.IndexManager {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if s := o.vaults[key]; s != nil {
		return s.Indexes
	}
	return nil
}

// IsRunning returns true if the orchestrator is running.
func (o *Orchestrator) IsRunning() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.running
}

// ListVaults returns all registered vault IDs.
func (o *Orchestrator) ListVaults() []uuid.UUID {
	o.mu.RLock()
	defer o.mu.RUnlock()
	keys := make([]uuid.UUID, 0, len(o.vaults))
	for k := range o.vaults {
		keys = append(keys, k)
	}
	return keys
}

// ListIngesters returns all registered ingester IDs.
func (o *Orchestrator) ListIngesters() []uuid.UUID {
	o.mu.RLock()
	defer o.mu.RUnlock()
	keys := make([]uuid.UUID, 0, len(o.ingesters))
	for k := range o.ingesters {
		keys = append(keys, k)
	}
	return keys
}

// QueryEngine returns the query engine registered under the given key.
// Returns nil if not found.
func (o *Orchestrator) QueryEngine(key uuid.UUID) *query.Engine {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if s := o.vaults[key]; s != nil {
		return s.Query
	}
	return nil
}

// MultiVaultQueryEngine returns a query engine that searches across all vaults.
// Vault predicates in queries (e.g., "vault=prod") filter which vaults are searched.
func (o *Orchestrator) MultiVaultQueryEngine() *query.Engine {
	return query.NewWithRegistry(o, o.logger)
}
