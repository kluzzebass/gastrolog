package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

// RegisterStore adds a store to the registry.
func (o *Orchestrator) RegisterStore(store *Store) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stores[store.ID] = store
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
func (o *Orchestrator) RegisterIngester(id uuid.UUID, r Ingester) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ingesters[id] = r
	if o.ingesterStats[id] == nil {
		o.ingesterStats[id] = &IngesterStats{}
	}
}

// SetFilterSet sets the filter set for attribute-based message filtering.
// Must be called before Start() or Ingest().
// If not set, messages are sent to all stores (legacy fan-out behavior).
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
}

// ChunkManager returns the chunk manager registered under the given key.
// Returns nil if not found.
func (o *Orchestrator) ChunkManager(key uuid.UUID) chunk.ChunkManager {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if s := o.stores[key]; s != nil {
		return s.Chunks
	}
	return nil
}

// IndexManager returns the index manager registered under the given key.
// Returns nil if not found.
func (o *Orchestrator) IndexManager(key uuid.UUID) index.IndexManager {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if s := o.stores[key]; s != nil {
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

// ListStores returns all registered store IDs.
func (o *Orchestrator) ListStores() []uuid.UUID {
	o.mu.RLock()
	defer o.mu.RUnlock()
	keys := make([]uuid.UUID, 0, len(o.stores))
	for k := range o.stores {
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
	if s := o.stores[key]; s != nil {
		return s.Query
	}
	return nil
}

// MultiStoreQueryEngine returns a query engine that searches across all stores.
// Store predicates in queries (e.g., "store=prod") filter which stores are searched.
func (o *Orchestrator) MultiStoreQueryEngine() *query.Engine {
	return query.NewWithRegistry(o, o.logger)
}
