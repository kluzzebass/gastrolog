package orchestrator

import (
	"cmp"
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/query"
)

// RegisterStore adds a store to the registry.
func (o *Orchestrator) RegisterStore(store *Store) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stores[store.ID] = store
}

// RegisterChunkManager adds a chunk manager to the registry.
// Deprecated: use RegisterStore(NewStore(...)) instead.
func (o *Orchestrator) RegisterChunkManager(key string, cm chunk.ChunkManager) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if s := o.stores[key]; s != nil {
		s.Chunks = cm
		return
	}
	o.stores[key] = NewStore(key, cm, nil, nil)
}

// RegisterIndexManager adds an index manager to the registry.
// Deprecated: use RegisterStore(NewStore(...)) instead.
func (o *Orchestrator) RegisterIndexManager(key string, im index.IndexManager) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if s := o.stores[key]; s != nil {
		s.Indexes = im
		return
	}
	o.stores[key] = NewStore(key, nil, im, nil)
}

// RegisterQueryEngine adds a query engine to the registry.
// Deprecated: use RegisterStore(NewStore(...)) instead.
func (o *Orchestrator) RegisterQueryEngine(key string, qe *query.Engine) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if s := o.stores[key]; s != nil {
		s.Query = qe
		return
	}
	o.stores[key] = NewStore(key, nil, nil, qe)
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
func (o *Orchestrator) RegisterIngester(id string, r Ingester) {
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
func (o *Orchestrator) UnregisterIngester(id string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.ingesters, id)
}

// ChunkManager returns the chunk manager registered under the given key.
// Returns nil if not found.
func (o *Orchestrator) ChunkManager(key string) chunk.ChunkManager {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if s := o.stores[cmp.Or(key, "default")]; s != nil {
		return s.Chunks
	}
	return nil
}

// ChunkManagers returns all registered chunk manager keys.
func (o *Orchestrator) ChunkManagers() []string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	keys := make([]string, 0, len(o.stores))
	for k := range o.stores {
		keys = append(keys, k)
	}
	return keys
}

// IndexManager returns the index manager registered under the given key.
// Returns nil if not found.
func (o *Orchestrator) IndexManager(key string) index.IndexManager {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if s := o.stores[cmp.Or(key, "default")]; s != nil {
		return s.Indexes
	}
	return nil
}

// IndexManagers returns all registered index manager keys.
func (o *Orchestrator) IndexManagers() []string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	keys := make([]string, 0, len(o.stores))
	for k := range o.stores {
		keys = append(keys, k)
	}
	return keys
}

// Ingesters returns all registered ingester IDs.
func (o *Orchestrator) Ingesters() []string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	keys := make([]string, 0, len(o.ingesters))
	for k := range o.ingesters {
		keys = append(keys, k)
	}
	return keys
}

// Running returns true if the orchestrator is running.
func (o *Orchestrator) Running() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.running
}

// IsRunning is an alias for Running.
func (o *Orchestrator) IsRunning() bool {
	return o.Running()
}

// ListStores returns all registered store IDs.
// This is an alias for ChunkManagers.
func (o *Orchestrator) ListStores() []string {
	return o.ChunkManagers()
}

// ListIngesters returns all registered ingester IDs.
// This is an alias for Ingesters.
func (o *Orchestrator) ListIngesters() []string {
	return o.Ingesters()
}

// QueryEngine returns the query engine registered under the given key.
// Returns nil if not found.
func (o *Orchestrator) QueryEngine(key string) *query.Engine {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if s := o.stores[cmp.Or(key, "default")]; s != nil {
		return s.Query
	}
	return nil
}

// MultiStoreQueryEngine returns a query engine that searches across all stores.
// Store predicates in queries (e.g., "store=prod") filter which stores are searched.
func (o *Orchestrator) MultiStoreQueryEngine() *query.Engine {
	return query.NewWithRegistry(o, o.logger)
}
