package orchestrator

import (
	"cmp"
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/query"
)

// RegisterChunkManager adds a chunk manager to the registry.
func (o *Orchestrator) RegisterChunkManager(key string, cm chunk.ChunkManager) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.chunks[key] = cm
}

// RegisterIndexManager adds an index manager to the registry.
func (o *Orchestrator) RegisterIndexManager(key string, im index.IndexManager) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.indexes[key] = im
}

// RegisterQueryEngine adds a query engine to the registry.
func (o *Orchestrator) RegisterQueryEngine(key string, qe *query.Engine) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.queries[key] = qe
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
	return o.chunks[cmp.Or(key, "default")]
}

// ChunkManagers returns all registered chunk manager keys.
func (o *Orchestrator) ChunkManagers() []string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	keys := make([]string, 0, len(o.chunks))
	for k := range o.chunks {
		keys = append(keys, k)
	}
	return keys
}

// IndexManager returns the index manager registered under the given key.
// Returns nil if not found.
func (o *Orchestrator) IndexManager(key string) index.IndexManager {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.indexes[cmp.Or(key, "default")]
}

// IndexManagers returns all registered index manager keys.
func (o *Orchestrator) IndexManagers() []string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	keys := make([]string, 0, len(o.indexes))
	for k := range o.indexes {
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
	return o.queries[cmp.Or(key, "default")]
}

// MultiStoreQueryEngine returns a query engine that searches across all stores.
// Store predicates in queries (e.g., "store=prod") filter which stores are searched.
func (o *Orchestrator) MultiStoreQueryEngine() *query.Engine {
	return query.NewWithRegistry(o, o.logger)
}
