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

// RegisterReceiver adds a receiver to the registry.
// Must be called before Start().
func (o *Orchestrator) RegisterReceiver(id string, r Receiver) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.receivers[id] = r
}

// SetRouter sets the router for attribute-based message routing.
// Must be called before Start() or Ingest().
// If not set, messages are routed to all stores (legacy fan-out behavior).
func (o *Orchestrator) SetRouter(r *Router) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.router = r
}

// UnregisterReceiver removes a receiver from the registry.
// Must be called before Start() or after Stop().
func (o *Orchestrator) UnregisterReceiver(id string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.receivers, id)
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

// Receivers returns all registered receiver IDs.
func (o *Orchestrator) Receivers() []string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	keys := make([]string, 0, len(o.receivers))
	for k := range o.receivers {
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

// ListReceivers returns all registered receiver IDs.
// This is an alias for Receivers.
func (o *Orchestrator) ListReceivers() []string {
	return o.Receivers()
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
