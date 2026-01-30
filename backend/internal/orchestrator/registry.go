package orchestrator

import (
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
	if key == "" {
		key = "default"
	}
	return o.chunks[key]
}
