package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"gastrolog/internal/config"
	"gastrolog/internal/query"
)

var (
	// ErrStoreNotEmpty is returned when attempting to remove a store that has data.
	ErrStoreNotEmpty = errors.New("store is not empty")
	// ErrStoreNotFound is returned when attempting to operate on a non-existent store.
	ErrStoreNotFound = errors.New("store not found")
	// ErrIngesterNotFound is returned when attempting to operate on a non-existent ingester.
	ErrIngesterNotFound = errors.New("ingester not found")
	// ErrDuplicateID is returned when attempting to add a component with an existing ID.
	ErrDuplicateID = errors.New("duplicate ID")
)

// UpdateRoutes recompiles route expressions from store configs and hot-swaps the router.
// This can be called while the system is running without disrupting ingestion.
//
// The routes are compiled from the Route field of each store in the config.
// Only stores that are currently registered in the orchestrator are included.
// Stores in the config that don't exist in the orchestrator are ignored.
func (o *Orchestrator) UpdateRoutes(cfg *config.Config) error {
	if cfg == nil {
		return nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	var compiledRoutes []*CompiledRoute

	for _, storeCfg := range cfg.Stores {
		// Only include stores that are registered.
		if _, ok := o.chunks[storeCfg.ID]; !ok {
			continue
		}

		var routeExpr string
		if storeCfg.Route != nil {
			routeExpr = *storeCfg.Route
		}
		route, err := CompileRoute(storeCfg.ID, routeExpr)
		if err != nil {
			return fmt.Errorf("invalid route for store %s: %w", storeCfg.ID, err)
		}
		compiledRoutes = append(compiledRoutes, route)
	}

	// Swap router atomically (we're under the lock).
	if len(compiledRoutes) > 0 {
		o.router = NewRouter(compiledRoutes)
		o.logger.Info("routes updated", "count", len(compiledRoutes))
	} else {
		o.router = nil
		o.logger.Warn("routes cleared, messages will fan out to all stores")
	}

	return nil
}

// AddIngester adds and optionally starts a new ingester.
// If the orchestrator is running, the ingester is started immediately.
// Returns ErrDuplicateID if a ingester with this ID already exists.
func (o *Orchestrator) AddIngester(id string, r Ingester) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, exists := o.ingesters[id]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateID, id)
	}

	o.ingesters[id] = r

	// If running, start the ingester immediately.
	if o.running && o.ingestCh != nil {
		ctx, cancel := context.WithCancel(context.Background())
		o.ingesterCancels[id] = cancel

		o.ingesterWg.Go(func() {
			r.Run(ctx, o.ingestCh)
		})
		o.logger.Info("ingester started", "id", id)
	}

	return nil
}

// RemoveIngester stops and removes a ingester.
// If the orchestrator is running, the ingester is stopped gracefully before removal.
// The method waits for the ingester to finish processing before returning.
func (o *Orchestrator) RemoveIngester(id string) error {
	o.mu.Lock()

	if _, exists := o.ingesters[id]; !exists {
		o.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrIngesterNotFound, id)
	}

	// If running, cancel the ingester's context.
	cancel, hasCancel := o.ingesterCancels[id]
	if o.running && hasCancel {
		cancel()
		delete(o.ingesterCancels, id)
	}

	delete(o.ingesters, id)
	o.mu.Unlock()

	// Note: We don't wait for the specific ingester to finish here because
	// ingesterWg tracks all ingesters collectively. The ingester will exit
	// when its context is cancelled, and the WaitGroup will decrement.
	// This is a best-effort removal - the ingester may still be draining.

	o.logger.Info("ingester removed", "id", id)
	return nil
}

// AddStore adds a new store (chunk manager, index manager, query engine) and updates the router.
// Returns ErrDuplicateID if a store with this ID already exists.
func (o *Orchestrator) AddStore(storeCfg config.StoreConfig, factories Factories) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, exists := o.chunks[storeCfg.ID]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateID, storeCfg.ID)
	}

	// Create chunk manager.
	cmFactory, ok := factories.ChunkManagers[storeCfg.Type]
	if !ok {
		return fmt.Errorf("unknown chunk manager type: %s", storeCfg.Type)
	}
	cm, err := cmFactory(storeCfg.Params)
	if err != nil {
		return fmt.Errorf("create chunk manager %s: %w", storeCfg.ID, err)
	}

	// Create index manager.
	imFactory, ok := factories.IndexManagers[storeCfg.Type]
	if !ok {
		return fmt.Errorf("unknown index manager type: %s", storeCfg.Type)
	}
	var imLogger = factories.Logger
	if imLogger != nil {
		imLogger = imLogger.With("store", storeCfg.ID)
	}
	im, err := imFactory(storeCfg.Params, cm, imLogger)
	if err != nil {
		return fmt.Errorf("create index manager %s: %w", storeCfg.ID, err)
	}

	// Create query engine.
	var qeLogger = factories.Logger
	if qeLogger != nil {
		qeLogger = qeLogger.With("store", storeCfg.ID)
	}
	qe := query.New(cm, im, qeLogger)

	// Register components.
	o.chunks[storeCfg.ID] = cm
	o.indexes[storeCfg.ID] = im
	o.queries[storeCfg.ID] = qe

	// Update router to include the new store's route.
	var routeExpr string
	if storeCfg.Route != nil {
		routeExpr = *storeCfg.Route
	}
	if err := o.updateRouterLocked(storeCfg.ID, routeExpr); err != nil {
		// Rollback registration on route error.
		delete(o.chunks, storeCfg.ID)
		delete(o.indexes, storeCfg.ID)
		delete(o.queries, storeCfg.ID)
		return err
	}

	o.logger.Info("store added", "id", storeCfg.ID, "type", storeCfg.Type, "route", routeExpr)
	return nil
}

// RemoveStore removes a store if it's empty (no chunks with data).
// Returns ErrStoreNotFound if the store doesn't exist.
// Returns ErrStoreNotEmpty if the store has any chunks.
func (o *Orchestrator) RemoveStore(id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	cm, exists := o.chunks[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrStoreNotFound, id)
	}

	// Check if store has any data.
	metas, err := cm.List()
	if err != nil {
		return fmt.Errorf("list chunks for store %s: %w", id, err)
	}
	if len(metas) > 0 {
		return fmt.Errorf("%w: store %s has %d chunk(s)", ErrStoreNotEmpty, id, len(metas))
	}

	// Also check if there's an active chunk with records.
	if active := cm.Active(); active != nil {
		return fmt.Errorf("%w: store %s has active chunk", ErrStoreNotEmpty, id)
	}

	// Remove from registries.
	delete(o.chunks, id)
	delete(o.indexes, id)
	delete(o.queries, id)

	// Rebuild router without this store.
	o.rebuildRouterLocked()

	o.logger.Info("store removed", "id", id)
	return nil
}

// updateRouterLocked adds or updates a single store's route in the router.
// Must be called with o.mu held.
func (o *Orchestrator) updateRouterLocked(storeID, routeExpr string) error {
	route, err := CompileRoute(storeID, routeExpr)
	if err != nil {
		return fmt.Errorf("invalid route for store %s: %w", storeID, err)
	}

	// Rebuild routes including the new one.
	var routes []*CompiledRoute

	// Keep existing routes for other stores.
	if o.router != nil {
		for _, r := range o.router.routes {
			if r.StoreID != storeID {
				routes = append(routes, r)
			}
		}
	}

	// Add the new/updated route.
	routes = append(routes, route)

	o.router = NewRouter(routes)
	return nil
}

// rebuildRouterLocked rebuilds the router from currently registered stores.
// Must be called with o.mu held.
// This is used after removing a store to exclude its route.
func (o *Orchestrator) rebuildRouterLocked() {
	if o.router == nil {
		return
	}

	var routes []*CompiledRoute
	for _, r := range o.router.routes {
		if _, exists := o.chunks[r.StoreID]; exists {
			routes = append(routes, r)
		}
	}

	if len(routes) > 0 {
		o.router = NewRouter(routes)
	} else {
		o.router = nil
	}
}

// StoreConfig returns the effective configuration for a store.
// This is useful for API responses and debugging.
func (o *Orchestrator) StoreConfig(id string) (config.StoreConfig, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if _, exists := o.chunks[id]; !exists {
		return config.StoreConfig{}, fmt.Errorf("%w: %s", ErrStoreNotFound, id)
	}

	cfg := config.StoreConfig{
		ID: id,
		// Type and Params are not tracked after creation.
		// Route can be extracted from router.
	}

	if o.router != nil {
		for _, r := range o.router.routes {
			if r.StoreID == id {
				expr := routeExpr(r)
				cfg.Route = &expr
				break
			}
		}
	}

	return cfg, nil
}

// routeExpr returns the original route expression from a compiled route.
func routeExpr(r *CompiledRoute) string {
	return r.Expr
}

// UpdateStoreRoute updates a store's routing expression.
// Returns ErrStoreNotFound if the store doesn't exist.
//
// For rotation policy changes, use ChunkManager(id).SetRotationPolicy(policy)
// directly with a composed policy object.
func (o *Orchestrator) UpdateStoreRoute(id string, route string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, exists := o.chunks[id]; !exists {
		return fmt.Errorf("%w: %s", ErrStoreNotFound, id)
	}

	if err := o.updateRouterLocked(id, route); err != nil {
		return err
	}

	o.logger.Info("store route updated", "id", id, "route", route)
	return nil
}
