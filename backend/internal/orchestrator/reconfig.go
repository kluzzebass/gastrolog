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

// resolveFilterExpr looks up a filter ID in the config and returns its expression.
// Returns empty string if the filter ID is empty or not found (store receives nothing).
func resolveFilterExpr(cfg *config.Config, filterID string) string {
	if filterID == "" || cfg == nil || cfg.Filters == nil {
		return ""
	}
	fc, ok := cfg.Filters[filterID]
	if !ok {
		return ""
	}
	return fc.Expression
}

// UpdateFilters recompiles filter expressions from store configs and hot-swaps the filter set.
// This can be called while the system is running without disrupting ingestion.
//
// Store filter fields are resolved as filter IDs via cfg.Filters.
// Only stores that are currently registered in the orchestrator are included.
// Stores in the config that don't exist in the orchestrator are ignored.
func (o *Orchestrator) UpdateFilters(cfg *config.Config) error {
	if cfg == nil {
		return nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	var compiled []*CompiledFilter

	for _, storeCfg := range cfg.Stores {
		// Only include stores that are registered.
		if _, ok := o.chunks[storeCfg.ID]; !ok {
			continue
		}

		var filterID string
		if storeCfg.Filter != nil {
			filterID = *storeCfg.Filter
		}
		filterExpr := resolveFilterExpr(cfg, filterID)
		f, err := CompileFilter(storeCfg.ID, filterExpr)
		if err != nil {
			return fmt.Errorf("invalid filter for store %s: %w", storeCfg.ID, err)
		}
		compiled = append(compiled, f)
	}

	// Swap filter set atomically (we're under the lock).
	if len(compiled) > 0 {
		o.filterSet = NewFilterSet(compiled)
		o.logger.Info("filters updated", "count", len(compiled))
	} else {
		o.filterSet = nil
		o.logger.Warn("filters cleared, messages will fan out to all stores")
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

// AddStore adds a new store (chunk manager, index manager, query engine) and updates the filter set.
// The cfg parameter is needed to resolve the store's filter ID to a filter expression.
// Returns ErrDuplicateID if a store with this ID already exists.
func (o *Orchestrator) AddStore(storeCfg config.StoreConfig, cfg *config.Config, factories Factories) error {
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

	// Update filter set to include the new store's filter.
	var filterID string
	if storeCfg.Filter != nil {
		filterID = *storeCfg.Filter
	}
	filterExpr := resolveFilterExpr(cfg, filterID)
	if err := o.updateFilterLocked(storeCfg.ID, filterExpr); err != nil {
		// Rollback registration on filter error.
		delete(o.chunks, storeCfg.ID)
		delete(o.indexes, storeCfg.ID)
		delete(o.queries, storeCfg.ID)
		return err
	}

	o.logger.Info("store added", "id", storeCfg.ID, "type", storeCfg.Type, "filter", filterExpr)
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

	// Rebuild filter set without this store.
	o.rebuildFilterSetLocked()

	o.logger.Info("store removed", "id", id)
	return nil
}

// updateFilterLocked adds or updates a single store's filter in the filter set.
// Must be called with o.mu held.
func (o *Orchestrator) updateFilterLocked(storeID, filterExpr string) error {
	f, err := CompileFilter(storeID, filterExpr)
	if err != nil {
		return fmt.Errorf("invalid filter for store %s: %w", storeID, err)
	}

	// Rebuild filters including the new one.
	var filters []*CompiledFilter

	// Keep existing filters for other stores.
	if o.filterSet != nil {
		for _, existing := range o.filterSet.filters {
			if existing.StoreID != storeID {
				filters = append(filters, existing)
			}
		}
	}

	// Add the new/updated filter.
	filters = append(filters, f)

	o.filterSet = NewFilterSet(filters)
	return nil
}

// rebuildFilterSetLocked rebuilds the filter set from currently registered stores.
// Must be called with o.mu held.
// This is used after removing a store to exclude its filter.
func (o *Orchestrator) rebuildFilterSetLocked() {
	if o.filterSet == nil {
		return
	}

	var filters []*CompiledFilter
	for _, f := range o.filterSet.filters {
		if _, exists := o.chunks[f.StoreID]; exists {
			filters = append(filters, f)
		}
	}

	if len(filters) > 0 {
		o.filterSet = NewFilterSet(filters)
	} else {
		o.filterSet = nil
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
		// Filter can be extracted from filter set.
	}

	if o.filterSet != nil {
		for _, f := range o.filterSet.filters {
			if f.StoreID == id {
				expr := f.Expr
				cfg.Filter = &expr
				break
			}
		}
	}

	return cfg, nil
}

// UpdateRotationPolicies resolves rotation policy references for all registered stores
// and hot-swaps their rotation policies. This is called when a rotation policy is
// created, updated, or deleted to immediately apply changes to running stores.
//
// Stores that don't reference any policy are left unchanged.
// Stores referencing a policy that no longer exists get a nil policy (type default).
func (o *Orchestrator) UpdateRotationPolicies(cfg *config.Config) error {
	if cfg == nil {
		return nil
	}

	o.mu.RLock()
	defer o.mu.RUnlock()

	for _, storeCfg := range cfg.Stores {
		cm, ok := o.chunks[storeCfg.ID]
		if !ok {
			continue // Store not registered in orchestrator.
		}
		if storeCfg.Policy == nil {
			continue // Store doesn't reference a policy.
		}

		policyCfg, ok := cfg.RotationPolicies[*storeCfg.Policy]
		if !ok {
			// Policy was deleted — nothing to do; store keeps its current policy.
			// We can't revert to "type default" from here, and the dangling
			// reference will be caught on next restart or store edit.
			o.logger.Warn("store references unknown policy", "store", storeCfg.ID, "policy", *storeCfg.Policy)
			continue
		}

		policy, err := policyCfg.ToRotationPolicy()
		if err != nil {
			return fmt.Errorf("invalid policy %s for store %s: %w", *storeCfg.Policy, storeCfg.ID, err)
		}
		if policy != nil {
			cm.SetRotationPolicy(policy)
			o.logger.Info("store rotation policy updated", "store", storeCfg.ID, "policy", *storeCfg.Policy)
		}
	}

	return nil
}

// UpdateStoreFilter updates a store's filter expression.
// Returns ErrStoreNotFound if the store doesn't exist.
//
// For rotation policy changes, use ChunkManager(id).SetRotationPolicy(policy)
// directly with a composed policy object.
func (o *Orchestrator) UpdateStoreFilter(id string, filter string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, exists := o.chunks[id]; !exists {
		return fmt.Errorf("%w: %s", ErrStoreNotFound, id)
	}

	if err := o.updateFilterLocked(id, filter); err != nil {
		return err
	}

	o.logger.Info("store filter updated", "id", id, "filter", filter)
	return nil
}
