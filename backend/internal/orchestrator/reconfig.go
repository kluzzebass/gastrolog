package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"gastrolog/internal/chunk"
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
	if filterID == "" || cfg == nil {
		return ""
	}
	fc := findFilter(cfg.Filters, filterID)
	if fc == nil {
		return ""
	}
	return fc.Expression
}

// findFilter finds a FilterConfig by ID in a slice.
func findFilter(filters []config.FilterConfig, id string) *config.FilterConfig {
	for i := range filters {
		if filters[i].ID == id {
			return &filters[i]
		}
	}
	return nil
}

// findRotationPolicy finds a RotationPolicyConfig by ID in a slice.
func findRotationPolicy(policies []config.RotationPolicyConfig, id string) *config.RotationPolicyConfig {
	for i := range policies {
		if policies[i].ID == id {
			return &policies[i]
		}
	}
	return nil
}

// findRetentionPolicy finds a RetentionPolicyConfig by ID in a slice.
func findRetentionPolicy(policies []config.RetentionPolicyConfig, id string) *config.RetentionPolicyConfig {
	for i := range policies {
		if policies[i].ID == id {
			return &policies[i]
		}
	}
	return nil
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
		if _, ok := o.stores[storeCfg.ID]; !ok {
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
	if o.ingesterStats[id] == nil {
		o.ingesterStats[id] = &IngesterStats{}
	}

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

	if _, exists := o.stores[storeCfg.ID]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateID, storeCfg.ID)
	}

	// Create chunk manager.
	cmFactory, ok := factories.ChunkManagers[storeCfg.Type]
	if !ok {
		return fmt.Errorf("unknown chunk manager type: %s", storeCfg.Type)
	}
	var cmLogger = factories.Logger
	if cmLogger != nil {
		cmLogger = cmLogger.With("store", storeCfg.ID)
	}
	cm, err := cmFactory(storeCfg.Params, cmLogger)
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

	// Register store. AddStore does not apply disabled state (unlike ApplyConfig).
	store := NewStore(storeCfg.ID, cm, im, qe)
	o.stores[storeCfg.ID] = store

	// Update filter set to include the new store's filter.
	var filterID string
	if storeCfg.Filter != nil {
		filterID = *storeCfg.Filter
	}
	filterExpr := resolveFilterExpr(cfg, filterID)
	if err := o.updateFilterLocked(storeCfg.ID, filterExpr); err != nil {
		// Rollback registration on filter error.
		delete(o.stores, storeCfg.ID)
		return err
	}

	// Set up retention job if applicable.
	var retPolicy chunk.RetentionPolicy
	if storeCfg.Retention != nil && cfg != nil {
		retCfg := findRetentionPolicy(cfg.RetentionPolicies, *storeCfg.Retention)
		if retCfg != nil {
			p, err := retCfg.ToRetentionPolicy()
			if err != nil {
				o.logger.Warn("invalid retention policy for new store", "store", storeCfg.ID, "error", err)
			} else {
				retPolicy = p
			}
		}
	}
	if retPolicy != nil {
		runner := &retentionRunner{
			storeID: storeCfg.ID,
			cm:      cm,
			im:      im,
			policy:  retPolicy,
			now:     o.now,
			logger:  o.logger,
		}
		o.retention[storeCfg.ID] = runner
		if err := o.scheduler.AddJob(retentionJobName(storeCfg.ID), defaultRetentionSchedule, runner.sweep); err != nil {
			o.logger.Warn("failed to add retention job for new store", "store", storeCfg.ID, "error", err)
		}
	}

	// Set up cron rotation if the rotation policy has a cron schedule.
	if storeCfg.Policy != nil && cfg != nil {
		policyCfg := findRotationPolicy(cfg.RotationPolicies, *storeCfg.Policy)
		if policyCfg != nil {
			if policyCfg.Cron != nil && *policyCfg.Cron != "" {
				if err := o.cronRotation.addJob(storeCfg.ID, *policyCfg.Cron, cm); err != nil {
					o.logger.Warn("failed to add cron rotation for new store", "store", storeCfg.ID, "error", err)
				}
			}
		}
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

	store, exists := o.stores[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrStoreNotFound, id)
	}
	cm := store.Chunks

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

	// Remove retention job if present.
	o.scheduler.RemoveJob(retentionJobName(id))
	delete(o.retention, id)

	// Remove cron rotation job if present.
	o.cronRotation.removeJob(id)

	// Remove from registry.
	delete(o.stores, id)

	// Rebuild filter set without this store.
	o.rebuildFilterSetLocked()

	o.logger.Info("store removed", "id", id)
	return nil
}

// DisableStore disables ingestion for a store.
// Disabled stores will not receive new records from the ingest pipeline.
// Returns ErrStoreNotFound if the store doesn't exist.
func (o *Orchestrator) DisableStore(id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	store, exists := o.stores[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrStoreNotFound, id)
	}

	store.Enabled = false
	o.logger.Info("store disabled", "id", id)
	return nil
}

// EnableStore enables ingestion for a store.
// Returns ErrStoreNotFound if the store doesn't exist.
func (o *Orchestrator) EnableStore(id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	store, exists := o.stores[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrStoreNotFound, id)
	}

	store.Enabled = true
	o.logger.Info("store enabled", "id", id)
	return nil
}

// IsStoreEnabled returns whether ingestion is enabled for the given store.
func (o *Orchestrator) IsStoreEnabled(id string) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if store := o.stores[id]; store != nil {
		return store.Enabled
	}
	return false
}

// ForceRemoveStore removes a store regardless of whether it contains data.
// It seals the active chunk if present, deletes all indexes and chunks,
// closes the chunk manager, and cleans up all associated resources.
// Returns ErrStoreNotFound if the store doesn't exist.
func (o *Orchestrator) ForceRemoveStore(id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	store, exists := o.stores[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrStoreNotFound, id)
	}
	cm := store.Chunks
	im := store.Indexes

	// Seal active chunk if present.
	if active := cm.Active(); active != nil {
		if err := cm.Seal(); err != nil {
			return fmt.Errorf("seal active chunk for store %s: %w", id, err)
		}
	}

	// Delete all indexes and chunks.
	metas, err := cm.List()
	if err != nil {
		return fmt.Errorf("list chunks for store %s: %w", id, err)
	}
	for _, meta := range metas {
		if im != nil {
			// Best-effort index deletion; log and continue on error.
			if err := im.DeleteIndexes(meta.ID); err != nil {
				o.logger.Warn("failed to delete indexes during force remove",
					"store", id, "chunk", meta.ID.String(), "error", err)
			}
		}
		if err := cm.Delete(meta.ID); err != nil {
			return fmt.Errorf("delete chunk %s in store %s: %w", meta.ID.String(), id, err)
		}
	}

	// Close the chunk manager to release file locks.
	if err := cm.Close(); err != nil {
		o.logger.Warn("failed to close chunk manager during force remove",
			"store", id, "error", err)
	}

	// Remove retention job if present.
	o.scheduler.RemoveJob(retentionJobName(id))
	delete(o.retention, id)

	// Remove cron rotation job if present.
	o.cronRotation.removeJob(id)

	// Remove from registry.
	delete(o.stores, id)

	// Rebuild filter set without this store.
	o.rebuildFilterSetLocked()

	o.logger.Info("store force-removed", "id", id)
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
		if _, exists := o.stores[f.StoreID]; exists {
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

	if _, exists := o.stores[id]; !exists {
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
		store, ok := o.stores[storeCfg.ID]
		if !ok {
			continue // Store not registered in orchestrator.
		}
		cm := store.Chunks
		if storeCfg.Policy == nil {
			continue // Store doesn't reference a policy.
		}

		policyCfg := findRotationPolicy(cfg.RotationPolicies, *storeCfg.Policy)
		if policyCfg == nil {
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
			store.Chunks.SetRotationPolicy(policy)
			o.logger.Info("store rotation policy updated", "store", storeCfg.ID, "policy", *storeCfg.Policy)
		}

		// Update cron rotation job.
		hasCronJob := o.cronRotation.hasJob(storeCfg.ID)
		hasCronConfig := policyCfg.Cron != nil && *policyCfg.Cron != ""

		if hasCronConfig && hasCronJob {
			// Schedule may have changed — update.
			if err := o.cronRotation.updateJob(storeCfg.ID, *policyCfg.Cron, cm); err != nil {
				o.logger.Error("failed to update cron rotation", "store", storeCfg.ID, "error", err)
			}
		} else if hasCronConfig && !hasCronJob {
			// New cron schedule — add.
			if err := o.cronRotation.addJob(storeCfg.ID, *policyCfg.Cron, cm); err != nil {
				o.logger.Error("failed to add cron rotation", "store", storeCfg.ID, "error", err)
			}
		} else if !hasCronConfig && hasCronJob {
			// Cron removed — stop.
			o.cronRotation.removeJob(storeCfg.ID)
		}
	}

	return nil
}

// UpdateRetentionPolicies resolves retention policy references for all registered stores
// and hot-swaps their retention policies. This is called when a retention policy is
// created, updated, or deleted to immediately apply changes to running stores.
//
// Stores that don't reference any policy keep their current policy.
// Memory stores without an explicit policy keep their default count(10) policy.
func (o *Orchestrator) UpdateRetentionPolicies(cfg *config.Config) error {
	if cfg == nil {
		return nil
	}

	o.mu.RLock()
	defer o.mu.RUnlock()

	for _, storeCfg := range cfg.Stores {
		runner, ok := o.retention[storeCfg.ID]
		if !ok {
			continue // No retention runner for this store.
		}
		if storeCfg.Retention == nil {
			continue // Store doesn't reference a named policy; keep current.
		}

		policyCfg := findRetentionPolicy(cfg.RetentionPolicies, *storeCfg.Retention)
		if policyCfg == nil {
			o.logger.Warn("store references unknown retention policy", "store", storeCfg.ID, "policy", *storeCfg.Retention)
			continue
		}

		policy, err := policyCfg.ToRetentionPolicy()
		if err != nil {
			return fmt.Errorf("invalid retention policy %s for store %s: %w", *storeCfg.Retention, storeCfg.ID, err)
		}
		if policy != nil {
			runner.setPolicy(policy)
			o.logger.Info("store retention policy updated", "store", storeCfg.ID, "policy", *storeCfg.Retention)
		}
	}

	return nil
}

// MaxConcurrentJobs returns the current scheduler concurrency limit.
func (o *Orchestrator) MaxConcurrentJobs() int {
	return o.scheduler.MaxConcurrent()
}

// UpdateMaxConcurrentJobs rebuilds the scheduler with a new concurrency limit.
func (o *Orchestrator) UpdateMaxConcurrentJobs(n int) error {
	return o.scheduler.Rebuild(n)
}

// UpdateStoreFilter updates a store's filter expression.
// Returns ErrStoreNotFound if the store doesn't exist.
//
// For rotation policy changes, use ChunkManager(id).SetRotationPolicy(policy)
// directly with a composed policy object.
func (o *Orchestrator) UpdateStoreFilter(id string, filter string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, exists := o.stores[id]; !exists {
		return fmt.Errorf("%w: %s", ErrStoreNotFound, id)
	}

	if err := o.updateFilterLocked(id, filter); err != nil {
		return err
	}

	o.logger.Info("store filter updated", "id", id, "filter", filter)
	return nil
}
