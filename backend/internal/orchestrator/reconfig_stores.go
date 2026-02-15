package orchestrator

import (
	"context"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/config"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

// AddStore adds a new store (chunk manager, index manager, query engine) and updates the filter set.
// Loads the full config internally to resolve the store's filter ID to a filter expression.
// Returns ErrDuplicateID if a store with this ID already exists.
func (o *Orchestrator) AddStore(ctx context.Context, storeCfg config.StoreConfig, factories Factories) error {
	cfg, err := o.loadConfig(ctx)
	if err != nil {
		return fmt.Errorf("load config for AddStore: %w", err)
	}
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
	var filterID uuid.UUID
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
		o.scheduler.Describe(retentionJobName(storeCfg.ID), fmt.Sprintf("Delete expired chunks from '%s'", storeCfg.Name))
	}

	// Set up cron rotation if the rotation policy has a cron schedule.
	if storeCfg.Policy != nil && cfg != nil {
		policyCfg := findRotationPolicy(cfg.RotationPolicies, *storeCfg.Policy)
		if policyCfg != nil {
			if policyCfg.Cron != nil && *policyCfg.Cron != "" {
				if err := o.cronRotation.addJob(storeCfg.ID, storeCfg.Name, *policyCfg.Cron, cm); err != nil {
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
func (o *Orchestrator) RemoveStore(id uuid.UUID) error {
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
func (o *Orchestrator) DisableStore(id uuid.UUID) error {
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
func (o *Orchestrator) EnableStore(id uuid.UUID) error {
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
func (o *Orchestrator) IsStoreEnabled(id uuid.UUID) bool {
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
func (o *Orchestrator) ForceRemoveStore(id uuid.UUID) error {
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

// StoreConfig returns the effective configuration for a store.
// This is useful for API responses and debugging.
func (o *Orchestrator) StoreConfig(id uuid.UUID) (config.StoreConfig, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if _, exists := o.stores[id]; !exists {
		return config.StoreConfig{}, fmt.Errorf("%w: %s", ErrStoreNotFound, id)
	}

	cfg := config.StoreConfig{
		ID: id,
		// Type and Params are not tracked after creation.
		// Filter is a UUID reference; the orchestrator doesn't track the original
		// filter UUID reference, so it's left nil here.
	}

	return cfg, nil
}

// UpdateStoreFilter updates a store's filter expression.
// Returns ErrStoreNotFound if the store doesn't exist.
//
// For rotation policy changes, use ChunkManager(id).SetRotationPolicy(policy)
// directly with a composed policy object.
func (o *Orchestrator) UpdateStoreFilter(id uuid.UUID, filter string) error {
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
