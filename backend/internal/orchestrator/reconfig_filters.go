package orchestrator

import (
	"context"
	"fmt"

	"gastrolog/internal/config"

	"github.com/google/uuid"
)

// resolveFilterExpr looks up a filter ID in the config and returns its expression.
// Returns empty string if the filter ID is nil or not found (store receives nothing).
func resolveFilterExpr(cfg *config.Config, filterID uuid.UUID) string {
	if filterID == uuid.Nil || cfg == nil {
		return ""
	}
	fc := findFilter(cfg.Filters, filterID)
	if fc == nil {
		return ""
	}
	return fc.Expression
}

// findFilter finds a FilterConfig by ID in a slice.
func findFilter(filters []config.FilterConfig, id uuid.UUID) *config.FilterConfig {
	for i := range filters {
		if filters[i].ID == id {
			return &filters[i]
		}
	}
	return nil
}

// ReloadFilters loads the full config and recompiles filter expressions for all
// registered stores. This can be called while the system is running without
// disrupting ingestion.
//
// Store filter fields are resolved as filter IDs via cfg.Filters.
// Only stores that are currently registered in the orchestrator are included.
// Stores in the config that don't exist in the orchestrator are ignored.
func (o *Orchestrator) ReloadFilters(ctx context.Context) error {
	cfg, err := o.loadConfig(ctx)
	if err != nil {
		return fmt.Errorf("load config for filter reload: %w", err)
	}
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

		var filterID uuid.UUID
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

// updateFilterLocked adds or updates a single store's filter in the filter set.
// Must be called with o.mu held.
func (o *Orchestrator) updateFilterLocked(storeID uuid.UUID, filterExpr string) error {
	fs, err := o.filterSet.AddOrUpdate(storeID, filterExpr)
	if err != nil {
		return err
	}
	o.filterSet = fs
	return nil
}

// rebuildFilterSetLocked rebuilds the filter set from currently registered stores.
// Must be called with o.mu held.
// This is used after removing a store to exclude its filter.
func (o *Orchestrator) rebuildFilterSetLocked() {
	if o.filterSet == nil {
		return
	}

	var removed []uuid.UUID
	for _, f := range o.filterSet.filters {
		if _, exists := o.stores[f.StoreID]; !exists {
			removed = append(removed, f.StoreID)
		}
	}

	if len(removed) > 0 {
		o.filterSet = o.filterSet.Without(removed...)
	}
}
