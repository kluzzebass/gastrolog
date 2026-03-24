package orchestrator

import (
	"context"
	"fmt"

	"gastrolog/internal/config"

	"github.com/google/uuid"
)

// resolveFilterExpr looks up a filter ID in the config and returns its expression.
// Returns empty string if the filter ID is nil or not found (vault receives nothing).
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

// resolveVaultNodeID finds the node that owns the vault's first (active) tier.
// Returns empty string if the vault has no tiers or the active tier is unassigned.
func resolveVaultNodeID(cfg *config.Config, vaultID uuid.UUID) string {
	for _, v := range cfg.Vaults {
		if v.ID != vaultID || len(v.TierIDs) == 0 {
			continue
		}
		tier := findTierConfig(cfg.Tiers, v.TierIDs[0])
		if tier != nil {
			return tier.NodeID
		}
	}
	return ""
}

// ReloadFilters loads the full config and recompiles filter expressions from
// routes for all registered vaults. This can be called while the system is
// running without disrupting ingestion.
//
// Route filter_id fields are resolved via cfg.Filters. Only destination
// vaults that are currently registered in the orchestrator are included.
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

	return o.reloadFiltersFromRoutes(cfg)
}

// reloadFiltersFromRoutes builds the FilterSet from route configuration.
// Must be called with o.mu held or at startup (before Start).
//
// For each enabled route, resolves the filter expression and compiles a
// CompiledFilter for each destination vault. If multiple routes target
// the same vault, the last route's filter wins (AddOrUpdate replaces).
func (o *Orchestrator) reloadFiltersFromRoutes(cfg *config.Config) error {
	if cfg == nil {
		return nil
	}

	var fs *FilterSet
	for _, route := range cfg.Routes {
		if !route.Enabled {
			continue
		}
		if route.EjectOnly {
			continue // eject-only routes excluded from live FilterSet
		}

		var filterExpr string
		if route.FilterID != nil {
			filterExpr = resolveFilterExpr(cfg, *route.FilterID)
		}

		for _, destID := range route.Destinations {
			// Determine which node owns the vault's hot tier (first tier).
			// Ingestion always targets the hot tier — even if this node has
			// a non-hot tier for the same vault.
			hotTierNode := resolveVaultNodeID(cfg, destID)

			nodeID := ""
			switch {
			case o.draining[destID] != nil:
				nodeID = o.draining[destID].TargetNodeID
			case hotTierNode == "" || hotTierNode == o.localNodeID:
				// Hot tier is local (or unassigned) — append locally if registered.
				if _, ok := o.vaults[destID]; !ok {
					continue // not registered locally
				}
			case o.forwarder != nil:
				// Hot tier is on a remote node — forward.
				nodeID = hotTierNode
			default:
				continue // single-node mode, skip remote
			}
			var err error
			fs, err = fs.AddOrUpdateWithNodeAndRoute(destID, filterExpr, nodeID, route.ID)
			if err != nil {
				return fmt.Errorf("invalid filter for route %s, vault %s: %w", route.ID, destID, err)
			}
		}
	}

	// Swap filter set atomically (we're under the lock).
	o.filterSet = fs
	if fs != nil {
		o.logger.Info("filters updated from routes", "count", len(fs.filters))
	} else {
		o.logger.Warn("no route filters compiled, ingested records will be dropped")
	}

	return nil
}

// updateFilterLocked adds or updates a single vault's filter in the filter set.
// Must be called with o.mu held.
func (o *Orchestrator) updateFilterLocked(vaultID uuid.UUID, filterExpr string) error {
	fs, err := o.filterSet.AddOrUpdate(vaultID, filterExpr)
	if err != nil {
		return err
	}
	o.filterSet = fs
	return nil
}

// rebuildFilterSetLocked rebuilds the filter set from currently registered vaults.
// Must be called with o.mu held.
// This is used after removing a vault to exclude its filter.
func (o *Orchestrator) rebuildFilterSetLocked() {
	if o.filterSet == nil {
		return
	}

	var removed []uuid.UUID
	for _, f := range o.filterSet.filters {
		if f.NodeID != "" {
			continue // remote vault — not expected in o.vaults
		}
		if _, exists := o.vaults[f.VaultID]; !exists {
			removed = append(removed, f.VaultID)
		}
	}

	if len(removed) > 0 {
		o.filterSet = o.filterSet.Without(removed...)
	}
}
