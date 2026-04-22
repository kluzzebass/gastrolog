package orchestrator

import (
	"context"
	"fmt"
	"gastrolog/internal/glid"

	"gastrolog/internal/system"
)

// resolveFilterExpr looks up a filter ID in the config and returns its expression.
// Returns empty string if the filter ID is nil or not found (vault receives nothing).
func resolveFilterExpr(cfg *system.Config, filterID glid.GLID) string {
	if filterID == glid.Nil || cfg == nil {
		return ""
	}
	fc := findFilter(cfg.Filters, filterID)
	if fc == nil {
		return ""
	}
	return fc.Expression
}

// findFilter finds a FilterConfig by ID in a slice.
func findFilter(filters []system.FilterConfig, id glid.GLID) *system.FilterConfig {
	for i := range filters {
		if filters[i].ID == id {
			return &filters[i]
		}
	}
	return nil
}

// resolveVaultNodeID finds the node that owns the vault's first (active) tier.
// Returns empty string if the vault has no tiers or the active tier is unassigned.
func resolveVaultNodeID(sys *system.System, vaultID glid.GLID) string {
	cfg := &sys.Config
	rt := &sys.Runtime
	for _, v := range cfg.Vaults {
		tierIDs := system.VaultTierIDs(cfg.Tiers, v.ID)
		if v.ID != vaultID || len(tierIDs) == 0 {
			continue
		}
		tier := findTierConfig(cfg.Tiers, tierIDs[0])
		if tier != nil {
			return system.LeaderNodeID(rt.TierPlacements[tier.ID], rt.NodeStorageConfigs)
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
	sys, err := o.loadSystem(ctx)
	if err != nil {
		return fmt.Errorf("load system for filter reload: %w", err)
	}
	if sys == nil {
		return nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	return o.reloadFiltersFromRoutes(sys)
}

// reloadFiltersFromRoutes builds the FilterSet from route configuration.
// Must be called with o.mu held or at startup (before Start).
//
// For each enabled route, resolves the filter expression and compiles a
// CompiledFilter for each destination vault. If multiple routes target
// the same vault, the last route's filter wins (AddOrUpdate replaces).
func (o *Orchestrator) reloadFiltersFromRoutes(sys *system.System) error {
	if sys == nil {
		return nil
	}
	cfg := &sys.Config

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
			hotTierNode := resolveVaultNodeID(sys, destID)

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

	// Redirect records queued for nodes whose vault target changed.
	// This drains the old node's forward buffer and re-enqueues to the
	// new node, preventing record loss during leader failover.
	if o.forwarder != nil && o.filterSet != nil && fs != nil {
		o.redirectStaleForwards(o.filterSet, fs)
	}

	// Swap filter set atomically (we're under the lock).
	oldCount := 0
	if o.filterSet != nil {
		oldCount = len(o.filterSet.filters)
	}
	newCount := 0
	if fs != nil {
		newCount = len(fs.filters)
	}
	o.filterSet = fs
	if fs == nil && oldCount > 0 {
		o.logger.Warn("no route filters compiled, ingested records will be dropped")
	} else if newCount != oldCount {
		o.logger.Info("filters updated from routes", "count", newCount)
	}

	return nil
}

// redirectStaleForwards compares old and new filter sets and redirects
// queued records when a vault's target node changed (e.g. leader failover).
func (o *Orchestrator) redirectStaleForwards(prev, next *FilterSet) {
	oldNodes := make(map[glid.GLID]string)
	for _, f := range prev.filters {
		if f.NodeID != "" {
			oldNodes[f.VaultID] = f.NodeID
		}
	}
	for _, f := range next.filters {
		prev, hadOld := oldNodes[f.VaultID]
		if !hadOld || prev == f.NodeID {
			continue
		}
		o.forwarder.RedirectNode(prev, f.NodeID)
	}
}

// updateFilterLocked adds or updates a single vault's filter in the filter set.
// Must be called with o.mu held.
func (o *Orchestrator) updateFilterLocked(vaultID glid.GLID, filterExpr string) error {
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

	var removed []glid.GLID
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
