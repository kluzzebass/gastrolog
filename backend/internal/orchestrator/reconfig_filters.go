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

// resolveRouteTarget determines where an ingested record for the given vault
// should be written. Returns (nodeID, skip). Empty nodeID = write locally.
// skip=true means this destination should be excluded from the filter set.
// Must be called with o.mu held.
func (o *Orchestrator) resolveRouteTarget(cfg *config.Config, destID uuid.UUID) (string, bool) {
	if d := o.draining[destID]; d != nil {
		return d.TargetNodeID, false
	}
	hotTierNode := resolveVaultNodeID(cfg, destID)
	switch {
	case hotTierNode == "" || hotTierNode == o.localNodeID:
		v, ok := o.vaults[destID]
		if !ok {
			return "", true // not registered locally — skip
		}
		// Config says local — forward to the tier Raft leader if it's
		// a different node, so records reach the write authority.
		if leaderNode := o.activeTierLeaderNodeID(v); leaderNode != "" && leaderNode != o.localNodeID && o.forwarder != nil {
			return leaderNode, false
		}
		return "", false // write locally
	case o.forwarder != nil:
		return hotTierNode, false
	default:
		return "", true // single-node mode, skip remote
	}
}

// isActiveTierLeader returns true if this node is the Raft leader for the
// vault's active (first) tier. Must be called with o.mu held.
func (o *Orchestrator) isActiveTierLeader(v *Vault) bool {
	if len(v.Tiers) == 0 {
		return true // no tiers — treat as local
	}
	return v.Tiers[0].IsLeader()
}

// activeTierLeaderNodeID returns the node ID of the current Raft leader
// for the vault's active tier. Returns "" if unknown or no Raft group.
func (o *Orchestrator) activeTierLeaderNodeID(v *Vault) string {
	if len(v.Tiers) == 0 {
		return ""
	}
	t := v.Tiers[0]
	if t.RaftLeaderNodeID != nil {
		return t.RaftLeaderNodeID()
	}
	return ""
}

// resolveVaultNodeID finds the node that owns the vault's first (active) tier.
// Returns empty string if the vault has no tiers or the active tier is unassigned.
func resolveVaultNodeID(cfg *config.Config, vaultID uuid.UUID) string {
	for _, v := range cfg.Vaults {
		tierIDs := config.VaultTierIDs(cfg.Tiers, v.ID)
		if v.ID != vaultID || len(tierIDs) == 0 {
			continue
		}
		tier := findTierConfig(cfg.Tiers, tierIDs[0])
		if tier != nil {
			return tier.LeaderNodeID(cfg.NodeStorageConfigs)
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
			nodeID, skip := o.resolveRouteTarget(cfg, destID)
			if skip {
				continue
			}
			var err error
			fs, err = fs.AddOrUpdateWithNodeAndRoute(destID, filterExpr, nodeID, route.ID)
			if err != nil {
				return fmt.Errorf("invalid filter for route %s, vault %s: %w", route.ID, destID, err)
			}
		}
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
