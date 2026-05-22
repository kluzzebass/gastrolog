package orchestrator

import (
	"context"
	"fmt"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// resolveVaultNodeID was deleted with gastrolog-mqxo4. Route destinations
// no longer carry an owning node ID — every record dispatches directly
// to the active chunk's Receiving members via the per-vault FSM.

// ReloadFilters loads the full config and rebuilds the routing table
// from the current route set. Renamed from the Phase-4 "filter reload"
// concept but kept under the same method name so existing dispatch
// callers (FSM notification fan-out) continue to work without churn.
//
// gastrolog-4kkoo (Phase 5): the routing table is per-route now —
// expressions live inline on RouteConfig.Stages, evaluated in priority
// order with first-match-wins semantics.
func (o *Orchestrator) ReloadFilters(ctx context.Context) error {
	sys, err := o.loadSystem(ctx)
	if err != nil {
		return fmt.Errorf("load system for routes reload: %w", err)
	}
	if sys == nil {
		return nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	return o.reloadRoutesFromConfig(sys)
}

// reloadRoutesFromConfig builds a new RouteSet from the cluster-wide
// route table. Each enabled route becomes one CompiledRoute with all
// of its destinations attached. Routes whose destinations are entirely
// non-routable from this node (no local registration, no forwarder)
// are skipped.
//
// Must be called with o.mu held or at startup (before Start).
func (o *Orchestrator) reloadRoutesFromConfig(sys *system.System) error {
	if sys == nil {
		return nil
	}
	cfg := &sys.Config

	var compiled []*CompiledRoute
	for _, route := range cfg.Routes {
		if !route.Enabled {
			continue
		}

		// Under fan-out (gastrolog-mqxo4) route destinations carry only
		// the vault ID. The dispatcher resolves the active chunk's
		// Receiving members at dispatch time via the per-vault FSM —
		// every node has the FSM state, so every node can dispatch to
		// any vault. Pre-mqxo4 this loop resolved the destination's
		// "hot" node and stamped it on RouteDestination.NodeID so the
		// ingest path could route remote-vault records via the
		// forwarder; that distinction is gone.
		dests := make([]RouteDestination, 0, len(route.Destinations))
		for _, destID := range route.Destinations {
			dests = append(dests, RouteDestination{VaultID: destID})
		}

		if len(dests) == 0 {
			// Route has no routable destinations from this node — skip.
			continue
		}

		cr, err := CompileRoute(
			route.ID,
			route.Name,
			route.Priority,
			route.MatchExpression(),
			dests,
			string(route.Distribution),
		)
		if err != nil {
			return fmt.Errorf("compile route %s: %w", route.Name, err)
		}
		compiled = append(compiled, cr)
	}

	next := NewRouteSet(compiled)

	// Redirect records queued for nodes whose vault target changed.
	// This drains the old node's forward buffer and re-enqueues to the
	// new node, preventing record loss during leader failover.
	if o.forwarder != nil && o.routeSet != nil {
		o.redirectStaleForwards(o.routeSet, next)
	}

	// Swap atomically (we hold the lock).
	oldRouteCount := 0
	if o.routeSet != nil {
		oldRouteCount = len(o.routeSet.routes)
	}
	newRouteCount := len(next.routes)
	o.routeSet = next
	if newRouteCount == 0 && oldRouteCount > 0 {
		o.logger.Warn("no routes compiled, ingested records will be dropped")
	} else if newRouteCount != oldRouteCount {
		o.logger.Info("routes recompiled", "count", newRouteCount)
	}

	return nil
}

// redirectStaleForwards compares the old and new route sets and
// redirects queued records when a destination vault's target node
// changed (e.g. leader failover). Walks the union of destinations
// across both sets so that vaults dropping out of the table also get
// drained.
func (o *Orchestrator) redirectStaleForwards(prev, next *RouteSet) {
	if prev == nil || next == nil {
		return
	}
	oldNodes := make(map[glid.GLID]string)
	for _, r := range prev.routes {
		for _, d := range r.Destinations {
			if d.NodeID != "" {
				oldNodes[d.VaultID] = d.NodeID
			}
		}
	}
	for _, r := range next.routes {
		for _, d := range r.Destinations {
			old, hadOld := oldNodes[d.VaultID]
			if !hadOld || old == d.NodeID {
				continue
			}
			o.forwarder.RedirectNode(old, d.NodeID)
		}
	}
}

// rebuildRouteSetLocked drops destinations from the active RouteSet
// that point at vaults no longer registered locally. Caller must hold
// o.mu. Used after RemoveVault so the routing table doesn't keep
// referencing a vault we just stopped serving.
func (o *Orchestrator) rebuildRouteSetLocked() {
	if o.routeSet == nil {
		return
	}
	rebuilt := make([]*CompiledRoute, 0, len(o.routeSet.routes))
	for _, r := range o.routeSet.routes {
		dests := make([]RouteDestination, 0, len(r.Destinations))
		for _, d := range r.Destinations {
			if d.NodeID != "" {
				dests = append(dests, d) // remote — keep
				continue
			}
			if _, exists := o.vaults[d.VaultID]; exists {
				dests = append(dests, d)
			}
		}
		if len(dests) == 0 {
			continue
		}
		cr := *r // shallow copy preserves DNF pointer
		cr.Destinations = dests
		rebuilt = append(rebuilt, &cr)
	}
	if len(rebuilt) == 0 {
		o.routeSet = nil
		return
	}
	o.routeSet = NewRouteSet(rebuilt)
}
