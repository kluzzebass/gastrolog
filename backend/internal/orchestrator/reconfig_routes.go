package orchestrator

import (
	"context"
	"fmt"

	"gastrolog/internal/system"
)

// ReloadFilters loads the full config and republishes the pipeline routing
// table (and reconciles Origin/Home vault registrations) from the current
// route set. The method name is kept for callsite stability across the FSM
// dispatch fan-out.
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

// reloadRoutesFromConfig republishes the pipeline routing table and reconciles
// the set of pipeline-registered vaults from the cluster-wide route table.
// Must be called with o.mu held or at startup (before Start).
func (o *Orchestrator) reloadRoutesFromConfig(sys *system.System) error {
	if sys == nil {
		return nil
	}
	return o.reloadPipelineFromConfig(sys)
}
