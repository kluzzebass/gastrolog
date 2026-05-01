package orchestrator

import (
	"context"
	"errors"

	"gastrolog/internal/system"
)

var (
	// ErrVaultNotEmpty is returned when attempting to remove a vault that has data.
	ErrVaultNotEmpty = errors.New("vault is not empty")
	// ErrVaultNotFound is returned when attempting to operate on a non-existent vault.
	ErrVaultNotFound = errors.New("vault not found")
	// ErrTierNotLocal is returned when a tier instance is not registered on
	// this node (typically because placement reconfiguration evicted it),
	// even though the vault still exists cluster-wide. Distinct from
	// ErrVaultNotFound so log lines don't suggest the vault was deleted
	// during legitimate placement churn. See gastrolog-2t48z.
	ErrTierNotLocal = errors.New("tier not registered on this node")
	// ErrIngesterNotFound is returned when attempting to operate on a non-existent ingester.
	ErrIngesterNotFound = errors.New("ingester not found")
	// ErrVaultDisabled is returned when attempting to append to a disabled vault.
	ErrVaultDisabled = errors.New("vault disabled")
	// ErrDuplicateID is returned when attempting to add a component with an existing ID.
	ErrDuplicateID = errors.New("duplicate ID")
	// ErrNoSystemLoader is returned when a hot-update method is called without a SystemLoader.
	ErrNoSystemLoader = errors.New("no config loader configured")
)

// loadSystem loads the full system state (config + runtime) via the SystemLoader.
// Returns ErrNoSystemLoader if no SystemLoader is set.
func (o *Orchestrator) loadSystem(ctx context.Context) (*system.System, error) {
	if o.sysLoader == nil {
		return nil, ErrNoSystemLoader
	}
	return o.sysLoader.Load(ctx)
}

// MaxConcurrentJobs returns the current scheduler concurrency limit.
func (o *Orchestrator) MaxConcurrentJobs() int {
	return o.scheduler.MaxConcurrent()
}

// UpdateMaxConcurrentJobs rebuilds the scheduler with a new concurrency limit.
func (o *Orchestrator) UpdateMaxConcurrentJobs(n int) error {
	return o.scheduler.Rebuild(n)
}
