package orchestrator

import (
	"context"
	"errors"

	"gastrolog/internal/config"
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
	// ErrNoConfigLoader is returned when a hot-update method is called without a ConfigLoader.
	ErrNoConfigLoader = errors.New("no config loader configured")
)

// loadConfig loads the full configuration via the ConfigLoader.
// Returns ErrNoConfigLoader if no ConfigLoader is set.
func (o *Orchestrator) loadConfig(ctx context.Context) (*config.Config, error) {
	if o.cfgLoader == nil {
		return nil, ErrNoConfigLoader
	}
	return o.cfgLoader.Load(ctx)
}

// MaxConcurrentJobs returns the current scheduler concurrency limit.
func (o *Orchestrator) MaxConcurrentJobs() int {
	return o.scheduler.MaxConcurrent()
}

// UpdateMaxConcurrentJobs rebuilds the scheduler with a new concurrency limit.
func (o *Orchestrator) UpdateMaxConcurrentJobs(n int) error {
	return o.scheduler.Rebuild(n)
}
