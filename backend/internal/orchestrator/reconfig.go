package orchestrator

import (
	"context"
	"errors"
	"strings"

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

// IsPlacementChurnErr reports whether err signals a benign placement-state
// drift between the calling node and a peer: the peer either has no record
// of the vault, has the vault but the tier instance was evicted, or the
// caller's cached placement view is stale. None of these are real failures
// — they are expected during placement reconfiguration and should not
// drive WARN-level log spam. See gastrolog-5z607.
//
// Handles both local and cross-RPC error origins. Local sentinels round-
// trip via errors.Is. Cross-RPC errors are flattened to strings at the
// cluster boundary (handler concatenates "import failed: " etc.), so a
// substring fallback catches the rendered wording too.
func IsPlacementChurnErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrVaultNotFound) || errors.Is(err, ErrTierNotLocal) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "vault not found") ||
		strings.Contains(msg, "tier not registered on this node")
}

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
