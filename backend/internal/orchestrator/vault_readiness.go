package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gastrolog/internal/glid"
)

// readinessRefreshInterval governs how often the orchestrator recomputes
// the cached LocalVaultsReplicationReady value. 500 ms keeps staleness well
// under the default K8s readiness probe period (10 s) while letting the
// /readyz handler stay strictly lock-free. See gastrolog-5n6xz.
const readinessRefreshInterval = 500 * time.Millisecond

// Vault readiness — canonical definition.
//
// A vault on this node is "ready" iff:
//   1. It has at least one local vault instance (its Instance is non-nil). A vault
//      registered with zero local instances is a routing shell; it cannot serve
//      reads or writes and callers must forward to a peer that holds the
//      data.
//   2. Every local instance's FSM has applied at least one log entry (or has
//      restored from a snapshot). Before this, the instance manifest is
//      incomplete — acting on it risks data loss or divergent state. A nil
//      IsFSMReady callback is the single-node/memory instance case and is
//      treated as always ready.
//
// The readiness gate applies to ingest, query, and control paths on the
// local node. RPC-level fallbacks (forward to a peer) live above this
// check — once a caller is certain its own node owns the vault, it must
// pass this gate before touching instance managers or the FSM.
//
// Use `Vault.ReadinessErr()` when you already hold a non-nil *Vault (e.g.
// from a map lookup or argument) and `vaultReplicationReadinessErr(id, v)`
// when `v` may be nil (map lookup before validation).
//
// Readiness was introduced in gastrolog-4ip1o.

// ErrVaultNotReady is returned when the vault exists locally but replicated
// instance metadata (vault control-plane / vault-ctl FSM) has not applied far enough
// for safe reads or writes. Callers should retry with backoff.
var ErrVaultNotReady = errors.New("vault not ready")

// ReadinessErr reports whether the vault is ready for reads and writes on
// this node. Returns nil when ready, ErrVaultNotReady with detail otherwise.
// See the package-level canonical definition in vault_readiness.go.
func (v *Vault) ReadinessErr() error {
	t := v.Instance
	if t == nil {
		return fmt.Errorf("%w: %s (no instance)", ErrVaultNotReady, v.ID)
	}
	if t.IsFSMReady != nil && !t.IsFSMReady() {
		return fmt.Errorf("%w: vault %s metadata not ready", ErrVaultNotReady, v.ID)
	}
	return nil
}

// signalVaultReadyChange wakes every goroutine blocked in WaitVaultReady so
// it can re-evaluate the readiness predicate. Called after any vault registry
// mutation (register, instance add/remove, unregister). Safe to call while
// holding o.mu — notify.Signal uses its own leaf mutex. See gastrolog-3sdnn.
//
// Registry mutations are the complete set of readiness transitions for the
// drain-target use case: a vault's local instance is only ever built (via
// AddVault / AddVaultInstance) once its vault-ctl Raft group already spans
// every node and has applied past index 0, so ReadinessErr's IsFSMReady gate
// is already satisfied at the moment the instance lands in the registry —
// there is no separate async "FSM became ready" edge to notify here.
func (o *Orchestrator) signalVaultReadyChange() {
	if o.vaultReadySignal != nil {
		o.vaultReadySignal.Notify()
	}
}

// vaultReadyState reports whether the named vault is currently ready to accept
// transferred records, mirroring the success condition of ListAllChunkMetas
// (the predicate the old ForwardListChunks poll observed):
//   - vault absent from the registry -> not ready, not present
//   - vault registered with no local instance (routing shell) -> ready
//   - vault registered with a local instance -> ready iff replication-ready
//
// present distinguishes "never created yet" (keep waiting) from "was here,
// now gone" (deleted while waiting) for WaitVaultReady's callers.
func (o *Orchestrator) vaultReadyState(vaultID glid.GLID) (ready, present bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	v := o.vaults[vaultID]
	if v == nil {
		return false, false
	}
	if v.Instance == nil {
		// A registered routing shell lists chunks successfully (returns an
		// empty set), which is exactly what the old poll treated as ready.
		return true, true
	}
	return v.ReadinessErr() == nil, true
}

// WaitVaultReady blocks until the named vault is registered and ready to
// accept transferred records on this node, or ctx is cancelled. It is the
// event-driven replacement for the source node's 100ms ForwardListChunks
// poll: the vault-ready broadcast wakes the waiter at each registry change
// and it re-evaluates the same predicate the poll used. See gastrolog-3sdnn.
//
// Behavior across the four waiter states:
//   - already ready: returns nil immediately.
//   - becomes ready: wakes on the registration broadcast and returns nil.
//   - never ready: blocks until ctx cancels/expires, returning ctx.Err().
//   - deleted while waiting: once the vault has been observed present and
//     then disappears, returns ErrVaultNotFound rather than hanging.
func (o *Orchestrator) WaitVaultReady(ctx context.Context, vaultID glid.GLID) error {
	sawPresent := false
	for {
		// Capture the wakeup channel BEFORE reading state so a change that
		// lands between the read and the select cannot be missed (the
		// captured channel is already closed and the select returns at once).
		ch := o.vaultReadySignal.C()

		ready, present := o.vaultReadyState(vaultID)
		if ready {
			return nil
		}
		if present {
			sawPresent = true
		} else if sawPresent {
			return fmt.Errorf("%w: %s (deleted while waiting for readiness)", ErrVaultNotFound, vaultID)
		}

		select {
		case <-ch:
			// Registry changed; loop and re-evaluate.
		case <-ctx.Done():
			return fmt.Errorf("wait vault %s ready: %w", vaultID, ctx.Err())
		}
	}
}

// vaultReplicationReadinessErr handles the "vault may be nil" caller shape
// (map lookup followed by readiness check). Returns ErrVaultNotFound for nil
// vaults and otherwise delegates to Vault.ReadinessErr.
func vaultReplicationReadinessErr(vaultID glid.GLID, v *Vault) error {
	if v == nil {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	return v.ReadinessErr()
}

// LocalVaultsReplicationReady reports whether every vault that hosts at least
// one local vault instance has replication metadata ready. Returns the value
// cached by the readiness refresher goroutine — never blocks on o.mu, so the
// /readyz HTTP handler stays responsive during long-held writer activity
// (vault-ctl AddVoter bursts on K8s scale-out, vault registration, etc.).
//
// Staleness is bounded by readinessRefreshInterval (typically ~500 ms),
// which is well under the K8s readiness probe period. The cache is seeded
// true at construction so newly-created orchestrators with empty vault maps
// match legacy synchronous semantics.
//
// Tests that need a synchronous result (RegisterVault → assert ready in the
// same step, without spinning up the refresher) should call
// liveReplicationReady. See gastrolog-5n6xz, gastrolog-4ip1o.
func (o *Orchestrator) LocalVaultsReplicationReady() bool {
	return o.cachedReplicationReady.Load()
}

// liveReplicationReady computes the same predicate synchronously by walking
// the vault map under o.mu.RLock. The cached LocalVaultsReplicationReady
// uses this on each refresher tick; tests use it directly when they need
// an immediate answer after a registry mutation.
func (o *Orchestrator) liveReplicationReady() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	for _, v := range o.vaults {
		if v.Instance == nil {
			continue
		}
		if err := v.ReadinessErr(); err != nil {
			return false
		}
	}
	return true
}

// runReadinessRefresher periodically recomputes liveReplicationReady and
// publishes the result into cachedReplicationReady. Uses TryRLock so a
// writer holding o.mu cannot starve the refresher itself — when the lock
// is unavailable, the cache stays at its last-good value and the next tick
// retries. This is the second half of the /readyz responsiveness fix: even
// if computing the live value would block, the cache continues to reflect
// the last successful observation rather than freezing the HTTP handler.
//
// Exits when ctx is cancelled. See gastrolog-5n6xz.
func (o *Orchestrator) runReadinessRefresher(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if o.mu.TryRLock() {
			ready := o.liveReplicationReadyLocked()
			o.mu.RUnlock()
			o.cachedReplicationReady.Store(ready)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// liveReplicationReadyLocked is the lock-free body of liveReplicationReady,
// extracted so the readiness refresher can compute the predicate after
// acquiring o.mu via TryRLock without re-entering the lock.
func (o *Orchestrator) liveReplicationReadyLocked() bool {
	for _, v := range o.vaults {
		if v.Instance == nil {
			continue
		}
		if err := v.ReadinessErr(); err != nil {
			return false
		}
	}
	return true
}
