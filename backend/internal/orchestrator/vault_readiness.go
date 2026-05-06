package orchestrator

import (
	"errors"
	"fmt"

	"gastrolog/internal/glid"
)

// Vault readiness — canonical definition.
//
// A vault on this node is "ready" iff:
//   1. It has at least one local tier instance (len(Vault.Tiers) > 0). A vault
//      registered with zero local tiers is a routing shell; it cannot serve
//      reads or writes and callers must forward to a peer that holds the
//      data.
//   2. Every local tier's FSM has applied at least one log entry (or has
//      restored from a snapshot). Before this, the tier manifest is
//      incomplete — acting on it risks data loss or divergent state. A nil
//      IsFSMReady callback is the single-node/memory tier case and is
//      treated as always ready.
//
// The readiness gate applies to ingest, query, and control paths on the
// local node. RPC-level fallbacks (forward to a peer) live above this
// check — once a caller is certain its own node owns the vault, it must
// pass this gate before touching tier managers or the FSM.
//
// Use `Vault.ReadinessErr()` when you already hold a non-nil *Vault (e.g.
// from a map lookup or argument) and `vaultReplicationReadinessErr(id, v)`
// when `v` may be nil (map lookup before validation).
//
// Readiness was introduced in gastrolog-4ip1o.

// ErrVaultNotReady is returned when the vault exists locally but replicated
// tier metadata (vault control-plane / tier FSM) has not applied far enough
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
// one local tier instance has replication metadata ready. Vaults registered
// with zero local tiers are ignored so routing-only shells do not fail
// load-balancer readiness (gastrolog-4ip1o).
func (o *Orchestrator) LocalVaultsReplicationReady() bool {
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
