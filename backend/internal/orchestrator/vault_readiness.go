package orchestrator

import (
	"errors"
	"fmt"

	"gastrolog/internal/glid"
)

// ErrVaultNotReady is returned when the vault exists locally but replicated
// tier metadata (vault control-plane / tier FSM) has not applied far enough
// for safe reads or writes. Callers should retry with backoff.
var ErrVaultNotReady = errors.New("vault not ready")

func vaultReplicationReadinessErr(vaultID glid.GLID, v *Vault) error {
	if v == nil {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if len(v.Tiers) == 0 {
		return fmt.Errorf("%w: %s (no tiers)", ErrVaultNotReady, vaultID)
	}
	for _, t := range v.Tiers {
		if t.IsFSMReady == nil {
			continue
		}
		if !t.IsFSMReady() {
			return fmt.Errorf("%w: vault %s tier %s metadata not ready", ErrVaultNotReady, vaultID, t.TierID)
		}
	}
	return nil
}

// LocalVaultsReplicationReady reports whether every vault that hosts at least
// one local tier instance has replication metadata ready (tier FSM applied).
// Vaults registered with zero local tiers are ignored so routing-only shells
// do not fail load-balancer readiness (gastrolog-4ip1o).
func (o *Orchestrator) LocalVaultsReplicationReady() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	for id, v := range o.vaults {
		if len(v.Tiers) == 0 {
			continue
		}
		if err := vaultReplicationReadinessErr(id, v); err != nil {
			return false
		}
	}
	return true
}
