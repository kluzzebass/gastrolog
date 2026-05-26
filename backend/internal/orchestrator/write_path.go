package orchestrator

import (
	"errors"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// ErrV2WritePathNotImplemented is returned when a vault is opted into write
// model v2 before later phases implement spool ingestion.
var ErrV2WritePathNotImplemented = errors.New("v2 write model is not implemented yet")

// vaultWriteModel returns the resolved write model for a registered vault.
// Unknown vault IDs default to V1.
func (o *Orchestrator) vaultWriteModel(vaultID glid.GLID) system.VaultWriteModel {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if v := o.vaults[vaultID]; v != nil {
		return v.WriteModel
	}
	return system.VaultWriteModelV1
}

// dispatchDestinationWrite appends one record to a local destination vault.
// Route fan-out may invoke this once per destination; replica fan-out is
// handled inside the V1 path after the local append succeeds.
func (o *Orchestrator) dispatchDestinationWrite(vaultID glid.GLID, rec chunk.Record) (*replicationTask, []remoteForwardTarget, error) {
	switch wm := o.vaultWriteModel(vaultID); wm {
	case system.VaultWriteModelV2:
		return nil, nil, ErrV2WritePathNotImplemented
	case system.VaultWriteModelV1:
		return o.appendLocalV1(vaultID, rec)
	default:
		// Defensive: vault shells should only carry resolved models.
		return o.appendLocalV1(vaultID, rec)
	}
}

// appendLocalV1 is the V1 destination write path: synchronous active-chunk
// append, then optional replica fan-out to RF followers.
func (o *Orchestrator) appendLocalV1(vaultID glid.GLID, rec chunk.Record) (*replicationTask, []remoteForwardTarget, error) {
	return o.appendLocal(vaultID, rec)
}

// SyncVaultConfig mirrors operator-facing vault config onto the registered
// vault shell without rebuilding instances.
func (o *Orchestrator) SyncVaultConfig(cfg system.VaultConfig) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	vault := o.vaults[cfg.ID]
	if vault == nil {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, cfg.ID)
	}
	vault.Name = cfg.Name
	vault.Enabled = cfg.Enabled
	vault.StorageType = string(cfg.Type)
	vault.WriteModel = cfg.ResolveWriteModel()
	return nil
}
