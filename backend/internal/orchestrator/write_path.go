package orchestrator

import (
	"errors"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// ErrSequencedWriteUnavailable is returned when a vault uses the sequenced
// write model but allocator authority is not wired on this node.
var ErrSequencedWriteUnavailable = errors.New("sequenced write path unavailable: vault-ctl allocator not wired")

// vaultWriteModel returns the resolved write model for a registered vault.
// Unknown vault IDs default to the chunk-append write model.
func (o *Orchestrator) vaultWriteModel(vaultID glid.GLID) system.VaultWriteModel {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if v := o.vaults[vaultID]; v != nil {
		return v.WriteModel
	}
	return system.VaultWriteModelChunkAppend
}

// dispatchDestinationWrite appends one record to a local destination vault.
// Route fan-out may invoke this once per destination; replica fan-out is
// handled inside the chunk-append path after the local append succeeds.
//
// Sequenced writes must not gate on VaultPlacement.Leader (Phase 0.6);
// uses vault-ctl leader in later phases.
func (o *Orchestrator) dispatchDestinationWrite(vaultID glid.GLID, rec chunk.Record) (*replicationTask, []remoteForwardTarget, error) {
	switch wm := o.vaultWriteModel(vaultID); wm {
	case system.VaultWriteModelSequenced:
		if o.seqAssignReady(vaultID) {
			return o.appendLocalSequenced(vaultID, rec)
		}
		return nil, nil, ErrSequencedWriteUnavailable
	case system.VaultWriteModelChunkAppend:
		return o.appendLocalChunk(vaultID, rec)
	default:
		// Defensive: vault shells should only carry resolved models.
		return o.appendLocalChunk(vaultID, rec)
	}
}

// appendLocalChunk is the chunk-append destination write path: synchronous
// active-chunk append, then optional replica fan-out to RF followers.
func (o *Orchestrator) appendLocalChunk(vaultID glid.GLID, rec chunk.Record) (*replicationTask, []remoteForwardTarget, error) {
	return o.appendLocal(vaultID, rec)
}

func (o *Orchestrator) seqAssignReady(vaultID glid.GLID) bool {
	if o.groupMgr != nil {
		return true
	}
	return o.testSeqFSM[vaultID] != nil
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
