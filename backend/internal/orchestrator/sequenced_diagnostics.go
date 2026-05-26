package orchestrator

import (
	"errors"
	"fmt"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

var ErrVaultNotSequenced = errors.New("vault is not configured for sequenced write model")

// SequencedVaultDiagnostics is operator-facing local state for sequenced vaults.
type SequencedVaultDiagnostics struct {
	NodeID                   string
	WriteModel               system.VaultWriteModel
	SpoolWatermark           uint64
	IngestHighWatermark      uint64
	FenceHighWatermark       uint64
	MaterializationWatermark uint64
	ConvergenceWatermark     uint64
	Allocator                vaultctlfsm.SeqAllocatorSnapshot
	Fences                   vaultctlfsm.FenceSnapshot
}

// SequencedVaultDiagnostics returns local sequenced write-path state for inspection.
func (o *Orchestrator) SequencedVaultDiagnostics(vaultID glid.GLID) (SequencedVaultDiagnostics, error) {
	wm := o.vaultWriteModel(vaultID)
	if wm != system.VaultWriteModelSequenced {
		return SequencedVaultDiagnostics{}, fmt.Errorf("%w: %s", ErrVaultNotSequenced, wm)
	}

	diag := SequencedVaultDiagnostics{
		NodeID:     o.localNodeID,
		WriteModel: wm,
	}

	for _, snap := range o.VaultSnapshots() {
		if snap.ID != vaultID {
			continue
		}
		diag.SpoolWatermark = snap.SpoolWatermark
		diag.IngestHighWatermark = snap.IngestHighWatermark
		diag.FenceHighWatermark = snap.FenceHighWatermark
		diag.MaterializationWatermark = snap.MaterializationWatermark
		diag.ConvergenceWatermark = snap.ConvergenceWatermark
		break
	}

	sub, err := o.vaultCtlSubFSM(vaultID)
	if err != nil {
		return diag, err
	}
	if sub != nil {
		diag.Allocator = sub.SeqAllocatorState()
		diag.Fences = sub.FenceState()
	}
	return diag, nil
}
