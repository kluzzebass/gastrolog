package orchestrator

import (
	"context"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// localSeqPresentForReconcile reports whether seq is locally covered for fence
// reconcile. Spool (bySeq + durable store) is the primary probe. After a fence
// batch is materialized, durable M_r plus materialization coverage witness that
// every seq in (prev, upper] was present at materialize time — spool reclaim
// or empty bySeq must not false-negative on later reconcile passes.
//
// Chunk scan is a tertiary fallback when durable M_r proves the seq was
// materialized but spool no longer holds the slot.
func (o *Orchestrator) localSeqPresentForReconcile(vaultID glid.GLID, fence vaultctlfsm.FenceRecord, seq uint64) bool {
	store := o.vaultSpoolStore(vaultID)
	if _, err := store.ReadByVaultSeq(context.Background(), vaultID, seq); err == nil {
		return true
	}
	if seq <= fence.PrevBoundSeq || seq > fence.UpperBoundSeq {
		return false
	}

	alloc := vaultctlfsm.SeqAllocatorSnapshot{}
	if sub, err := o.vaultCtlSubFSM(vaultID); err == nil && sub != nil {
		alloc = sub.SeqAllocatorState()
	}
	if seqInBurnedTail(seq, alloc.BurnedTails) {
		return false
	}

	cov, ok := o.LatestMaterializationCoverage(vaultID)
	if ok && cov.Fence.ID == fence.ID && cov.Fence.UpperBoundSeq == fence.UpperBoundSeq {
		if store.MaterializationWatermark() >= fence.UpperBoundSeq {
			return true
		}
	}
	mr := store.MaterializationWatermark()
	if mr >= fence.UpperBoundSeq && mr >= seq {
		return o.seqPresentInMaterializedChunks(vaultID, seq)
	}
	return false
}

// reconcileFenceConvergence classifies holes for a materialized fence and
// advances C_r when no assigned-missing holes remain locally. Unassigned gaps
// from burned lease tails are ignored by design.
func (o *Orchestrator) reconcileFenceConvergence(vaultID glid.GLID, fence vaultctlfsm.FenceRecord) error {
	if o.vaultWriteModel(vaultID) != system.VaultWriteModelSequenced {
		return nil
	}
	store := o.vaultSpoolStore(vaultID)
	if fence.UpperBoundSeq <= store.ConvergenceWatermark() {
		return nil
	}
	if store.MaterializationWatermark() < fence.UpperBoundSeq {
		return fmt.Errorf("reconcile: fence %d not materialized locally (M_r=%d)", fence.ID, store.MaterializationWatermark())
	}

	alloc := vaultctlfsm.SeqAllocatorSnapshot{}
	if sub, err := o.vaultCtlSubFSM(vaultID); err == nil && sub != nil {
		alloc = sub.SeqAllocatorState()
	}
	holes := ClassifyFenceHoles(fence, alloc, func(seq uint64) bool {
		return o.localSeqPresentForReconcile(vaultID, fence, seq)
	})
	if missing := assignedMissingHoles(holes); len(missing) > 0 {
		o.scheduleSpoolSlotHeal(vaultID, fence, missing)
		return fmt.Errorf("reconcile fence %d: %d assigned-missing hole(s)", fence.ID, len(missing))
	}
	store.setConvergenceWatermark(fence.UpperBoundSeq)
	return nil
}

func (o *Orchestrator) convergenceWatermark(vaultID glid.GLID) uint64 {
	if ss := o.vaultSpoolStore(vaultID); ss != nil {
		return ss.ConvergenceWatermark()
	}
	return 0
}

// IsFenceConvergeSealed reports whether local C_r covers fence upper bound.
func (o *Orchestrator) IsFenceConvergeSealed(vaultID glid.GLID, fence vaultctlfsm.FenceRecord) bool {
	return o.convergenceWatermark(vaultID) >= fence.UpperBoundSeq
}

type vaultSeqChunkScanner interface {
	ChunkContainsVaultSeq(id chunk.ChunkID, seq uint64) (bool, error)
}

func (o *Orchestrator) seqPresentInMaterializedChunks(vaultID glid.GLID, seq uint64) bool {
	if seq == 0 {
		return false
	}
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil || vault.Instance == nil {
		return false
	}
	list := vault.Instance.ListManifest
	if list == nil {
		return false
	}
	cm := vault.Instance.Chunks
	scanner, ok := cm.(vaultSeqChunkScanner)
	if !ok {
		return false
	}
	for _, id := range list() {
		found, err := scanner.ChunkContainsVaultSeq(id, seq)
		if err == nil && found {
			return true
		}
	}
	return false
}
