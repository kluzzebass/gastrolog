package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	spoolfile "gastrolog/internal/spool/file"
)

// ErrMaterializeMissingSeq is returned when spool lacks a sequence in the
// fenced range required for local materialization.
var ErrMaterializeMissingSeq = errors.New("materialize: missing assigned sequence in fence range")

// FenceMaterializationCoverage summarizes which sequences were present when a
// fence batch was materialized locally. Reconcile uses MissingSeqs to classify
// assigned-missing holes (P5.b).
type FenceMaterializationCoverage struct {
	Fence       vaultctlfsm.FenceRecord
	RecordCount int
	MissingSeqs []uint64
}

type spoolReclaimSetter interface {
	SetReclaimThroughSeq(seq uint64)
}

// materializeFence reads (M_r, F_n] from local spool, writes a sealed chunk,
// and advances M_r to fence.UpperBoundSeq. Duplicate EventIDs in the fence
// range collapse to one chunk row (lowest VaultSeq wins).
func (o *Orchestrator) materializeFence(vaultID glid.GLID, fence vaultctlfsm.FenceRecord) (*FenceMaterializationCoverage, error) {
	if o.vaultWriteModel(vaultID) != system.VaultWriteModelSequenced {
		return nil, nil
	}
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if vault.Instance == nil {
		return nil, fmt.Errorf("%w: %s (no instance)", ErrVaultNotFound, vaultID)
	}

	store := vault.ensureSpoolStore(o)
	if fence.UpperBoundSeq <= store.MaterializationWatermark() {
		return nil, nil
	}

	start := fence.PrevBoundSeq
	if mr := store.MaterializationWatermark(); mr > start {
		start = mr
	}

	coverage := &FenceMaterializationCoverage{Fence: fence}
	alloc := vaultctlfsm.SeqAllocatorSnapshot{}
	if sub, err := o.vaultCtlSubFSM(vaultID); err == nil && sub != nil {
		alloc = sub.SeqAllocatorState()
	}
	var records []chunk.Record
	seenEventID := make(map[chunk.EventID]struct{})
	for seq := start + 1; seq <= fence.UpperBoundSeq; seq++ {
		rec, err := store.ReadByVaultSeq(context.Background(), vaultID, seq)
		if err != nil {
			if seqInBurnedTail(seq, alloc.BurnedTails) {
				continue
			}
			coverage.MissingSeqs = append(coverage.MissingSeqs, seq)
			continue
		}
		if _, dup := seenEventID[rec.EventID]; dup {
			continue // lowest VaultSeq wins (fence iteration is ascending)
		}
		seenEventID[rec.EventID] = struct{}{}
		records = append(records, rec)
	}
	if len(coverage.MissingSeqs) > 0 {
		return coverage, fmt.Errorf("%w: fence %d missing %d seq(s)", ErrMaterializeMissingSeq, fence.ID, len(coverage.MissingSeqs))
	}
	if len(records) == 0 {
		store.setMaterializationWatermark(fence.UpperBoundSeq)
		o.advanceSpoolReclaimWatermark(vault, fence.UpperBoundSeq)
		o.recordMaterializationCoverage(vaultID, coverage)
		if err := o.reconcileFenceConvergence(vaultID, fence); err != nil {
			o.vaultOpsLogger.Warn("sequenced reconcile after materialize",
				"vault", vaultID,
				"fence", fence.ID,
				"error", err)
		}
		return coverage, nil
	}

	cm := vault.Instance.Chunks
	for i := range records {
		if _, _, err := cm.Append(records[i]); err != nil {
			return coverage, fmt.Errorf("materialize fence %d append seq %d: %w", fence.ID, records[i].VaultSeq, err)
		}
	}
	coverage.RecordCount = len(records)

	active := cm.Active()
	if active == nil || active.RecordCount == 0 {
		return coverage, fmt.Errorf("materialize fence %d: empty active chunk after append", fence.ID)
	}
	chunkID := active.ID
	if err := cm.Seal(); err != nil {
		return coverage, fmt.Errorf("materialize fence %d seal: %w", fence.ID, err)
	}

	o.mu.RLock()
	o.schedulePostSeal(vaultID, cm, chunkID)
	o.mu.RUnlock()

	store.setMaterializationWatermark(fence.UpperBoundSeq)
	o.advanceSpoolReclaimWatermark(vault, fence.UpperBoundSeq)
	o.recordMaterializationCoverage(vaultID, coverage)
	if err := o.reconcileFenceConvergence(vaultID, fence); err != nil {
		o.vaultOpsLogger.Warn("sequenced reconcile after materialize",
			"vault", vaultID,
			"fence", fence.ID,
			"error", err)
	}
	return coverage, nil
}

func (o *Orchestrator) advanceSpoolReclaimWatermark(vault *Vault, seq uint64) {
	store := vault.spool
	if store == nil || store.store == nil {
		return
	}
	if rs, ok := store.store.(*spoolfile.Manager); ok {
		rs.SetReclaimThroughSeq(seq)
		return
	}
	if rs, ok := store.store.(spoolReclaimSetter); ok {
		rs.SetReclaimThroughSeq(seq)
	}
}

func (o *Orchestrator) recordMaterializationCoverage(vaultID glid.GLID, cov *FenceMaterializationCoverage) {
	if cov == nil {
		return
	}
	o.materializationCoverage.Store(vaultID, cov)
}

// LatestMaterializationCoverage returns the most recent local materialization
// summary for operator inspection and reconcile handoff.
func (o *Orchestrator) LatestMaterializationCoverage(vaultID glid.GLID) (*FenceMaterializationCoverage, bool) {
	v, ok := o.materializationCoverage.Load(vaultID)
	if !ok {
		return nil, false
	}
	cov, ok := v.(*FenceMaterializationCoverage)
	return cov, ok
}

func (o *Orchestrator) materializationWatermark(vaultID glid.GLID) uint64 {
	if ss := o.vaultSpoolStore(vaultID); ss != nil {
		return ss.MaterializationWatermark()
	}
	return 0
}

func (o *Orchestrator) sweepSequencedMaterialization(vaultID glid.GLID) {
	if o.vaultWriteModel(vaultID) != system.VaultWriteModelSequenced {
		return
	}
	mr := o.materializationWatermark(vaultID)
	for _, rec := range o.FenceState(vaultID).Records {
		if rec.UpperBoundSeq <= mr {
			continue
		}
		_, _ = o.materializeFence(vaultID, rec)
	}
}

var materializeInflight sync.Map

func (o *Orchestrator) scheduleMaterializeFence(vaultID glid.GLID, fence vaultctlfsm.FenceRecord) {
	if o.vaultWriteModel(vaultID) != system.VaultWriteModelSequenced {
		return
	}
	key := vaultID.String() + ":" + strconv.FormatUint(fence.ID, 10)
	if _, loaded := materializeInflight.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	go func() {
		defer materializeInflight.Delete(key)
		if _, err := o.materializeFence(vaultID, fence); err != nil {
			o.vaultOpsLogger.Warn("sequenced materialization failed",
				"vault", vaultID,
				"fence", fence.ID,
				"upper", fence.UpperBoundSeq,
				"error", err)
		}
	}()
}
