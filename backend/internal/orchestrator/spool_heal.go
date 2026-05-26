package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// SpoolSlotFetcher pulls one spool slot from a remote cluster peer.
type SpoolSlotFetcher interface {
	ReadSpoolSeq(ctx context.Context, nodeID string, vaultID glid.GLID, seq uint64) (chunk.Record, bool, error)
}

var spoolHealInflight sync.Map // key: vaultID:fenceID

// SetSpoolSlotFetcher wires cross-node spool slot reads for recovery heal.
func (o *Orchestrator) SetSpoolSlotFetcher(fetcher SpoolSlotFetcher) {
	o.spoolSlotFetcher = fetcher
}

// SetSpoolReplicaWriteFilterForTest drops replica spool writes matching the predicate (tests only).
func (o *Orchestrator) SetSpoolReplicaWriteFilterForTest(filter func(vaultID glid.GLID, rec chunk.Record) bool) {
	o.spoolReplicaWriteFilter = filter
}

func (o *Orchestrator) scheduleSpoolSlotHeal(vaultID glid.GLID, fence vaultctlfsm.FenceRecord, missing []uint64) {
	if len(missing) == 0 {
		return
	}
	key := vaultID.String() + ":" + strconv.FormatUint(fence.ID, 10)
	if _, loaded := spoolHealInflight.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	go func() {
		defer spoolHealInflight.Delete(key)
		o.healAssignedMissingSpoolSlots(vaultID, fence, missing)
	}()
}

func (o *Orchestrator) healAssignedMissingSpoolSlots(vaultID glid.GLID, fence vaultctlfsm.FenceRecord, missing []uint64) {
	for _, seq := range missing {
		if o.localSeqPresentForReconcile(vaultID, fence, seq) {
			continue
		}
		rec, err := o.pullSpoolSlotFromPeers(vaultID, seq)
		if err != nil {
			o.vaultOpsLogger.Warn("spool slot heal: peer pull failed",
				"vault", vaultID,
				"fence", fence.ID,
				"vault_seq", seq,
				"error", err)
			continue
		}
		if err := o.applySpoolReplicaWriteFiltered(vaultID, rec); err != nil {
			o.vaultOpsLogger.Warn("spool slot heal: apply failed",
				"vault", vaultID,
				"fence", fence.ID,
				"vault_seq", seq,
				"error", err)
			continue
		}
	}
	if err := o.reconcileFenceConvergence(vaultID, fence); err != nil {
		o.vaultOpsLogger.Warn("spool slot heal: reconcile still blocked",
			"vault", vaultID,
			"fence", fence.ID,
			"error", err)
	}
}

func (o *Orchestrator) pullSpoolSlotFromPeers(vaultID glid.GLID, seq uint64) (chunk.Record, error) {
	if o.spoolSlotFetcher == nil {
		return chunk.Record{}, errors.New("spool slot fetcher not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, nodeID := range o.spoolHealPeers(vaultID) {
		rec, found, err := o.spoolSlotFetcher.ReadSpoolSeq(ctx, nodeID, vaultID, seq)
		if err != nil {
			continue
		}
		if found {
			return rec, nil
		}
	}
	return chunk.Record{}, fmt.Errorf("no peer has vault_seq %d", seq)
}

func (o *Orchestrator) spoolHealPeers(vaultID glid.GLID) []string {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return nil
	}
	seen := make(map[string]struct{})
	var peers []string
	add := func(nodeID string) {
		if nodeID == "" || nodeID == o.localNodeID {
			return
		}
		if _, ok := seen[nodeID]; ok {
			return
		}
		seen[nodeID] = struct{}{}
		peers = append(peers, nodeID)
	}
	for _, tgt := range vault.seqFanOutTargets {
		add(tgt.NodeID)
	}
	if vault.Instance != nil {
		for _, tgt := range vault.Instance.FollowerTargets {
			add(tgt.NodeID)
		}
	}
	return peers
}

func (o *Orchestrator) applySpoolReplicaWriteFiltered(vaultID glid.GLID, rec chunk.Record) error {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if vault.WriteModel != system.VaultWriteModelSequenced {
		return errors.New("spool replica write on vault without sequenced write model")
	}
	if err := o.ensureSpoolWindowForVaultSeq(vaultID, rec.VaultSeq); err != nil {
		return err
	}
	return vault.ensureSpoolStore(o).PutReplicaWrite(rec)
}
