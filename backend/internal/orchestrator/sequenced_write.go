package orchestrator

import (
	"errors"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// appendLocalSequenced assigns a destination-vault sequence, appends to spool,
// fans out to replicas, and commits acceptance (H) only after W-of-N durability.
// Caller MUST hold o.mu.RLock.
func (o *Orchestrator) appendLocalSequenced(vaultID glid.GLID, rec *chunk.Record) (*replicationTask, []remoteForwardTarget, error) {
	vault := o.vaults[vaultID]
	if vault == nil {
		return nil, nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if !vault.Enabled {
		return nil, nil, fmt.Errorf("%w: %s", ErrVaultDisabled, vaultID)
	}
	if err := vaultReplicationReadinessErr(vaultID, vault); err != nil {
		return nil, nil, err
	}
	if vault.Instance == nil {
		return nil, nil, fmt.Errorf("%w: %s (no instance)", ErrVaultNotFound, vaultID)
	}

	seq, err := o.assignDestinationVaultSeq(vaultID, rec.EventID)
	if err != nil {
		return nil, nil, err
	}
	rec.VaultSeq = seq
	rec.VaultID = vaultID

	store := vault.ensureSpoolStore(o)
	if err := store.AppendTentative(*rec); err != nil {
		return nil, nil, err
	}
	o.progressTrigger.Signal()

	vaultInst := vault.Instance
	task, remotes, err := o.sequencedFollowerFanOut(vault, vaultID, vaultInst, store, rec)
	if err != nil {
		return nil, nil, err
	}
	return task, remotes, nil
}

func (o *Orchestrator) sequencedFanOutTargets(vault *Vault, vaultInst *VaultInstance) []system.ReplicationTarget {
	if vault != nil && len(vault.seqFanOutTargets) > 0 {
		return vault.seqFanOutTargets
	}
	if vaultInst == nil {
		return nil
	}
	// Fallback when seqFanOutTargets is stale or unset. V2 ingest authority is
	// not placement-leader residency — fan out to every other replica.
	var out []system.ReplicationTarget
	if vaultInst.LeaderNodeID != "" && vaultInst.LeaderNodeID != o.localNodeID {
		out = append(out, system.ReplicationTarget{NodeID: vaultInst.LeaderNodeID})
	}
	for _, tgt := range vaultInst.FollowerTargets {
		if tgt.NodeID != "" && tgt.NodeID != o.localNodeID {
			out = append(out, tgt)
		}
	}
	return out
}

func (o *Orchestrator) sequencedFollowerFanOut(vault *Vault, vaultID glid.GLID, vaultInst *VaultInstance, store *vaultSpoolStore, rec *chunk.Record) (*replicationTask, []remoteForwardTarget, error) {
	targets := o.sequencedFanOutTargets(vault, vaultInst)
	if len(targets) == 0 {
		if err := store.CommitAcceptance(*rec); err != nil {
			return nil, nil, err
		}
		return nil, nil, nil
	}
	if rec.WaitForReplica {
		return &replicationTask{
			vaultID: vaultID,
			targets: targets,
		}, nil, nil
	}
	remotes := o.forwardSequencedToFollowers(vault, vaultID, targets, *rec)
	if len(remotes) == 0 {
		localOK := o.sequencedLocalFollowerWrites(vaultID, *rec)
		o.tryCommitSequencedAcceptance(vaultID, *rec, localOK)
	}
	return nil, remotes, nil
}

func (o *Orchestrator) forwardSequencedToFollowers(_ *Vault, vaultID glid.GLID, targets []system.ReplicationTarget, rec chunk.Record) []remoteForwardTarget {
	var remotes []remoteForwardTarget
	for _, tgt := range targets {
		if tgt.NodeID == o.localNodeID {
			if err := o.applySpoolReplicaWrite(vaultID, rec); err != nil {
				o.vaultOpsLogger.Warn("sequenced replication: local follower spool write failed",
					"vault", vaultID, "error", err)
			}
			continue
		}
		remotes = append(remotes, remoteForwardTarget{
			nodeID:  tgt.NodeID,
			vaultID: vaultID,
		})
	}
	return remotes
}

func (o *Orchestrator) ensureSpoolWindowForVaultSeq(vaultID glid.GLID, seq uint64) error {
	v := o.vaults[vaultID]
	if v == nil {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	store := v.ensureSpoolStore(o)
	if sub, err := o.vaultCtlSubFSM(vaultID); err == nil && sub != nil {
		for _, lease := range sub.SeqAllocatorState().ActiveSwaths {
			if seq >= lease.RangeStart && seq <= lease.RangeEnd {
				return store.EnsureSwathWindow(lease.RangeStart, lease.RangeEnd)
			}
		}
	}
	batch := uint64(defaultSeqLeaseBatch)
	start := ((seq-1)/batch)*batch + 1
	end := start + batch - 1
	return store.EnsureSwathWindow(start, end)
}

func (o *Orchestrator) applySpoolReplicaWrite(vaultID glid.GLID, rec chunk.Record) error {
	if o.spoolReplicaWriteFilter != nil && o.spoolReplicaWriteFilter(vaultID, rec) {
		return nil
	}
	vault := o.vaults[vaultID]
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
