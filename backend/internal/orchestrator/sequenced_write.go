package orchestrator

import (
	"errors"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// appendLocalSequenced assigns a destination-vault sequence, persists interim
// assignment metadata, and fans out to replicas without chunk append.
// Caller MUST hold o.mu.RLock.
func (o *Orchestrator) appendLocalSequenced(vaultID glid.GLID, rec chunk.Record) (*replicationTask, []remoteForwardTarget, error) {
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

	store := vault.ensureInterimSeqStore()
	if err := store.PutLeaderAssignment(rec); err != nil {
		return nil, nil, err
	}
	o.progressTrigger.Signal()

	vaultInst := vault.Instance
	var task *replicationTask
	var remotes []remoteForwardTarget
	if vaultInst.ShouldForwardToFollowers() {
		if rec.WaitForReplica {
			task = &replicationTask{
				vaultID: vaultID,
				targets: vaultInst.FollowerTargets,
			}
		} else {
			remotes = o.forwardSequencedToFollowers(vault, vaultID, vaultInst, rec)
		}
	}
	return task, remotes, nil
}

func (o *Orchestrator) forwardSequencedToFollowers(_ *Vault, vaultID glid.GLID, vaultInst *VaultInstance, rec chunk.Record) []remoteForwardTarget {
	var remotes []remoteForwardTarget
	for _, tgt := range vaultInst.FollowerTargets {
		if tgt.NodeID == o.localNodeID {
			if err := o.applyInterimReplicaWrite(vaultID, rec); err != nil {
				o.vaultOpsLogger.Warn("sequenced replication: local follower interim write failed",
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

func (o *Orchestrator) applyInterimReplicaWrite(vaultID glid.GLID, rec chunk.Record) error {
	vault := o.vaults[vaultID]
	if vault == nil {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if vault.WriteModel != system.VaultWriteModelSequenced {
		return errors.New("interim replica write on vault without sequenced write model")
	}
	return vault.ensureInterimSeqStore().PutReplicaAssignment(rec)
}
