package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

func (o *Orchestrator) requiredSpoolDurability(vault *Vault) int {
	if vault == nil {
		return 1
	}
	rf := int(vault.ReplicationFactor)
	if rf <= 0 {
		rf = 1
	}
	return rf
}

func (o *Orchestrator) tryCommitSequencedAcceptance(vaultID glid.GLID, rec chunk.Record, followerSuccesses int) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil || vault.WriteModel != system.VaultWriteModelSequenced {
		return
	}
	required := o.requiredSpoolDurability(vault)
	if 1+followerSuccesses < required {
		return
	}
	store := vault.ensureSpoolStore(o)
	if err := store.CommitAcceptance(rec); err != nil {
		o.vaultOpsLogger.Warn("sequenced write: acceptance commit failed",
			"vault", vaultID, "seq", rec.VaultSeq, "error", err)
	}
}

func (o *Orchestrator) commitSequencedPendingAcks(pa *pendingAcks, rec chunk.Record) {
	if pa == nil {
		return
	}
	for _, t := range pa.replication {
		o.tryCommitSequencedAcceptance(t.vaultID, rec, len(t.targets))
	}
}

// sequencedLocalFollowerWrites counts same-node follower targets whose spool
// already contains the record (written synchronously before remote fan-out).
func (o *Orchestrator) sequencedLocalFollowerWrites(vaultID glid.GLID, rec chunk.Record) int {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil || vault.Instance == nil {
		return 0
	}
	n := 0
	for _, tgt := range vault.Instance.FollowerTargets {
		if tgt.NodeID != o.localNodeID {
			continue
		}
		store := vault.ensureSpoolStore(o)
		if seq, ok := store.LookupSeq(rec.EventID); ok && seq == rec.VaultSeq {
			n++
		}
	}
	return n
}
