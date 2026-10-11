package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// sweepChunkTombstones runs on every node for every vault: each node purges
// its copies of tombstoned chunks and confirms the delete sequence the purge
// covered, and the vault-ctl leader prunes the chunk tombstones every member
// of the group has confirmed. Every node takes part because every node is a
// vault-ctl member, and a node outside a delete's expected-acks set can still
// hold the chunk in its chunk store or chunk staging.
//
// A pass whose config cannot be loaded does nothing: without the node's file
// storages it cannot reach every root a chunk copy may be under, and a purge
// that skipped one must not be confirmed.
func (o *Orchestrator) sweepChunkTombstones() {
	sys, err := o.loadSystem(context.Background())
	if err != nil && !errors.Is(err, ErrNoSystemLoader) {
		o.logger.Warn("chunk tombstone sweep: config unavailable; skipping", "error", err)
		return
	}
	o.mu.RLock()
	vaultIDs := make([]glid.GLID, 0, len(o.vaults))
	for vaultID := range o.vaults {
		vaultIDs = append(vaultIDs, vaultID)
	}
	o.mu.RUnlock()
	for _, vaultID := range vaultIDs {
		o.sweepChunkTombstonesForVault(sys, vaultID)
	}
}

func (o *Orchestrator) sweepChunkTombstonesForVault(sys *system.System, vaultID glid.GLID) {
	fsm, applier, isLeader, ok := o.vaultCtlHandle(vaultID)
	if !ok || !fsm.Ready() {
		return
	}
	log := o.logger.With("vault", vaultID)
	covered := fsm.DeleteSeq()
	purged, err := o.purgeTombstonedChunks(sys, vaultID, fsm)
	if len(purged) > 0 {
		log.Info("chunk tombstone sweep: removed tombstoned chunk staging dirs", "chunks", len(purged))
	}
	switch {
	case err != nil:
		log.Warn("chunk tombstone sweep: purge incomplete; not confirming", "error", err)
	case covered > fsm.ChunkPurgeConfirmed(o.localNodeID):
		if err := applier.Apply(vaultctlfsm.MarshalConfirmChunkPurge(o.localNodeID, covered)); err != nil {
			log.Warn("chunk tombstone sweep: purge confirmation failed", "delete_seq", covered, "error", err)
		}
	}
	if !isLeader() {
		return
	}
	members, ok := o.vaultCtlMembers(vaultID)
	if !ok || !fsm.ChunkTombstonesPrunable(members) {
		return
	}
	if err := applier.Apply(vaultctlfsm.MarshalPruneChunkTombstones(members)); err != nil {
		log.Warn("chunk tombstone sweep: tombstone prune failed", "error", err)
	}
}

// purgeTombstonedChunks deletes this node's copies of the vault's tombstoned
// chunks: from the vault instance's chunk store when the vault has an instance
// here, and from the chunk staging under every pipeline root the vault may
// have on this node, whether or not this node is a home. Returns the chunks
// purged from staging; an error means a tombstoned chunk may still be on this
// node.
func (o *Orchestrator) purgeTombstonedChunks(sys *system.System, vaultID glid.GLID, fsm *vaultctlfsm.FSM) ([]chunk.ChunkID, error) {
	var errs []error
	if rec := o.vaultReconciler(vaultID); rec != nil {
		errs = append(errs, rec.purgeLocalOrphans())
	}
	var purged []chunk.ChunkID
	for _, root := range o.vaultStagingRoots(sys, vaultID) {
		ids, err := purgeTombstonedChunkStaging(fsm, pipelineChunkRoot(root))
		purged = append(purged, ids...)
		errs = append(errs, err)
	}
	return purged, errors.Join(errs...)
}

func (o *Orchestrator) vaultReconciler(vaultID glid.GLID) *VaultLifecycleReconciler {
	o.mu.RLock()
	defer o.mu.RUnlock()
	vault := o.vaults[vaultID]
	if vault == nil || vault.Instance == nil {
		return nil
	}
	return vault.Instance.Reconciler
}

// purgeTombstonedChunkStaging removes the chunk dir under chunkRoot of every
// chunk fsm records as tombstoned and has neither in its manifest nor in a
// pending delete. Dirs of unknown chunks stay: only a tombstone authorises a
// delete. Returns the purged chunk IDs, and an error when the listing or a
// delete failed. A missing chunkRoot holds nothing.
func purgeTombstonedChunkStaging(fsm *vaultctlfsm.FSM, chunkRoot string) ([]chunk.ChunkID, error) {
	if fsm == nil || !fsm.Ready() {
		return nil, errors.New("vault-ctl FSM not ready")
	}
	entries, err := os.ReadDir(chunkRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list chunk staging %s: %w", chunkRoot, err)
	}
	var (
		purged []chunk.ChunkID
		errs   []error
	)
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		id, err := chunk.ParseChunkID(ent.Name())
		if err != nil {
			continue
		}
		if !fsm.IsTombstoned(id) || fsm.Get(id) != nil || fsm.PendingDelete(id) != nil {
			continue
		}
		if err := os.RemoveAll(filepath.Join(chunkRoot, ent.Name())); err != nil {
			errs = append(errs, fmt.Errorf("remove chunk %s: %w", id, err))
			continue
		}
		purged = append(purged, id)
	}
	return purged, errors.Join(errs...)
}
