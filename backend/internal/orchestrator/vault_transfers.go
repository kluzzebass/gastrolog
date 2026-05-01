package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/index"
)

// --- Single-chunk move ---

// MoveChunk moves a single sealed chunk from source to destination vault.
// Used by retention-triggered migration to move individual chunks.
// Supports filesystem-level moves (local), record-level import (local), and
// cross-node transfer (remote destination via RemoteTransferrer).
func (o *Orchestrator) MoveChunk(ctx context.Context, chunkID chunk.ChunkID, srcID, dstID glid.GLID) error {
	srcCM, err := o.findChunkManagerForChunk(srcID, chunkID)
	if err != nil {
		return err
	}

	// Check if the destination vault is on a remote node.
	dstNodeID, remote, err := o.isRemoteVault(ctx, dstID)
	if err != nil {
		return err
	}
	if remote {
		return o.moveChunkRemote(ctx, chunkID, srcID, srcCM, dstID, dstNodeID)
	}

	dstCM, dstIM, err := o.activeTierManagers(dstID)
	if err != nil {
		return err
	}

	// Try filesystem move first (O(1) rename, only works for file-backed vaults).
	srcMover, srcOk := srcCM.(chunk.ChunkMover)
	dstMover, dstOk := dstCM.(chunk.ChunkMover)
	if srcOk && dstOk {
		return o.moveChunkFS(ctx, chunkID, srcMover, dstMover, dstIM)
	}

	// Fallback: stream records as a new sealed chunk (works for any vault type).
	cursor, err := srcCM.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open chunk %s: %w", chunkID, err)
	}
	// Close-on-return as a safety net; we Close explicitly post-stream
	// so the cursor's per-chunk RLock (gastrolog-26zu1) is released
	// before deleteSourceChunk takes the write lock — otherwise the
	// same goroutine self-deadlocks on RLock→Lock upgrade.
	cursorClosed := false
	defer func() {
		if !cursorClosed {
			_ = cursor.Close()
		}
	}()
	imported, err := dstCM.ImportRecords(chunkID, chunk.CursorIterator(cursor))
	if err != nil {
		return fmt.Errorf("import records into destination: %w", err)
	}
	if dstIM != nil && imported.ID != (chunk.ChunkID{}) {
		if err := dstIM.BuildIndexes(ctx, imported.ID); err != nil {
			o.logger.Warn("retention migrate: failed to build indexes for imported chunk",
				"chunk", imported.ID.String(), "error", err)
		}
	}

	// Release cursor's read lock before deleting the source chunk.
	_ = cursor.Close()
	cursorClosed = true

	// Delete from source after successful import.
	if err := o.deleteSourceChunk(srcID, chunkID); err != nil {
		return err
	}

	return nil
}

// moveChunkRemote transfers a sealed chunk to a vault on another node.
// Reads records from the source chunk and sends them via the RemoteTransferrer.
// The destination imports them as a new sealed chunk (no mixing with active chunk).
func (o *Orchestrator) moveChunkRemote(ctx context.Context, chunkID chunk.ChunkID, srcID glid.GLID, srcCM chunk.ChunkManager, dstID glid.GLID, dstNodeID string) error {
	if o.transferrer == nil {
		return fmt.Errorf("remote vault %s on node %s: no remote transferrer configured (single-node mode)", dstID, dstNodeID)
	}

	cursor, err := srcCM.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open chunk %s: %w", chunkID, err)
	}
	cursorClosed := false
	defer func() {
		if !cursorClosed {
			_ = cursor.Close()
		}
	}()

	if err := o.transferrer.TransferRecords(ctx, dstNodeID, dstID, chunk.CursorIterator(cursor)); err != nil {
		return fmt.Errorf("transfer chunk %s to node %s: %w", chunkID, dstNodeID, err)
	}

	// Release cursor's read lock before deleting the source chunk
	// (gastrolog-26zu1: same-goroutine RLock→Lock upgrade would
	// deadlock).
	_ = cursor.Close()
	cursorClosed = true

	// Delete from source after successful transfer.
	if err := o.deleteSourceChunk(srcID, chunkID); err != nil {
		return err
	}

	return nil
}

// resolveVaultNode loads config and returns the NodeID for the given vault.
// With tiered storage, vaults no longer have a NodeID. All nodes can serve
// all vaults. This always returns empty string (local).
func (o *Orchestrator) resolveVaultNode(ctx context.Context, vaultID glid.GLID) (string, error) {
	if o.sysLoader == nil {
		return "", errors.New("config loader not configured")
	}
	sys, err := o.sysLoader.Load(ctx)
	if err != nil {
		return "", fmt.Errorf("load config: %w", err)
	}
	for _, v := range sys.Config.Vaults {
		if v.ID == vaultID {
			return "", nil
		}
	}
	return "", fmt.Errorf("%w: %s (not in config)", ErrVaultNotFound, vaultID)
}

// isRemoteVault reports whether a vault lives on another node.
// Returns the destination node ID and whether it's remote.
func (o *Orchestrator) isRemoteVault(ctx context.Context, vaultID glid.GLID) (string, bool, error) {
	// If the vault is registered locally, it's not remote.
	if o.VaultExists(vaultID) {
		return "", false, nil
	}

	nodeID, err := o.resolveVaultNode(ctx, vaultID)
	if err != nil {
		return "", false, err
	}
	if nodeID == "" || nodeID == o.localNodeID {
		// Vault should be local but isn't registered — real ErrVaultNotFound.
		return "", false, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	return nodeID, true, nil
}

// deleteSourceChunk removes the chunk from the source vault on the tier
// that owns it. Routes through the receipt protocol when a reconciler is
// wired (production) so the source-side delete propagates cluster-wide
// via CmdRequestDelete. Falls back to a direct local delete for
// memory-mode tiers without Raft. Reason "vault-migrate-source-expire"
// lands in pendingDeletes audit. See gastrolog-51gme.
func (o *Orchestrator) deleteSourceChunk(srcID glid.GLID, chunkID chunk.ChunkID) error {
	tier, err := o.findTierForChunk(srcID, chunkID)
	if err != nil {
		return err
	}
	if tier.Reconciler != nil {
		return tier.Reconciler.deleteChunk(chunkID, "vault-migrate-source-expire", o.placementMembership(tier))
	}
	if tier.Indexes != nil {
		if err := tier.Indexes.DeleteIndexes(chunkID); err != nil {
			o.logger.Warn("retention migrate: failed to delete source indexes",
				"chunk", chunkID.String(), "error", err)
		}
	}
	if err := tier.Chunks.Delete(chunkID); err != nil {
		return fmt.Errorf("delete source chunk %s: %w", chunkID, err)
	}
	return nil
}

// moveChunkFS performs a filesystem-level move of a sealed chunk between vaults.
func (o *Orchestrator) moveChunkFS(ctx context.Context, chunkID chunk.ChunkID, srcMover, dstMover chunk.ChunkMover, dstIM index.IndexManager) error {
	srcDir := srcMover.ChunkDir(chunkID)
	dstDir := dstMover.ChunkDir(chunkID)

	if err := srcMover.Disown(chunkID); err != nil {
		return fmt.Errorf("disown chunk %s: %w", chunkID, err)
	}

	if err := chunkfile.MoveDir(srcDir, dstDir); err != nil {
		if _, adoptErr := srcMover.Adopt(chunkID); adoptErr != nil {
			o.logger.Error("failed to restore chunk after move error",
				"chunk", chunkID.String(), "error", adoptErr)
		}
		return fmt.Errorf("move chunk %s: %w", chunkID, err)
	}

	if _, err := dstMover.Adopt(chunkID); err != nil {
		return fmt.Errorf("adopt chunk %s in destination: %w", chunkID, err)
	}

	if dstIM != nil {
		if err := dstIM.BuildIndexes(ctx, chunkID); err != nil {
			o.logger.Warn("retention migrate: failed to build indexes for moved chunk",
				"chunk", chunkID.String(), "error", err)
		}
	}
	return nil
}

// --- Vault drain (cross-node migration on reassignment) ---

// DrainVault starts an async migration of a vault's sealed chunks to a target
// node. The vault remains registered locally (for search) but the filter set
// routes new records to the target node via RecordForwarder. Once all sealed
// chunks are transferred, the vault is unregistered locally.
//
// Role: runs on whichever node is losing the vault (source node). Dispatch
// invokes this after config reassignment via CmdPutVault / routing changes
// move placement to `targetNodeID`. It does not require tier leadership —
// local chunks are streamed to the target regardless of leader/follower
// role; the target decides how to integrate them.
//
// Readiness: no explicit Vault.ReadinessErr gate. Drain operates on local
// chunk files directly, not through the FSM, so an unready FSM does not
// block transfer. New writes are redirected via the filter rebuild before
// drain starts, so inbound records cannot race the drain.
func (o *Orchestrator) DrainVault(ctx context.Context, vaultID glid.GLID, targetNodeID string) error {
	sys, err := o.loadSystem(ctx)
	if err != nil {
		return fmt.Errorf("load config for drain: %w", err)
	}

	o.mu.Lock()
	vault := o.vaults[vaultID]
	if vault == nil {
		o.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if _, already := o.draining[vaultID]; already {
		o.mu.Unlock()
		return fmt.Errorf("vault %s is already draining", vaultID)
	}

	// Mark as draining so filter reload treats this vault as remote.
	drainCtx, cancel := context.WithCancel(context.Background())
	ds := &drainState{TargetNodeID: targetNodeID, Cancel: cancel}
	o.draining[vaultID] = ds

	// Rebuild filters — draining vault will forward to targetNodeID.
	if err := o.reloadFiltersFromRoutes(sys); err != nil {
		delete(o.draining, vaultID)
		cancel()
		o.mu.Unlock()
		return fmt.Errorf("reload filters for drain: %w", err)
	}

	// Remove per-tier retention and rotation jobs (no longer needed locally).
	if vault := o.vaults[vaultID]; vault != nil {
		o.removeVaultJobs(vaultID, vault)
	}

	o.mu.Unlock()

	// Seal active chunk outside the lock — flush any locally-buffered records.
	if _, err := o.SealActive(vaultID, glid.Nil); err != nil {
		o.logger.Warn("drain: failed to seal active chunk", "vault", vaultID, "error", err)
	}

	// Submit async job.
	jobName := "drain:" + vaultID.String()
	jobID := o.scheduler.Submit(jobName, func(ctx context.Context, job *JobProgress) {
		o.drainWorker(drainCtx, vaultID, targetNodeID, job)
	})
	o.scheduler.Describe(jobName, "Drain vault to node "+targetNodeID)

	o.mu.Lock()
	if d, ok := o.draining[vaultID]; ok {
		d.JobID = jobID
	}
	o.mu.Unlock()

	o.logger.Info("vault drain started", "vault", vaultID, "target_node", targetNodeID, "job", jobID)
	return nil
}

// drainWorker runs in the scheduler, transferring sealed chunks one by one.
func (o *Orchestrator) drainWorker(ctx context.Context, vaultID glid.GLID, targetNodeID string, job *JobProgress) {
	cm, err := o.activeTierChunkManager(vaultID)
	if err != nil {
		job.Fail(o.now(), fmt.Sprintf("get chunk manager: %v", err))
		return
	}

	// Wait for the target node to create the vault before transferring.
	// Both nodes process the same Raft notification independently —
	// without this, the transfer RPCs can hit ErrVaultNotFound.
	if o.transferrer != nil {
		if err := o.transferrer.WaitVaultReady(ctx, targetNodeID, vaultID); err != nil {
			job.Fail(o.now(), fmt.Sprintf("target vault not ready: %v", err))
			return
		}
	}

	// Transfer all currently sealed chunks.
	if !o.drainSealed(ctx, vaultID, cm, targetNodeID, job) {
		return // drainSealed already called job.Fail
	}

	// Final seal: catch any records that were appended between
	// DrainVault's SealActive and the worker starting (e.g. from
	// ForwardRecords RPCs from nodes with stale filter sets).
	if _, err := o.SealActive(vaultID, glid.Nil); err != nil {
		o.logger.Warn("drain: final seal", "vault", vaultID, "error", err)
	}
	if !o.drainSealed(ctx, vaultID, cm, targetNodeID, job) {
		return
	}

	o.finishDrain(vaultID)
}

// drainSealed lists sealed chunks and transfers each to the target node.
// Returns false if the transfer failed (job.Fail was called).
func (o *Orchestrator) drainSealed(ctx context.Context, vaultID glid.GLID, cm chunk.ChunkManager, targetNodeID string, job *JobProgress) bool {
	metas, err := cm.List()
	if err != nil {
		job.Fail(o.now(), fmt.Sprintf("list chunks: %v", err))
		return false
	}

	for _, meta := range metas {
		if !meta.Sealed {
			continue
		}
		if ctx.Err() != nil {
			job.Fail(o.now(), "drain cancelled")
			return false
		}
		if err := o.moveChunkRemote(ctx, meta.ID, vaultID, cm, vaultID, targetNodeID); err != nil {
			job.Fail(o.now(), fmt.Sprintf("transfer chunk %s: %v", meta.ID, err))
			return false
		}
		job.AddRecords(meta.RecordCount)
		job.IncrChunks()
	}
	return true
}

// finishDrain cleans up after all chunks have been transferred.
func (o *Orchestrator) finishDrain(vaultID glid.GLID) {
	o.mu.Lock()
	defer o.mu.Unlock()

	delete(o.draining, vaultID)

	vault := o.vaults[vaultID]
	if vault == nil {
		return // already removed (e.g. CancelDrain + ForceRemoveVault raced)
	}

	// Cancel pending post-seal/compress/index jobs before closing the chunk manager
	// to prevent use-after-close on the managers they capture.
	vaultPrefix := vaultID.String()
	o.scheduler.RemoveJobsByPrefix("post-seal:" + vaultPrefix)
	o.scheduler.RemoveJobsByPrefix("compress:" + vaultPrefix)
	o.scheduler.RemoveJobsByPrefix("index-build:" + vaultPrefix)

	if err := vault.Close(); err != nil {
		o.logger.Warn("drain: failed to close vault", "vault", vaultID, "error", err)
	}

	delete(o.vaults, vaultID)
	o.rebuildFilterSetLocked()

	o.logger.Info("vault drain completed, vault unregistered", "vault", vaultID)
}

// CancelDrain cancels an in-progress drain and restores local routing.
func (o *Orchestrator) CancelDrain(ctx context.Context, vaultID glid.GLID) error {
	sys, err := o.loadSystem(ctx)
	if err != nil {
		return fmt.Errorf("load config for cancel drain: %w", err)
	}

	o.mu.Lock()
	ds, ok := o.draining[vaultID]
	if !ok {
		o.mu.Unlock()
		return fmt.Errorf("vault %s is not draining", vaultID)
	}

	ds.Cancel()
	delete(o.draining, vaultID)

	if err := o.reloadFiltersFromRoutes(sys); err != nil {
		o.logger.Warn("cancel drain: failed to reload filters", "vault", vaultID, "error", err)
	}
	o.mu.Unlock()

	o.logger.Info("vault drain cancelled", "vault", vaultID)
	return nil
}

// IsDraining reports whether a vault is currently being drained.
func (o *Orchestrator) IsDraining(vaultID glid.GLID) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	_, ok := o.draining[vaultID]
	return ok
}

