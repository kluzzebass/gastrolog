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

	dstCM, dstIM, err := o.activeManagers(dstID)
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
	// Close-on-return as a safety net; we Close explicitly post-stream so
	// the cursor's per-chunk RLock is released before deleteSourceChunk
	// takes the write lock — otherwise the same goroutine self-deadlocks
	// on RLock→Lock upgrade.
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
			o.chunkMoveLogger.Warn("retention migrate: failed to build indexes for imported chunk",
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
	// (same-goroutine RLock→Lock upgrade would deadlock).
	_ = cursor.Close()
	cursorClosed = true

	// Delete from source after successful transfer.
	if err := o.deleteSourceChunk(srcID, chunkID); err != nil {
		return err
	}

	return nil
}

// resolveVaultNode loads config and returns the NodeID for the given vault.
// With vault storage, vaults no longer have a NodeID. All nodes can serve
// all vaults. This always returns empty string (local).
func (o *Orchestrator) resolveVaultNode(ctx context.Context, vaultID glid.GLID) (string, error) {
	loader := o.systemLoader()
	if loader == nil {
		return "", errors.New("config loader not configured")
	}
	sys, err := loader.Load(ctx)
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

// deleteSourceChunk removes the chunk from the source vault on the instance
// that owns it. Routes through the receipt protocol when a reconciler is
// wired (production) so the source-side delete propagates cluster-wide
// via CmdRequestDelete. Falls back to a direct local delete for
// memory-mode vaults without Raft. Reason "vault-migrate-source-expire"
// lands in pendingDeletes audit.
func (o *Orchestrator) deleteSourceChunk(srcID glid.GLID, chunkID chunk.ChunkID) error {
	vaultInst, err := o.findInstanceForChunk(srcID, chunkID)
	if err != nil {
		return err
	}
	if vaultInst.Reconciler != nil {
		return vaultInst.Reconciler.deleteChunk(chunkID, "vault-migrate-source-expire", o.placementMembership(vaultInst))
	}
	if vaultInst.Indexes != nil {
		if err := vaultInst.Indexes.DeleteIndexes(chunkID); err != nil {
			o.chunkMoveLogger.Warn("retention migrate: failed to delete source indexes",
				"chunk", chunkID.String(), "error", err)
		}
	}
	if err := vaultInst.Chunks.Delete(chunkID); err != nil {
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
			o.chunkMoveLogger.Error("failed to restore chunk after move error",
				"chunk", chunkID.String(), "error", adoptErr)
		}
		return fmt.Errorf("move chunk %s: %w", chunkID, err)
	}

	if _, err := dstMover.Adopt(chunkID); err != nil {
		return fmt.Errorf("adopt chunk %s in destination: %w", chunkID, err)
	}

	if dstIM != nil {
		if err := dstIM.BuildIndexes(ctx, chunkID); err != nil {
			o.chunkMoveLogger.Warn("retention migrate: failed to build indexes for moved chunk",
				"chunk", chunkID.String(), "error", err)
		}
	}
	return nil
}
