package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/index"

	"github.com/google/uuid"
)

// --- Single-chunk move ---

// MoveChunk moves a single sealed chunk from source to destination vault.
// Used by retention-triggered migration to move individual chunks.
// Supports both filesystem-level moves (ChunkMover) and record-level copy.
func (o *Orchestrator) MoveChunk(ctx context.Context, chunkID chunk.ChunkID, srcID, dstID uuid.UUID) error {
	srcCM, err := o.chunkManager(srcID)
	if err != nil {
		return err
	}
	dstCM, dstIM, err := o.vaultManagers(dstID)
	if err != nil {
		return err
	}

	// Try filesystem move first.
	srcMover, srcOk := srcCM.(chunk.ChunkMover)
	dstMover, dstOk := dstCM.(chunk.ChunkMover)
	if srcOk && dstOk {
		return o.moveChunkFS(ctx, chunkID, srcMover, dstMover, dstIM)
	}

	// Fallback: copy records for the single chunk.
	cursor, err := srcCM.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open chunk %s: %w", chunkID, err)
	}
	defer func() { _ = cursor.Close() }()

	for {
		rec, _, readErr := cursor.Next()
		if errors.Is(readErr, chunk.ErrNoMoreRecords) {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read chunk %s: %w", chunkID, readErr)
		}
		rec = rec.Copy()
		if _, _, appendErr := dstCM.AppendPreserved(rec); appendErr != nil {
			return fmt.Errorf("append record: %w", appendErr)
		}
	}

	// Delete from source after successful copy.
	if err := o.vaults[srcID].Indexes.DeleteIndexes(chunkID); err != nil {
		o.logger.Warn("retention migrate: failed to delete source indexes",
			"chunk", chunkID.String(), "error", err)
	}
	if err := srcCM.Delete(chunkID); err != nil {
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

// --- Multi-vault operations ---

// CopyRecords copies all records from source to destination, reporting progress via job.
// After copying, it seals the destination's active chunk and builds indexes.
func (o *Orchestrator) CopyRecords(ctx context.Context, srcID, dstID uuid.UUID, job *JobProgress) error {
	srcCM, err := o.chunkManager(srcID)
	if err != nil {
		return err
	}
	dstCM, dstIM, err := o.vaultManagers(dstID)
	if err != nil {
		return err
	}

	metas, err := srcCM.List()
	if err != nil {
		return err
	}

	job.SetRunning(int64(len(metas)))

	for _, meta := range metas {
		cursor, err := srcCM.OpenCursor(meta.ID)
		if err != nil {
			return fmt.Errorf("open chunk %s: %w", meta.ID, err)
		}

		for {
			rec, _, readErr := cursor.Next()
			if errors.Is(readErr, chunk.ErrNoMoreRecords) {
				break
			}
			if readErr != nil {
				_ = cursor.Close()
				return fmt.Errorf("read chunk %s: %w", meta.ID, readErr)
			}

			rec = rec.Copy()
			if _, _, appendErr := dstCM.AppendPreserved(rec); appendErr != nil {
				_ = cursor.Close()
				return fmt.Errorf("append record: %w", appendErr)
			}
			job.AddRecords(1)
		}
		_ = cursor.Close()
		job.IncrChunks()
	}

	// Seal the active chunk if it has data, so we can build indexes.
	if active := dstCM.Active(); active != nil && active.RecordCount > 0 {
		if err := dstCM.Seal(); err != nil {
			return fmt.Errorf("seal final chunk: %w", err)
		}
	}

	// Build indexes for all sealed chunks.
	dstMetas, err := dstCM.List()
	if err != nil {
		return err
	}
	for _, meta := range dstMetas {
		if meta.Sealed && dstIM != nil {
			if err := dstIM.BuildIndexes(ctx, meta.ID); err != nil {
				return fmt.Errorf("build indexes for chunk %s: %w", meta.ID, err)
			}
		}
	}

	return nil
}

// MoveChunks moves sealed chunks from source to destination using filesystem-level moves.
// Both vaults must implement chunk.ChunkMover (caller should verify with SupportsChunkMove).
func (o *Orchestrator) MoveChunks(ctx context.Context, srcID, dstID uuid.UUID, job *JobProgress) error {
	srcCM, err := o.chunkManager(srcID)
	if err != nil {
		return err
	}
	dstCM, dstIM, err := o.vaultManagers(dstID)
	if err != nil {
		return err
	}

	srcMover := srcCM.(chunk.ChunkMover)
	dstMover := dstCM.(chunk.ChunkMover)

	metas, err := srcCM.List()
	if err != nil {
		return err
	}

	job.SetRunning(int64(len(metas)))

	for _, meta := range metas {
		if !meta.Sealed {
			continue
		}

		srcDir := srcMover.ChunkDir(meta.ID)
		dstDir := dstMover.ChunkDir(meta.ID)

		// Untrack from source.
		if err := srcMover.Disown(meta.ID); err != nil {
			return fmt.Errorf("disown chunk %s: %w", meta.ID, err)
		}

		// Move directory.
		if err := chunkfile.MoveDir(srcDir, dstDir); err != nil {
			// Attempt to restore source tracking on failure.
			if _, adoptErr := srcMover.Adopt(meta.ID); adoptErr != nil {
				job.AddErrorDetail(fmt.Sprintf("failed to restore chunk %s after move error: %v", meta.ID, adoptErr))
			}
			return fmt.Errorf("move chunk %s: %w", meta.ID, err)
		}

		// Register in destination.
		if _, err := dstMover.Adopt(meta.ID); err != nil {
			return fmt.Errorf("adopt chunk %s in destination: %w", meta.ID, err)
		}

		// Build indexes for the moved chunk in the destination.
		if dstIM != nil {
			if err := dstIM.BuildIndexes(ctx, meta.ID); err != nil {
				job.AddErrorDetail(fmt.Sprintf("build indexes for chunk %s: %v", meta.ID, err))
			}
		}

		job.AddRecords(meta.RecordCount)
		job.IncrChunks()
	}

	return nil
}

// TransferParams describes a vault-to-vault data movement operation.
type TransferParams struct {
	SrcID uuid.UUID
	DstID uuid.UUID

	// Description is a human-readable label for the job (shown in the UI).
	Description string

	// CleanupSrc is called after the source vault is removed from the orchestrator.
	// It handles config vault deletion and filesystem cleanup.
	// A nil CleanupSrc means no external cleanup is needed.
	CleanupSrc func(ctx context.Context) error
}

// MigrateVault freezes the source (disable + seal), moves data to destination,
// then removes the source. The operation runs as an async job.
// The source must already be disabled and sealed by the caller.
func (o *Orchestrator) MigrateVault(ctx context.Context, p TransferParams) string {
	canMoveChunks := o.SupportsChunkMove(p.SrcID, p.DstID)

	jobName := "migrate:" + p.SrcID.String() + "->" + p.DstID.String()
	jobID := o.scheduler.Submit(jobName, func(ctx context.Context, job *JobProgress) {
		var mergeErr error
		if canMoveChunks {
			mergeErr = o.MoveChunks(ctx, p.SrcID, p.DstID, job)
		} else {
			mergeErr = o.CopyRecords(ctx, p.SrcID, p.DstID, job)
		}
		if mergeErr != nil {
			job.Fail(o.now(), fmt.Sprintf("merge records: %v", mergeErr))
			return
		}

		if err := o.ForceRemoveVault(p.SrcID); err != nil {
			job.Fail(o.now(), fmt.Sprintf("delete source: %v", err))
			return
		}

		if p.CleanupSrc != nil {
			if err := p.CleanupSrc(ctx); err != nil {
				o.logger.Warn("cleanup: source cleanup failed", "src", p.SrcID, "error", err)
			}
		}
	})
	if p.Description != "" {
		o.scheduler.Describe(jobName, p.Description)
	}

	return jobID
}

// MergeVaults seals the source, moves data to destination, then removes the
// source. The operation runs as an async job. The source must already be
// disabled by the caller.
func (o *Orchestrator) MergeVaults(ctx context.Context, p TransferParams) string {
	canMoveChunks := o.SupportsChunkMove(p.SrcID, p.DstID)

	jobName := "merge:" + p.SrcID.String() + "->" + p.DstID.String()
	jobID := o.scheduler.Submit(jobName, func(ctx context.Context, job *JobProgress) {
		if err := o.SealActive(p.SrcID); err != nil {
			job.Fail(o.now(), fmt.Sprintf("seal source: %v", err))
			return
		}

		var err error
		if canMoveChunks {
			err = o.MoveChunks(ctx, p.SrcID, p.DstID, job)
		} else {
			err = o.CopyRecords(ctx, p.SrcID, p.DstID, job)
		}
		if err != nil {
			job.Fail(o.now(), fmt.Sprintf("merge records: %v", err))
			return
		}

		if err := o.ForceRemoveVault(p.SrcID); err != nil {
			job.Fail(o.now(), fmt.Sprintf("delete source: %v", err))
			return
		}

		if p.CleanupSrc != nil {
			if err := p.CleanupSrc(ctx); err != nil {
				o.logger.Warn("cleanup: source cleanup failed", "src", p.SrcID, "error", err)
			}
		}
	})
	if p.Description != "" {
		o.scheduler.Describe(jobName, p.Description)
	}

	return jobID
}
