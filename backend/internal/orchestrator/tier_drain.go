package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"

	"gastrolog/internal/chunk"
	"gastrolog/internal/system"
)

// TierDrainMode determines where chunks go during a tier drain.
type TierDrainMode int

const (
	// TierDrainDecommission transitions chunks to the next tier in the vault chain.
	TierDrainDecommission TierDrainMode = iota
	// TierDrainRebalance replicates chunks to the same tier on a different node.
	TierDrainRebalance
)

// tierDrainState tracks an in-progress tier drain.
type tierDrainState struct {
	VaultID      glid.GLID
	TierID       glid.GLID
	Mode         TierDrainMode
	TargetNodeID string // only for rebalance mode
	JobID        string
	Cancel       context.CancelFunc
}

// ErrTierDraining is returned when an operation targets a tier that is mid-drain.
var ErrTierDraining = errors.New("tier is draining")

// tierDrainKey returns the map key for the tierDraining map.
func tierDrainKey(vaultID, tierID glid.GLID) string {
	return vaultID.String() + ":" + tierID.String()
}

// DrainTier starts an async drain of a tier's chunks. In decommission mode,
// chunks transition to the next tier in the vault chain. In rebalance mode,
// chunks replicate to the same tier on the target node.
func (o *Orchestrator) DrainTier(ctx context.Context, vaultID, tierID glid.GLID, mode TierDrainMode, targetNodeID string) error {
	sys, err := o.loadSystem(ctx)
	if err != nil {
		return fmt.Errorf("load config for tier drain: %w", err)
	}

	o.mu.Lock()
	vault := o.vaults[vaultID]
	if vault == nil {
		o.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}

	key := tierDrainKey(vaultID, tierID)
	if _, already := o.tierDraining[key]; already {
		o.mu.Unlock()
		return fmt.Errorf("tier %s in vault %s is already draining", tierID, vaultID)
	}

	// Find the tier instance.
	var tier *TierInstance
	for _, t := range vault.Tiers {
		if t.TierID == tierID {
			tier = t
			break
		}
	}
	if tier == nil {
		o.mu.Unlock()
		return fmt.Errorf("tier %s not found in vault %s", tierID, vaultID)
	}

	// Validate mode-specific requirements.
	switch mode {
	case TierDrainDecommission:
		_, _, err := resolveNextTierInChain(&sys.Config, vaultID, tierID)
		if err != nil {
			o.mu.Unlock()
			// Terminal tier — just expire all chunks instead of transitioning.
			// That's fine, we'll handle it in the worker.
		}
	case TierDrainRebalance:
		if targetNodeID == "" {
			o.mu.Unlock()
			return errors.New("target node required for rebalance drain")
		}
		if o.transferrer == nil {
			o.mu.Unlock()
			return errors.New("no remote transferrer configured (single-node mode)")
		}
	}

	// Mark as draining.
	drainCtx, cancel := context.WithCancel(context.Background())
	ds := &tierDrainState{
		VaultID:      vaultID,
		TierID:       tierID,
		Mode:         mode,
		TargetNodeID: targetNodeID,
		Cancel:       cancel,
	}
	o.tierDraining[key] = ds

	// Remove retention/rotation jobs for this tier so they don't interfere.
	delete(o.retention, retentionKey(tier.TierID, tier.StorageID))

	// Seal the active chunk.
	cm := tier.Chunks
	o.mu.Unlock()

	if active := cm.Active(); active != nil {
		if err := cm.Seal(); err != nil {
			o.logger.Warn("tier drain: failed to seal active chunk",
				"vault", vaultID, "tier", tierID, "error", err)
		}
	}

	// Submit async drain job.
	jobName := fmt.Sprintf("drain-tier:%s:%s", vaultID, tierID)
	jobID := o.scheduler.Submit(jobName, func(ctx2 context.Context, job *JobProgress) {
		o.tierDrainWorker(drainCtx, vaultID, tierID, mode, targetNodeID)
	})
	o.scheduler.Describe(jobName, fmt.Sprintf("Drain tier %s from vault", tierID))

	o.mu.Lock()
	if d, ok := o.tierDraining[key]; ok {
		d.JobID = jobID
	}
	o.mu.Unlock()

	o.logger.Info("tier drain started",
		"vault", vaultID, "tier", tierID,
		"mode", drainModeName(mode), "target", targetNodeID)
	return nil
}

// tierDrainWorker is the async job that transfers all chunks and cleans up.
func (o *Orchestrator) tierDrainWorker(ctx context.Context, vaultID, tierID glid.GLID, mode TierDrainMode, targetNodeID string) {
	// Always clean up drain state on exit — leaked state keeps Raft groups alive.
	// But only notify completion (vault config update) on success.
	success := false
	defer func() {
		if success {
			o.finishTierDrain(vaultID, tierID)
		} else {
			o.cancelTierDrainState(vaultID, tierID)
		}
	}()

	sys, err := o.loadSystem(ctx)
	if err != nil {
		o.logger.Error("tier drain: failed to load config", "vault", vaultID, "tier", tierID, "error", err)
		return
	}

	o.mu.RLock()
	vault := o.vaults[vaultID]
	if vault == nil {
		o.mu.RUnlock()
		return
	}
	var tier *TierInstance
	for _, t := range vault.Tiers {
		if t.TierID == tierID {
			tier = t
			break
		}
	}
	o.mu.RUnlock()

	if tier == nil {
		return
	}

	// Transfer all sealed chunks.
	if !o.drainTierChunks(ctx, sys, vaultID, tierID, tier, mode, targetNodeID) {
		return // context cancelled or error — defer handles cleanup
	}

	// Final seal to catch any stragglers.
	if active := tier.Chunks.Active(); active != nil {
		if err := tier.Chunks.Seal(); err != nil {
			o.logger.Warn("tier drain: final seal failed", "vault", vaultID, "tier", tierID, "error", err)
		}
		o.drainTierChunks(ctx, sys, vaultID, tierID, tier, mode, targetNodeID)
	}

	success = true
}

// drainTierChunks transfers all sealed chunks from the tier. Returns false if cancelled.
func (o *Orchestrator) drainTierChunks(ctx context.Context, sys *system.System, vaultID, tierID glid.GLID, tier *TierInstance, mode TierDrainMode, targetNodeID string) bool {
	metas, err := tier.Chunks.List()
	if err != nil {
		o.logger.Error("tier drain: list chunks failed", "vault", vaultID, "tier", tierID, "error", err)
		return false
	}

	for _, meta := range metas {
		if !meta.Sealed {
			continue
		}
		select {
		case <-ctx.Done():
			return false
		default:
		}

		if err := o.drainOneChunk(ctx, sys, vaultID, tierID, tier, meta.ID, mode, targetNodeID); err != nil {
			o.logger.Error("tier drain: chunk transfer failed",
				"vault", vaultID, "tier", tierID, "chunk", meta.ID, "error", err)
			continue // best effort — try the rest
		}
	}
	return true
}

// drainCursorToRecords consumes all records from a cursor into a slice.
// Used to convert a chunk cursor to the record slice expected by
// TierReplicator.ImportSealedChunk.
func drainCursorToRecords(cursor chunk.RecordCursor) ([]chunk.Record, error) {
	var records []chunk.Record
	for {
		rec, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			return records, nil
		}
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
}

// drainOneChunk transfers a single chunk and deletes the source.
func (o *Orchestrator) drainOneChunk(ctx context.Context, sys *system.System, vaultID, tierID glid.GLID, tier *TierInstance, chunkID chunk.ChunkID, mode TierDrainMode, targetNodeID string) error {
	cursor, err := tier.Chunks.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	defer func() { _ = cursor.Close() }()

	switch mode {
	case TierDrainDecommission:
		if err := o.drainChunkToNextTier(ctx, sys, vaultID, tierID, cursor); err != nil {
			return err
		}

	case TierDrainRebalance:
		if o.tierReplicator == nil {
			return errors.New("tier drain rebalance: tier replicator not configured")
		}
		records, err := drainCursorToRecords(cursor)
		if err != nil {
			return fmt.Errorf("read chunk for rebalance: %w", err)
		}
		if err := o.tierReplicator.ImportSealedChunk(ctx, targetNodeID, vaultID, tierID, chunkID, records); err != nil {
			return fmt.Errorf("replicate to target node: %w", err)
		}
	}

	// Delete source chunk.
	if tier.Indexes != nil {
		if err := tier.Indexes.DeleteIndexes(chunkID); err != nil {
			o.logger.Warn("tier drain: delete source indexes failed", "vault", vaultID, "tier", tierID, "chunk", chunkID, "error", err)
		}
	}
	if err := tier.Chunks.Delete(chunkID); err != nil {
		return fmt.Errorf("delete source chunk: %w", err)
	}

	o.logger.Info("tier drain: chunk transferred",
		"vault", vaultID, "tier", tierID, "chunk", chunkID, "mode", drainModeName(mode))
	return nil
}

// finishTierDrain cleans up after a completed or cancelled tier drain.
func (o *Orchestrator) finishTierDrain(vaultID, tierID glid.GLID) {
	key := tierDrainKey(vaultID, tierID)

	o.mu.Lock()
	ds, ok := o.tierDraining[key]
	if ok {
		delete(o.tierDraining, key)
		if ds.Cancel != nil {
			ds.Cancel()
		}
	}
	o.mu.Unlock()

	// Remove the tier instance (closes managers, deletes remaining data).
	// Drain has already migrated chunks to the target; the destructive wipe
	// on the source tier is the correct semantics here.
	if o.DeleteTierFromVault(vaultID, tierID) {
		o.logger.Info("tier drain: completed",
			"vault", vaultID, "tier", tierID)
	}

	// Notify the dispatch layer to remove the tier from the vault's tier
	// list in system. This fires a vault-put through Raft, causing all
	// nodes to rebuild the vault without the drained tier.
	if o.OnTierDrainComplete != nil {
		o.OnTierDrainComplete(context.Background(), vaultID, tierID)
	}
}

// cancelTierDrainState removes drain state without triggering vault config
// updates or Raft group destruction. Used when the drain worker exits early
// (error, vault already gone, etc.) to prevent leaked drain state.
func (o *Orchestrator) cancelTierDrainState(vaultID, tierID glid.GLID) {
	key := tierDrainKey(vaultID, tierID)

	o.mu.Lock()
	ds, ok := o.tierDraining[key]
	if ok {
		delete(o.tierDraining, key)
		if ds.Cancel != nil {
			ds.Cancel()
		}
	}
	o.mu.Unlock()

	if ok {
		o.logger.Info("tier drain: state cleaned up (drain did not complete)",
			"vault", vaultID, "tier", tierID)
	}
}

// CancelTierDrain aborts an in-progress tier drain. The tier remains in the
// vault with whatever chunks haven't been transferred yet.
func (o *Orchestrator) CancelTierDrain(vaultID, tierID glid.GLID) error {
	key := tierDrainKey(vaultID, tierID)

	o.mu.Lock()
	defer o.mu.Unlock()

	ds, ok := o.tierDraining[key]
	if !ok {
		return fmt.Errorf("tier %s in vault %s is not draining", tierID, vaultID)
	}

	ds.Cancel()
	delete(o.tierDraining, key)
	o.scheduler.RemoveJob(ds.JobID)

	o.logger.Info("tier drain: cancelled", "vault", vaultID, "tier", tierID)
	return nil
}

// IsTierDraining returns true if the given tier is currently draining.
func (o *Orchestrator) IsTierDraining(vaultID, tierID glid.GLID) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	_, ok := o.tierDraining[tierDrainKey(vaultID, tierID)]
	return ok
}

// drainChunkToNextTier streams a chunk to the next tier in the vault chain.
// If the tier is terminal, returns nil (chunk will just be deleted).
func (o *Orchestrator) drainChunkToNextTier(ctx context.Context, sys *system.System, vaultID, tierID glid.GLID, cursor chunk.RecordCursor) error {
	nextTierID, nextTierCfg, _ := resolveNextTierInChain(&sys.Config, vaultID, tierID)
	if nextTierCfg == nil {
		return nil // terminal tier — caller deletes the chunk
	}
	nextLeader := system.LeaderNodeID(sys.Runtime.TierPlacements[nextTierCfg.ID], sys.Runtime.NodeStorageConfigs)
	remote := nextLeader != "" && nextLeader != o.localNodeID

	if remote {
		if o.transferrer == nil {
			return errors.New("no transferrer for remote transition")
		}
		return o.transferrer.StreamToTier(ctx, nextLeader, vaultID, nextTierID, chunk.CursorIterator(cursor))
	}

	iter := chunk.CursorIterator(cursor)
	for {
		rec, iterErr := iter()
		if errors.Is(iterErr, chunk.ErrNoMoreRecords) {
			return nil
		}
		if iterErr != nil {
			return fmt.Errorf("read chunk: %w", iterErr)
		}
		if err := o.AppendToTier(vaultID, nextTierID, chunk.ChunkID{}, rec); err != nil {
			return fmt.Errorf("append to next tier: %w", err)
		}
	}
}

func drainModeName(m TierDrainMode) string {
	switch m {
	case TierDrainDecommission:
		return "decommission"
	case TierDrainRebalance:
		return "rebalance"
	default:
		return "unknown"
	}
}
