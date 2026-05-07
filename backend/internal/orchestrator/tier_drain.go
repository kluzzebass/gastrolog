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
//
// Role: **tier leader only**. The drain walks the tier's chunks and applies
// CmdDeleteChunk / CmdTransitionStreamed to the vault control-plane Raft,
// which only the leader may write to. Callers must check `tier.IsLeader()`
// before invoking — callers in dispatch do so explicitly.
//
// Readiness: no explicit Vault.ReadinessErr gate. Drain is itself a
// readiness-affecting state change, so it runs as soon as the tier instance
// is present. Individual operations inside the drain use the standard tier
// FSM gates.
func (o *Orchestrator) DrainTier(ctx context.Context, vaultID, tierID glid.GLID, mode TierDrainMode, targetNodeID string) error {
	if _, err := o.loadSystem(ctx); err != nil {
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
	var tier *VaultInstance
	if vault.Instance != nil && vault.Instance.TierID == tierID {
		tier = vault.Instance
	}
	if tier == nil {
		o.mu.Unlock()
		return fmt.Errorf("tier %s not found in vault %s", tierID, vaultID)
	}

	// Validate mode-specific requirements.
	switch mode {
	case TierDrainDecommission:
		// Phase 4 (gastrolog-42f9z): the multi-tier chain is gone (Phase 2
		// collapsed it) and the transition concept is gone with it.
		// Decommission drain just fires retention events on every chunk —
		// the routing engine + retention path produce the same observable
		// behavior the old "transition to next tier" produced.
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
	var tier *VaultInstance
	if vault.Instance != nil && vault.Instance.TierID == tierID {
		tier = vault.Instance
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
func (o *Orchestrator) drainTierChunks(ctx context.Context, sys *system.System, vaultID, tierID glid.GLID, tier *VaultInstance, mode TierDrainMode, targetNodeID string) bool {
	metas, err := tier.Chunks.List()
	if err != nil {
		o.logger.Error("tier drain: list chunks failed", "vault", vaultID, "tier", tierID, "error", err)
		return false
	}

	for _, meta := range metas {
		// Phase 3 (gastrolog-1huz5): overlay through FSM so Sealing
		// chunks (active-form sealed locally but GLCB not yet committed)
		// are skipped. Drain ships sealed-form GLCBs; a Sealing chunk
		// would race with concurrent PostSealProcess.
		if tier.OverlayFromFSM != nil {
			meta = tier.OverlayFromFSM(meta)
		}
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
// ChunkReplicator.ImportSealedChunk.
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
func (o *Orchestrator) drainOneChunk(ctx context.Context, sys *system.System, vaultID, tierID glid.GLID, tier *VaultInstance, chunkID chunk.ChunkID, mode TierDrainMode, targetNodeID string) error {
	cursor, err := tier.Chunks.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	// Close-on-return is the safety net; we Close explicitly post-stream
	// before deleteDrainSource so the cursor's per-chunk RLock
	// (gastrolog-26zu1) is released before Delete tries to take the
	// write lock — otherwise the same goroutine self-deadlocks on
	// RLock→Lock upgrade.
	cursorClosed := false
	defer func() {
		if !cursorClosed {
			_ = cursor.Close()
		}
	}()

	switch mode {
	case TierDrainDecommission:
		// Phase 4 (gastrolog-42f9z): no "next tier" anymore. Decommission
		// just destroys the chunks — equivalent to firing a retention
		// event with no matching retention-trigger routes (the legacy
		// expire behavior). Phase 5's richer routing table will let
		// operators provide a destination route to receive drained
		// chunks if needed.
		_ = cursor // caller closes
		_ = sys

	case TierDrainRebalance:
		if o.chunkReplicator == nil {
			return errors.New("tier drain rebalance: tier replicator not configured")
		}
		records, err := drainCursorToRecords(cursor)
		if err != nil {
			return fmt.Errorf("read chunk for rebalance: %w", err)
		}
		if err := o.chunkReplicator.ImportSealedChunk(ctx, targetNodeID, vaultID, tierID, chunkID, records); err != nil {
			return fmt.Errorf("replicate to target node: %w", err)
		}
	}

	// Release the cursor's read lock before deleteDrainSource tries to
	// take the write lock on the same chunk.
	_ = cursor.Close()
	cursorClosed = true

	// Delete source chunk via the receipt protocol when wired (production)
	// or via direct local cleanup otherwise (memory-mode tiers without a
	// reconciler). Reason "tier-drain" lands in pendingDeletes audit. See
	// gastrolog-51gme.
	if err := o.deleteDrainSource(tier, vaultID, tierID, chunkID); err != nil {
		return err
	}

	o.logger.Info("tier drain: chunk transferred",
		"vault", vaultID, "tier", tierID, "chunk", chunkID, "mode", drainModeName(mode))
	return nil
}

// deleteDrainSource removes a successfully-drained source chunk. Routes
// through the receipt protocol when a reconciler is wired; falls back to
// the direct local delete for memory-mode tiers. Extracted from
// drainOneChunk to keep nestif within lint thresholds.
func (o *Orchestrator) deleteDrainSource(tier *VaultInstance, vaultID, tierID glid.GLID, chunkID chunk.ChunkID) error {
	if tier.Reconciler != nil {
		if err := tier.Reconciler.deleteChunk(chunkID, "tier-drain", o.placementMembership(tier)); err != nil {
			return fmt.Errorf("delete source chunk: %w", err)
		}
		return nil
	}
	if tier.Indexes != nil {
		if err := tier.Indexes.DeleteIndexes(chunkID); err != nil {
			o.logger.Warn("tier drain: delete source indexes failed",
				"vault", vaultID, "tier", tierID, "chunk", chunkID, "error", err)
		}
	}
	if err := tier.Chunks.Delete(chunkID); err != nil {
		return fmt.Errorf("delete source chunk: %w", err)
	}
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
