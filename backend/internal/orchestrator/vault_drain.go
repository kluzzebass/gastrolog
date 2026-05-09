package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"

	"gastrolog/internal/chunk"
	"gastrolog/internal/system"
)

// DrainMode determines where chunks go during a inst drain.
type DrainMode int

const (
	// DrainDecommission transitions chunks to the next inst in the vault chain.
	DrainDecommission DrainMode = iota
	// DrainRebalance replicates chunks to the same inst on a different node.
	DrainRebalance
)

// instDrainState tracks an in-progress inst drain.
type instDrainState struct {
	VaultID      glid.GLID
	InstanceID       glid.GLID
	Mode         DrainMode
	TargetNodeID string // only for rebalance mode
	JobID        string
	Cancel       context.CancelFunc
}

// ErrInstDraining is returned when an operation targets a inst that is mid-drain.
var ErrInstDraining = errors.New("inst is draining")

// instDrainKey returns the map key for the instDraining map.
func instDrainKey(vaultID glid.GLID) string {
	return vaultID.String() + ":" + vaultID.String()
}

// DrainInstance starts an async drain of a inst's chunks. In decommission mode,
// chunks transition to the next inst in the vault chain. In rebalance mode,
// chunks replicate to the same inst on the target node.
//
// Role: **vault leader only**. The drain walks the vault's chunks and applies
// CmdDeleteChunk / CmdTransitionStreamed to the vault control-plane Raft,
// which only the leader may write to. Callers must check `inst.IsLeader()`
// before invoking — callers in dispatch do so explicitly.
//
// Readiness: no explicit Vault.ReadinessErr gate. Drain is itself a
// readiness-affecting state change, so it runs as soon as the inst instance
// is present. Individual operations inside the drain use the standard inst
// FSM gates.
func (o *Orchestrator) DrainInstance(ctx context.Context, vaultID glid.GLID, mode DrainMode, targetNodeID string) error {
	if _, err := o.loadSystem(ctx); err != nil {
		return fmt.Errorf("load config for inst drain: %w", err)
	}

	o.mu.Lock()
	vault := o.vaults[vaultID]
	if vault == nil {
		o.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}

	key := instDrainKey(vaultID)
	if _, already := o.instDraining[key]; already {
		o.mu.Unlock()
		return fmt.Errorf("vault %s in vault %s is already draining", vaultID, vaultID)
	}

	// Find the inst instance.
	var inst *VaultInstance
	if vault.Instance != nil && vault.Instance.VaultID == vaultID {
		inst = vault.Instance
	}
	if inst == nil {
		o.mu.Unlock()
		return fmt.Errorf("vault %s not found in vault %s", vaultID, vaultID)
	}

	// Validate mode-specific requirements.
	switch mode {
	case DrainDecommission:
		// Phase 4 (gastrolog-42f9z): the multi-transition chain is gone (Phase 2
		// collapsed it) and the transition concept is gone with it.
		// Decommission drain just fires retention events on every chunk —
		// the routing engine + retention path produce the same observable
		// behavior the old "transition to next inst" produced.
	case DrainRebalance:
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
	ds := &instDrainState{
		VaultID:      vaultID,
		InstanceID:       vaultID,
		Mode:         mode,
		TargetNodeID: targetNodeID,
		Cancel:       cancel,
	}
	o.instDraining[key] = ds

	// Remove retention/rotation jobs for this inst so they don't interfere.
	delete(o.retention, retentionKey(inst.VaultID, inst.StorageID))

	// Seal the active chunk.
	cm := inst.Chunks
	o.mu.Unlock()

	if active := cm.Active(); active != nil {
		if err := cm.Seal(); err != nil {
			o.logger.Warn("inst drain: failed to seal active chunk",
				"vault", vaultID, "error", err)
		}
	}

	// Submit async drain job.
	jobName := fmt.Sprintf("drain-inst:%s:%s", vaultID, vaultID)
	jobID := o.scheduler.Submit(jobName, func(ctx2 context.Context, job *JobProgress) {
		o.instDrainWorker(drainCtx, vaultID, mode, targetNodeID)
	})
	o.scheduler.Describe(jobName, fmt.Sprintf("Drain vault %s from vault", vaultID))

	o.mu.Lock()
	if d, ok := o.instDraining[key]; ok {
		d.JobID = jobID
	}
	o.mu.Unlock()

	o.logger.Info("inst drain started",
		"vault", vaultID,
		"mode", drainModeName(mode), "target", targetNodeID)
	return nil
}

// instDrainWorker is the async job that transfers all chunks and cleans up.
func (o *Orchestrator) instDrainWorker(ctx context.Context, vaultID glid.GLID, mode DrainMode, targetNodeID string) {
	// Always clean up drain state on exit — leaked state keeps Raft groups alive.
	// But only notify completion (vault config update) on success.
	success := false
	defer func() {
		if success {
			o.finishInstDrain(vaultID)
		} else {
			o.cancelInstDrainState(vaultID)
		}
	}()

	sys, err := o.loadSystem(ctx)
	if err != nil {
		o.logger.Error("inst drain: failed to load config", "vault", vaultID, "error", err)
		return
	}

	o.mu.RLock()
	vault := o.vaults[vaultID]
	if vault == nil {
		o.mu.RUnlock()
		return
	}
	var inst *VaultInstance
	if vault.Instance != nil && vault.Instance.VaultID == vaultID {
		inst = vault.Instance
	}
	o.mu.RUnlock()

	if inst == nil {
		return
	}

	// Transfer all sealed chunks.
	if !o.drainInstChunks(ctx, sys, vaultID, inst, mode, targetNodeID) {
		return // context cancelled or error — defer handles cleanup
	}

	// Final seal to catch any stragglers.
	if active := inst.Chunks.Active(); active != nil {
		if err := inst.Chunks.Seal(); err != nil {
			o.logger.Warn("inst drain: final seal failed", "vault", vaultID, "error", err)
		}
		o.drainInstChunks(ctx, sys, vaultID, inst, mode, targetNodeID)
	}

	success = true
}

// drainInstChunks transfers all sealed chunks from the inst. Returns false if cancelled.
func (o *Orchestrator) drainInstChunks(ctx context.Context, sys *system.System, vaultID glid.GLID, inst *VaultInstance, mode DrainMode, targetNodeID string) bool {
	metas, err := inst.Chunks.List()
	if err != nil {
		o.logger.Error("inst drain: list chunks failed", "vault", vaultID, "error", err)
		return false
	}

	for _, meta := range metas {
		// Phase 3 (gastrolog-1huz5): overlay through FSM so Sealing
		// chunks (active-form sealed locally but GLCB not yet committed)
		// are skipped. Drain ships sealed-form GLCBs; a Sealing chunk
		// would race with concurrent PostSealProcess.
		if inst.OverlayFromFSM != nil {
			meta = inst.OverlayFromFSM(meta)
		}
		if !meta.Sealed {
			continue
		}
		select {
		case <-ctx.Done():
			return false
		default:
		}

		if err := o.drainOneChunk(ctx, sys, vaultID, inst, meta.ID, mode, targetNodeID); err != nil {
			o.logger.Error("inst drain: chunk transfer failed",
				"vault", vaultID, "chunk", meta.ID, "error", err)
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
func (o *Orchestrator) drainOneChunk(ctx context.Context, sys *system.System, vaultID glid.GLID, inst *VaultInstance, chunkID chunk.ChunkID, mode DrainMode, targetNodeID string) error {
	cursor, err := inst.Chunks.OpenCursor(chunkID)
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
	case DrainDecommission:
		// Phase 4 (gastrolog-42f9z): no "next inst" anymore. Decommission
		// just destroys the chunks — equivalent to firing a retention
		// event with no matching retention-trigger routes (the legacy
		// expire behavior). Phase 5's richer routing table will let
		// operators provide a destination route to receive drained
		// chunks if needed.
		_ = cursor // caller closes
		_ = sys

	case DrainRebalance:
		if o.chunkReplicator == nil {
			return errors.New("inst drain rebalance: inst replicator not configured")
		}
		records, err := drainCursorToRecords(cursor)
		if err != nil {
			return fmt.Errorf("read chunk for rebalance: %w", err)
		}
		if err := o.chunkReplicator.ImportSealedChunk(ctx, targetNodeID, vaultID, chunkID, records); err != nil {
			return fmt.Errorf("replicate to target node: %w", err)
		}
	}

	// Release the cursor's read lock before deleteDrainSource tries to
	// take the write lock on the same chunk.
	_ = cursor.Close()
	cursorClosed = true

	// Delete source chunk via the receipt protocol when wired (production)
	// or via direct local cleanup otherwise (memory-mode tiers without a
	// reconciler). Reason "inst-drain" lands in pendingDeletes audit. See
	// gastrolog-51gme.
	if err := o.deleteDrainSource(inst, vaultID, chunkID); err != nil {
		return err
	}

	o.logger.Info("inst drain: chunk transferred",
		"vault", vaultID, "chunk", chunkID, "mode", drainModeName(mode))
	return nil
}

// deleteDrainSource removes a successfully-drained source chunk. Routes
// through the receipt protocol when a reconciler is wired; falls back to
// the direct local delete for memory-mode tiers. Extracted from
// drainOneChunk to keep nestif within lint thresholds.
func (o *Orchestrator) deleteDrainSource(inst *VaultInstance, vaultID glid.GLID, chunkID chunk.ChunkID) error {
	if inst.Reconciler != nil {
		if err := inst.Reconciler.deleteChunk(chunkID, "inst-drain", o.placementMembership(inst)); err != nil {
			return fmt.Errorf("delete source chunk: %w", err)
		}
		return nil
	}
	if inst.Indexes != nil {
		if err := inst.Indexes.DeleteIndexes(chunkID); err != nil {
			o.logger.Warn("inst drain: delete source indexes failed",
				"vault", vaultID, "chunk", chunkID, "error", err)
		}
	}
	if err := inst.Chunks.Delete(chunkID); err != nil {
		return fmt.Errorf("delete source chunk: %w", err)
	}
	return nil
}

// finishInstDrain cleans up after a completed or cancelled inst drain.
func (o *Orchestrator) finishInstDrain(vaultID glid.GLID) {
	key := instDrainKey(vaultID)

	o.mu.Lock()
	ds, ok := o.instDraining[key]
	if ok {
		delete(o.instDraining, key)
		if ds.Cancel != nil {
			ds.Cancel()
		}
	}
	o.mu.Unlock()

	// Remove the inst instance (closes managers, deletes remaining data).
	// Drain has already migrated chunks to the target; the destructive wipe
	// on the source inst is the correct semantics here.
	_ = vaultID
	if o.DeleteVaultInstance(vaultID) {
		o.logger.Info("inst drain: completed",
			"vault", vaultID)
	}

	// Notify the dispatch layer to remove the inst from the vault's inst
	// list in system. This fires a vault-put through Raft, causing all
	// nodes to rebuild the vault without the drained inst.
	if o.OnTierDrainComplete != nil {
		o.OnTierDrainComplete(context.Background(), vaultID, vaultID)
	}
}

// cancelInstDrainState removes drain state without triggering vault config
// updates or Raft group destruction. Used when the drain worker exits early
// (error, vault already gone, etc.) to prevent leaked drain state.
func (o *Orchestrator) cancelInstDrainState(vaultID glid.GLID) {
	key := instDrainKey(vaultID)

	o.mu.Lock()
	ds, ok := o.instDraining[key]
	if ok {
		delete(o.instDraining, key)
		if ds.Cancel != nil {
			ds.Cancel()
		}
	}
	o.mu.Unlock()

	if ok {
		o.logger.Info("inst drain: state cleaned up (drain did not complete)",
			"vault", vaultID)
	}
}

// CancelTierDrain aborts an in-progress inst drain. The inst remains in the
// vault with whatever chunks haven't been transferred yet.
func (o *Orchestrator) CancelTierDrain(vaultID glid.GLID) error {
	key := instDrainKey(vaultID)

	o.mu.Lock()
	defer o.mu.Unlock()

	ds, ok := o.instDraining[key]
	if !ok {
		return fmt.Errorf("vault %s in vault %s is not draining", vaultID, vaultID)
	}

	ds.Cancel()
	delete(o.instDraining, key)
	o.scheduler.RemoveJob(ds.JobID)

	o.logger.Info("inst drain: cancelled", "vault", vaultID)
	return nil
}

// IsTierDraining returns true if the given inst is currently draining.
func (o *Orchestrator) IsTierDraining(vaultID glid.GLID) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	_, ok := o.instDraining[instDrainKey(vaultID)]
	return ok
}

func drainModeName(m DrainMode) string {
	switch m {
	case DrainDecommission:
		return "decommission"
	case DrainRebalance:
		return "rebalance"
	default:
		return "unknown"
	}
}
