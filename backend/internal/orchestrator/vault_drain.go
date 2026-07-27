package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"

	"gastrolog/internal/chunk"
	"gastrolog/internal/system"
)

// DrainMode determines where chunks go during an instance drain.
type DrainMode int

const (
	// DrainDecommission transitions chunks to the next instance in the vault chain.
	DrainDecommission DrainMode = iota
	// DrainRebalance replicates chunks to the same instance on a different node.
	DrainRebalance
)

// vaultDrainState tracks an in-progress vault drain.
type vaultDrainState struct {
	VaultID      glid.GLID
	Mode         DrainMode
	TargetNodeID string // only for rebalance mode
	JobID        string
	Cancel       context.CancelFunc
}

// ErrVaultDraining is returned when an operation targets a vault that is mid-drain.
var ErrVaultDraining = errors.New("vault is draining")

// vaultDrainKey returns the map key for the vaultDraining map.
func vaultDrainKey(vaultID glid.GLID) string {
	return vaultID.String() + ":" + vaultID.String()
}

// DrainInstance starts an async drain of an instance's chunks. In decommission mode,
// chunks transition to the next instance in the vault chain. In rebalance mode,
// chunks replicate to the same instance on the target node.
//
// Role: **vault leader only**. The drain walks the vault's chunks and drives
// deletes/transitions through the vault control-plane Raft (the receipt
// protocol's CmdRequestDelete), which only the leader may write to. Callers
// must check `instance.IsLeader()` before invoking — callers in dispatch do
// so explicitly.
//
// Readiness: no explicit Vault.ReadinessErr gate. Drain is itself a
// readiness-affecting state change, so it runs as soon as the vault instance
// is present. Individual operations inside the drain use the standard instance
// FSM gates.
func (o *Orchestrator) DrainInstance(ctx context.Context, vaultID glid.GLID, mode DrainMode, targetNodeID string) error {
	if _, err := o.loadSystem(ctx); err != nil {
		return fmt.Errorf("load config for vault drain: %w", err)
	}

	o.mu.Lock()
	vault := o.vaults[vaultID]
	if vault == nil {
		o.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}

	key := vaultDrainKey(vaultID)
	if _, already := o.vaultDraining[key]; already {
		o.mu.Unlock()
		return fmt.Errorf("vault %s in vault %s is already draining", vaultID, vaultID)
	}

	// Find the vault instance.
	var vaultInst *VaultInstance
	if vault.Instance != nil && vault.Instance.VaultID == vaultID {
		vaultInst = vault.Instance
	}
	if vaultInst == nil {
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
		// behavior the old "transition to next instance" produced.
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
	ds := &vaultDrainState{
		VaultID:      vaultID,
		Mode:         mode,
		TargetNodeID: targetNodeID,
		Cancel:       cancel,
	}
	o.vaultDraining[key] = ds

	// Remove retention/rotation jobs for this instance so they don't interfere.
	delete(o.retention, retentionKey(vaultInst.VaultID, vaultInst.StorageID))

	// Seal the active chunk.
	cm := vaultInst.Chunks
	o.mu.Unlock()

	if active := cm.Active(); active != nil {
		if err := cm.Seal(); err != nil {
			o.drainLogger.Warn("vault drain: failed to seal active chunk",
				"vault", vaultID, "error", err)
		}
	}

	// Submit async drain job. Describe BEFORE submitting — see
	// scheduleReplication for why (missing label on the Scheduled event,
	// leaked descriptions entry when the job finishes first). gastrolog-69sjlj.
	jobName := fmt.Sprintf("drain-vault:%s:%s", vaultID, vaultID)
	o.scheduler.Describe(jobName, fmt.Sprintf("Drain vault %s from vault", vaultID))
	jobID := o.scheduler.Submit(jobName, func(ctx2 context.Context, job *JobProgress) {
		o.vaultDrainWorker(drainCtx, vaultID, mode, targetNodeID)
	})

	o.mu.Lock()
	if d, ok := o.vaultDraining[key]; ok {
		d.JobID = jobID
	}
	o.mu.Unlock()

	o.drainLogger.Info("vault drain started",
		"vault", vaultID,
		"mode", drainModeName(mode), "target", targetNodeID)
	return nil
}

// vaultDrainWorker is the async job that transfers all chunks and cleans up.
func (o *Orchestrator) vaultDrainWorker(ctx context.Context, vaultID glid.GLID, mode DrainMode, targetNodeID string) {
	// Always clean up drain state on exit — leaked state keeps Raft groups alive.
	// But only notify completion (vault config update) on success.
	success := false
	defer func() {
		if success {
			o.finishVaultDrain(vaultID)
		} else {
			o.cancelVaultDrainState(vaultID)
		}
	}()

	sys, err := o.loadSystem(ctx)
	if err != nil {
		o.drainLogger.Error("vault drain: failed to load config", "vault", vaultID, "error", err)
		return
	}

	o.mu.RLock()
	vault := o.vaults[vaultID]
	if vault == nil {
		o.mu.RUnlock()
		return
	}
	var vaultInst *VaultInstance
	if vault.Instance != nil && vault.Instance.VaultID == vaultID {
		vaultInst = vault.Instance
	}
	o.mu.RUnlock()

	if vaultInst == nil {
		return
	}

	// Transfer all sealed chunks.
	if !o.drainVaultChunks(ctx, sys, vaultID, vaultInst, mode, targetNodeID) {
		return // context cancelled or error — defer handles cleanup
	}

	// Final seal to catch any stragglers. Seal() only fires
	// AnnounceBeginSeal (Active → Sealing); the post-seal pipeline owns the
	// matching AnnounceSeal, so it must be scheduled or the manifest entry
	// parks in Sealing forever and drainVaultChunks' FSM grounding keeps
	// skipping the chunk as not-yet-Sealed.
	if active := vaultInst.Chunks.Active(); active != nil {
		chunkID := active.ID
		if err := vaultInst.Chunks.Seal(); err != nil {
			o.drainLogger.Warn("vault drain: final seal failed", "vault", vaultID, "error", err)
		} else {
			o.postSealWork(vaultID, vaultInst.Chunks, chunkID)
		}
		o.drainVaultChunks(ctx, sys, vaultID, vaultInst, mode, targetNodeID)
	}

	success = true
}

// drainVaultChunks transfers all sealed chunks from the instance. Returns false if cancelled.
func (o *Orchestrator) drainVaultChunks(ctx context.Context, sys *system.System, vaultID glid.GLID, vaultInst *VaultInstance, mode DrainMode, targetNodeID string) bool {
	metas, err := vaultInst.Chunks.List()
	if err != nil {
		o.drainLogger.Error("vault drain: list chunks failed", "vault", vaultID, "error", err)
		return false
	}

	for _, meta := range metas {
		// Phase 3 (gastrolog-1huz5): overlay through FSM so Sealing
		// chunks (active-form sealed locally but GLCB not yet committed)
		// are skipped. Drain ships sealed-form GLCBs; a Sealing chunk
		// would race with concurrent PostSealProcess.
		meta = o.groundChunkMeta(vaultID, meta)
		if !meta.Sealed {
			continue
		}
		select {
		case <-ctx.Done():
			return false
		default:
		}

		if err := o.drainOneChunk(ctx, sys, vaultID, vaultInst, meta.ID, mode, targetNodeID); err != nil {
			o.drainLogger.Error("vault drain: chunk transfer failed",
				"vault", vaultID, "chunk", meta.ID, "error", err)
			continue // best effort — try the rest
		}
	}
	return true
}

// drainOneChunk transfers a single chunk and deletes the source.
func (o *Orchestrator) drainOneChunk(ctx context.Context, sys *system.System, vaultID glid.GLID, vaultInst *VaultInstance, chunkID chunk.ChunkID, mode DrainMode, targetNodeID string) error {
	cursor, err := vaultInst.Chunks.OpenCursor(chunkID)
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
		// Phase 4 (gastrolog-42f9z): no "next instance" anymore. Decommission
		// just destroys the chunks — equivalent to firing a retention
		// event with no matching retention-trigger routes (the legacy
		// expire behavior). Phase 5's richer routing table will let
		// operators provide a destination route to receive drained
		// chunks if needed.
		_ = cursor // caller closes
		_ = sys

	case DrainRebalance:
		if o.chunkReplicator == nil {
			return errors.New("vault drain rebalance: vaultInst replicator not configured")
		}
		if err := o.chunkReplicator.ImportSealedChunk(ctx, targetNodeID, vaultID, chunkID, chunk.CursorIterator(cursor)); err != nil {
			return fmt.Errorf("replicate to target node: %w", err)
		}
	}

	// Release the cursor's read lock before deleteDrainSource tries to
	// take the write lock on the same chunk.
	_ = cursor.Close()
	cursorClosed = true

	// Delete source chunk via the receipt protocol when wired (production)
	// or via direct local cleanup otherwise (memory-mode vaults without a
	// reconciler). Reason "instance-drain" lands in pendingDeletes audit. See
	// gastrolog-51gme.
	if err := o.deleteDrainSource(vaultInst, vaultID, chunkID); err != nil {
		return err
	}

	o.drainLogger.Info("vault drain: chunk transferred",
		"vault", vaultID, "chunk", chunkID, "mode", drainModeName(mode))
	return nil
}

// deleteDrainSource removes a successfully-drained source chunk. Routes
// through the receipt protocol when a reconciler is wired; falls back to
// the direct local delete for memory-mode vaults. Extracted from
// drainOneChunk to keep nestif within lint thresholds.
func (o *Orchestrator) deleteDrainSource(vaultInst *VaultInstance, vaultID glid.GLID, chunkID chunk.ChunkID) error {
	if vaultInst.Reconciler != nil {
		if err := vaultInst.Reconciler.deleteChunk(chunkID, "vault-drain", o.placementMembership(vaultInst)); err != nil {
			return fmt.Errorf("delete source chunk: %w", err)
		}
		return nil
	}
	if vaultInst.Indexes != nil {
		if err := vaultInst.Indexes.DeleteIndexes(chunkID); err != nil {
			o.drainLogger.Warn("vault drain: delete source indexes failed",
				"vault", vaultID, "chunk", chunkID, "error", err)
		}
	}
	if err := vaultInst.Chunks.Delete(chunkID); err != nil {
		return fmt.Errorf("delete source chunk: %w", err)
	}
	return nil
}

// finishVaultDrain cleans up after a completed or cancelled instance drain.
func (o *Orchestrator) finishVaultDrain(vaultID glid.GLID) {
	key := vaultDrainKey(vaultID)

	o.mu.Lock()
	ds, ok := o.vaultDraining[key]
	if ok {
		delete(o.vaultDraining, key)
		if ds.Cancel != nil {
			ds.Cancel()
		}
	}
	o.mu.Unlock()

	// Remove the vault instance (closes managers, deletes remaining data).
	// Drain has already migrated chunks to the target; the destructive wipe
	// on the source instance is the correct semantics here.
	_ = vaultID
	if o.DeleteVaultInstance(vaultID) {
		o.drainLogger.Info("vault drain: completed",
			"vault", vaultID)
	}

	// Drain completion no longer fires a config-mutation callback —
	// placement updates are the source of truth for instance lifecycle
	// under the per-vault model.
}

// cancelVaultDrainState removes drain state without triggering vault config
// updates or Raft group destruction. Used when the drain worker exits early
// (error, vault already gone, etc.) to prevent leaked drain state.
func (o *Orchestrator) cancelVaultDrainState(vaultID glid.GLID) {
	key := vaultDrainKey(vaultID)

	o.mu.Lock()
	ds, ok := o.vaultDraining[key]
	if ok {
		delete(o.vaultDraining, key)
		if ds.Cancel != nil {
			ds.Cancel()
		}
	}
	o.mu.Unlock()

	if ok {
		o.drainLogger.Info("vault drain: state cleaned up (drain did not complete)",
			"vault", vaultID)
	}
}

// CancelInstanceDrain aborts an in-progress instance drain. The instance remains in the
// vault with whatever chunks haven't been transferred yet.
func (o *Orchestrator) CancelInstanceDrain(vaultID glid.GLID) error {
	key := vaultDrainKey(vaultID)

	o.mu.Lock()
	defer o.mu.Unlock()

	ds, ok := o.vaultDraining[key]
	if !ok {
		return fmt.Errorf("vault %s in vault %s is not draining", vaultID, vaultID)
	}

	ds.Cancel()
	delete(o.vaultDraining, key)
	o.scheduler.RemoveJob(ds.JobID)

	o.drainLogger.Info("vault drain: cancelled", "vault", vaultID)
	return nil
}

// IsInstanceDraining returns true if the given instance is currently draining.
func (o *Orchestrator) IsInstanceDraining(vaultID glid.GLID) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	_, ok := o.vaultDraining[vaultDrainKey(vaultID)]
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
