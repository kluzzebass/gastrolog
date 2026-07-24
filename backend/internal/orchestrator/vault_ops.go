package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"sync"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/index/analyzer"
	"gastrolog/internal/orchestrator/pipeline"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// IndexInfo describes a single index for a chunk.
type IndexInfo struct {
	Name       string
	Exists     bool
	EntryCount int64
	SizeBytes  int64
}

// ChunkIndexReport aggregates chunk seal status and per-index info.
type ChunkIndexReport struct {
	Sealed  bool
	Indexes []IndexInfo
}

// activeManagers returns chunk and index managers for the vault's active
// instance. Returns ErrVaultNotFound if the vault doesn't exist or is
// not registered locally.
func (o *Orchestrator) activeManagers(vaultID glid.GLID) (chunk.ChunkManager, index.IndexManager, error) {
	o.mu.RLock()
	s := o.vaults[vaultID]
	o.mu.RUnlock()
	if s == nil {
		return nil, nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if err := vaultReplicationReadinessErr(vaultID, s); err != nil {
		return nil, nil, err
	}
	cm, im := s.ChunkManager(), s.IndexManager()
	if cm == nil {
		return nil, nil, fmt.Errorf("%w: %s (no instance)", ErrVaultNotFound, vaultID)
	}
	return cm, im, nil
}

// activeChunkManager returns the chunk manager for the vault's instance.
func (o *Orchestrator) activeChunkManager(vaultID glid.GLID) (chunk.ChunkManager, error) {
	o.mu.RLock()
	s := o.vaults[vaultID]
	o.mu.RUnlock()
	if s == nil {
		return nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if err := vaultReplicationReadinessErr(vaultID, s); err != nil {
		return nil, err
	}
	cm := s.ChunkManager()
	if cm == nil {
		return nil, fmt.Errorf("%w: %s (no instance)", ErrVaultNotFound, vaultID)
	}
	return cm, nil
}

// activeIndexManager returns the index manager for the vault's instance.
func (o *Orchestrator) activeIndexManager(vaultID glid.GLID) (index.IndexManager, error) {
	o.mu.RLock()
	s := o.vaults[vaultID]
	o.mu.RUnlock()
	if s == nil {
		return nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if err := vaultReplicationReadinessErr(vaultID, s); err != nil {
		return nil, err
	}
	im := s.IndexManager()
	if im == nil {
		return nil, fmt.Errorf("%w: %s (no instance)", ErrVaultNotFound, vaultID)
	}
	return im, nil
}

// findManagersForChunk returns the chunk and index managers for the vault's
// instance that owns the given chunk (metadata match or active chunk ID).
// IndexManager may be nil if the vault has no index backend.
func (o *Orchestrator) findManagersForChunk(vaultID glid.GLID, chunkID chunk.ChunkID) (chunk.ChunkManager, index.IndexManager, error) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return nil, nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if err := vaultReplicationReadinessErr(vaultID, vault); err != nil {
		return nil, nil, err
	}
	if vaultInst := vault.Instance; vaultInst != nil {
		if _, err := vaultInst.Chunks.Meta(chunkID); err == nil {
			return vaultInst.Chunks, vaultInst.Indexes, nil
		}
		if active := vaultInst.Chunks.Active(); active != nil && active.ID == chunkID {
			return vaultInst.Chunks, vaultInst.Indexes, nil
		}
	}
	return nil, nil, fmt.Errorf("%w: chunk %s in vault %s", chunk.ErrChunkNotFound, chunkID, vaultID)
}

// findChunkManagerForChunk returns the vault's chunk manager when the
// chunk lives there.
func (o *Orchestrator) findChunkManagerForChunk(vaultID glid.GLID, chunkID chunk.ChunkID) (chunk.ChunkManager, error) {
	cm, _, err := o.findManagersForChunk(vaultID, chunkID)
	return cm, err
}

// findInstanceForChunk returns the vault's VaultInstance when the chunk lives
// there. Used by paths (vault migrate, drain) that need the reconciler +
// placement metadata to drive cluster-wide deletes through the receipt
// protocol. Errors if the vault is unknown / not ready, or if its
// instance does not hold the chunk.
func (o *Orchestrator) findInstanceForChunk(vaultID glid.GLID, chunkID chunk.ChunkID) (*VaultInstance, error) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if err := vaultReplicationReadinessErr(vaultID, vault); err != nil {
		return nil, err
	}
	if vaultInst := vault.Instance; vaultInst != nil {
		if _, err := vaultInst.Chunks.Meta(chunkID); err == nil {
			return vaultInst, nil
		}
		if active := vaultInst.Chunks.Active(); active != nil && active.ID == chunkID {
			return vaultInst, nil
		}
	}
	return nil, fmt.Errorf("%w: chunk %s in vault %s", chunk.ErrChunkNotFound, chunkID, vaultID)
}

// ArchiveChunk transitions a cloud-backed sealed chunk to an offline storage class.
func (o *Orchestrator) ArchiveChunk(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, storageClass string) error {
	cm, err := o.findChunkManagerForChunk(vaultID, chunkID)
	if err != nil {
		return err
	}
	archiver, ok := cm.(chunk.ChunkArchiver)
	if !ok {
		return errors.New("chunk manager does not support archival")
	}
	return archiver.ArchiveChunk(ctx, chunkID, storageClass)
}

// RestoreChunk initiates retrieval of an archived chunk.
func (o *Orchestrator) RestoreChunk(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, speed string, days int) error {
	cm, err := o.findChunkManagerForChunk(vaultID, chunkID)
	if err != nil {
		return err
	}
	archiver, ok := cm.(chunk.ChunkArchiver)
	if !ok {
		return errors.New("chunk manager does not support restore")
	}
	return archiver.RestoreChunk(ctx, chunkID, speed, days)
}

// --- Chunk read ---

// VaultChunkMeta pairs a chunk with the vault it belongs to.
type VaultChunkMeta struct {
	chunk.ChunkMeta
	VaultID   glid.GLID
	VaultType string
}

// ListLocalChunkMetas returns chunk metadata from the active local
// chunk manager — the per-node disk-derived view. Use this for
// per-node operations: search engine chunk discovery, retention
// sweeps, anything that needs to know "what chunks does THIS node
// hold on disk."
//
// For the cluster-authoritative view ("what chunks exist for this
// vault according to the FSM"), use ListClusterChunkMetas — disk
// is only one node's slice of cluster reality.
//
// Renamed from ListChunkMetas per audit finding F5
// (docs/disk-authority-audit.md, gastrolog-3alnf). Old name removed
// so every caller is forced to choose local-vs-cluster explicitly.
func (o *Orchestrator) ListLocalChunkMetas(vaultID glid.GLID) ([]chunk.ChunkMeta, error) {
	cm, err := o.activeChunkManager(vaultID)
	if err != nil {
		return nil, err
	}
	return cm.List()
}

// ListClusterChunkMetas returns chunk metadata from the vault-ctl
// FSM manifest — the cluster-authoritative view. Use this for RPC
// surfaces and display: anything that needs to answer "what chunks
// does this vault have, cluster-wide?" regardless of which node
// happens to be serving the query.
//
// The FSM is keyed per-vault and replicated across every vault-ctl
// group member; reading it on any node returns the same set
// (modulo replication lag). Sealed-state is FSM-authoritative;
// per-chunk fields like RecordCount/DataBytes come from the FSM
// manifest entry, which the leader populated at seal time.
//
// Returns (nil, nil) when this node has no vault-ctl group for the
// vault (placement excludes it, or single-node mode without
// GroupManager). Callers that need cluster-coverage in that case
// must fan out via the server's directRemoteSearcher / forwarder
// surfaces.
//
// Added per audit finding F5 (docs/disk-authority-audit.md,
// gastrolog-3alnf).
func (o *Orchestrator) ListClusterChunkMetas(vaultID glid.GLID) ([]chunk.ChunkMeta, error) {
	if o.groupMgr == nil {
		return nil, nil
	}
	g := o.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(vaultID))
	if g == nil {
		return nil, nil
	}
	var fsm *vaultctlfsm.FSM
	switch raw := g.FSM.(type) {
	case *vaultctlfsm.FSM:
		fsm = raw
	case *vaultraft.FSM:
		fsm = raw.EnsureVaultFSM(vaultID)
	default:
		return nil, nil
	}
	entries := fsm.List()
	out := make([]chunk.ChunkMeta, 0, len(entries))
	for _, e := range entries {
		out = append(out, manifestEntryToChunkMeta(e, e.IsSealed()))
	}
	return out, nil
}

// ListAllChunkMetas returns chunk metadata from ALL local instances of a vault,
// each tagged with its instance ID and type.
//
// Sealed chunks are projected from the vault-ctl FSM manifest
// (VaultManifestEntriesFromCtlFSM) so every voter sees the full cluster chunk
// set regardless of how many blobs this node's chunk manager has registered
// yet. Without this, ListChunks fell back to Chunks.List() (local replicas
// only) and the inspector showed a handful of chunks whenever remote fan-out
// timed out under load. Memory-mode vaults without an FSM fall back to the
// local chunk manager. The active head chunk is appended from the chunk
// manager when it is not yet in the FSM snapshot.
//
// Caller-side deduplication across nodes happens in the server's ListChunks.
func (o *Orchestrator) ListAllChunkMetas(vaultID glid.GLID) ([]VaultChunkMeta, error) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	vaultInst := vault.Instance
	if vaultInst == nil {
		return nil, nil
	}
	if err := vaultReplicationReadinessErr(vaultID, vault); err != nil {
		return nil, err
	}

	vaultType := vaultInst.Type
	overlay := vaultInst.OverlayFromFSM

	var entries []vaultctlfsm.ManifestEntry
	if fsmEntries := o.VaultManifestEntriesFromCtlFSM(vaultID); len(fsmEntries) > 0 {
		entries = fsmEntries
	} else {
		entries = vaultManifestEntries(vaultInst)
	}
	if len(entries) == 0 {
		return nil, nil
	}

	result := make([]VaultChunkMeta, 0, len(entries))
	seen := make(map[chunk.ChunkID]struct{}, len(entries))
	for _, e := range entries {
		m := e.ToChunkMeta()
		o.overlayPipelineChunkMetaBounds(vaultID, &m)
		if overlay != nil {
			m = overlay(m)
		}
		result = append(result, VaultChunkMeta{
			ChunkMeta: m,
			VaultID:   vaultInst.VaultID,
			VaultType: vaultType,
		})
		seen[m.ID] = struct{}{}
	}

	// Active chunk: chunk manager holds fresher running maxima than the last
	// FSM apply for the open head (documented manifest exception).
	if active := vaultInst.Chunks.Active(); active != nil {
		if _, ok := seen[active.ID]; !ok {
			m := *active
			if overlay != nil {
				m = overlay(m)
			}
			result = append(result, VaultChunkMeta{
				ChunkMeta: m,
				VaultID:   vaultInst.VaultID,
				VaultType: vaultType,
			})
		}
	}

	return result, nil
}

// GetChunkMeta returns metadata for a specific chunk. The result is overlaid
// from the vault-ctl FSM if the chunk belongs to an instance with a Raft group, so
// CloudBacked / Archived reflect the cluster-wide truth rather than this
// node's local chunk-manager view. See gastrolog-asg4l.
func (o *Orchestrator) GetChunkMeta(vaultID glid.GLID, chunkID chunk.ChunkID) (chunk.ChunkMeta, error) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return chunk.ChunkMeta{}, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if err := vaultReplicationReadinessErr(vaultID, vault); err != nil {
		return chunk.ChunkMeta{}, err
	}
	if vaultInst := vault.Instance; vaultInst != nil {
		m, err := vaultInst.Chunks.Meta(chunkID)
		if err == nil {
			if vaultInst.OverlayFromFSM != nil {
				m = vaultInst.OverlayFromFSM(m)
			}
			return m, nil
		}
	}
	return chunk.ChunkMeta{}, chunk.ErrChunkNotFound
}

// GetVaultChunkMeta returns metadata for a specific chunk with instance info.
func (o *Orchestrator) GetVaultChunkMeta(vaultID glid.GLID, chunkID chunk.ChunkID) (VaultChunkMeta, error) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return VaultChunkMeta{}, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if err := vaultReplicationReadinessErr(vaultID, vault); err != nil {
		return VaultChunkMeta{}, err
	}
	if vaultInst := vault.Instance; vaultInst != nil {
		m, err := vaultInst.Chunks.Meta(chunkID)
		if err == nil {
			if vaultInst.OverlayFromFSM != nil {
				m = vaultInst.OverlayFromFSM(m)
			}
			return VaultChunkMeta{
				ChunkMeta: m,
				VaultID:   vaultInst.VaultID,
				VaultType: vaultInst.Type,
			}, nil
		}
	}
	return VaultChunkMeta{}, chunk.ErrChunkNotFound
}

// OpenCursor opens a record cursor for the given chunk on the instance that owns it.
func (o *Orchestrator) OpenCursor(vaultID glid.GLID, chunkID chunk.ChunkID) (chunk.RecordCursor, error) {
	cm, err := o.findChunkManagerForChunk(vaultID, chunkID)
	if err != nil {
		return nil, err
	}
	return cm.OpenCursor(chunkID)
}

// VaultExists returns true if a vault with the given ID is registered.
func (o *Orchestrator) VaultExists(vaultID glid.GLID) bool {
	o.mu.RLock()
	s := o.vaults[vaultID]
	o.mu.RUnlock()
	return s != nil
}

// VaultType returns the type string for a registered vault, or "" if not found.
func (o *Orchestrator) VaultType(vaultID glid.GLID) string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if v := o.vaults[vaultID]; v != nil {
		return v.Type()
	}
	return ""
}

// MissingVaultInstance returns true if the vault's local instance list
// differs from the given instance IDs — either instances were added or
// removed.
func (o *Orchestrator) MissingVaultInstance(vaultID glid.GLID, vaultIDs []glid.GLID) bool {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return false
	}
	// Single-instance model: at most one local instance ID per vault.
	var local glid.GLID
	if t := vault.Instance; t != nil {
		local = t.VaultID
	}
	expected := make(map[glid.GLID]bool, len(vaultIDs))
	for _, id := range vaultIDs {
		expected[id] = true
		if local != id {
			return true // instance added
		}
	}
	if (local != glid.GLID{}) && !expected[local] {
		return true // instance removed
	}
	return false
}

// LocalInstanceIDs returns the vault IDs currently instantiated for the given vault.
func (o *Orchestrator) LocalInstanceIDs(vaultID glid.GLID) []glid.GLID {
	o.mu.RLock()
	defer o.mu.RUnlock()
	vault := o.vaults[vaultID]
	if vault == nil {
		return nil
	}
	if t := vault.Instance; t != nil {
		return []glid.GLID{t.VaultID}
	}
	return nil
}

// FindLocalVaultInstance returns the VaultInstance for the given vault, or
// nil if not local. Exported for cross-package use.
func (o *Orchestrator) FindLocalVaultInstance(vaultID glid.GLID) *VaultInstance {
	return o.findLocalVaultInstance(vaultID)
}

// findLocalVaultInstance returns the VaultInstance for the given vault, or
// nil if the vault has no local instance.
func (o *Orchestrator) findLocalVaultInstance(vaultID glid.GLID) *VaultInstance {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil || vault.Instance == nil {
		return nil
	}
	return vault.Instance
}


// replaceForwardedChunk seals (if active) and deletes a pre-existing chunk
// on a follower to make room for the canonical sealed version from the leader.
// The pre-existing chunk may come from:
//   - ChunkReplicator.AppendRecords syncing records as the leader's active
//     chunk fills up (and the follower may have missed some due to a brief
//     network disruption, leaving its copy slightly behind the leader's)
//   - a catchup path that fills follower state from scratch
//
// Retries if a concurrent Append reopens the active chunk between seal and
// delete.
//
// Uses DeleteNoAnnounce: this is a LOCAL cleanup operation on a single
// follower. It must NOT propagate the delete via vault-ctl Raft — the canonical
// sealed chunk is about to replace it locally via ImportRecords, which will
// fire its own AnnounceCreate/AnnounceSeal for the replacement.
func replaceForwardedChunk(cm chunk.ChunkManager, chunkID chunk.ChunkID, isActive bool) error {
	if isActive {
		if err := cm.Seal(); err != nil {
			return fmt.Errorf("seal forwarded chunk %s: %w", chunkID, err)
		}
	}
	if err := chunk.DeleteNoAnnounce(cm, chunkID); errors.Is(err, chunk.ErrActiveChunk) {
		if sealErr := cm.Seal(); sealErr != nil {
			return fmt.Errorf("re-seal forwarded chunk %s: %w", chunkID, sealErr)
		}
		if err = chunk.DeleteNoAnnounce(cm, chunkID); err != nil {
			return fmt.Errorf("delete forwarded chunk %s (after re-seal): %w", chunkID, err)
		}
	} else if err != nil {
		return fmt.Errorf("delete forwarded chunk %s: %w", chunkID, err)
	}
	return nil
}

// proposePruneNodeForVault fans CmdPruneNode out to every instance sub-FSM
// in the vault after the vault-ctl Raft leader removed a node from the
// voter set. Each instance's applier transparently routes the propose to
// the leader, so this callback can fire from the leader's reconcile
// pass without needing per-vault leadership checks. See gastrolog-51gme
// step 10.
//
// afterVaultCtlRestore is wired from vaultraft.FSM.SetOnAfterRestore and
// must return immediately — GroupManager.CreateGroup holds groupMgr.mu
// across NewRaft→fsm.Restore, and the deferred worker calls
// groupMgr.GetGroup. scheduleAfterVaultCtlRestore runs rewire +
// ReconcileFromSnapshot asynchronously (gastrolog-4tadr).
func (o *Orchestrator) afterVaultCtlRestore(vaultID glid.GLID) {
	o.scheduleAfterVaultCtlRestore(vaultID)
}

func (o *Orchestrator) scheduleAfterVaultCtlRestore(vaultID glid.GLID) {
	if _, loaded := o.ctlRestorePending.LoadOrStore(vaultID, struct{}{}); loaded {
		return
	}
	o.auxWg.Go(func() {
		defer o.ctlRestorePending.Delete(vaultID)
		o.runAfterVaultCtlRestore(vaultID)
	})
}

func (o *Orchestrator) runAfterVaultCtlRestore(vaultID glid.GLID) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	var t *VaultInstance
	if vault != nil {
		t = vault.Instance
	}
	o.mu.RUnlock()

	// Snapshot install swaps in new vaultctlfsm sub-FSM objects — rebind
	// reconciler + instance callbacks before reconciling. See
	// rewireVaultInstanceAfterCtlRestore.
	liveFSM := o.rewireVaultInstanceAfterCtlRestore(vaultID, t)
	if t != nil && t.Reconciler != nil && liveFSM != nil {
		t.Reconciler.ReconcileFromSnapshot(liveFSM)
	}
	if o.pipeline != nil && o.isPipelineIngestVault(vaultID) {
		o.recoverPipelineVaultAfterRestore(vaultID)
	}
	// Cloud-upload catch-up on snapshot restore: Restore replaces the FSM
	// wholesale with no per-entry seal effects, so a chunk that became sealed-
	// but-not-cloud-backed in the installed snapshot never scheduled an upload
	// on this node. This replaces the retired 5s backfill tick's discovery role
	// for the snapshot-restore gap (gastrolog-576bm). No-op unless this node is
	// the vault's uploader.
	o.cloudUploadCatchupForVault(vaultID)
	o.vaultOpsLogger.Info("vault-ctl after-restore reconcile complete",
		"vault", vaultID, "has_instance", t != nil)
}

// onVaultCtlLeadGained wakes the chunking worker when this home becomes the
// vault-ctl leader. Planning and build passes run in the worker loop.
func (o *Orchestrator) onVaultCtlLeadGained(vaultID glid.GLID) {
	// Cloud-upload catch-up on leadership change: a chunk that sealed while
	// this node was not the vault's uploader never saw the live onSeal upload
	// effect here. This replaces the retired 5s backfill tick's discovery role
	// for the leadership-change case (gastrolog-576bm). Applies to both
	// type=cloud vaults (gated on vault-ctl leadership) and cloud-backed
	// pipeline vaults (gated on CloudStore/placement leadership); the catch-up
	// is a no-op unless this node is actually the uploader.
	o.cloudUploadCatchupForVault(vaultID)

	if o.pipeline == nil || !o.isPipelineIngestVault(vaultID) {
		return
	}
	o.pipeline.NotifyChunkingVault(vaultID)
	o.pipeline.NotifyPublishRetry()
	o.pipeline.NotifyCollectionVault(vaultID)
}

// reconcilePipelineAfterCtlRestore rebinds pipeline managers to the live vault-ctl
// FSM after snapshot restore and runs chunk recover. Returns ErrUnknownVault when
// pipeline has not registered the vault on this home yet — caller must defer.
func (o *Orchestrator) reconcilePipelineAfterCtlRestore(vaultID glid.GLID) error {
	if o.pipeline == nil || !o.isPipelineIngestVault(vaultID) {
		return nil
	}
	if err := o.rewirePipelineAfterCtlRestore(vaultID); err != nil {
		return err
	}
	if err := o.pipeline.RecoverVault(context.Background(), vaultID); err != nil {
		return err
	}
	o.pipeline.NotifyChunkingVault(vaultID)
	return nil
}

// finishPendingPipelineCtlRestore completes a deferred ctl-restore reconcile after
// pipeline RegisterVault. Vault-ctl Restore can finish before ApplyConfig registers
// pipeline homes; without this hook rewire never runs against the live FSM.
//
// The reconcile runs on its own goroutine: callers reach here from
// reloadPipelineFromConfig while HOLDING o.mu (write) — reconcileFilters and
// ReloadFilters both lock around the reload — and the reconcile chain takes
// o.mu.RLock (isPipelineIngestVault). Go's RWMutex is non-reentrant, so
// running it inline self-deadlocked the whole orchestrator the first time a
// node rejoined via snapshot restore (gastrolog-3wpfet: node bricked seconds
// after rejoin, every o.mu user queued forever). The goroutine simply blocks
// until the caller releases the write lock — the exact ordering we want.
// LoadAndDelete keeps it exactly-once per restore, and the FSM restore hook
// already runs this reconcile concurrently, so the contract permits async.
func (o *Orchestrator) finishPendingPipelineCtlRestore(vaultID glid.GLID) {
	if _, ok := o.pendingPipelineCtlRestore.LoadAndDelete(vaultID); !ok {
		return
	}
	go func() {
		if err := o.reconcilePipelineAfterCtlRestore(vaultID); err != nil {
			o.vaultOpsLogger.Warn("pipeline ctl-restore reconcile after register failed",
				"vault", vaultID, "error", err)
		}
	}()
}

// rewirePipelineAfterCtlRestore rebinds chunking/collection to the live vault-ctl
// sub-FSM. vaultraft.FSM.Restore allocates fresh sub-FSMs; pipeline managers
// otherwise keep reading the pre-restore object and the planner stalls.
func (o *Orchestrator) rewirePipelineAfterCtlRestore(vaultID glid.GLID) error {
	fsm, applier, _, ok := o.vaultCtlHandle(vaultID)
	if !ok || fsm == nil {
		return nil
	}
	root, err := o.originRoot(vaultID)
	if err != nil {
		return err
	}
	cfg := pipeline.RewireVaultConfig{
		FSM:     fsm,
		Applier: applier,
	}
	if o.segmentPuller != nil {
		lookup := func() *vaultctlfsm.FSM {
			f, _, _, ok := o.vaultCtlHandle(vaultID)
			if ok && f != nil {
				return f
			}
			return fsm
		}
		cfg.Log = &segmentLogReader{lookup: lookup, localNodeID: o.localNodeID, vaultRoot: root, placement: func() []string { return o.vaultPlacementNodeIDs(vaultID) }}
		cfg.Pull = &segmentPullClient{
			lookup:      lookup,
			puller:      o.segmentPuller,
			localNodeID: o.localNodeID,
			vaultRoot:   root,
		}
		cfg.Receipts = &segmentReceiptCommitter{applier: applier, localNodeID: o.localNodeID}
	}
	return o.pipeline.RewireVaultAfterCtlRestore(vaultID, cfg)
}

func (o *Orchestrator) recoverPipelineVaultAfterRestore(vaultID glid.GLID) {
	if err := o.reconcilePipelineAfterCtlRestore(vaultID); err != nil {
		if errors.Is(err, chunking.ErrUnknownVault) {
			o.pendingPipelineCtlRestore.Store(vaultID, struct{}{})
			return
		}
		o.vaultOpsLogger.Warn("vault-ctl after-restore: pipeline reconcile failed",
			"vault", vaultID, "error", err)
	}
}

// Errors are logged at warn but do not abort: the next reconcile pass
// will re-propose CmdPruneNode for any instance where the apply failed
// (the FSM's applyPruneNode is idempotent — repeated prunes for the
// same node are no-ops on entries where it's already gone).
func (o *Orchestrator) proposePruneNodeForVault(vaultID glid.GLID, removedNodeID string) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	if vault == nil {
		o.mu.RUnlock()
		return
	}
	t := vault.Instance
	o.mu.RUnlock()

	if t == nil || t.ApplyRaftPruneNode == nil {
		return
	}
	if err := t.ApplyRaftPruneNode(removedNodeID); err != nil {
		o.vaultOpsLogger.Warn("prune-node propose failed",
			"vault", vaultID,
			"removed_node", removedNodeID, "error", err)
	}
}

// placementMembership returns the set of node IDs that participate in a
// instance's chunk-lifecycle obligations: the local node (always present
// because callers run on the leader path) plus every follower target's
// node ID, with duplicates collapsed. Suitable for passing as the
// expectedFrom argument to VaultLifecycleReconciler.deleteChunk.
//
// Returned in deterministic order (local first, then follower targets in
// their declared order) so that audit-log output is reproducible across
// runs even though the FSM-side encoding stores expectedFrom as a map.
// See gastrolog-51gme.
func (o *Orchestrator) placementMembership(vaultInst *VaultInstance) []string {
	expected := make([]string, 0, 1+len(vaultInst.FollowerTargets))
	seen := map[string]bool{}
	if o.localNodeID != "" {
		expected = append(expected, o.localNodeID)
		seen[o.localNodeID] = true
	}
	for _, t := range vaultInst.FollowerTargets {
		if t.NodeID == "" || seen[t.NodeID] {
			continue
		}
		seen[t.NodeID] = true
		expected = append(expected, t.NodeID)
	}
	return expected
}


// --- Chunk write ---

// ImportChunkRecords creates a new sealed chunk from the given records in the
// target vault. Used by the ForwardImportRecords handler to receive cross-node
// chunk migrations. Works with any ChunkManager type (file or memory).
// Compression and index builds are scheduled asynchronously via the scheduler.
func (o *Orchestrator) ImportChunkRecords(ctx context.Context, vaultID glid.GLID, next chunk.RecordIterator) error {
	cm, _, err := o.activeManagers(vaultID)
	if err != nil {
		return err
	}

	meta, err := cm.ImportRecords(chunk.ChunkID{}, next)
	if err != nil {
		return fmt.Errorf("import records: %w", err)
	}

	if meta.ID != (chunk.ChunkID{}) {
		o.schedulePostSeal(vaultID, cm, meta.ID)
	}

	return nil
}

// ImportToVault imports records as a sealed chunk into a specific instance,
// preserving the given chunk ID. Used by sealed-chunk replication —
// the follower receives a sealed chunk from the leader with the same ID.
// Schedules postSealWork for local indexing (followers need indexes for queries)
// but won't trigger further replication (gated by !IsFollower in
// followerReplicationTargets).
func (o *Orchestrator) ImportToVault(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, next chunk.RecordIterator) error {
	vaultInst := o.findLocalVaultInstance(vaultID)
	if vaultInst == nil {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	return o.ImportToInstanceStorage(ctx, vaultID, "", chunkID, next)
}

// ImportToInstanceStorage imports a sealed chunk into the local vault
// instance, optionally constrained to a specific storage. When storageID is
// empty, the local instance is targeted unconditionally. Used by same-node
// replication to route to specific file storage instances.
func (o *Orchestrator) ImportToInstanceStorage(ctx context.Context, vaultID glid.GLID, storageID string, chunkID chunk.ChunkID, next chunk.RecordIterator) error {
	if o.isPipelineIngestVault(vaultID) {
		drainIterator(next)
		o.vaultOpsLogger.Debug("replication: record-stream import refused for pipeline vault",
			"vault", vaultID, "chunk", chunkID)
		return nil
	}
	// Look up the instance under lock, then release BEFORE the import.
	// ImportRecords reads from a network stream and can block — holding
	// RLock during a network read starves writers (FSM dispatcher) and
	// deadlocks the entire orchestrator.
	type vaultRef struct {
		cm           chunk.ChunkManager
		isFollower   bool
		isTombstoned func(chunk.ChunkID) bool
	}
	ref := func() *vaultRef {
		o.mu.RLock()
		defer o.mu.RUnlock()
		vault := o.vaults[vaultID]
		if vault == nil {
			return nil
		}
		if t := vault.Instance; t != nil && (storageID == "" || t.StorageID == storageID) {
			return &vaultRef{cm: t.Chunks, isFollower: t.IsFollower, isTombstoned: t.IsTombstoned}
		}
		return nil
	}()
	if ref == nil {
		// "instance not registered on this node" rather than "vault not found":
		// the vault almost always still exists cluster-wide; only the local
		// instance was evicted from this node by placement churn (or never
		// landed here in the first place). See gastrolog-2t48z.
		return fmt.Errorf("%w: vault %s", ErrInstanceNotLocal, vaultID)
	}
	// Reject stale ImportSealed RPCs for chunks the cluster already deleted.
	// The race is: leader schedules replication, retention fires, delete is
	// committed via Raft (populates tombstone), then the late replication
	// RPC arrives. Without this check the receiver would recreate the chunk.
	// See gastrolog-11rzz.
	if ref.isTombstoned != nil && ref.isTombstoned(chunkID) {
		return fmt.Errorf("%w: import sealed chunk %s into vault %s", chunk.ErrChunkTombstoned, chunkID, vaultID)
	}
	cm := ref.cm

	// Serialize SetNextChunkID + ImportRecords per vault instance to prevent
	// concurrent replication messages from interleaving the two calls.
	// Key includes storageID so same-node replicas can import in parallel.
	importKey := vaultID.String() + ":" + storageID
	muVal, _ := o.importMu.LoadOrStore(importKey, &sync.Mutex{})
	vaultMu := muVal.(*sync.Mutex)
	vaultMu.Lock()
	defer vaultMu.Unlock()

	// Check if this chunk already exists (sealed or active).
	_, metaErr := cm.Meta(chunkID)
	activeIsChunk := false
	if active := cm.Active(); active != nil && active.ID == chunkID {
		activeIsChunk = true
	}

	chunkExists := metaErr == nil || activeIsChunk

	// Leader: idempotent skip — canonical version is already here.
	if chunkExists && !ref.isFollower {
		o.vaultOpsLogger.Debug("replication: chunk already exists, skipping import",
			"vault", vaultID, "chunk", chunkID.String())
		drainIterator(next)
		return nil
	}

	// Follower: replace the forwarded version (may be incomplete due to
	// fire-and-forget drops) with the canonical sealed chunk.
	if chunkExists {
		if err := replaceForwardedChunk(cm, chunkID, activeIsChunk); err != nil {
			drainIterator(next)
			return err
		}
		o.vaultOpsLogger.Debug("replication: replacing forwarded chunk with canonical version",
			"vault", vaultID, "chunk", chunkID.String())
	}

	meta, err := cm.ImportRecords(chunkID, next)
	if err != nil {
		return fmt.Errorf("import to vault %s: %w", vaultID, err)
	}
	o.vaultOpsLogger.Debug("replication: sealed chunk imported",
		"vault", vaultID,
		"chunk", meta.ID.String(), "records", meta.RecordCount)

	return o.finalizeImportedChunk(vaultID, cm, meta, ref.isTombstoned)
}

// finalizeImportedChunk handles the post-import steps: vault-ctl Raft
// announcement, tombstone re-check, and (if not tombstoned) post-seal
// work scheduling. See gastrolog-11rzz for the ordering rationale.
//
// Ordering matters: announce first, then re-check tombstone. This covers
// the race where a delete finalizes between ImportRecords and our check
// — if we checked first we'd miss it; announcing first propagates the
// create through the vault-ctl FSM (which rejects it when tombstoned via the
// applyCreate guard), so by the time we re-check the tombstone state is
// authoritative and any on-disk files we wrote are orphans we must
// clean up explicitly.
func (o *Orchestrator) finalizeImportedChunk(vaultID glid.GLID, cm chunk.ChunkManager, meta chunk.ChunkMeta, isTombstoned func(chunk.ChunkID) bool) error {
	if meta.ID == (chunk.ChunkID{}) {
		return nil
	}

	if ann, ok := cm.(chunk.AnnouncerGetter); ok {
		if a := ann.GetAnnouncer(); a != nil {
			a.AnnounceCreate(meta.ID, meta.WriteStart, meta.IngestStart, meta.SourceStart)
			// Phase 3 (gastrolog-1huz5): Active → Sealing → Sealed in
			// quick succession. The imported chunk is already sealed on
			// the source; we're projecting that final state onto the
			// destination's FSM.
			a.AnnounceBeginSeal(meta.ID)
			a.AnnounceSeal(meta.ID, meta.WriteEnd, meta.RecordCount, meta.Bytes, meta.IngestStart, meta.IngestEnd, meta.SourceEnd, meta.IngestTSMonotonic)
			// Section offsets (CmdAttachOffsets) already replicated from
			// the original sealing leader via Raft; followers inherit
			// them through the FSM and don't need to re-announce. See
			// gastrolog-1dg3i.
		}
	}

	if isTombstoned != nil && isTombstoned(meta.ID) {
		if del, ok := cm.(chunk.SilentDeleter); ok {
			_ = del.DeleteSilent(meta.ID)
		} else {
			_ = cm.Delete(meta.ID)
		}
		o.vaultOpsLogger.Debug("replication: post-import tombstone detected, deleted just-created chunk",
			"vault", vaultID, "chunk", meta.ID.String())
		return nil
	}

	o.postSealWork(vaultID, cm, meta.ID)
	return nil
}

// drainIterator reads and discards all remaining records from an iterator.
// Used when skipping an import but the caller (network stream) still needs
// its data consumed.
func drainIterator(next chunk.RecordIterator) {
	for {
		if _, err := next(); err != nil {
			return
		}
	}
}

// SealActive seals the per-instance chunk manager's active chunk on the
// matching local vault. Returns the number of vaults sealed; no-op if the
// active chunk is empty or absent. Used by the SealVault RPC and the drain
// path to flush any residual active chunk before migration.
//
// Readiness: no Vault.ReadinessErr gate — seal operates on the in-memory
// active chunk, not the FSM. Seal is also a step on the drain path (which
// runs even with lagging followers), so gating here would create a
// chicken-and-egg deadlock with readiness recovery.
//
// After sealing, schedules the post-seal pipeline (compress, index, and
// sealed-chunk replication to followers) via schedulePostSeal.
func (o *Orchestrator) SealActive(vaultID glid.GLID) (int, error) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return 0, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}

	var sealed int
	vaultInst := vault.Instance
	if vaultInst == nil {
		return 0, nil
	}
	active := vaultInst.Chunks.Active()
	if active != nil && active.RecordCount > 0 {
		chunkID := active.ID
		if err := vaultInst.Chunks.Seal(); err != nil {
			return sealed, fmt.Errorf("seal vault %s: %w", vaultID, err)
		}
		sealed++
		o.schedulePostSeal(vaultID, vaultInst.Chunks, chunkID)
	}
	return sealed, nil
}

// --- Index ops ---

// IndexSizes returns the size in bytes for each index of a chunk.
func (o *Orchestrator) IndexSizes(vaultID glid.GLID, chunkID chunk.ChunkID) (map[string]int64, error) {
	_, im, err := o.findManagersForChunk(vaultID, chunkID)
	if err != nil {
		return nil, err
	}
	if im == nil {
		return map[string]int64{}, nil
	}
	return im.IndexSizes(chunkID), nil
}

// IndexesComplete reports whether all indexes exist for the given chunk.
func (o *Orchestrator) IndexesComplete(vaultID glid.GLID, chunkID chunk.ChunkID) (bool, error) {
	_, im, err := o.findManagersForChunk(vaultID, chunkID)
	if err != nil {
		return false, err
	}
	if im == nil {
		return false, nil
	}
	return im.IndexesComplete(chunkID)
}

// BuildIndexes builds all indexes for a sealed chunk.
func (o *Orchestrator) BuildIndexes(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID) error {
	_, im, err := o.findManagersForChunk(vaultID, chunkID)
	if err != nil {
		return err
	}
	if im == nil {
		return errors.New("no index manager for chunk vaultInst")
	}
	return im.BuildIndexes(ctx, chunkID)
}

// DeleteIndexes removes all indexes for a chunk.
func (o *Orchestrator) DeleteIndexes(vaultID glid.GLID, chunkID chunk.ChunkID) error {
	_, im, err := o.findManagersForChunk(vaultID, chunkID)
	if err != nil {
		return err
	}
	if im == nil {
		return nil
	}
	return im.DeleteIndexes(chunkID)
}

// --- Composite ---

// ChunkIndexInfos returns seal status and per-index info for a chunk.
func (o *Orchestrator) ChunkIndexInfos(vaultID glid.GLID, chunkID chunk.ChunkID) (*ChunkIndexReport, error) {
	cm, im, err := o.findManagersForChunk(vaultID, chunkID)
	if err != nil {
		return nil, err
	}

	meta, err := cm.Meta(chunkID)
	if err != nil {
		return nil, err
	}
	if im == nil {
		return nil, errors.New("no index manager for chunk vaultInst")
	}

	sizes := im.IndexSizes(chunkID)

	report := &ChunkIndexReport{
		Sealed:  meta.Sealed,
		Indexes: make([]IndexInfo, 0, 7),
	}

	// Token index
	if idx, err := im.OpenTokenIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "token", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["token"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "token"})
	}

	// Attr key index
	if idx, err := im.OpenAttrKeyIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "attr_key", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["attr_key"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "attr_key"})
	}

	// Attr value index
	if idx, err := im.OpenAttrValueIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "attr_val", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["attr_val"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "attr_val"})
	}

	// Attr kv index
	if idx, err := im.OpenAttrKVIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "attr_kv", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["attr_kv"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "attr_kv"})
	}

	// KV key index
	if idx, _, err := im.OpenKVKeyIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "kv_key", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["kv_key"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "kv_key"})
	}

	// KV value index
	if idx, _, err := im.OpenKVValueIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "kv_val", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["kv_val"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "kv_val"})
	}

	// KV combined index
	if idx, _, err := im.OpenKVIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "kv_kv", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["kv_kv"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "kv_kv"})
	}

	return report, nil
}

// NewAnalyzer returns an index analyzer backed by the vault's active (ingest)
// instance. For a specific chunk (possibly on a non-active instance), use
// NewAnalyzerForChunk.
func (o *Orchestrator) NewAnalyzer(vaultID glid.GLID) (*analyzer.Analyzer, error) {
	cm, im, err := o.activeManagers(vaultID)
	if err != nil {
		return nil, err
	}
	return analyzer.New(cm, im), nil
}

// NewAnalyzerForChunk returns an analyzer backed by the instance that owns chunkID.
func (o *Orchestrator) NewAnalyzerForChunk(vaultID glid.GLID, chunkID chunk.ChunkID) (*analyzer.Analyzer, error) {
	cm, im, err := o.findManagersForChunk(vaultID, chunkID)
	if err != nil {
		return nil, err
	}
	if im == nil {
		return nil, errors.New("no index manager for chunk vaultInst")
	}
	return analyzer.New(cm, im), nil
}
