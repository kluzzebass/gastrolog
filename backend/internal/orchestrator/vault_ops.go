package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"

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
// There is deliberately no neutral-named variant: every caller must
// choose local-vs-cluster explicitly.
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
// (VaultManifestEntriesIncludingOpen) so every voter sees the full cluster chunk
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

	var entries []vaultctlfsm.ManifestEntry
	if fsmEntries := o.VaultManifestEntriesIncludingOpen(vaultID); len(fsmEntries) > 0 {
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
		m := o.projectManifestEntry(vaultID, e)
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
			m = o.groundChunkMeta(vaultID, m)
			result = append(result, VaultChunkMeta{
				ChunkMeta: m,
				VaultID:   vaultInst.VaultID,
				VaultType: vaultType,
			})
		}
	}

	return result, nil
}

func (o *Orchestrator) projectManifestEntry(vaultID glid.GLID, e vaultctlfsm.ManifestEntry) chunk.ChunkMeta {
	m := e.ToChunkMeta()
	o.overlayPipelineChunkMetaBounds(vaultID, &m)
	return o.groundChunkMeta(vaultID, m)
}

// ErrNoVaultManifest reports that this node can read no manifest for a vault:
// it has not joined the vault's vault-ctl group and hosts no instance of it.
var ErrNoVaultManifest = errors.New("no manifest for vault on this node")

// ListClusterChunkMetasIncludingOpen returns the vault's chunk set, open
// chunks included, with each chunk listed once however many homes hold a
// copy. It answers identically on every vault-ctl voter, including nodes that
// host no instance of the vault — where ListAllChunkMetas reports nothing.
// On a node that hosts an instance it is ListAllChunkMetas, so the open-head
// exception applies there too.
func (o *Orchestrator) ListClusterChunkMetasIncludingOpen(vaultID glid.GLID) ([]chunk.ChunkMeta, error) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()

	if vault != nil && vault.Instance != nil {
		metas, err := o.ListAllChunkMetas(vaultID)
		if err != nil {
			return nil, err
		}
		out := make([]chunk.ChunkMeta, len(metas))
		for i, m := range metas {
			out[i] = m.ChunkMeta
		}
		return out, nil
	}

	f := o.vaultCtlFSMForVault(vaultID)
	if f == nil {
		if vault == nil {
			return nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
		}
		return nil, fmt.Errorf("%w: %s", ErrNoVaultManifest, vaultID)
	}
	entries := f.ListIncludingPipelineManifest()
	out := make([]chunk.ChunkMeta, len(entries))
	for i, e := range entries {
		out[i] = o.projectManifestEntry(vaultID, e)
	}
	return out, nil
}

// GetChunkMeta returns metadata for a specific chunk. The result is overlaid
// from the vault-ctl FSM if the chunk belongs to an instance with a Raft group, so
// CloudBacked / Archived reflect the cluster-wide truth rather than this
// node's local chunk-manager view.
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
			m = o.groundChunkMeta(vaultID, m)
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
			m = o.groundChunkMeta(vaultID, m)
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

// afterVaultCtlRestore is wired from vaultraft.FSM.SetOnAfterRestore and
// must return immediately — GroupManager.CreateGroup holds groupMgr.mu
// across NewRaft→fsm.Restore, and the deferred worker calls
// groupMgr.GetGroup. scheduleAfterVaultCtlRestore runs rewire +
// ReconcileFromSnapshot asynchronously.
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
	// Snapshot install swaps in new vaultctlfsm sub-FSM objects: bind the
	// vault and its instance to the replacements before reconciling.
	if g := o.vaultCtlGroup(vaultID); g != nil {
		o.bindVaultCtlGroup(vaultID, g)
	}
	o.mu.Lock()
	var t *VaultInstance
	if vault := o.vaults[vaultID]; vault != nil {
		t = vault.Instance
	}
	liveFSM := o.bindVaultInstanceToCtlLocked(vaultID, t)
	o.mu.Unlock()
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
	// for the snapshot-restore gap. No-op unless this node is the vault's
	// uploader.
	o.cloudUploadCatchupForVault(vaultID)
	o.vaultOpsLogger.Info("vault-ctl after-restore reconcile complete",
		"vault", vaultID, "has_instance", t != nil)
}

// onVaultCtlLeadGained wakes the chunking worker when this home becomes the
// vault-ctl leader. Planning and build passes run in the worker loop.
//
// Gaining vault-ctl leadership is also the upstream edge for the
// reconciler's leader-only categories: fire
// ReconcileMembershipCatchup so a late-joining leader pulls missing
// replicas, retires unrecoverable FSM entries, prunes nodes outside the
// placement, and retracts abandoned transfer announces immediately instead of
// waiting for the periodic backstop tick. Applies to every vault with a
// reconciler, not just pipeline ingest vaults — the replicated-vault
// missing-replica and outside-placement prune categories are exactly the ones a
// new leader must run. Dispatched on auxWg: the categories propose Raft
// applies that route to this new leader's apply queue, so they must not
// run on the leadership-tracking goroutine.
func (o *Orchestrator) onVaultCtlLeadGained(vaultID glid.GLID) {
	// Cloud-upload catch-up on leadership change: a chunk that sealed while
	// this node was not the vault's uploader never saw the live onSeal upload
	// effect here. This replaces the retired 5s backfill tick's discovery role
	// for the leadership-change case. Applies to both
	// type=cloud vaults (gated on vault-ctl leadership) and cloud-backed
	// pipeline vaults (gated on CloudStore/placement leadership); the catch-up
	// is a no-op unless this node is actually the uploader.
	o.cloudUploadCatchupForVault(vaultID)

	o.mu.RLock()
	var rec *VaultLifecycleReconciler
	if vault := o.vaults[vaultID]; vault != nil && vault.Instance != nil {
		rec = vault.Instance.Reconciler
	}
	o.mu.RUnlock()
	if rec != nil {
		o.auxWg.Go(rec.ReconcileMembershipCatchup)
	}

	if o.pipeline == nil || !o.isPipelineIngestVault(vaultID) {
		return
	}
	o.pipeline.NotifyChunkingVault(vaultID)
	o.pipeline.NotifyPublishRetry()
	o.pipeline.NotifyCollectionVault(vaultID)
}

// reconcilePipelineAfterCtlRestore rebinds pipeline managers to the live vault-ctl
// FSM after snapshot restore and, on a chunking home, runs chunk recover. Returns
// ErrUnknownVault when pipeline has not registered the vault on this node yet —
// caller must defer.
func (o *Orchestrator) reconcilePipelineAfterCtlRestore(vaultID glid.GLID) error {
	if o.pipeline == nil || !o.isPipelineIngestVault(vaultID) {
		return nil
	}
	if err := o.rewirePipelineAfterCtlRestore(vaultID); err != nil {
		return err
	}
	if !o.pipeline.ChunkingRegistered(vaultID) {
		return nil
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
// node rejoined via snapshot restore — the node bricked seconds after rejoin,
// every o.mu user queued forever. The goroutine simply blocks
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

// rewirePipelineAfterCtlRestore rebinds release consumption, chunking, and
// collection to the live vault-ctl sub-FSM. vaultraft.FSM.Restore allocates
// fresh sub-FSMs; pipeline managers otherwise keep reading the pre-restore
// object — the planner stalls and released segments are never purged.
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

// proposePruneNodeForVault proposes CmdPruneNode on the vault's chunk FSM
// after the vault-ctl Raft leader removed a node from the voter set: the
// node left the cluster, so its delete obligations and holder claims go
// with it. Proposed through the vault-ctl group itself rather than a local
// vault instance — the vault-ctl leader need not be one of the vault's
// homes. A failed propose is logged; the outside-placement prune on the
// vault's placement leader removes the same node once placement drops it.
func (o *Orchestrator) proposePruneNodeForVault(vaultID glid.GLID, removedNodeID string) {
	data := vaultraft.MarshalVaultChunkCommand(vaultID, vaultctlfsm.MarshalPruneNode(removedNodeID))
	if err := o.ApplyVaultControlPlane(vaultID, data); err != nil {
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

// SealActive seals every open chunk of a vault that holds records — the
// chunk manager's active chunk and, for a pipeline vault, the open chunk
// manifest — and returns how many it sealed. A vault can carry both at once
// (a recovered chunk-manager active chunk beside the pipeline's open
// manifest), and each is sealed independently. Used by the SealVault RPC.
//
// When the chunk-manager seal succeeds and the manifest seal fails, the
// returned count still reports the chunk that was sealed alongside the error.
//
// Readiness: no Vault.ReadinessErr gate — seal operates on the in-memory
// active chunk, not the FSM.
func (o *Orchestrator) SealActive(vaultID glid.GLID) (int, error) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return 0, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}

	sealed, err := o.sealChunkManagerActive(vaultID, vault.Instance)
	if err != nil {
		return 0, err
	}
	manifestSealed, err := o.sealOpenManifest(vaultID)
	if err != nil {
		return sealed, err
	}
	return sealed + manifestSealed, nil
}

// sealChunkManagerActive seals the chunk manager's active chunk when it holds
// records, and schedules the post-seal pipeline (compress, index, upload).
// That scheduling is mandatory, not
// an optimization: Chunks.Seal() only announces Active → Sealing, and the
// post-seal pipeline owns the AnnounceSeal that promotes the vault-ctl
// manifest entry to Sealed.
func (o *Orchestrator) sealChunkManagerActive(vaultID glid.GLID, vaultInst *VaultInstance) (int, error) {
	if vaultInst == nil {
		return 0, nil
	}
	active := vaultInst.Chunks.Active()
	if active == nil || active.RecordCount == 0 {
		return 0, nil
	}
	chunkID := active.ID
	if err := vaultInst.Chunks.Seal(); err != nil {
		return 0, fmt.Errorf("seal vault %s: %w", vaultID, err)
	}
	o.schedulePostSeal(vaultID, vaultInst.Chunks, chunkID)
	return 1, nil
}

// sealOpenManifest seals a pipeline vault's open chunk manifest when it holds
// records. Only the chunking leader may commit the seal; reaching any other
// node while the manifest holds records is ErrNotChunkingLeader, never a
// silent zero.
func (o *Orchestrator) sealOpenManifest(vaultID glid.GLID) (int, error) {
	if !o.isPipelineIngestVault(vaultID) {
		return 0, nil
	}
	o.mu.RLock()
	pl := o.pipeline
	o.mu.RUnlock()
	if pl == nil {
		return 0, nil
	}
	sealed, err := pl.SealOpenChunk(vaultID)
	switch {
	case err == nil:
		if sealed {
			return 1, nil
		}
		return 0, nil
	case errors.Is(err, chunking.ErrNotLeader):
		return 0, fmt.Errorf("%w: vault %s", ErrNotChunkingLeader, vaultID)
	case errors.Is(err, chunking.ErrUnknownVault):
		if o.openManifestHoldsRecords(vaultID) {
			return 0, fmt.Errorf("%w: vault %s is not homed on this node", ErrNotChunkingLeader, vaultID)
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("seal open chunk of vault %s: %w", vaultID, err)
	}
}

// openManifestHoldsRecords reads the vault-ctl FSM, which every node holds,
// for an open chunk manifest with records.
func (o *Orchestrator) openManifestHoldsRecords(vaultID glid.GLID) bool {
	fsm, _, _, ok := o.vaultCtlHandle(vaultID)
	if !ok {
		return false
	}
	open := fsm.OpenChunk()
	return open != nil && open.TotalRecords > 0
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
