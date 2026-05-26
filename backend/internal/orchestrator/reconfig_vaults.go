package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/cluster"
	"gastrolog/internal/index"
	"gastrolog/internal/lifecycle"
	"gastrolog/internal/query"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
	"gastrolog/internal/vaultraft"

	hraft "github.com/hashicorp/raft"
)

// resolveVaultDir resolves a file vault's "dir" parameter relative to vaultsDir.
// If dir is empty, defaults to "vaults/<vaultName>". Relative paths are joined
// with vaultsDir (which defaults to homeDir when --vaults is not set). The
// returned map is always a new copy — the caller's params are never mutated.
// The stored config retains the original relative path so each node resolves
// independently against its own directory.
func resolveVaultDir(params map[string]string, vaultsDir, vaultID string) map[string]string {
	dir := params["dir"]
	if dir == "" {
		dir = filepath.Join("vaults", vaultID)
	}
	if !filepath.IsAbs(dir) && vaultsDir != "" {
		dir = filepath.Join(vaultsDir, dir)
	}
	out := maps.Clone(params)
	if out == nil {
		out = make(map[string]string)
	}
	out["dir"] = dir
	return out
}

// AddVault adds a new vault (chunk manager, index manager, query engine) and updates the filter set.
// Loads the full config internally to resolve the vault's vault IDs to vault configs.
// Returns ErrDuplicateID if a vault with this ID already exists.
func (o *Orchestrator) AddVault(ctx context.Context, vaultCfg system.VaultConfig, factories Factories) error {
	sys, err := o.loadSystem(ctx)
	if err != nil {
		return fmt.Errorf("load config for AddVault: %w", err)
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if _, exists := o.vaults[vaultCfg.ID]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateID, vaultCfg.ID)
	}

	instance, err := o.buildVaultInstance(sys, vaultCfg, factories)
	if err != nil {
		return fmt.Errorf("build vault instance for %s: %w", vaultCfg.ID, err)
	}

	// Register vault (even when no instance exists locally — placements may
	// land on this node later via handleVaultPut).
	vault := NewVault(vaultCfg.ID, instance)
	vault.Name = vaultCfg.Name
	vault.StorageType = string(vaultCfg.Type)
	vault.WriteModel = vaultCfg.ResolveWriteModel()
	o.vaults[vaultCfg.ID] = vault

	// Recompile the routing table immediately so the vault can receive
	// records right away. The rotation sweep also reconciles every 15s
	// as a safety net.
	if sys != nil {
		_ = o.reloadRoutesFromConfig(sys)
	}

	// Rotation and retention are reconciled by the discovery-based sweep
	// jobs on their next tick.

	o.logger.Info("vault added", "id", vaultCfg.ID, "name", vaultCfg.Name, "has_instance", instance != nil)
	return nil
}

// Rotation and retention are handled by discovery-based sweep jobs
// (rotationSweep and retentionSweepAll). No per-vault setup needed during AddVault.

func findVaultConfig(vaults []system.VaultConfig, id glid.GLID) *system.VaultConfig {
	for i := range vaults {
		if vaults[i].ID == id {
			return &vaults[i]
		}
	}
	return nil
}

// resolveRetentionRulesFromVault converts vault retention rules to resolved retentionRule objects.
func resolveRetentionRulesFromVault(cfg *system.Config, vaultCfg system.VaultConfig) ([]retentionRule, error) {
	// Phase 4 (gastrolog-42f9z): retention rules carry only the trigger
	// policy. The action enum is gone — every fired event streams records
	// through the routing engine and always destroys the chunk.
	var rules []retentionRule
	for _, b := range vaultCfg.RetentionRules {
		retCfg := findRetentionPolicy(cfg.RetentionPolicies, b.RetentionPolicyID)
		if retCfg == nil {
			return nil, fmt.Errorf("vault %s references unknown retention policy: %s", vaultCfg.ID, b.RetentionPolicyID)
		}
		policy, err := retCfg.ToRetentionPolicy()
		if err != nil {
			return nil, fmt.Errorf("invalid retention policy %s for vault %s: %w", b.RetentionPolicyID, vaultCfg.ID, err)
		}
		if policy == nil {
			continue
		}

		// Phase 4 (gastrolog-42f9z): retention has no decision layer
		// anymore. A fired retention event always streams the chunk's
		// records through the routing engine and always destroys the
		// chunk. The retention rule carries only the trigger policy.
		rules = append(rules, retentionRule{
			policy: policy,
		})
	}
	return rules, nil
}

// RemoveVault removes a vault if it's empty (no chunks with data).
// Returns ErrVaultNotFound if the vault doesn't exist.
// Returns ErrVaultNotEmpty if the vault has any chunks.
//
// Authority: the vault-ctl FSM manifest is canonical for whether a
// vault holds data. Per audit finding F3 (gastrolog-4vym6), the
// emptiness check consults the FSM first; the local Chunks.List()
// view is corroborating evidence at most (a sync-lagged follower
// or post-recovery node may have less on disk than the FSM
// records). If either source reports non-empty, removal is refused
// — defense in depth against either an FSM read failure or a
// stale-disk-but-fresh-FSM state.
func (o *Orchestrator) RemoveVault(id glid.GLID) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	vault, exists := o.vaults[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, id)
	}

	// Primary: FSM manifest. Returns (nil, nil) when the FSM isn't
	// reachable from this node (memory-mode or no GroupManager); we
	// fall through to the local check in that case rather than
	// quietly allowing removal of a vault we can't authoritatively
	// inspect.
	fsmMetas, err := o.listClusterChunkMetasLocked(id)
	if err != nil {
		return fmt.Errorf("list FSM chunks for vault %s: %w", id, err)
	}
	for _, m := range fsmMetas {
		if m.RecordCount > 0 || !m.Sealed {
			return fmt.Errorf("%w: vault %s has data in FSM manifest", ErrVaultNotEmpty, id)
		}
	}

	// Corroborating: local disk view. Catches the inverse case where
	// the FSM has caught up to empty but residual chunks linger on
	// disk (orphans, partial repatriation, etc.) — gastrolog-3y8py
	// already preserves data-bearing orphans, so this is the operator-
	// safe stance.
	if vaultInst := vault.Instance; vaultInst != nil {
		metas, err := vaultInst.Chunks.List()
		if err != nil {
			return fmt.Errorf("list chunks for vault %s: %w", id, err)
		}
		for _, m := range metas {
			if m.RecordCount > 0 || !m.Sealed {
				return fmt.Errorf("%w: vault %s has data on local disk", ErrVaultNotEmpty, id)
			}
		}
		if active := vaultInst.Chunks.Active(); active != nil {
			return fmt.Errorf("%w: vault %s has active chunk", ErrVaultNotEmpty, id)
		}
	}

	o.teardownVault(id, vault)
	o.logger.Info("vault removed", "id", id, "name", vault.Name, "type", vault.Type())
	return nil
}

// listClusterChunkMetasLocked is the FSM-manifest variant of
// ListClusterChunkMetas — same body, but assumes the orchestrator
// mutex is already held (RemoveVault holds o.mu for the full
// duration). Inlined to avoid a recursive lock when reading FSM
// chunk metadata as part of mutation guards.
func (o *Orchestrator) listClusterChunkMetasLocked(vaultID glid.GLID) ([]chunk.ChunkMeta, error) {
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

// removeVaultJobs removes retention runners and cron rotation jobs for a vault
// without closing managers or unregistering. Used by UnregisterVault and drain.
func (o *Orchestrator) removeVaultJobs(id glid.GLID, vault *Vault) {
	if vaultInst := vault.Instance; vaultInst != nil {
		delete(o.retention, retentionKey(vaultInst.VaultID, vaultInst.StorageID))
	}
	o.cronRotation.removeAllForVault(id)
}

// teardownVault performs the common cleanup for all vault removal paths:
// cancels pending jobs, closes managers, removes from registry, rebuilds filters.
func (o *Orchestrator) teardownVault(id glid.GLID, vault *Vault) {
	o.destroyVaultControlPlaneRaftGroup(id)

	// Cancel pending post-seal/compress/index jobs to prevent use-after-close.
	vaultPrefix := id.String()
	o.scheduler.RemoveJobsByPrefix("post-seal:" + vaultPrefix)
	o.scheduler.RemoveJobsByPrefix("compress:" + vaultPrefix)
	o.scheduler.RemoveJobsByPrefix("index-build:" + vaultPrefix)

	// Remove per-instance retention runner and cron rotation jobs.
	if vaultInst := vault.Instance; vaultInst != nil {
		delete(o.retention, retentionKey(vaultInst.VaultID, vaultInst.StorageID))
	}
	o.cronRotation.removeAllForVault(id)

	// Close chunk/index managers to release file locks.
	if err := vault.Close(); err != nil {
		o.logger.Warn("failed to close vault during teardown", "vault", id, "error", err)
	}

	delete(o.vaults, id)
	o.rebuildRouteSetLocked()
}

// DisableVault disables ingestion for a vault.
// Disabled vaults will not receive new records from the ingest pipeline.
// Returns ErrVaultNotFound if the vault doesn't exist.
func (o *Orchestrator) DisableVault(id glid.GLID) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	vault, exists := o.vaults[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, id)
	}

	vault.Enabled = false
	o.logger.Info("vault disabled", "id", id, "name", vault.Name, "type", vault.Type())
	return nil
}

// EnableVault enables ingestion for a vault.
// Returns ErrVaultNotFound if the vault doesn't exist.
func (o *Orchestrator) EnableVault(id glid.GLID) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	vault, exists := o.vaults[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, id)
	}

	vault.Enabled = true
	o.logger.Info("vault enabled", "id", id, "name", vault.Name, "type", vault.Type())
	return nil
}

// IsVaultEnabled returns whether ingestion is enabled for the given vault.
func (o *Orchestrator) IsVaultEnabled(id glid.GLID) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if vault := o.vaults[id]; vault != nil {
		return vault.Enabled
	}
	return false
}

// ForceRemoveVault removes a vault regardless of whether it contains data.
// It seals the active chunk if present, deletes all indexes and chunks,
// closes the chunk manager, and cleans up all associated resources.
// Returns ErrVaultNotFound if the vault doesn't exist.
func (o *Orchestrator) ForceRemoveVault(id glid.GLID) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	vault, exists := o.vaults[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, id)
	}
	// Seal active chunk and delete all data on this node's instance.
	if err := o.forceRemoveVaultData(id, vault.Instance); err != nil {
		return err
	}

	o.teardownVault(id, vault)
	o.logger.Info("vault force-removed", "id", id, "name", vault.Name, "type", vault.Type())
	return nil
}

// forceRemoveVaultData seals the active chunk (if any), deletes all
// indexes, and locally drops every chunk on the given instance. LOCAL
// cleanup only — uses chunk.DeleteNoAnnounce so per-chunk deletes do not
// fan across vault-ctl Raft. See gastrolog-4vz40 / sealAndDeleteAllChunks.
func (o *Orchestrator) forceRemoveVaultData(id glid.GLID, vaultInst *VaultInstance) error {
	if vaultInst == nil {
		return nil
	}
	cm := vaultInst.Chunks
	im := vaultInst.Indexes

	if active := cm.Active(); active != nil {
		if err := cm.Seal(); err != nil {
			return fmt.Errorf("seal active chunk for vault %s: %w", id, err)
		}
	}

	metas, err := cm.List()
	if err != nil {
		return fmt.Errorf("list chunks for vault %s: %w", id, err)
	}
	for _, meta := range metas {
		if im != nil {
			if err := im.DeleteIndexes(meta.ID); err != nil {
				o.logger.Warn("failed to delete indexes during force remove",
					"vault", id, "chunk", meta.ID.String(), "error", err)
			}
		}
		if err := chunk.DeleteNoAnnounce(cm, meta.ID); err != nil {
			return fmt.Errorf("delete chunk %s in vault %s: %w", meta.ID.String(), id, err)
		}
	}
	return nil
}

// sealAndDeleteAllChunks seals the active chunk (if any), then deletes all
// chunks and their indexes. Returns the number of chunks found. Errors are
// logged with the given prefix but do not abort the cleanup.
//
// CRITICAL: this is a LOCAL cleanup path. It MUST use DeleteNoAnnounce so
// each chunk delete does not fire AnnounceDelete → CmdDeleteChunk on the
// vault-ctl Raft. The announcement would propagate to every voter and trigger
// FSM.applyDelete + onDelete on each node, physically wiping the chunk
// across the entire cluster. The intended cluster-wide effect (when
// RemoveVaultInstance reacts to placement loss, or DeleteVaultInstance
// reacts to an admin teardown) comes from each node independently running
// its own RemoveVaultInstance as the config change propagates — not from
// per-chunk delete announcements out of one node. See gastrolog-4vz40.
func (o *Orchestrator) sealAndDeleteAllChunks(vaultInst *VaultInstance, op string, vaultID glid.GLID) int {
	if active := vaultInst.Chunks.Active(); active != nil {
		if err := vaultInst.Chunks.Seal(); err != nil {
			o.logger.Warn(op+": seal failed", "vault", vaultID, "error", err)
		}
	}
	metas, err := vaultInst.Chunks.List()
	if err != nil {
		o.logger.Warn(op+": list failed", "vault", vaultID, "error", err)
		return 0
	}
	for _, m := range metas {
		if vaultInst.Indexes != nil {
			if err := vaultInst.Indexes.DeleteIndexes(m.ID); err != nil {
				o.logger.Warn(op+": delete indexes failed", "vault", vaultID, "chunk", m.ID, "error", err)
			}
		}
		if err := chunk.DeleteNoAnnounce(vaultInst.Chunks, m.ID); err != nil {
			o.logger.Warn(op+": delete chunk failed", "vault", vaultID, "chunk", m.ID, "error", err)
		}
	}
	return len(metas)
}

// RemoveVaultInstance unregisters a vault instance from this node WITHOUT
// destroying its on-disk data. Used when placement moves the instance elsewhere
// (transient — the node may well get the instance back seconds later when
// placement flaps back). The instance's Chunks/Indexes managers are closed, jobs
// are cancelled, and the VaultInstance is removed from the vault's vault list,
// but the chunk files and vault directory remain on disk. A subsequent
// AddVaultInstance will re-discover the existing chunks.
//
// For actual instance deletion (admin-driven), use DeleteVaultInstance which
// additionally wipes all chunks and removes the data directory.
//
// Returns true if an instance was removed.
//
// gastrolog-4vz40: previously this function always wiped chunks, which meant
// any placement flap (caused by transient peer-conn teardowns from
// peers.Invalidate) destroyed data cluster-wide. The destructive behaviour is
// now opt-in via DeleteVaultInstance.
func (o *Orchestrator) RemoveVaultInstance(vaultID glid.GLID) bool {
	return o.removeVaultInstance(vaultID, false)
}

// DeleteVaultInstance unregisters a vault instance AND permanently wipes its
// on-disk data (chunks, indexes, and the vault directory). Used only when a
// instance is being deliberately deleted (admin action via CmdDeleteVault, or
// post-drain cleanup).
//
// Returns true if an instance was removed.
func (o *Orchestrator) DeleteVaultInstance(vaultID glid.GLID) bool {
	return o.removeVaultInstance(vaultID, true)
}

func (o *Orchestrator) removeVaultInstance(vaultID glid.GLID, deleteData bool) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	vault, exists := o.vaults[vaultID]
	if !exists {
		return false
	}

	vaultInst := vault.Instance
	if vaultInst == nil {
		return false
	}

	// Cancel pending jobs for this vault.
	prefix := vaultID.String()
	o.scheduler.RemoveJobsByPrefix("post-seal:" + prefix + ":" + vaultID.String())
	o.scheduler.RemoveJobsByPrefix("compress:" + prefix + ":" + vaultID.String())
	o.scheduler.RemoveJobsByPrefix("index-build:" + prefix + ":" + vaultID.String())

	// Destructive cleanup — only on explicit deletion.
	if deleteData {
		o.sealAndDeleteAllChunks(vaultInst, "remove vault placement", vaultID)
	}

	// Drop FSM → chunk-manager hooks before Close. Placement can remove the
	// instance while the vault control-plane Raft group still receives
	// CmdDeleteChunk from the leader; without clearing, onDelete would call
	// DeleteSilent on an already-closed file.Manager.
	o.clearVaultFSMChunkCallbacks(vaultID)

	// Close managers.
	if err := vaultInst.Chunks.Close(); err != nil {
		o.logger.Warn("remove vault placement: close chunk manager failed", "vault", vaultID, "error", err)
	}

	// Remove the vault's data directory entirely — not just its chunk subdirs.
	// Without this, leftover files (.lock, cloud.idx) and the directory itself
	// accumulate on disk forever. See gastrolog-42j4n.
	if deleteData {
		if remover, ok := vaultInst.Chunks.(chunk.DirRemover); ok {
			if err := remover.RemoveDir(); err != nil {
				o.logger.Warn("remove vault placement: remove data directory failed", "vault", vaultID, "error", err)
			}
		}
	}

	// Remove retention runner and cron rotation for this vault.
	delete(o.retention, retentionKey(vaultInst.VaultID, vaultInst.StorageID))
	o.cronRotation.removeAllForVault(vaultID)

	// Drop the instance from the vault.
	vault.Instance = nil

	// On destructive removal, drop the vault entry entirely. For
	// non-destructive placement-driven removals, the vault shell stays so a
	// subsequent AddVaultInstance can rehydrate.
	if deleteData {
		delete(o.vaults, vaultID)
		o.rebuildRouteSetLocked()
		o.logger.Info("vault removed", "vault", vaultID)
	}

	o.logger.Info("vault placement removed",
		"vault", vaultID, "deleteData", deleteData)
	return true
}

// AddVaultInstance builds the single VaultInstance for an already-registered
// vault. The orchestrator picks up storage/policy fields from the VaultConfig.
func (o *Orchestrator) AddVaultInstance(ctx context.Context, vaultID glid.GLID, factories Factories) error {
	sys, err := o.loadSystem(ctx)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	o.mu.Lock()
	vault, exists := o.vaults[vaultID]
	if !exists {
		o.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	// Already present?
	if vault.Instance != nil && vault.Instance.VaultID == vaultID {
		o.mu.Unlock()
		return nil
	}
	o.mu.Unlock()

	cfg := &sys.Config
	rt := &sys.Runtime

	vaultCfg := findVaultConfig(cfg.Vaults, vaultID)
	if vaultCfg == nil {
		return fmt.Errorf("vault %s not found in config", vaultID)
	}

	o.ensureVaultControlPlaneRaftGroup(vaultID, rt.Nodes, factories)

	nscs := rt.NodeStorageConfigs
	placements := vaultCfg.Placements
	leaderNodeID := system.LeaderNodeID(placements, nscs)
	followerNodeIDs := system.FollowerNodeIDs(placements, nscs)
	isLeader := leaderNodeID == "" || leaderNodeID == o.localNodeID
	isFollower := slices.Contains(followerNodeIDs, o.localNodeID)
	if !isLeader && !isFollower {
		// No storage placement, but still join the vault control-plane Raft
		// group as a voter for this vault's replicated instance metadata.
		o.ensureVaultCtlMetadata(*vaultCfg, rt.Nodes, factories)
		return nil
	}

	var ti *VaultInstance
	if isLeader {
		t, err := o.buildLeaderInstance(sys, *vaultCfg, factories)
		if err != nil {
			return fmt.Errorf("build vault %s: %w", vaultID, err)
		}
		t.FollowerTargets = system.FollowerTargets(placements, nscs)
		ti = t
	} else {
		for _, tgt := range system.FollowerTargets(placements, nscs) {
			if tgt.NodeID != o.localNodeID {
				continue
			}
			t, err := o.buildInstanceForStorage(sys, *vaultCfg, factories, tgt.StorageID, true)
			if err != nil {
				return fmt.Errorf("build vault %s storage %s: %w", vaultID, tgt.StorageID, err)
			}
			t.IsFollower = true
			t.LeaderNodeID = leaderNodeID
			t.StorageID = tgt.StorageID
			t.Chunks.SetRotationPolicy(chunk.NeverRotatePolicy{})
			ti = t
			break
		}
	}

	if ti == nil {
		return nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	vault = o.vaults[vaultID]
	if vault == nil {
		_ = ti.Chunks.Close()
		return fmt.Errorf("%w: %s (disappeared during build)", ErrVaultNotFound, vaultID)
	}
	if vault.Instance != nil {
		// Race: someone built the instance ahead of us. Discard the
		// duplicate so we don't leak managers / file handles.
		_ = ti.Chunks.Close()
		return nil
	}
	vault.Instance = ti
	o.logger.Info("vault placement added", "vault", vaultID)
	return nil
}

// UnregisterVault removes a vault from the orchestrator without deleting any
// data. The chunk manager is closed (releasing connections/locks) but chunks
// and indexes are left intact in storage. This is the correct operation for
// cloud vault reassignment — the data lives in shared object storage and the
// new node will open a fresh Manager pointing to the same bucket.
func (o *Orchestrator) UnregisterVault(id glid.GLID) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	vault, exists := o.vaults[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, id)
	}

	// Cancel pending post-seal/compress/index jobs before closing the chunk manager.
	vaultPrefix := id.String()
	o.scheduler.RemoveJobsByPrefix("post-seal:" + vaultPrefix)
	o.scheduler.RemoveJobsByPrefix("compress:" + vaultPrefix)
	o.scheduler.RemoveJobsByPrefix("index-build:" + vaultPrefix)

	// Stop vault control-plane Raft before closing chunk managers — same
	// ordering as teardownVault. Otherwise trailing CmdDeleteChunk applies can
	// fire onDelete against a closed Manager.
	o.destroyVaultControlPlaneRaftGroup(id)

	if err := vault.Close(); err != nil {
		o.logger.Warn("failed to close vault during unregister",
			"vault", id, "error", err)
	}

	// Remove per-vault retention and rotation jobs.
	o.removeVaultJobs(id, vault)

	// Remove from registry.
	delete(o.vaults, id)
	o.rebuildRouteSetLocked()

	o.logger.Info("vault unregistered (data preserved)", "id", id, "name", vault.Name, "type", vault.Type())
	return nil
}

// VaultConfig returns the effective configuration for a vault.
// This is useful for API responses and debugging.
func (o *Orchestrator) VaultConfig(id glid.GLID) (system.VaultConfig, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if _, exists := o.vaults[id]; !exists {
		return system.VaultConfig{}, fmt.Errorf("%w: %s", ErrVaultNotFound, id)
	}

	cfg := system.VaultConfig{
		ID: id,
	}

	return cfg, nil
}

// gastrolog-4kkoo (Phase 5): UpdateVaultFilter is gone. Vaults no
// longer carry filters of their own — match expressions live inline on
// route stages. Operators change routing by editing the route, which
// triggers a NotifyRoutePut → ReloadFilters → reloadRoutesFromConfig
// cycle. The Phase-4 ergonomic API was test-only; no production caller
// existed.

// buildVaultInstances creates VaultInstance objects for each instance in the vault config.
// Every node joins every instance's Raft group regardless of storage placement
// (gastrolog-292yi). Nodes with storage placements also get a VaultInstance with
// a chunk manager; nodes without storage only participate in the Raft group.
func (o *Orchestrator) buildVaultInstance(sys *system.System, vaultCfg system.VaultConfig, factories Factories) (*VaultInstance, error) {
	rt := &sys.Runtime
	o.ensureVaultControlPlaneRaftGroup(vaultCfg.ID, rt.Nodes, factories)

	vaultID := vaultCfg.ID

	// Determine this node's role for this vault. With one-replica-per-node
	// (Phase 2 invariant) the node is at most one of: leader, follower, neither.
	nscs := rt.NodeStorageConfigs
	placements := vaultCfg.Placements
	leaderNodeID := system.LeaderNodeID(placements, nscs)
	followerNodeIDs := system.FollowerNodeIDs(placements, nscs)
	isLeader := leaderNodeID == "" || leaderNodeID == o.localNodeID
	isFollower := slices.Contains(followerNodeIDs, o.localNodeID)
	if !isLeader && !isFollower {
		// No storage placement on this node, but still join the vault
		// control-plane Raft group as a voter for replicated instance metadata.
		o.ensureVaultCtlMetadata(vaultCfg, rt.Nodes, factories)
		return nil, nil
	}

	if isLeader {
		ti, err := o.buildLeaderInstance(sys, vaultCfg, factories)
		if err != nil {
			o.alertVaultInitFailed(vaultID, vaultCfg.Name, err)
			return nil, nil
		}
		ti.FollowerTargets = system.FollowerTargets(placements, nscs)
		return ti, nil
	}

	// Follower: build the instance for this node's placement.
	for _, tgt := range system.FollowerTargets(placements, nscs) {
		if tgt.NodeID != o.localNodeID {
			continue
		}
		sti, err := o.buildInstanceForStorage(sys, vaultCfg, factories, tgt.StorageID, true)
		if err != nil {
			o.alertVaultInitFailed(vaultID, vaultCfg.Name, err)
			return nil, nil
		}
		sti.IsFollower = true
		sti.LeaderNodeID = leaderNodeID
		sti.StorageID = tgt.StorageID
		sti.Chunks.SetRotationPolicy(chunk.NeverRotatePolicy{})
		return sti, nil
	}
	return nil, nil
}

// alertVaultInitFailed logs a warning and raises an alert when a vault
// instance fails to initialize during build. The failed instance is retried on
// the next reconfig cycle. See gastrolog-68fqk.
func (o *Orchestrator) alertVaultInitFailed(vaultID glid.GLID, vaultName string, err error) {
	o.logger.Warn("buildVaultInstances: vaultInst init failed, skipping",
		"vault", vaultID, "name", vaultName, "error", err)
	if o.alerts != nil {
		o.alerts.Set(
			fmt.Sprintf("vault-init:%s", vaultID),
			alert.Error, "orchestrator",
			fmt.Sprintf("Vault %q failed to initialize: %v", vaultName, err),
		)
	}
}

// buildLeaderInstance creates the leader VaultInstance using the placement's
// storage ID. This avoids directory collisions with same-node follower placements
// that would occur if findLocalFileStorage picked a different storage by class.
func (o *Orchestrator) buildLeaderInstance(sys *system.System, vaultCfg system.VaultConfig, factories Factories) (*VaultInstance, error) {
	// Read placements from VaultConfig (mirrored from vault placements via
	// the FSM bridge — gastrolog-257l7).
	storageID := system.LeaderStorageID(vaultCfg.Placements)
	if storageID != "" && !strings.HasPrefix(storageID, system.SyntheticStoragePrefix) {
		ti, err := o.buildInstanceForStorage(sys, vaultCfg, factories, storageID, false)
		if err != nil {
			return nil, err
		}
		ti.StorageID = storageID
		return ti, nil
	}
	// Synthetic or unplaced — fall back to class-based resolution.
	ti, err := o.buildInstance(sys, vaultCfg, factories, false)
	if err != nil {
		return nil, err
	}
	ti.StorageID = storageID
	return ti, nil
}

// buildInstance creates a single VaultInstance from a VaultConfig.
// When isFollower is true, cloud backing params are stripped so the follower's
// PostSealProcess only runs compress + index without uploading to cloud storage.
// Cloud-backed vaults share a blob key (vault-ID/chunk-ID.glcb) — if the follower
// also uploads, it overwrites the leader's blob with a different-sized version,
// corrupting the leader's stored diskBytes and breaking all future cloud reads.
func (o *Orchestrator) buildInstance(sys *system.System, vaultCfg system.VaultConfig, factories Factories, isFollower bool) (*VaultInstance, error) {
	cfg := &sys.Config
	rt := &sys.Runtime
	factoryName := mapVaultTypeToFactory(vaultCfg.Type)

	// Create the vault-ctl Raft group BEFORE the chunk manager. Group creation is
	// fast (Raft log replay). Chunk manager creation is slow (scans disk for
	// existing chunks). Starting the Raft group early gives it time to elect
	// a leader and catch up while the chunk manager is loading.
	vaultGroup, applier, raftCB := o.ensureVaultCtlMetadata(vaultCfg, rt.Nodes, factories)

	// Build params from instance system.
	params := buildVaultParams(sys, vaultCfg, o.localNodeID)

	// Followers keep cloud store access for reads (queries) but skip uploads.
	// The leader owns the blob; the follower adopts it via RegisterCloudChunk
	// when the vault-ctl FSM propagates the upload announcement.
	if isFollower {
		params["_cloud_read_only"] = "true"
	}

	cmFactory, ok := factories.ChunkManagers[factoryName]
	if !ok {
		return nil, fmt.Errorf("unknown chunk manager type: %s (mapped from vaultInst type %s)", factoryName, vaultCfg.Type)
	}

	var cmLogger = factories.Logger
	if cmLogger != nil {
		cmLogger = cmLogger.With("vault", vaultCfg.ID)
	}

	cmParams := resolveVaultDir(params, factories.VaultsDir, vaultCfg.ID.String())
	cmParams["_expect_existing"] = "true"
	cmParams["_vault_id"] = vaultCfg.ID.String()

	// Resolve JSONL path relative to HomeDir (not VaultsDir).
	if p := cmParams["path"]; p != "" && !filepath.IsAbs(p) && factories.HomeDir != "" {
		cmParams["path"] = filepath.Join(factories.HomeDir, p)
	}

	cm, err := cmFactory(cmParams, cmLogger)
	if err != nil {
		return nil, fmt.Errorf("create chunk manager: %w", err)
	}

	// Apply rotation policy from instance.
	if err := applyRotationPolicy(cm, cfg.RotationPolicies, vaultCfg.RotationPolicyID); err != nil {
		_ = cm.Close()
		return nil, err
	}

	// Wire the Raft announcer now that both the group and chunk manager exist.
	setVaultRaftAnnouncer(cm, applier, o.phase, o.logger)
	// Wire the FSM-backed integrity verifier so cold-cache cloud downloads
	// reject blobs whose digest doesn't match what the leader stamped at
	// upload time. gastrolog-grnc3.
	setIntegrityVerifier(cm, o.IntegrityVerifier())

	// JSONL sinks are write-only — no query engine, no indexes.
	if vaultCfg.Type == system.VaultTypeJSONL {
		ti := &VaultInstance{
			VaultID: vaultCfg.ID,
			Type:    string(vaultCfg.Type),
			Chunks:  cm,
		}
		ti.applyRaftCallbacks(raftCB)
		o.attachLifecycleReconciler(ti, vaultCfg.ID, vaultGroup)
		wireVaultFSMOnDelete(vaultGroup, vaultCfg.ID, cm, nil, o, o.logger)
		return ti, nil
	}

	imFactory, ok := factories.IndexManagers[factoryName]
	if !ok {
		_ = cm.Close()
		return nil, fmt.Errorf("unknown index manager type: %s (mapped from vaultInst type %s)", factoryName, vaultCfg.Type)
	}
	var imLogger = factories.Logger
	if imLogger != nil {
		imLogger = imLogger.With("vault", vaultCfg.ID)
	}
	im, err := imFactory(cmParams, cm, imLogger)
	if err != nil {
		_ = cm.Close()
		return nil, fmt.Errorf("create index manager: %w", err)
	}

	var qeLogger = factories.Logger
	if qeLogger != nil {
		qeLogger = qeLogger.With("vault", vaultCfg.ID)
	}
	qe := query.New(cm, im, qeLogger)

	// Inject index builders into the chunk manager's post-seal pipeline.
	if processor, ok := cm.(chunk.ChunkPostSealProcessor); ok {
		processor.SetIndexBuilders([]chunk.ChunkIndexBuilder{im.BuildAdapter()})
	}

	ti := &VaultInstance{
		VaultID: vaultCfg.ID,
		Type:    string(vaultCfg.Type),
		Chunks:  cm,
		Indexes: im,
		Query:   qe,
	}
	ti.applyRaftCallbacks(raftCB)
	o.attachLifecycleReconciler(ti, vaultCfg.ID, vaultGroup)
	wireVaultFSMOnDelete(vaultGroup, vaultCfg.ID, cm, im, o, o.logger)
	wireVaultFSMOnUpload(vaultGroup, vaultCfg.ID, cm, o, o.logger)
	return ti, nil
}

// attachLifecycleReconciler constructs a VaultLifecycleReconciler for the given
// vault instance and binds it to the instance sub-FSM in the vault control-plane
// Raft group. Skipped silently when there is no group (memory-mode vaults
// without replication) — single-node deletes go straight through the chunk
// manager via deleteChunk's local-only fallback. See gastrolog-51gme.
//
// Multiple TIs on the same node share an instance sub-FSM (1:1:1 placement makes
// this rare, but possible). Each TI's reconciler.Wire() call rebinds the
// callback set on the FSM; last-writer-wins matches the existing OnDelete /
// OnUpload behavior wired alongside.
func (o *Orchestrator) attachLifecycleReconciler(ti *VaultInstance, vaultID glid.GLID, vaultGroup *raftgroup.Group) {
	ti.Reconciler = NewVaultLifecycleReconciler(o, vaultID, ti, o.localNodeID, o.baseLogger)
	if vaultGroup == nil {
		return
	}
	if vfsm, ok := vaultGroup.FSM.(*vaultraft.FSM); ok && vfsm != nil {
		ti.Reconciler.Wire(vfsm.EnsureVaultFSM(vaultID))
	}
}

// buildInstanceForStorage creates a VaultInstance whose data directory is
// resolved from a specific file storage ID. Used for both leaders with
// explicit storage placements and followers (one per node per instance).
func (o *Orchestrator) buildInstanceForStorage(sys *system.System, vaultCfg system.VaultConfig, factories Factories, storageID string, isFollower bool) (*VaultInstance, error) {
	cfg := &sys.Config
	rt := &sys.Runtime
	fs := findFileStorageByID(rt, storageID)
	if fs == nil {
		return nil, fmt.Errorf("file storage %s not found", storageID)
	}

	// Create the vault-ctl Raft group BEFORE the chunk manager — same rationale
	// as buildInstance: start elections while chunk loading is in progress.
	vaultGroup, applier, raftCB := o.ensureVaultCtlMetadata(vaultCfg, rt.Nodes, factories)

	// Build params normally, then override the dir with this storage's path.
	params := buildVaultParams(sys, vaultCfg, o.localNodeID)
	// Followers keep cloud store access for reads but skip uploads.
	if isFollower {
		params["_cloud_read_only"] = "true"
	}
	params["dir"] = filepath.Join(fs.Path, "vaults", vaultCfg.ID.String(), vaultCfg.ID.String())

	factoryName := mapVaultTypeToFactory(vaultCfg.Type)
	cmFactory, ok := factories.ChunkManagers[factoryName]
	if !ok {
		return nil, fmt.Errorf("unknown chunk manager type: %s", factoryName)
	}

	var cmLogger = factories.Logger
	if cmLogger != nil {
		cmLogger = cmLogger.With("vault", vaultCfg.ID, "storage", storageID)
	}

	cmParams := resolveVaultDir(params, factories.VaultsDir, vaultCfg.ID.String())
	cmParams["_expect_existing"] = "true"
	cmParams["_vault_id"] = vaultCfg.ID.String()

	cm, err := cmFactory(cmParams, cmLogger)
	if err != nil {
		return nil, fmt.Errorf("create chunk manager: %w", err)
	}

	if err := applyRotationPolicy(cm, cfg.RotationPolicies, vaultCfg.RotationPolicyID); err != nil {
		_ = cm.Close()
		return nil, err
	}

	// Wire Raft announcer now that chunk manager exists.
	setVaultRaftAnnouncer(cm, applier, o.phase, o.logger)

	// Follower replicas need index builders for local queries.
	imFactory, ok := factories.IndexManagers[factoryName]
	if !ok {
		_ = cm.Close()
		return nil, fmt.Errorf("unknown index manager type: %s", factoryName)
	}
	var imLogger = factories.Logger
	if imLogger != nil {
		imLogger = imLogger.With("vault", vaultCfg.ID, "storage", storageID)
	}
	im, err := imFactory(cmParams, cm, imLogger)
	if err != nil {
		_ = cm.Close()
		return nil, fmt.Errorf("create index manager: %w", err)
	}

	var qeLogger = factories.Logger
	if qeLogger != nil {
		qeLogger = qeLogger.With("vault", vaultCfg.ID, "storage", storageID)
	}
	qe := query.New(cm, im, qeLogger)

	if processor, ok := cm.(chunk.ChunkPostSealProcessor); ok {
		processor.SetIndexBuilders([]chunk.ChunkIndexBuilder{im.BuildAdapter()})
	}

	ti := &VaultInstance{
		VaultID: vaultCfg.ID,
		Type:    string(vaultCfg.Type),
		Chunks:  cm,
		Indexes: im,
		Query:   qe,
	}
	ti.applyRaftCallbacks(raftCB)
	o.attachLifecycleReconciler(ti, vaultCfg.ID, vaultGroup)
	wireVaultFSMOnDelete(vaultGroup, vaultCfg.ID, cm, im, o, o.logger)
	wireVaultFSMOnUpload(vaultGroup, vaultCfg.ID, cm, o, o.logger)
	return ti, nil
}

// findFileStorageByID resolves a file storage ID to its config across all nodes.
func findFileStorageByID(rt *system.Runtime, storageID string) *system.FileStorage {
	for _, nsc := range rt.NodeStorageConfigs {
		for i := range nsc.FileStorages {
			if nsc.FileStorages[i].ID.String() == storageID {
				return &nsc.FileStorages[i]
			}
		}
	}
	return nil
}

// applyRotationPolicy resolves and applies a rotation policy to a chunk manager.
func applyRotationPolicy(cm chunk.ChunkManager, policies []system.RotationPolicyConfig, policyID *glid.GLID) error {
	if policyID == nil {
		return nil
	}
	policyCfg := findRotationPolicy(policies, *policyID)
	if policyCfg == nil {
		return nil
	}
	policy, err := policyCfg.ToRotationPolicy()
	if err != nil {
		return fmt.Errorf("invalid rotation policy %s: %w", *policyID, err)
	}
	if policy != nil {
		cm.SetRotationPolicy(policy)
	}
	return nil
}

// vaultRaftCallbacks holds the callbacks returned by ensureVaultCtlMetadata.
func (o *Orchestrator) destroyVaultControlPlaneRaftGroup(vaultID glid.GLID) {
	if o.vaultCtlLeaders != nil {
		o.vaultCtlLeaders.Stop(vaultID)
	}
	if o.groupMgr == nil {
		return
	}
	gid := raftgroup.VaultControlPlaneGroupID(vaultID)
	if err := o.groupMgr.DestroyGroup(gid); err != nil && !errors.Is(err, raftgroup.ErrGroupNotFound) {
		o.logger.Debug("destroy vault control-plane raft group",
			"vault", vaultID, "error", err)
	}
}

// ensureVaultControlPlaneRaftGroup starts the per-vault control-plane Raft group
// when cluster mode is enabled (shared GroupManager + full member list).
// Idempotent; safe on every reconfigure sweep.
func (o *Orchestrator) ensureVaultControlPlaneRaftGroup(vaultID glid.GLID, clusterNodes []system.NodeConfig, factories Factories) {
	gid := raftgroup.VaultControlPlaneGroupID(vaultID)
	_, _ = o.tryStartClusterRaftGroup(gid, vaultraft.NewFSM(), clusterNodes, factories)
}

// tryStartClusterRaftGroup creates or returns an existing cluster-wide Raft group
// (symmetric seeding across all resolvable cluster nodes). The second return is
// the resolved member list when the group is (or will be) active on this node;
// both are nil when creation is deferred or fails.
func (o *Orchestrator) tryStartClusterRaftGroup(groupID string, fsm hraft.FSM, clusterNodes []system.NodeConfig, factories Factories) (*raftgroup.Group, []hraft.Server) {
	if factories.GroupManager == nil {
		return nil, nil
	}
	members := o.buildVaultRaftMembers(clusterNodes, factories)
	if len(members) < len(clusterNodes) {
		o.logger.Debug("cluster raft group: not all cluster nodes resolvable, deferring creation",
			"group", groupID,
			"have", len(members),
			"want", len(clusterNodes))
		return nil, nil
	}
	isMember := false
	for _, srv := range members {
		if string(srv.ID) == o.localNodeID {
			isMember = true
			break
		}
	}
	if !isMember {
		return nil, nil
	}
	g := factories.GroupManager.GetGroup(groupID)
	if g != nil {
		return g, members
	}
	g, err := factories.GroupManager.CreateGroup(raftgroup.GroupConfig{
		GroupID:     groupID,
		FSM:         fsm,
		SeedMembers: members,
	})
	if err != nil {
		o.logger.Warn("failed to create cluster raft group", "group", groupID, "error", err)
		return nil, nil
	}
	return g, members
}

type vaultRaftCallbacks struct {
	hasLeader           func() bool
	isLeader            func() bool
	isFSMReady          func() bool
	applyRequestDelete  func(id chunk.ChunkID, reason string, expectedFrom []string) error
	applyAckDelete      func(id chunk.ChunkID, nodeID string) error
	applyFinalizeDelete func(id chunk.ChunkID) error
	applyPruneNode      func(nodeID string) error
	applyRetPending     func(id chunk.ChunkID) error
	isTombstoned        func(id chunk.ChunkID) bool
	listChunks          func() []chunk.ChunkID
	listRetPending      func() []chunk.ChunkID
	overlayFromFSM      func(chunk.ChunkMeta) chunk.ChunkMeta
	chunkResidency      func(id chunk.ChunkID, placementNodeIDs []string) []string
	manifestEntries     func() []vaultctlfsm.ManifestEntry
	manifestEntry       func(id chunk.ChunkID) (vaultctlfsm.ManifestEntry, bool)
}

// ensureVaultCtlMetadata joins this node to the vault control-plane
// Raft group for the vault's vault (creating the group if needed) and
// returns the applier + callbacks for this instance's chunk metadata. Every
// instance in the same vault shares the same vault-ctl Raft group; each
// instance's chunk FSM is a sub-FSM keyed by instance ID (see vaultraft.FSM and
// vaultraft/vaultctlfsm.FSM). With no GroupManager, returns nils.
//
// Post-gastrolog-5xxbd there is no per-vault-ctl Raft group. The historical
// function name ensureVaultCtlMetadata is preserved as a no-op alias in
// tests only; production wires through this function.
//
// Call this BEFORE creating the chunk manager so Raft can start
// elections while chunk loading is still in progress.
func (o *Orchestrator) ensureVaultCtlMetadata(vaultCfg system.VaultConfig, clusterNodes []system.NodeConfig, factories Factories) (*raftgroup.Group, vaultctlfsm.Applier, vaultRaftCallbacks) {
	if factories.GroupManager == nil {
		return nil, nil, vaultRaftCallbacks{}
	}
	vaultGID := raftgroup.VaultControlPlaneGroupID(vaultCfg.ID)
	g, members := o.tryStartClusterRaftGroup(vaultGID, vaultraft.NewFSM(), clusterNodes, factories)
	if g == nil {
		return nil, nil, vaultRaftCallbacks{}
	}

	o.vaultCtlLeaders.SetDesiredMembers(vaultCfg.ID, members)
	o.vaultCtlLeaders.Start(vaultCfg.ID, g)

	vfsm, ok := g.FSM.(*vaultraft.FSM)
	if !ok || vfsm == nil {
		return nil, nil, vaultRaftCallbacks{}
	}
	// Wire the after-restore hook so that vault-ctl snapshot install on
	// this node triggers the receipt protocol's catchup pass on every
	// instance reconciler in the vault. Idempotent — calling SetOnAfterRestore
	// on every ensureVaultCtlMetadata invocation is fine; later calls
	// just rebind to the same closure. Without this hook, the receipt
	// protocol's pendingDeletes silently leak across snapshot install
	// boundaries (the bug gastrolog-51gme step 3 was supposed to close).
	vaultID := vaultCfg.ID
	vfsm.SetOnAfterRestore(func() { o.afterVaultCtlRestore(vaultID) })
	vaultFSM := vfsm.EnsureVaultFSM(vaultCfg.ID)
	r := g.Raft
	timeout := cluster.ReplicationTimeout

	var applier vaultctlfsm.Applier
	if factories.PeerConns != nil {
		applier = cluster.NewVaultCtlChunkApplyForwarder(r, vaultGID, vaultCfg.ID, factories.PeerConns, timeout)
	} else {
		applier = &vaultCtlApplier{o: o, vaultID: vaultCfg.ID}
	}

	return g, applier, buildVaultRaftCallbacks(r, vaultFSM, applier)
}

// buildVaultRaftCallbacks constructs the callback struct for replicated instance
// chunk metadata (vault control-plane Raft in cluster mode).
// Extracted from ensureVaultCtlMetadata to keep cognitive complexity within lint
// thresholds.
//
// Readiness uses the Raft applied index, not the FSM's ready flag. hraft
// filters LogNoop and LogConfiguration entries before calling FSM.Apply —
// only LogCommand entries hit the FSM — so a fresh cluster (bootstrap config
// + post-election no-op) advances r.AppliedIndex but never touches
// FSM.Apply. The vault FSM's own Ready flag only flips on user-triggered
// commands, which never fire before the first ingestion. Raft's applied
// index is the authoritative "this group is live on this node" signal:
// it advances on bootstrap, elections, snapshot restore, and normal
// replication alike.
//
// Before 5xxbd, vault-ctl FSM was a top-level Raft group whose Ready flag
// flipped on every apply in practice, because `CmdPutVault` was a LogCommand
// that hit it. After 5xxbd the instance sub-FSM only sees OpVaultChunkFSM commands,
// which a fresh vault with no chunks never sends — keying readiness on any
// FSM-level signal leaves every fresh vault wedged as "not ready" until
// first ingestion.
func buildVaultRaftCallbacks(r *hraft.Raft, fsm *vaultctlfsm.FSM, applier vaultctlfsm.Applier) vaultRaftCallbacks {
	return vaultRaftCallbacks{
		hasLeader:  func() bool { return r.Leader() != "" },
		isLeader:   func() bool { return r.State() == hraft.Leader },
		isFSMReady: func() bool { return r.AppliedIndex() > 0 },
		applyRequestDelete: func(id chunk.ChunkID, reason string, expectedFrom []string) error {
			return applier.Apply(vaultctlfsm.MarshalRequestDelete(id, time.Now(), reason, expectedFrom))
		},
		applyAckDelete: func(id chunk.ChunkID, nodeID string) error {
			return applier.Apply(vaultctlfsm.MarshalAckDelete(id, nodeID))
		},
		applyFinalizeDelete: func(id chunk.ChunkID) error {
			return applier.Apply(vaultctlfsm.MarshalFinalizeDelete(id))
		},
		applyPruneNode: func(nodeID string) error {
			return applier.Apply(vaultctlfsm.MarshalPruneNode(nodeID))
		},
		applyRetPending: func(id chunk.ChunkID) error {
			return applier.Apply(vaultctlfsm.MarshalRetentionPending(id))
		},
		isTombstoned: func(id chunk.ChunkID) bool {
			if fsm == nil {
				return false
			}
			return fsm.IsTombstoned(id)
		},
		listChunks: func() []chunk.ChunkID {
			if fsm == nil {
				return nil
			}
			entries := fsm.List()
			ids := make([]chunk.ChunkID, len(entries))
			for i := range entries {
				ids[i] = entries[i].ID
			}
			return ids
		},
		listRetPending: listFSMByFlag(fsm, func(e vaultctlfsm.ManifestEntry) bool { return e.RetentionPending }),
		overlayFromFSM: func(m chunk.ChunkMeta) chunk.ChunkMeta {
			if fsm == nil {
				return m
			}
			e := fsm.Get(m.ID)
			if e == nil {
				return m
			}
			m.CloudBacked = e.CloudBacked
			m.Archived = e.Archived
			// Phase 3 (gastrolog-1huz5): the FSM is the cluster-wide
			// source of truth for the lifecycle state. Local meta.Sealed
			// flips at sealActiveLocked time but the cluster doesn't see
			// the chunk as Sealed until sealToGLCB commits. Overlay
			// State so producer-side iteration (retention, upload,
			// archival sweep) can branch on cluster state without
			// jumping the gun on Sealing chunks.
			m.State = e.State
			// Keep the legacy bool synced with State so the unaudited
			// read sites (the bulk of the .Sealed checks across the
			// codebase) still behave correctly: Sealing reads as
			// not-yet-sealed.
			m.Sealed = e.State == chunk.ChunkStateSealed
			return m
		},
		chunkResidency: func(id chunk.ChunkID, placementNodeIDs []string) []string {
			if fsm == nil {
				return nil
			}
			return fsm.ChunkResidency(id, placementNodeIDs)
		},
		manifestEntries: func() []vaultctlfsm.ManifestEntry {
			if fsm == nil {
				return nil
			}
			return fsm.List()
		},
		manifestEntry: func(id chunk.ChunkID) (vaultctlfsm.ManifestEntry, bool) {
			if fsm == nil {
				return vaultctlfsm.ManifestEntry{}, false
			}
			e := fsm.Get(id)
			if e == nil {
				return vaultctlfsm.ManifestEntry{}, false
			}
			return *e, true
		},
	}
}

// listFSMByFlag returns a function that filters the FSM's entries by a
// boolean predicate (e.g., RetentionPending or TransitionStreamed).
func listFSMByFlag(fsm *vaultctlfsm.FSM, pred func(vaultctlfsm.ManifestEntry) bool) func() []chunk.ChunkID {
	return func() []chunk.ChunkID {
		if fsm == nil {
			return nil
		}
		var ids []chunk.ChunkID
		for _, e := range fsm.List() {
			if pred(e) {
				ids = append(ids, e.ID)
			}
		}
		return ids
	}
}

// setIntegrityVerifier wires the manifest-backed digest verifier into a chunk
// manager that supports it. Cold-cache cloud downloads consult the FSM-recorded
// hash and reject blobs whose actual digest doesn't match. See gastrolog-grnc3.
func setIntegrityVerifier(cm chunk.ChunkManager, v chunk.IntegrityVerifier) {
	if v == nil {
		return
	}
	setter, ok := cm.(chunk.IntegrityVerifierSetter)
	if !ok {
		return
	}
	setter.SetIntegrityVerifier(v)
}

// setVaultRaftAnnouncer wires the Raft announcer to a chunk manager after both
// the Raft group and chunk manager have been created. The applier handles
// routing to the vault ctl Raft leader when peers are configured. The phase parameter lets
// the announcer short-circuit during shutdown so trailing applies don't
// fire "raft is already shutdown" warnings (see gastrolog-1e5ke).
func setVaultRaftAnnouncer(cm chunk.ChunkManager, applier vaultctlfsm.Applier, phase *lifecycle.Phase, logger *slog.Logger) {
	if applier == nil {
		return
	}
	setter, ok := cm.(chunk.AnnouncerSetter)
	if !ok {
		return
	}
	setter.SetAnnouncer(vaultctlfsm.NewAnnouncer(applier, phase, logger))
}

// clearVaultFSMChunkCallbacks clears OnDelete/OnUpload for a vault's FSM slice
// in the vault control-plane group. Used before closing that vault's chunk
// manager when the Raft group may still deliver log entries
// (e.g. RemoveVaultInstance during placement loss).
func (o *Orchestrator) clearVaultFSMChunkCallbacks(vaultID glid.GLID) {
	if o.groupMgr == nil {
		return
	}
	g := o.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(vaultID))
	if g == nil {
		return
	}
	var fsm *vaultctlfsm.FSM
	switch raw := g.FSM.(type) {
	case *vaultctlfsm.FSM:
		fsm = raw
	case *vaultraft.FSM:
		fsm = raw.EnsureVaultFSM(vaultID)
	default:
		return
	}
	fsm.SetOnDelete(nil)
	fsm.SetOnUpload(nil)
	if o.logger != nil {
		o.logger.Debug("cleared vault-ctl FSM chunk callbacks before manager close",
			"vault", vaultID)
	}
}

// wireVaultFSMOnDelete sets up the vault-ctl FSM's OnDelete callback so that
// CmdDeleteChunk applied via Raft on this node deletes the local chunk
// files (and indexes if available). The callback uses chunk.SilentDeleter
// to avoid the announcer feedback loop — re-announcing the delete that
// just arrived from Raft would cause infinite re-application.
//
// Safe to call with nil group, nil cm, or a chunk manager that doesn't
// implement SilentDeleter (e.g. memory vaults): the callback is simply not
// wired in those cases.
//
// IMPORTANT: this callback acquires the chunk manager's m.mu via
// DeleteSilent. For the FSM apply goroutine to do this safely, no other
// goroutine may hold m.mu while waiting for a Raft round-trip (e.g. via
// the Announcer). The chunk.file.Manager's Seal/Append/Compress paths
// enforce this by releasing m.mu before calling the announcer; if a new
// path is added that holds the mutex during an announcer call, this
// callback will deadlock with it.
func wireVaultFSMOnDelete(g *raftgroup.Group, vaultID glid.GLID, cm chunk.ChunkManager, im index.IndexManager, o *Orchestrator, logger *slog.Logger) {
	if g == nil || cm == nil {
		return
	}
	var fsm *vaultctlfsm.FSM
	switch raw := g.FSM.(type) {
	case *vaultctlfsm.FSM:
		fsm = raw
	case *vaultraft.FSM:
		fsm = raw.EnsureVaultFSM(vaultID)
	default:
		return
	}
	// Wire OnCreate alongside OnDelete: the WatchChunks event bus needs
	// CREATED events as soon as a new active chunk is announced via
	// CmdCreateChunk so the inspector shows the chunk immediately rather
	// than only after seal. Fired on every node where the apply ran, same
	// as OnDelete — followers learn about the new chunk via Raft replication.
	// See gastrolog-3pf9w.
	fsm.SetOnCreate(func(e vaultctlfsm.ManifestEntry) {
		if o == nil {
			return
		}
		o.EmitChunkCreated(vaultID, manifestEntryToChunkMeta(e, false))
	})
	silent, ok := cm.(chunk.SilentDeleter)
	if !ok {
		return
	}
	fsm.SetOnDelete(func(id chunk.ChunkID) {
		// Emit a DELETED event regardless of local-delete outcome: the
		// FSM's authoritative chunks-map entry is gone, so the
		// inspector's projection on this node must drop the entry. Fire
		// on every node where the apply ran, even ones that never had
		// the chunk locally — they may have rendered it via the
		// cluster-wide ListChunks fan-out. See gastrolog-2ob86.
		if o != nil {
			defer o.EmitChunkDeleted(vaultID, id)
		}
		// Delete indexes first (they're metadata about the chunk).
		// ErrChunkNotFound-equivalent errors are expected during log replay
		// on a node that doesn't have the chunk locally — log at debug only.
		if im != nil {
			if err := im.DeleteIndexes(id); err != nil && logger != nil {
				logger.Debug("FSM onDelete: DeleteIndexes failed",
					"chunk", id, "error", err)
			}
		}
		// Then delete the chunk files. DeleteSilent skips the announcer.
		// ErrChunkNotFound / ErrActiveChunk are benign "nothing to delete"
		// cases (log replay on a node without the chunk, or a forwarded
		// chunk still being written). Debug-level only.
		if err := silent.DeleteSilent(id); err != nil && logger != nil {
			if errors.Is(err, chunk.ErrChunkNotFound) || errors.Is(err, chunk.ErrActiveChunk) ||
				errors.Is(err, chunkfile.ErrManagerClosed) {
				logger.Debug("FSM onDelete: DeleteSilent skipped",
					"chunk", id, "reason", err)
			} else {
				logger.Warn("FSM onDelete: DeleteSilent failed",
					"chunk", id, "error", err)
			}
		}
	})
}

// wireVaultFSMOnUpload connects the vault-ctl FSM's OnUpload callback to the
// chunk manager's RegisterCloudChunk method. When the FSM applies CmdUploadChunk
// (from the leader's AnnounceUpload), the follower's chunk manager registers
// the cloud chunk from metadata alone — no record streaming or S3 download.
func wireVaultFSMOnUpload(g *raftgroup.Group, vaultID glid.GLID, cm chunk.ChunkManager, o *Orchestrator, logger *slog.Logger) {
	if g == nil || cm == nil {
		return
	}
	var fsm *vaultctlfsm.FSM
	switch raw := g.FSM.(type) {
	case *vaultctlfsm.FSM:
		fsm = raw
	case *vaultraft.FSM:
		fsm = raw.EnsureVaultFSM(vaultID)
	default:
		return
	}
	registrar, ok := cm.(chunk.CloudChunkRegistrar)
	if !ok {
		return
	}
	fsm.SetOnUpload(func(e vaultctlfsm.ManifestEntry) {
		// Emit an UPLOADED event with the FSM's authoritative state —
		// every cluster node's FSM applies the same CmdUploadChunk
		// payload, so every node emits the same RecordCount /
		// DiskBytes / CloudBacked. Using local Manager.Meta instead
		// produced per-node variance and inspector flicker. See
		// gastrolog-3pf9w.
		defer func() {
			if o == nil {
				return
			}
			meta := manifestEntryToChunkMeta(e, true)
			meta.CloudBacked = true
			o.EmitChunkUploaded(vaultID, meta)
		}()
		info := chunk.CloudChunkInfo{
			WriteStart:      e.WriteStart,
			WriteEnd:        e.WriteEnd,
			IngestStart:     e.IngestStart,
			IngestEnd:       e.IngestEnd,
			SourceStart:     e.SourceStart,
			SourceEnd:       e.SourceEnd,
			RecordCount:     e.RecordCount,
			Bytes:           e.Bytes,
			DiskBytes:       e.DiskBytes,
			IngestIdxOffset: e.IngestIdxOffset,
			IngestIdxSize:   e.IngestIdxSize,
			SourceIdxOffset: e.SourceIdxOffset,
			SourceIdxSize:   e.SourceIdxSize,
		}
		if err := registrar.RegisterCloudChunk(e.ID, info); err != nil {
			if logger != nil {
				logger.Debug("FSM onUpload: RegisterCloudChunk failed",
					"chunk", e.ID, "error", err)
			}
		}
	})
}

// RefreshVaultCtlMembers re-derives the desired vault-ctl Raft group member
// list from the current cluster node list and applies it to every local
// vault's leader manager. The next reconcile pass on each vault-ctl Raft
// leader then AddVoter's new members and RemoveServer's departed ones.
//
// Called by the config dispatcher on NotifyNodeConfigPut / NotifyNodeConfigDeleted
// so cluster scale-out and scale-in propagate into per-vault Raft groups.
// Without this, vault-ctl groups stay frozen at their bootstrap membership
// and a freshly-joined node loops forever in pre-vote campaigns rejected
// by the original members with "node is not in configuration", blocking
// chunk catchup and RF expansion. See gastrolog-4zy8a.
//
// Partial resolution short-circuits: if any cluster node's address can't
// be resolved (transient — the cluster Raft config hasn't caught up yet),
// the refresh is skipped rather than passing a partial set to the
// reconciler, which would otherwise RemoveServer the missing entries.
// The next NotifyNodeConfigPut retries once the address is in cluster Raft.
func (o *Orchestrator) RefreshVaultCtlMembers(clusterNodes []system.NodeConfig, factories Factories) {
	if o.vaultCtlLeaders == nil {
		return
	}
	members := o.buildVaultRaftMembers(clusterNodes, factories)
	if len(members) == 0 {
		// Single-node mode (nil NodeAddressResolver) or empty node list —
		// no vault-ctl groups to refresh.
		return
	}
	if len(members) < len(clusterNodes) {
		o.logger.Debug("refresh vault-ctl members: not all nodes resolvable, skipping",
			"have", len(members), "want", len(clusterNodes))
		return
	}

	o.mu.RLock()
	vaultIDs := make([]glid.GLID, 0, len(o.vaults))
	for id := range o.vaults {
		vaultIDs = append(vaultIDs, id)
	}
	o.mu.RUnlock()

	for _, vaultID := range vaultIDs {
		// Joiners can land here with a vault that was registered before
		// their own NodeConfig had propagated into the cluster FSM —
		// tryStartClusterRaftGroup returned nil on the original
		// AddVault and the vault-ctl Raft group was never created on
		// this node. Re-attempt creation now that we have a complete
		// resolvable member set; the call is idempotent and returns
		// the existing group when it's already up. Without this,
		// joiners stay permanently missing from every vault-ctl group,
		// blocking AddVoter commits cluster-wide once quorum starts
		// requiring an ACK from a new voter. See gastrolog-5n6xz.
		o.ensureVaultControlPlaneRaftGroup(vaultID, clusterNodes, factories)
		o.vaultCtlLeaders.SetDesiredMembers(vaultID, members)
	}
}

// buildVaultRaftMembers returns ALL cluster nodes as Raft members for a vault
// control-plane Raft group. Every node participates regardless of which vaults
// it stores — nodes without local instance data still replicate instance metadata.
// See gastrolog-292yi.
func (o *Orchestrator) buildVaultRaftMembers(clusterNodes []system.NodeConfig, factories Factories) []hraft.Server {
	if factories.NodeAddressResolver == nil || len(clusterNodes) == 0 {
		return nil
	}
	var members []hraft.Server
	for _, node := range clusterNodes {
		nodeID := node.ID.String()
		if addr, ok := factories.NodeAddressResolver(nodeID); ok {
			members = append(members, hraft.Server{
				ID:      hraft.ServerID(nodeID),
				Address: hraft.ServerAddress(addr),
			})
		}
	}
	return members
}

func mapVaultTypeToFactory(t system.VaultType) string {
	switch t {
	case system.VaultTypeMemory:
		return "memory"
	case system.VaultTypeFile:
		return "file"
	case system.VaultTypeJSONL:
		return "jsonl"
	default:
		return string(t)
	}
}

// buildVaultParams builds a params map from a VaultConfig suitable for factory consumption.
func buildVaultParams(sys *system.System, vaultCfg system.VaultConfig, localNodeID string) map[string]string {
	rt := &sys.Runtime
	params := make(map[string]string)

	switch vaultCfg.Type {
	case system.VaultTypeMemory:
		if vaultCfg.MemoryBudgetBytes > 0 {
			params["budgetBytes"] = strconv.FormatUint(vaultCfg.MemoryBudgetBytes, 10)
		}

	case system.VaultTypeFile:
		// Single storage class for all file vaults — local-only and
		// cloud-backed alike. The active chunk and warm cache live at
		// the same chunkDir path post-step-7k. See gastrolog-4k5mg.
		if vaultCfg.IsCloud() {
			addCloudParams(params, &sys.Config, vaultCfg)
		}
		if fs := findLocalFileStorage(rt, localNodeID, vaultCfg.StorageClass); fs != nil {
			params["dir"] = filepath.Join(fs.Path, "vaults", vaultCfg.ID.String(), vaultCfg.ID.String())
		}

	case system.VaultTypeJSONL:
		if vaultCfg.Path != "" {
			params["path"] = vaultCfg.Path
		} else {
			// Default: jsonl/<vault-id>/<instance-id>.jsonl
			params["path"] = filepath.Join("jsonl", vaultCfg.ID.String(), vaultCfg.ID.String()+".jsonl")
		}
	}

	return params
}

// addCloudParams writes cloud-store credentials + bucket info into params
// for a cloud-backed file instance. Always records the cloud_service_id (snapshot
// onto every CmdUploadChunk via gastrolog-grnc3); no-op for the rest if the
// referenced cloud service entry is missing — the chunk manager will start
// without a CloudStore wired but still knows which service it would pin to.
func addCloudParams(params map[string]string, cfg *system.Config, vaultCfg system.VaultConfig) {
	params["cloud_service_id"] = vaultCfg.CloudServiceID.String()
	cs := findCloudService(cfg, *vaultCfg.CloudServiceID)
	if cs == nil {
		return
	}
	params["sealed_backing"] = cs.Provider
	params["bucket"] = cs.Bucket
	if cs.Region != "" {
		params["region"] = cs.Region
	}
	if cs.Endpoint != "" {
		params["endpoint"] = cs.Endpoint
	}
	if cs.AccessKey != "" {
		params["access_key"] = cs.AccessKey
	}
	if cs.SecretKey != "" {
		params["secret_key"] = cs.SecretKey
	}
}

// findLocalFileStorage finds a FileStorage on the given node with the given storage class.
func findLocalFileStorage(rt *system.Runtime, nodeID string, storageClass uint32) *system.FileStorage {
	if storageClass == 0 {
		return nil
	}
	for _, nsc := range rt.NodeStorageConfigs {
		if nsc.NodeID != nodeID {
			continue
		}
		for i := range nsc.FileStorages {
			if nsc.FileStorages[i].StorageClass == storageClass {
				return &nsc.FileStorages[i]
			}
		}
	}
	return nil
}

// findCloudService finds a CloudService by ID in the system.
func findCloudService(cfg *system.Config, id glid.GLID) *system.CloudService {
	for i := range cfg.CloudServices {
		if cfg.CloudServices[i].ID == id {
			return &cfg.CloudServices[i]
		}
	}
	return nil
}
