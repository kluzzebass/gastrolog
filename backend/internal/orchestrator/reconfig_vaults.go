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
	tierfsm "gastrolog/internal/tier/raftfsm"
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
// Loads the full config internally to resolve the vault's tier IDs to tier configs.
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

	tiers, err := o.buildTierInstances(sys, vaultCfg, factories)
	if err != nil {
		return fmt.Errorf("build tier instances for vault %s: %w", vaultCfg.ID, err)
	}

	// Register vault (even with zero tiers — tiers arrive incrementally via handleTierPut).
	vault := NewVault(vaultCfg.ID, tiers...)
	vault.Name = vaultCfg.Name
	o.vaults[vaultCfg.ID] = vault

	// Compile filters immediately so the vault can receive records right away.
	// The rotation sweep also reconciles filters every 15s as a safety net.
	if sys != nil {
		_ = o.reloadFiltersFromRoutes(sys)
	}

	// Rotation and retention are reconciled by the discovery-based sweep
	// jobs on their next tick.

	o.logger.Info("vault added", "id", vaultCfg.ID, "name", vaultCfg.Name, "tiers", len(tiers))
	return nil
}

// Rotation and retention are handled by discovery-based sweep jobs
// (rotationSweep and retentionSweepAll). No per-tier setup needed during AddVault.

// findTierConfig finds a TierConfig by ID in a slice.
func findTierConfig(tiers []system.TierConfig, id glid.GLID) *system.TierConfig {
	for i := range tiers {
		if tiers[i].ID == id {
			return &tiers[i]
		}
	}
	return nil
}

func findVaultConfig(vaults []system.VaultConfig, id glid.GLID) *system.VaultConfig {
	for i := range vaults {
		if vaults[i].ID == id {
			return &vaults[i]
		}
	}
	return nil
}

// resolveRetentionRulesFromTier converts tier retention rules to resolved retentionRule objects.
func resolveRetentionRulesFromTier(cfg *system.Config, vaultCfg system.VaultConfig, tierCfg *system.TierConfig) ([]retentionRule, error) {
	// Derive the retention action from the tier's position in the vault chain.
	// The stored action on the tier config is ignored — position is the source
	// of truth. This prevents stale actions when tiers are added/removed.
	tierIDs := system.VaultTierIDs(cfg.Tiers, vaultCfg.ID)
	tierIndex := slices.Index(tierIDs, tierCfg.ID)
	isLastTier := tierIndex < 0 || tierIndex == len(tierIDs)-1

	var rules []retentionRule
	for _, b := range tierCfg.RetentionRules {
		retCfg := findRetentionPolicy(cfg.RetentionPolicies, b.RetentionPolicyID)
		if retCfg == nil {
			return nil, fmt.Errorf("tier %s references unknown retention policy: %s", tierCfg.ID, b.RetentionPolicyID)
		}
		policy, err := retCfg.ToRetentionPolicy()
		if err != nil {
			return nil, fmt.Errorf("invalid retention policy %s for tier %s: %w", b.RetentionPolicyID, tierCfg.ID, err)
		}
		if policy == nil {
			continue
		}

		// Eject uses the stored action (explicit route targets).
		// Otherwise: transition if there's a next tier, expire if last.
		action := b.Action
		if action != system.RetentionActionEject {
			if isLastTier {
				action = system.RetentionActionExpire
			} else {
				action = system.RetentionActionTransition
			}
		}

		rules = append(rules, retentionRule{
			policy:        policy,
			action:        action,
			ejectRouteIDs: b.EjectRouteIDs,
		})
	}
	return rules, nil
}

// RemoveVault removes a vault if it's empty (no chunks with data).
// Returns ErrVaultNotFound if the vault doesn't exist.
// Returns ErrVaultNotEmpty if the vault has any chunks.
func (o *Orchestrator) RemoveVault(id glid.GLID) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	vault, exists := o.vaults[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, id)
	}
	// Check ALL tiers for data before allowing removal.
	for _, tier := range vault.Tiers {
		metas, err := tier.Chunks.List()
		if err != nil {
			return fmt.Errorf("list chunks for tier %s: %w", tier.TierID, err)
		}
		for _, m := range metas {
			if m.RecordCount > 0 || !m.Sealed {
				return fmt.Errorf("%w: tier %s has data", ErrVaultNotEmpty, tier.TierID)
			}
		}
		if active := tier.Chunks.Active(); active != nil {
			return fmt.Errorf("%w: tier %s has active chunk", ErrVaultNotEmpty, tier.TierID)
		}
	}

	o.teardownVault(id, vault)
	o.logger.Info("vault removed", "id", id, "name", vault.Name, "type", vault.Type())
	return nil
}

// removeVaultJobs removes retention runners and cron rotation jobs for a vault
// without closing managers or unregistering. Used by UnregisterVault and drain.
func (o *Orchestrator) removeVaultJobs(id glid.GLID, vault *Vault) {
	for _, tier := range vault.Tiers {
		delete(o.retention, retentionKey(tier.TierID, tier.StorageID))
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

	// Remove per-tier retention runners and cron rotation jobs.
	for _, tier := range vault.Tiers {
		delete(o.retention, retentionKey(tier.TierID, tier.StorageID))
	}
	o.cronRotation.removeAllForVault(id)

	// Close chunk/index managers to release file locks.
	if err := vault.Close(); err != nil {
		o.logger.Warn("failed to close vault during teardown", "vault", id, "error", err)
	}

	delete(o.vaults, id)
	o.rebuildFilterSetLocked()
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
	// Seal active chunks and delete all data across ALL tiers.
	for _, tier := range vault.Tiers {
		cm := tier.Chunks
		im := tier.Indexes

		// Seal active chunk if present.
		if active := cm.Active(); active != nil {
			if err := cm.Seal(); err != nil {
				return fmt.Errorf("seal active chunk for tier %s in vault %s: %w", tier.TierID, id, err)
			}
		}

		// Delete all indexes and chunks.
		metas, err := cm.List()
		if err != nil {
			return fmt.Errorf("list chunks for tier %s in vault %s: %w", tier.TierID, id, err)
		}
		for _, meta := range metas {
			if im != nil {
				// Best-effort index deletion; log and continue on error.
				if err := im.DeleteIndexes(meta.ID); err != nil {
					o.logger.Warn("failed to delete indexes during force remove",
						"vault", id, "tier", tier.TierID, "chunk", meta.ID.String(), "error", err)
				}
			}
			// LOCAL cleanup only — see sealAndDeleteAllChunks comment.
			// Each peer runs its own ForceRemoveVault when the vault
			// disappears from config; we must not fan a per-chunk
			// CmdDeleteChunk avalanche across vault-ctl Raft from one
			// node and have it cascade-delete on the others. Bug
			// gastrolog-4vz40.
			if err := chunk.DeleteNoAnnounce(cm, meta.ID); err != nil {
				return fmt.Errorf("delete chunk %s in tier %s vault %s: %w", meta.ID.String(), tier.TierID, id, err)
			}
		}
	}

	o.teardownVault(id, vault)
	o.logger.Info("vault force-removed", "id", id, "name", vault.Name, "type", vault.Type())
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
// RemoveTierFromVault reacts to placement loss, or DeleteTierFromVault
// reacts to an admin teardown) comes from each node independently running
// its own RemoveTierFromVault as the config change propagates — not from
// per-chunk delete announcements out of one node. See gastrolog-4vz40.
func (o *Orchestrator) sealAndDeleteAllChunks(tier *TierInstance, op string, tierID glid.GLID) int {
	if active := tier.Chunks.Active(); active != nil {
		if err := tier.Chunks.Seal(); err != nil {
			o.logger.Warn(op+": seal failed", "tier", tierID, "error", err)
		}
	}
	metas, err := tier.Chunks.List()
	if err != nil {
		o.logger.Warn(op+": list failed", "tier", tierID, "error", err)
		return 0
	}
	for _, m := range metas {
		if tier.Indexes != nil {
			if err := tier.Indexes.DeleteIndexes(m.ID); err != nil {
				o.logger.Warn(op+": delete indexes failed", "tier", tierID, "chunk", m.ID, "error", err)
			}
		}
		if err := chunk.DeleteNoAnnounce(tier.Chunks, m.ID); err != nil {
			o.logger.Warn(op+": delete chunk failed", "tier", tierID, "chunk", m.ID, "error", err)
		}
	}
	return len(metas)
}

// RemoveTierFromVault unregisters a tier instance from this node WITHOUT
// destroying its on-disk data. Used when placement moves the tier elsewhere
// (transient — the node may well get the tier back seconds later when
// placement flaps back). The tier's Chunks/Indexes managers are closed, jobs
// are cancelled, and the TierInstance is removed from the vault's tier list,
// but the chunk files and tier directory remain on disk. A subsequent
// AddTierToVault will re-discover the existing chunks.
//
// For actual tier deletion (admin-driven), use DeleteTierFromVault which
// additionally wipes all chunks and removes the data directory.
//
// Returns true if a tier was removed.
//
// gastrolog-4vz40: previously this function always wiped chunks, which meant
// any placement flap (caused by transient peer-conn teardowns from
// peers.Invalidate) destroyed data cluster-wide. The destructive behaviour is
// now opt-in via DeleteTierFromVault.
func (o *Orchestrator) RemoveTierFromVault(vaultID, tierID glid.GLID) bool {
	return o.removeTierFromVault(vaultID, tierID, false)
}

// DeleteTierFromVault unregisters a tier instance AND permanently wipes its
// on-disk data (chunks, indexes, and the tier directory). Used only when a
// tier is being deliberately deleted (admin action via CmdTierDeleted, or
// post-drain cleanup).
//
// Returns true if a tier was removed.
func (o *Orchestrator) DeleteTierFromVault(vaultID, tierID glid.GLID) bool {
	return o.removeTierFromVault(vaultID, tierID, true)
}

func (o *Orchestrator) removeTierFromVault(vaultID, tierID glid.GLID, deleteData bool) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	vault, exists := o.vaults[vaultID]
	if !exists {
		return false
	}

	idx := -1
	for i, t := range vault.Tiers {
		if t.TierID == tierID {
			idx = i
			break
		}
	}
	if idx < 0 {
		return false
	}

	tier := vault.Tiers[idx]

	// Cancel pending jobs for this tier.
	prefix := vaultID.String()
	o.scheduler.RemoveJobsByPrefix("post-seal:" + prefix + ":" + tierID.String())
	o.scheduler.RemoveJobsByPrefix("compress:" + prefix + ":" + tierID.String())
	o.scheduler.RemoveJobsByPrefix("index-build:" + prefix + ":" + tierID.String())

	// Destructive cleanup — only on explicit deletion.
	if deleteData {
		o.sealAndDeleteAllChunks(tier, "remove tier", tierID)
	}

	// Drop FSM → chunk-manager hooks before Close. Placement can remove this
	// tier while the vault control-plane Raft group still receives
	// CmdDeleteChunk for this tier from the leader; without clearing, onDelete
	// would call DeleteSilent on an already-closed file.Manager.
	o.clearTierFSMChunkCallbacks(vaultID, tierID)

	// Close managers.
	if err := tier.Chunks.Close(); err != nil {
		o.logger.Warn("remove tier: close chunk manager failed", "vault", vaultID, "tier", tierID, "error", err)
	}

	// Remove the tier's data directory entirely — not just its chunk subdirs.
	// Without this, leftover files (.lock, cloud.idx) and the tier dir itself
	// accumulate on disk forever. See gastrolog-42j4n.
	if deleteData {
		if remover, ok := tier.Chunks.(chunk.DirRemover); ok {
			if err := remover.RemoveDir(); err != nil {
				o.logger.Warn("remove tier: remove data directory failed", "vault", vaultID, "tier", tierID, "error", err)
			}
		}
	}

	// Remove retention runner and cron rotation for this tier.
	delete(o.retention, retentionKey(tier.TierID, tier.StorageID))
	o.cronRotation.removeAllForVault(vaultID)

	// Remove tier from vault's tier list.
	vault.Tiers = append(vault.Tiers[:idx], vault.Tiers[idx+1:]...)

	// If the vault has no tiers left, remove it entirely. Only do this on
	// destructive removal — for non-destructive placement-driven removals,
	// the vault shell must stay so a subsequent AddTierToVault can rehydrate.
	if deleteData && len(vault.Tiers) == 0 {
		delete(o.vaults, vaultID)
		o.rebuildFilterSetLocked()
		o.logger.Info("vault removed (no remaining tiers)", "vault", vaultID)
	}

	o.logger.Info("tier removed from vault",
		"vault", vaultID, "tier", tierID,
		"remaining_tiers", len(vault.Tiers),
		"deleteData", deleteData)
	return true
}

// AddTierToVault builds a single tier instance and adds it to an existing vault
// without tearing down any other tiers. This is the incremental counterpart to
// RemoveTierFromVault.
func (o *Orchestrator) AddTierToVault(ctx context.Context, vaultID, tierID glid.GLID, factories Factories) error {
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
	for _, t := range vault.Tiers {
		if t.TierID == tierID {
			o.mu.Unlock()
			return nil
		}
	}
	o.mu.Unlock()

	cfg := &sys.Config
	rt := &sys.Runtime
	tierCfg := findTierConfig(cfg.Tiers, tierID)
	if tierCfg == nil {
		return fmt.Errorf("tier %s not found in config", tierID)
	}

	vaultCfg := findVaultConfig(cfg.Vaults, vaultID)
	if vaultCfg == nil {
		return fmt.Errorf("vault %s not found in config", vaultID)
	}

	o.ensureVaultControlPlaneRaftGroup(vaultID, rt.Nodes, factories)

	nscs := rt.NodeStorageConfigs
	leaderNodeID := system.LeaderNodeID(rt.TierPlacements[tierCfg.ID], nscs)
	followerNodeIDs := system.FollowerNodeIDs(rt.TierPlacements[tierCfg.ID], nscs)
	isLeader := leaderNodeID == "" || leaderNodeID == o.localNodeID
	isFollower := slices.Contains(followerNodeIDs, o.localNodeID)
	if !isLeader && !isFollower {
		// No storage placement, but still join the vault control-plane Raft
		// group as a voter for this vault's replicated tier metadata.
		o.ensureVaultCtlTierMetadata(*tierCfg, rt.Nodes, factories)
		return nil
	}

	var ti *TierInstance
	if isLeader {
		t, err := o.buildLeaderTierInstance(sys, *vaultCfg, tierCfg, factories)
		if err != nil {
			return fmt.Errorf("build tier %s: %w", tierID, err)
		}
		t.FollowerTargets = system.FollowerTargets(rt.TierPlacements[tierCfg.ID], nscs)
		ti = t
	} else {
		for _, tgt := range system.FollowerTargets(rt.TierPlacements[tierCfg.ID], nscs) {
			if tgt.NodeID != o.localNodeID {
				continue
			}
			t, err := o.buildTierInstanceForStorage(sys, *vaultCfg, *tierCfg, factories, tgt.StorageID, true)
			if err != nil {
				return fmt.Errorf("build tier %s storage %s: %w", tierID, tgt.StorageID, err)
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
	vault.Tiers = append(vault.Tiers, ti)
	o.logger.Info("tier added to vault",
		"vault", vaultID, "tier", tierID, "total_tiers", len(vault.Tiers))
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

	// Remove per-tier retention and rotation jobs.
	o.removeVaultJobs(id, vault)

	// Remove from registry.
	delete(o.vaults, id)
	o.rebuildFilterSetLocked()

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

// UpdateVaultFilter updates a vault's filter expression.
// Returns ErrVaultNotFound if the vault doesn't exist.
//
// For rotation policy changes, use ChunkManager(id).SetRotationPolicy(policy)
// directly with a composed policy object.
func (o *Orchestrator) UpdateVaultFilter(id glid.GLID, filter string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, exists := o.vaults[id]; !exists {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, id)
	}

	if err := o.updateFilterLocked(id, filter); err != nil {
		return err
	}

	o.logger.Info("vault filter updated", "id", id, "filter", filter)
	return nil
}

// buildTierInstances creates TierInstance objects for each tier in the vault config.
// Every node joins every tier's Raft group regardless of storage placement
// (gastrolog-292yi). Nodes with storage placements also get a TierInstance with
// a chunk manager; nodes without storage only participate in the Raft group.
func (o *Orchestrator) buildTierInstances(sys *system.System, vaultCfg system.VaultConfig, factories Factories) ([]*TierInstance, error) {
	cfg := &sys.Config
	rt := &sys.Runtime
	o.ensureVaultControlPlaneRaftGroup(vaultCfg.ID, rt.Nodes, factories)

	tierIDs := system.VaultTierIDs(cfg.Tiers, vaultCfg.ID)
	if len(tierIDs) == 0 {
		return nil, nil // vault has no tiers yet — tiers are added incrementally via handleTierPut
	}

	nscs := rt.NodeStorageConfigs

	tiers := make([]*TierInstance, 0, len(tierIDs))
	for _, tierID := range tierIDs {
		tierCfg := findTierConfig(cfg.Tiers, tierID)
		if tierCfg == nil {
			// Tier config was deleted (e.g. drain-delete) but the vault still
			// references it. Skip gracefully — the vault tier list will be
			// cleaned up when the drain completes or on next reconciliation.
			o.logger.Warn("buildTierInstances: tier not in config, skipping",
				"vault", vaultCfg.ID, "tier", tierID)
			continue
		}

		// Determine if this node hosts this tier (as leader or follower).
		leaderNodeID := system.LeaderNodeID(rt.TierPlacements[tierCfg.ID], nscs)
		followerNodeIDs := system.FollowerNodeIDs(rt.TierPlacements[tierCfg.ID], nscs)
		isLeader := leaderNodeID == "" || leaderNodeID == o.localNodeID
		isFollower := slices.Contains(followerNodeIDs, o.localNodeID)
		if !isLeader && !isFollower {
			// No storage placement, but still join the vault control-plane Raft
			// group as a voter for this vault's replicated tier metadata.
			o.ensureVaultCtlTierMetadata(*tierCfg, rt.Nodes, factories)
			continue
		}

		if isLeader {
			ti, err := o.buildLeaderTierInstance(sys, vaultCfg, tierCfg, factories)
			if err != nil {
				o.alertTierInitFailed(tierID, tierCfg.Name, err)
				continue
			}
			ti.FollowerTargets = system.FollowerTargets(rt.TierPlacements[tierCfg.ID], nscs)
			tiers = append(tiers, ti)
		}

		// Follower: build one instance for this node's placement.
		// 1:1:1 constraint: at most one store per tier per node.
		if isFollower {
			localTargets := system.FollowerTargets(rt.TierPlacements[tierCfg.ID], nscs)
			for _, tgt := range localTargets {
				if tgt.NodeID != o.localNodeID {
					continue
				}
				sti, err := o.buildTierInstanceForStorage(sys, vaultCfg, *tierCfg, factories, tgt.StorageID, true)
				if err != nil {
					o.alertTierInitFailed(tierID, tierCfg.Name, err)
					break
				}
				sti.IsFollower = true
				sti.LeaderNodeID = leaderNodeID
				sti.StorageID = tgt.StorageID
				sti.Chunks.SetRotationPolicy(chunk.NeverRotatePolicy{})
				tiers = append(tiers, sti)
				break // 1:1:1: one store per tier per node
			}
		}
	}
	return tiers, nil
}

// alertTierInitFailed logs a warning and raises an alert for a tier that
// failed to initialize during vault build. The tier is skipped but the
// vault continues with its remaining tiers. The failed tier will be
// retried on the next reconfig cycle. See gastrolog-68fqk.
func (o *Orchestrator) alertTierInitFailed(tierID glid.GLID, tierName string, err error) {
	o.logger.Warn("buildTierInstances: tier init failed, skipping",
		"tier", tierID, "name", tierName, "error", err)
	if o.alerts != nil {
		o.alerts.Set(
			fmt.Sprintf("tier-init:%s", tierID),
			alert.Error, "orchestrator",
			fmt.Sprintf("Tier %q failed to initialize: %v", tierName, err),
		)
	}
}

// buildLeaderTierInstance creates the leader TierInstance using the placement's
// storage ID. This avoids directory collisions with same-node follower placements
// that would occur if findLocalFileStorage picked a different storage by class.
func (o *Orchestrator) buildLeaderTierInstance(sys *system.System, vaultCfg system.VaultConfig, tierCfg *system.TierConfig, factories Factories) (*TierInstance, error) {
	rt := &sys.Runtime
	storageID := system.LeaderStorageID(rt.TierPlacements[tierCfg.ID])
	if storageID != "" && !strings.HasPrefix(storageID, system.SyntheticStoragePrefix) {
		ti, err := o.buildTierInstanceForStorage(sys, vaultCfg, *tierCfg, factories, storageID, false)
		if err != nil {
			return nil, err
		}
		ti.StorageID = storageID
		return ti, nil
	}
	// Synthetic or unplaced — fall back to class-based resolution.
	ti, err := o.buildTierInstance(sys, vaultCfg, *tierCfg, factories, false)
	if err != nil {
		return nil, err
	}
	ti.StorageID = storageID
	return ti, nil
}

// buildTierInstance creates a single TierInstance from a TierConfig.
// When isFollower is true, cloud backing params are stripped so the follower's
// PostSealProcess only runs compress + index without uploading to cloud storage.
// Cloud tiers use a shared blob key (vault-ID/chunk-ID.glcb) — if the follower
// also uploads, it overwrites the leader's blob with a different-sized version,
// corrupting the leader's stored diskBytes and breaking all future cloud reads.
func (o *Orchestrator) buildTierInstance(sys *system.System, vaultCfg system.VaultConfig, tierCfg system.TierConfig, factories Factories, isFollower bool) (*TierInstance, error) {
	cfg := &sys.Config
	rt := &sys.Runtime
	// Map TierConfig.Type to factory name.
	factoryName := mapTierTypeToFactory(tierCfg.Type)

	// Create the vault-ctl Raft group BEFORE the chunk manager. Group creation is
	// fast (Raft log replay). Chunk manager creation is slow (scans disk for
	// existing chunks). Starting the Raft group early gives it time to elect
	// a leader and catch up while the chunk manager is loading.
	tierGroup, applier, raftCB := o.ensureVaultCtlTierMetadata(tierCfg, rt.Nodes, factories)

	// Build params from tier system.
	params := buildTierParams(sys, vaultCfg, tierCfg, o.localNodeID)

	// Followers keep cloud store access for reads (queries) but skip uploads.
	// The leader owns the blob; the follower adopts it via RegisterCloudChunk
	// when the tier FSM propagates the upload announcement.
	if isFollower {
		params["_cloud_read_only"] = "true"
	}

	cmFactory, ok := factories.ChunkManagers[factoryName]
	if !ok {
		return nil, fmt.Errorf("unknown chunk manager type: %s (mapped from tier type %s)", factoryName, tierCfg.Type)
	}

	var cmLogger = factories.Logger
	if cmLogger != nil {
		cmLogger = cmLogger.With("vault", vaultCfg.ID, "tier", tierCfg.ID)
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

	// Apply rotation policy from tier.
	if err := applyRotationPolicy(cm, cfg.RotationPolicies, tierCfg.RotationPolicyID); err != nil {
		_ = cm.Close()
		return nil, err
	}

	// Wire the Raft announcer now that both the group and chunk manager exist.
	setTierRaftAnnouncer(cm, applier, o.phase, o.logger)

	// JSONL sinks are write-only — no query engine, no indexes.
	if tierCfg.Type == system.TierTypeJSONL {
		ti := &TierInstance{
			TierID: tierCfg.ID,
			Type:   string(tierCfg.Type),
			Chunks: cm,
		}
		ti.applyRaftCallbacks(raftCB)
		o.attachLifecycleReconciler(ti, vaultCfg.ID, tierCfg.ID, tierGroup)
		wireTierFSMOnDelete(tierGroup, tierCfg.ID, cm, nil, o, o.logger)
		return ti, nil
	}

	imFactory, ok := factories.IndexManagers[factoryName]
	if !ok {
		_ = cm.Close()
		return nil, fmt.Errorf("unknown index manager type: %s (mapped from tier type %s)", factoryName, tierCfg.Type)
	}
	var imLogger = factories.Logger
	if imLogger != nil {
		imLogger = imLogger.With("vault", vaultCfg.ID, "tier", tierCfg.ID)
	}
	im, err := imFactory(cmParams, cm, imLogger)
	if err != nil {
		_ = cm.Close()
		return nil, fmt.Errorf("create index manager: %w", err)
	}

	var qeLogger = factories.Logger
	if qeLogger != nil {
		qeLogger = qeLogger.With("vault", vaultCfg.ID, "tier", tierCfg.ID)
	}
	qe := query.New(cm, im, qeLogger)

	// Inject index builders into the chunk manager's post-seal pipeline.
	if processor, ok := cm.(chunk.ChunkPostSealProcessor); ok {
		processor.SetIndexBuilders([]chunk.ChunkIndexBuilder{im.BuildAdapter()})
	}

	ti := &TierInstance{
		TierID:  tierCfg.ID,
		Type:    string(tierCfg.Type),
		Chunks:  cm,
		Indexes: im,
		Query:   qe,
	}
	ti.applyRaftCallbacks(raftCB)
	o.attachLifecycleReconciler(ti, vaultCfg.ID, tierCfg.ID, tierGroup)
	wireTierFSMOnDelete(tierGroup, tierCfg.ID, cm, im, o, o.logger)
	wireTierFSMOnUpload(tierGroup, tierCfg.ID, cm, o, o.logger)
	return ti, nil
}

// attachLifecycleReconciler constructs a TierLifecycleReconciler for the given
// tier instance and binds it to the tier sub-FSM in the vault control-plane
// Raft group. Skipped silently when there is no group (memory-mode tiers
// without replication) — single-node deletes go straight through the chunk
// manager via deleteChunk's local-only fallback. See gastrolog-51gme.
//
// Multiple TIs on the same node share a tier sub-FSM (1:1:1 placement makes
// this rare, but possible). Each TI's reconciler.Wire() call rebinds the
// callback set on the FSM; last-writer-wins matches the existing OnDelete /
// OnUpload behavior wired alongside.
func (o *Orchestrator) attachLifecycleReconciler(ti *TierInstance, vaultID, tierID glid.GLID, tierGroup *raftgroup.Group) {
	ti.Reconciler = NewTierLifecycleReconciler(o, vaultID, tierID, ti, o.localNodeID, o.logger)
	if tierGroup == nil {
		return
	}
	if vfsm, ok := tierGroup.FSM.(*vaultraft.FSM); ok && vfsm != nil {
		ti.Reconciler.Wire(vfsm.EnsureTierFSM(tierID))
	}
}

// buildTierInstanceForStorage creates a TierInstance whose data directory is
// resolved from a specific file storage ID. Used for both leaders with
// explicit storage placements and followers (one per node per tier).
func (o *Orchestrator) buildTierInstanceForStorage(sys *system.System, vaultCfg system.VaultConfig, tierCfg system.TierConfig, factories Factories, storageID string, isFollower bool) (*TierInstance, error) {
	cfg := &sys.Config
	rt := &sys.Runtime
	fs := findFileStorageByID(rt, storageID)
	if fs == nil {
		return nil, fmt.Errorf("file storage %s not found", storageID)
	}

	// Create the vault-ctl Raft group BEFORE the chunk manager — same rationale
	// as buildTierInstance: start elections while chunk loading is in progress.
	tierGroup, applier, raftCB := o.ensureVaultCtlTierMetadata(tierCfg, rt.Nodes, factories)

	// Build params normally, then override the dir with this storage's path.
	params := buildTierParams(sys, vaultCfg, tierCfg, o.localNodeID)
	// Followers keep cloud store access for reads but skip uploads.
	if isFollower {
		params["_cloud_read_only"] = "true"
	}
	params["dir"] = filepath.Join(fs.Path, "vaults", vaultCfg.ID.String(), tierCfg.ID.String())

	factoryName := mapTierTypeToFactory(tierCfg.Type)
	cmFactory, ok := factories.ChunkManagers[factoryName]
	if !ok {
		return nil, fmt.Errorf("unknown chunk manager type: %s", factoryName)
	}

	var cmLogger = factories.Logger
	if cmLogger != nil {
		cmLogger = cmLogger.With("vault", vaultCfg.ID, "tier", tierCfg.ID, "storage", storageID)
	}

	cmParams := resolveVaultDir(params, factories.VaultsDir, vaultCfg.ID.String())
	cmParams["_expect_existing"] = "true"
	cmParams["_vault_id"] = vaultCfg.ID.String()

	cm, err := cmFactory(cmParams, cmLogger)
	if err != nil {
		return nil, fmt.Errorf("create chunk manager: %w", err)
	}

	if err := applyRotationPolicy(cm, cfg.RotationPolicies, tierCfg.RotationPolicyID); err != nil {
		_ = cm.Close()
		return nil, err
	}

	// Wire Raft announcer now that chunk manager exists.
	setTierRaftAnnouncer(cm, applier, o.phase, o.logger)

	// Follower replicas need index builders for local queries.
	imFactory, ok := factories.IndexManagers[factoryName]
	if !ok {
		_ = cm.Close()
		return nil, fmt.Errorf("unknown index manager type: %s", factoryName)
	}
	var imLogger = factories.Logger
	if imLogger != nil {
		imLogger = imLogger.With("vault", vaultCfg.ID, "tier", tierCfg.ID, "storage", storageID)
	}
	im, err := imFactory(cmParams, cm, imLogger)
	if err != nil {
		_ = cm.Close()
		return nil, fmt.Errorf("create index manager: %w", err)
	}

	var qeLogger = factories.Logger
	if qeLogger != nil {
		qeLogger = qeLogger.With("vault", vaultCfg.ID, "tier", tierCfg.ID, "storage", storageID)
	}
	qe := query.New(cm, im, qeLogger)

	if processor, ok := cm.(chunk.ChunkPostSealProcessor); ok {
		processor.SetIndexBuilders([]chunk.ChunkIndexBuilder{im.BuildAdapter()})
	}

	ti := &TierInstance{
		TierID:  tierCfg.ID,
		Type:    string(tierCfg.Type),
		Chunks:  cm,
		Indexes: im,
		Query:   qe,
	}
	ti.applyRaftCallbacks(raftCB)
	o.attachLifecycleReconciler(ti, vaultCfg.ID, tierCfg.ID, tierGroup)
	wireTierFSMOnDelete(tierGroup, tierCfg.ID, cm, im, o, o.logger)
	wireTierFSMOnUpload(tierGroup, tierCfg.ID, cm, o, o.logger)
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

// tierRaftCallbacks holds the callbacks returned by ensureVaultCtlTierMetadata.
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
	members := o.buildTierRaftMembers(clusterNodes, factories)
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

type tierRaftCallbacks struct {
	hasLeader               func() bool
	isLeader                func() bool
	isFSMReady              func() bool
	applyRequestDelete      func(id chunk.ChunkID, reason string, expectedFrom []string) error
	applyAckDelete          func(id chunk.ChunkID, nodeID string) error
	applyFinalizeDelete     func(id chunk.ChunkID) error
	applyPruneNode          func(nodeID string) error
	applyRetPending         func(id chunk.ChunkID) error
	applyTransitionStreamed func(id chunk.ChunkID) error
	applyTransitionReceived func(sourceChunkID chunk.ChunkID) error
	hasTransitionReceipt    func(sourceChunkID chunk.ChunkID) bool
	isTombstoned            func(id chunk.ChunkID) bool
	listChunks              func() []chunk.ChunkID
	listRetPending          func() []chunk.ChunkID
	listTransitionStreamed  func() []chunk.ChunkID
	overlayFromFSM          func(chunk.ChunkMeta) chunk.ChunkMeta
}

// ensureVaultCtlTierMetadata joins this node to the vault control-plane
// Raft group for the tier's vault (creating the group if needed) and
// returns the applier + callbacks for this tier's chunk metadata. Every
// tier in the same vault shares the same vault-ctl Raft group; each
// tier's chunk FSM is a sub-FSM keyed by tier ID (see vaultraft.FSM and
// tier/raftfsm.FSM). With no GroupManager, returns nils.
//
// Post-gastrolog-5xxbd there is no per-vault-ctl Raft group. The historical
// function name ensureVaultCtlTierMetadata is preserved as a no-op alias in
// tests only; production wires through this function.
//
// Call this BEFORE creating the chunk manager so Raft can start
// elections while chunk loading is still in progress.
func (o *Orchestrator) ensureVaultCtlTierMetadata(tierCfg system.TierConfig, clusterNodes []system.NodeConfig, factories Factories) (*raftgroup.Group, tierfsm.Applier, tierRaftCallbacks) {
	if factories.GroupManager == nil {
		return nil, nil, tierRaftCallbacks{}
	}
	vaultGID := raftgroup.VaultControlPlaneGroupID(tierCfg.VaultID)
	g, members := o.tryStartClusterRaftGroup(vaultGID, vaultraft.NewFSM(), clusterNodes, factories)
	if g == nil {
		return nil, nil, tierRaftCallbacks{}
	}

	o.vaultCtlLeaders.SetDesiredMembers(tierCfg.VaultID, members)
	o.vaultCtlLeaders.Start(tierCfg.VaultID, g)

	vfsm, ok := g.FSM.(*vaultraft.FSM)
	if !ok || vfsm == nil {
		return nil, nil, tierRaftCallbacks{}
	}
	// Wire the after-restore hook so that vault-ctl snapshot install on
	// this node triggers the receipt protocol's catchup pass on every
	// tier reconciler in the vault. Idempotent — calling SetOnAfterRestore
	// on every ensureVaultCtlTierMetadata invocation is fine; later calls
	// just rebind to the same closure. Without this hook, the receipt
	// protocol's pendingDeletes silently leak across snapshot install
	// boundaries (the bug gastrolog-51gme step 3 was supposed to close).
	vaultID := tierCfg.VaultID
	vfsm.SetOnAfterRestore(func() { o.afterVaultCtlRestore(vaultID) })
	tierFSM := vfsm.EnsureTierFSM(tierCfg.ID)
	r := g.Raft
	timeout := cluster.ReplicationTimeout

	var applier tierfsm.Applier
	if factories.PeerConns != nil {
		applier = cluster.NewVaultCtlTierApplyForwarder(r, vaultGID, tierCfg.ID, factories.PeerConns, timeout)
	} else {
		applier = &vaultCtlTierApplier{o: o, vaultID: tierCfg.VaultID, tierID: tierCfg.ID}
	}

	return g, applier, buildTierRaftCallbacks(r, tierFSM, applier)
}

// buildTierRaftCallbacks constructs the callback struct for replicated tier
// chunk metadata (vault control-plane Raft in cluster mode).
// Extracted from ensureVaultCtlTierMetadata to keep cognitive complexity within lint
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
// Before 5xxbd, tier FSM was a top-level Raft group whose Ready flag
// flipped on every apply in practice, because `CmdPutTier` was a LogCommand
// that hit it. After 5xxbd the tier sub-FSM only sees OpTierFSM commands,
// which a fresh vault with no chunks never sends — keying readiness on any
// FSM-level signal leaves every fresh vault wedged as "not ready" until
// first ingestion.
func buildTierRaftCallbacks(r *hraft.Raft, fsm *tierfsm.FSM, applier tierfsm.Applier) tierRaftCallbacks {
	return tierRaftCallbacks{
		hasLeader:  func() bool { return r.Leader() != "" },
		isLeader:   func() bool { return r.State() == hraft.Leader },
		isFSMReady: func() bool { return r.AppliedIndex() > 0 },
		applyRequestDelete: func(id chunk.ChunkID, reason string, expectedFrom []string) error {
			return applier.Apply(tierfsm.MarshalRequestDelete(id, time.Now(), reason, expectedFrom))
		},
		applyAckDelete: func(id chunk.ChunkID, nodeID string) error {
			return applier.Apply(tierfsm.MarshalAckDelete(id, nodeID))
		},
		applyFinalizeDelete: func(id chunk.ChunkID) error {
			return applier.Apply(tierfsm.MarshalFinalizeDelete(id))
		},
		applyPruneNode: func(nodeID string) error {
			return applier.Apply(tierfsm.MarshalPruneNode(nodeID))
		},
		applyRetPending: func(id chunk.ChunkID) error {
			return applier.Apply(tierfsm.MarshalRetentionPending(id))
		},
		applyTransitionStreamed: func(id chunk.ChunkID) error {
			return applier.Apply(tierfsm.MarshalTransitionStreamed(id))
		},
		applyTransitionReceived: func(sourceChunkID chunk.ChunkID) error {
			return applier.Apply(tierfsm.MarshalTransitionReceived(sourceChunkID))
		},
		hasTransitionReceipt: func(sourceChunkID chunk.ChunkID) bool {
			if fsm == nil {
				return false
			}
			return fsm.HasTransitionReceipt(sourceChunkID)
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
		listRetPending:         listFSMByFlag(fsm, func(e tierfsm.ManifestEntry) bool { return e.RetentionPending }),
		listTransitionStreamed: listFSMByFlag(fsm, func(e tierfsm.ManifestEntry) bool { return e.TransitionStreamed }),
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
			m.NumFrames = e.NumFrames
			return m
		},
	}
}

// listFSMByFlag returns a function that filters the FSM's entries by a
// boolean predicate (e.g., RetentionPending or TransitionStreamed).
func listFSMByFlag(fsm *tierfsm.FSM, pred func(tierfsm.ManifestEntry) bool) func() []chunk.ChunkID {
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

// setTierRaftAnnouncer wires the Raft announcer to a chunk manager after both
// the Raft group and chunk manager have been created. The applier handles
// routing to the vault ctl Raft leader when peers are configured. The phase parameter lets
// the announcer short-circuit during shutdown so trailing applies don't
// fire "raft is already shutdown" warnings (see gastrolog-1e5ke).
func setTierRaftAnnouncer(cm chunk.ChunkManager, applier tierfsm.Applier, phase *lifecycle.Phase, logger *slog.Logger) {
	if applier == nil {
		return
	}
	setter, ok := cm.(chunk.AnnouncerSetter)
	if !ok {
		return
	}
	setter.SetAnnouncer(tierfsm.NewAnnouncer(applier, phase, logger))
}

// clearTierFSMChunkCallbacks clears OnDelete/OnUpload for a tier's FSM slice
// in the vault control-plane group. Used before closing that tier's chunk
// manager when the Raft group may still deliver log entries for this tier
// (e.g. RemoveTierFromVault while other tiers in the same vault stay open).
func (o *Orchestrator) clearTierFSMChunkCallbacks(vaultID, tierID glid.GLID) {
	if o.groupMgr == nil {
		return
	}
	g := o.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(vaultID))
	if g == nil {
		return
	}
	var fsm *tierfsm.FSM
	switch raw := g.FSM.(type) {
	case *tierfsm.FSM:
		fsm = raw
	case *vaultraft.FSM:
		fsm = raw.EnsureTierFSM(tierID)
	default:
		return
	}
	fsm.SetOnDelete(nil)
	fsm.SetOnUpload(nil)
	if o.logger != nil {
		o.logger.Debug("cleared tier FSM chunk callbacks before manager close",
			"vault", vaultID, "tier", tierID)
	}
}

// wireTierFSMOnDelete sets up the tier FSM's OnDelete callback so that
// CmdDeleteChunk applied via Raft on this node deletes the local chunk
// files (and indexes if available). The callback uses chunk.SilentDeleter
// to avoid the announcer feedback loop — re-announcing the delete that
// just arrived from Raft would cause infinite re-application.
//
// Safe to call with nil group, nil cm, or a chunk manager that doesn't
// implement SilentDeleter (e.g. memory tiers): the callback is simply not
// wired in those cases.
//
// IMPORTANT: this callback acquires the chunk manager's m.mu via
// DeleteSilent. For the FSM apply goroutine to do this safely, no other
// goroutine may hold m.mu while waiting for a Raft round-trip (e.g. via
// the Announcer). The chunk.file.Manager's Seal/Append/Compress paths
// enforce this by releasing m.mu before calling the announcer; if a new
// path is added that holds the mutex during an announcer call, this
// callback will deadlock with it.
func wireTierFSMOnDelete(g *raftgroup.Group, tierID glid.GLID, cm chunk.ChunkManager, im index.IndexManager, o *Orchestrator, logger *slog.Logger) {
	if g == nil || cm == nil {
		return
	}
	var fsm *tierfsm.FSM
	switch raw := g.FSM.(type) {
	case *tierfsm.FSM:
		fsm = raw
	case *vaultraft.FSM:
		fsm = raw.EnsureTierFSM(tierID)
	default:
		return
	}
	silent, ok := cm.(chunk.SilentDeleter)
	if !ok {
		return
	}
	fsm.SetOnDelete(func(id chunk.ChunkID) {
		// Notify WatchChunks subscribers regardless of local-delete
		// outcome: the FSM's authoritative chunks-map entry is gone,
		// so the inspector's view on this node is stale. Fire on
		// every node where the apply ran, even ones that never had
		// the chunk locally (they may still have rendered it via
		// the cluster-wide ListChunks fan-out). See gastrolog-2ob86.
		if o != nil {
			defer o.NotifyChunkChange()
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

// wireTierFSMOnUpload connects the tier FSM's OnUpload callback to the
// chunk manager's RegisterCloudChunk method. When the FSM applies CmdUploadChunk
// (from the leader's AnnounceUpload), the follower's chunk manager registers
// the cloud chunk from metadata alone — no record streaming or S3 download.
func wireTierFSMOnUpload(g *raftgroup.Group, tierID glid.GLID, cm chunk.ChunkManager, o *Orchestrator, logger *slog.Logger) {
	if g == nil || cm == nil {
		return
	}
	var fsm *tierfsm.FSM
	switch raw := g.FSM.(type) {
	case *tierfsm.FSM:
		fsm = raw
	case *vaultraft.FSM:
		fsm = raw.EnsureTierFSM(tierID)
	default:
		return
	}
	registrar, ok := cm.(chunk.CloudChunkRegistrar)
	if !ok {
		return
	}
	fsm.SetOnUpload(func(e tierfsm.ManifestEntry) {
		// Notify WatchChunks subscribers: the chunk transitioned to
		// cloud-backed (DiskBytes / NumFrames / CloudBacked changed
		// in the FSM), which the inspector renders. Fire regardless
		// of RegisterCloudChunk outcome — FSM state is authoritative.
		// See gastrolog-2ob86.
		if o != nil {
			defer o.NotifyChunkChange()
		}
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
			NumFrames:       e.NumFrames,
		}
		if err := registrar.RegisterCloudChunk(e.ID, info); err != nil {
			if logger != nil {
				logger.Debug("FSM onUpload: RegisterCloudChunk failed",
					"chunk", e.ID, "error", err)
			}
		}
	})
}

// buildTierRaftMembers returns ALL cluster nodes as Raft members for a vault
// control-plane Raft group. Every node participates regardless of which tiers
// it stores — nodes without local tier data still replicate tier metadata.
// See gastrolog-292yi.
func (o *Orchestrator) buildTierRaftMembers(clusterNodes []system.NodeConfig, factories Factories) []hraft.Server {
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

func mapTierTypeToFactory(t system.TierType) string {
	switch t {
	case system.TierTypeMemory:
		return "memory"
	case system.TierTypeFile:
		return "file"
	case system.TierTypeCloud:
		return "file"
	case system.TierTypeJSONL:
		return "jsonl"
	default:
		return string(t)
	}
}

// buildTierParams builds a params map from a TierConfig suitable for factory consumption.
func buildTierParams(sys *system.System, vaultCfg system.VaultConfig, tierCfg system.TierConfig, localNodeID string) map[string]string { //nolint:gocognit // flat type-switch mapping
	cfg := &sys.Config
	rt := &sys.Runtime
	params := make(map[string]string)

	switch tierCfg.Type {
	case system.TierTypeMemory:
		if tierCfg.MemoryBudgetBytes > 0 {
			params["budgetBytes"] = strconv.FormatUint(tierCfg.MemoryBudgetBytes, 10)
		}

	case system.TierTypeFile:
		if fs := findLocalFileStorage(rt, localNodeID, tierCfg.StorageClass); fs != nil {
			params["dir"] = filepath.Join(fs.Path, "vaults", vaultCfg.ID.String(), tierCfg.ID.String())
		}

	case system.TierTypeJSONL:
		if tierCfg.Path != "" {
			params["path"] = tierCfg.Path
		} else {
			// Default: jsonl/<vault-id>/<tier-id>.jsonl
			params["path"] = filepath.Join("jsonl", vaultCfg.ID.String(), tierCfg.ID.String()+".jsonl")
		}

	case system.TierTypeCloud:
		if tierCfg.CloudServiceID != nil { //nolint:nestif // cloud params resolution
			cs := findCloudService(cfg, *tierCfg.CloudServiceID)
			if cs != nil {
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
		}
		// Cloud tiers also need a local file storage for active chunks.
		if fs := findLocalFileStorage(rt, localNodeID, tierCfg.ActiveChunkClass); fs != nil {
			params["dir"] = filepath.Join(fs.Path, "vaults", vaultCfg.ID.String(), tierCfg.ID.String())
		}
		// Cache directory for locally-cached GLCB blobs (avoids cloud range requests).
		if fs := findLocalFileStorage(rt, localNodeID, tierCfg.CacheClass); fs != nil {
			params["cache_dir"] = filepath.Join(fs.Path, "cache", vaultCfg.ID.String(), tierCfg.ID.String())
		}
		if tierCfg.CacheEviction != "" {
			params["cache_eviction"] = tierCfg.CacheEviction
		}
		if tierCfg.CacheBudget != "" {
			params["cache_budget"] = tierCfg.CacheBudget
		}
		if tierCfg.CacheTTL != "" {
			params["cache_ttl"] = tierCfg.CacheTTL
		}
	}

	return params
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
