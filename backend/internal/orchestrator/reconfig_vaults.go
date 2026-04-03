package orchestrator

import (
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/config"
	"gastrolog/internal/multiraft"
	"gastrolog/internal/query"

	"github.com/google/uuid"
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
func (o *Orchestrator) AddVault(ctx context.Context, vaultCfg config.VaultConfig, factories Factories) error {
	cfg, err := o.loadConfig(ctx)
	if err != nil {
		return fmt.Errorf("load config for AddVault: %w", err)
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if _, exists := o.vaults[vaultCfg.ID]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateID, vaultCfg.ID)
	}

	tiers, err := o.buildTierInstances(cfg, vaultCfg, factories)
	if err != nil {
		return fmt.Errorf("build tier instances for vault %s: %w", vaultCfg.ID, err)
	}

	// If all tiers are assigned to other nodes, skip this vault locally.
	if len(tiers) == 0 {
		o.logger.Info("vault has no local tiers, skipping AddVault", "id", vaultCfg.ID, "name", vaultCfg.Name)
		return nil
	}

	// Register vault.
	vault := NewVault(vaultCfg.ID, tiers...)
	vault.Name = vaultCfg.Name
	o.vaults[vaultCfg.ID] = vault

	// Compile filters immediately so the vault can receive records right away.
	// The rotation sweep also reconciles filters every 15s as a safety net.
	if cfg != nil {
		_ = o.reloadFiltersFromRoutes(cfg)
	}

	// Rotation and retention are reconciled by the discovery-based sweep
	// jobs on their next tick.

	o.logger.Info("vault added", "id", vaultCfg.ID, "name", vaultCfg.Name, "tiers", len(tiers))
	return nil
}

// Rotation and retention are handled by discovery-based sweep jobs
// (rotationSweep and retentionSweepAll). No per-tier setup needed during AddVault.

// findTierConfig finds a TierConfig by ID in a slice.
func findTierConfig(tiers []config.TierConfig, id uuid.UUID) *config.TierConfig {
	for i := range tiers {
		if tiers[i].ID == id {
			return &tiers[i]
		}
	}
	return nil
}

func findVaultConfig(vaults []config.VaultConfig, id uuid.UUID) *config.VaultConfig {
	for i := range vaults {
		if vaults[i].ID == id {
			return &vaults[i]
		}
	}
	return nil
}

// resolveRetentionRulesFromTier converts tier retention rules to resolved retentionRule objects.
func resolveRetentionRulesFromTier(cfg *config.Config, vaultCfg config.VaultConfig, tierCfg *config.TierConfig) ([]retentionRule, error) {
	// Derive the retention action from the tier's position in the vault chain.
	// The stored action on the tier config is ignored — position is the source
	// of truth. This prevents stale actions when tiers are added/removed.
	tierIndex := slices.Index(vaultCfg.TierIDs, tierCfg.ID)
	isLastTier := tierIndex < 0 || tierIndex == len(vaultCfg.TierIDs)-1

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
		if action != config.RetentionActionEject {
			if isLastTier {
				action = config.RetentionActionExpire
			} else {
				action = config.RetentionActionTransition
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
func (o *Orchestrator) RemoveVault(id uuid.UUID) error {
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
func (o *Orchestrator) removeVaultJobs(id uuid.UUID, vault *Vault) {
	for _, tier := range vault.Tiers {
		delete(o.retention, retentionKey(tier.TierID, tier.StorageID))
	}
	o.cronRotation.removeAllForVault(id)
}

// teardownVault performs the common cleanup for all vault removal paths:
// cancels pending jobs, closes managers, removes from registry, rebuilds filters.
func (o *Orchestrator) teardownVault(id uuid.UUID, vault *Vault) {
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
func (o *Orchestrator) DisableVault(id uuid.UUID) error {
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
func (o *Orchestrator) EnableVault(id uuid.UUID) error {
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
func (o *Orchestrator) IsVaultEnabled(id uuid.UUID) bool {
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
func (o *Orchestrator) ForceRemoveVault(id uuid.UUID) error {
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
			if err := cm.Delete(meta.ID); err != nil {
				return fmt.Errorf("delete chunk %s in tier %s vault %s: %w", meta.ID.String(), tier.TierID, id, err)
			}
		}
	}

	o.teardownVault(id, vault)
	o.logger.Info("vault force-removed", "id", id, "name", vault.Name, "type", vault.Type())
	return nil
}

// DeleteTierData deletes all chunks and indexes for a tier, then removes the
// data directory. Called when a tier is deleted with delete_data=true.
func (o *Orchestrator) DeleteTierData(tierID uuid.UUID) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	for _, vault := range o.vaults {
		for _, tier := range vault.Tiers {
			if tier.TierID != tierID {
				continue
			}
			// Seal active chunk so we can delete it.
			if active := tier.Chunks.Active(); active != nil {
				_ = tier.Chunks.Seal()
			}
			metas, err := tier.Chunks.List()
			if err != nil {
				o.logger.Warn("delete tier data: list failed", "tier", tierID, "error", err)
				return
			}
			for _, m := range metas {
				if tier.Indexes != nil {
					_ = tier.Indexes.DeleteIndexes(m.ID)
				}
				_ = tier.Chunks.Delete(m.ID)
			}
			o.logger.Info("tier data deleted", "tier", tierID, "chunks", len(metas))
			return
		}
	}
}

// RemoveTierFromVault removes a single tier instance from a vault, deletes its
// data (chunks + indexes), closes its managers, and cleans up retention/rotation
// jobs. Used when this node loses ownership of a tier (rebalancing, RF reduction).
// Returns true if a tier was removed.
func (o *Orchestrator) RemoveTierFromVault(vaultID, tierID uuid.UUID) bool {
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

	// Delete all chunk data and indexes.
	if active := tier.Chunks.Active(); active != nil {
		_ = tier.Chunks.Seal()
	}
	metas, err := tier.Chunks.List()
	if err == nil {
		for _, m := range metas {
			if tier.Indexes != nil {
				_ = tier.Indexes.DeleteIndexes(m.ID)
			}
			_ = tier.Chunks.Delete(m.ID)
		}
	}

	// Close managers.
	_ = tier.Chunks.Close()

	// Remove retention runner and cron rotation for this tier.
	delete(o.retention, retentionKey(tier.TierID, tier.StorageID))
	o.cronRotation.removeAllForVault(vaultID)

	// Remove tier from vault's tier list.
	vault.Tiers = append(vault.Tiers[:idx], vault.Tiers[idx+1:]...)

	// If the vault has no tiers left, remove it entirely.
	if len(vault.Tiers) == 0 {
		delete(o.vaults, vaultID)
		o.rebuildFilterSetLocked()
		o.logger.Info("vault removed (no remaining tiers)", "vault", vaultID)
	}

	o.logger.Info("tier removed from vault",
		"vault", vaultID, "tier", tierID, "remaining_tiers", len(vault.Tiers))
	return true
}

// AddTierToVault builds a single tier instance and adds it to an existing vault
// without tearing down any other tiers. This is the incremental counterpart to
// RemoveTierFromVault.
func (o *Orchestrator) AddTierToVault(ctx context.Context, vaultID, tierID uuid.UUID, factories Factories) error {
	cfg, err := o.loadConfig(ctx)
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

	tierCfg := findTierConfig(cfg.Tiers, tierID)
	if tierCfg == nil {
		return fmt.Errorf("tier %s not found in config", tierID)
	}

	vaultCfg := findVaultConfig(cfg.Vaults, vaultID)
	if vaultCfg == nil {
		return fmt.Errorf("vault %s not found in config", vaultID)
	}

	nscs := cfg.NodeStorageConfigs
	leaderNodeID := tierCfg.LeaderNodeID(nscs)
	followerNodeIDs := tierCfg.FollowerNodeIDs(nscs)
	isLeader := leaderNodeID == "" || leaderNodeID == o.localNodeID
	isFollower := slices.Contains(followerNodeIDs, o.localNodeID)
	if !isLeader && !isFollower {
		return nil // this node doesn't host this tier
	}

	var ti *TierInstance
	if isLeader {
		t, err := o.buildPrimaryTierInstance(cfg, *vaultCfg, tierCfg, factories)
		if err != nil {
			return fmt.Errorf("build tier %s: %w", tierID, err)
		}
		t.FollowerTargets = tierCfg.FollowerTargets(nscs)
		ti = t
	} else {
		for _, tgt := range tierCfg.FollowerTargets(nscs) {
			if tgt.NodeID != o.localNodeID {
				continue
			}
			t, err := o.buildTierInstanceForStorage(cfg, *vaultCfg, *tierCfg, factories, tgt.StorageID, true)
			if err != nil {
				return fmt.Errorf("build tier %s storage %s: %w", tierID, tgt.StorageID, err)
			}
			t.IsFollower = true
			t.LeaderNodeID = leaderNodeID
			t.StorageID = tgt.StorageID
			t.Chunks.SetRotationPolicy(chunk.NeverRotatePolicy{})
			raftCB := o.wireTierRaftGroup(t.Chunks, *tierCfg, nscs, factories, true)
			t.HasRaftLeader = raftCB.hasLeader
			t.IsRaftLeader = raftCB.isLeader
			t.ApplyRaftDelete = raftCB.applyDelete
			t.ListManifest = raftCB.listChunks
			t.ApplyRaftRetentionPending = raftCB.applyRetPending
			t.ListRetentionPending = raftCB.listRetPending
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
func (o *Orchestrator) UnregisterVault(id uuid.UUID) error {
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
func (o *Orchestrator) VaultConfig(id uuid.UUID) (config.VaultConfig, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if _, exists := o.vaults[id]; !exists {
		return config.VaultConfig{}, fmt.Errorf("%w: %s", ErrVaultNotFound, id)
	}

	cfg := config.VaultConfig{
		ID: id,
	}

	return cfg, nil
}

// UpdateVaultFilter updates a vault's filter expression.
// Returns ErrVaultNotFound if the vault doesn't exist.
//
// For rotation policy changes, use ChunkManager(id).SetRotationPolicy(policy)
// directly with a composed policy object.
func (o *Orchestrator) UpdateVaultFilter(id uuid.UUID, filter string) error {
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
// Tiers whose leader/follower storages don't resolve to the local node are skipped
// (placement manager assigns storages via Raft).
func (o *Orchestrator) buildTierInstances(cfg *config.Config, vaultCfg config.VaultConfig, factories Factories) ([]*TierInstance, error) {
	if len(vaultCfg.TierIDs) == 0 {
		return nil, fmt.Errorf("vault %s has no tier IDs", vaultCfg.ID)
	}

	nscs := cfg.NodeStorageConfigs

	closeTiers := func(ts []*TierInstance) {
		for _, t := range ts {
			_ = t.Chunks.Close()
		}
	}

	tiers := make([]*TierInstance, 0, len(vaultCfg.TierIDs))
	for _, tierID := range vaultCfg.TierIDs {
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
		leaderNodeID := tierCfg.LeaderNodeID(nscs)
		followerNodeIDs := tierCfg.FollowerNodeIDs(nscs)
		isLeader := leaderNodeID == "" || leaderNodeID == o.localNodeID
		isFollower := slices.Contains(followerNodeIDs, o.localNodeID)
		if !isLeader && !isFollower {
			continue
		}

		if isLeader {
			ti, err := o.buildPrimaryTierInstance(cfg, vaultCfg, tierCfg, factories)
			if err != nil {
				closeTiers(tiers)
				return nil, fmt.Errorf("build tier %s: %w", tierID, err)
			}
			ti.FollowerTargets = tierCfg.FollowerTargets(nscs)
			tiers = append(tiers, ti)
		}

		// Follower: build one instance for this node's placement.
		// 1:1:1 constraint: at most one store per tier per node.
		if isFollower {
			localTargets := tierCfg.FollowerTargets(nscs)
			for _, tgt := range localTargets {
				if tgt.NodeID != o.localNodeID {
					continue
				}
				sti, err := o.buildTierInstanceForStorage(cfg, vaultCfg, *tierCfg, factories, tgt.StorageID, true)
				if err != nil {
					closeTiers(tiers)
					return nil, fmt.Errorf("build tier %s storage %s: %w", tierID, tgt.StorageID, err)
				}
				sti.IsFollower = true
				sti.LeaderNodeID = leaderNodeID
				sti.StorageID = tgt.StorageID
				sti.Chunks.SetRotationPolicy(chunk.NeverRotatePolicy{})
				// Create the tier Raft group (non-bootstrapped) so this
				// node accepts AppendEntries from the leader.
				raftCB := o.wireTierRaftGroup(sti.Chunks, *tierCfg, nscs, factories, true)
				sti.HasRaftLeader = raftCB.hasLeader
				sti.IsRaftLeader = raftCB.isLeader
				sti.ApplyRaftDelete = raftCB.applyDelete
				sti.ListManifest = raftCB.listChunks
				sti.ApplyRaftRetentionPending = raftCB.applyRetPending
				sti.ListRetentionPending = raftCB.listRetPending
				tiers = append(tiers, sti)
				break // 1:1:1: one store per tier per node
			}
		}
	}
	return tiers, nil
}

// buildPrimaryTierInstance creates the leader TierInstance using the placement's
// storage ID. This avoids directory collisions with same-node follower placements
// that would occur if findLocalFileStorage picked a different storage by class.
func (o *Orchestrator) buildPrimaryTierInstance(cfg *config.Config, vaultCfg config.VaultConfig, tierCfg *config.TierConfig, factories Factories) (*TierInstance, error) {
	storageID := tierCfg.LeaderStorageID()
	if storageID != "" && !strings.HasPrefix(storageID, config.SyntheticStoragePrefix) {
		ti, err := o.buildTierInstanceForStorage(cfg, vaultCfg, *tierCfg, factories, storageID, false)
		if err != nil {
			return nil, err
		}
		ti.StorageID = storageID
		// Wire tier Raft group for the leader — buildTierInstanceForStorage
		// doesn't call wireTierRaftGroup itself.
		raftCB := o.wireTierRaftGroup(ti.Chunks, *tierCfg, cfg.NodeStorageConfigs, factories, false)
		ti.HasRaftLeader = raftCB.hasLeader
		ti.IsRaftLeader = raftCB.isLeader
		ti.ApplyRaftDelete = raftCB.applyDelete
		ti.ListManifest = raftCB.listChunks
		ti.ApplyRaftRetentionPending = raftCB.applyRetPending
		ti.ListRetentionPending = raftCB.listRetPending
		return ti, nil
	}
	// Synthetic or unplaced — fall back to class-based resolution.
	ti, err := o.buildTierInstance(cfg, vaultCfg, *tierCfg, factories, false)
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
func (o *Orchestrator) buildTierInstance(cfg *config.Config, vaultCfg config.VaultConfig, tierCfg config.TierConfig, factories Factories, isFollower bool) (*TierInstance, error) {
	// Map TierConfig.Type to factory name.
	factoryName := mapTierTypeToFactory(tierCfg.Type)

	// Build params from tier config.
	params := buildTierParams(cfg, vaultCfg, tierCfg, o.localNodeID)

	// Followers must NOT upload to cloud storage. The leader owns the
	// cloud blob; the follower keeps a local compressed copy for queries.
	if isFollower {
		delete(params, "sealed_backing")
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
	if tierCfg.RotationPolicyID != nil { //nolint:nestif // policy resolution
		policyCfg := findRotationPolicy(cfg.RotationPolicies, *tierCfg.RotationPolicyID)
		if policyCfg != nil {
			policy, pErr := policyCfg.ToRotationPolicy()
			if pErr != nil {
				_ = cm.Close()
				return nil, fmt.Errorf("invalid rotation policy %s: %w", *tierCfg.RotationPolicyID, pErr)
			}
			if policy != nil {
				cm.SetRotationPolicy(policy)
			}
		}
	}

	// Wire Raft-backed metadata announcer if a GroupManager is available.
	raftCB := o.wireTierRaftGroup(cm, tierCfg, cfg.NodeStorageConfigs, factories, isFollower)

	// JSONL sinks are write-only — no query engine, no indexes.
	if tierCfg.Type == config.TierTypeJSONL {
		return &TierInstance{
			TierID:          tierCfg.ID,
			Type:            string(tierCfg.Type),
			Chunks:          cm,
			HasRaftLeader:   raftCB.hasLeader,
			IsRaftLeader:    raftCB.isLeader,
			ApplyRaftDelete:           raftCB.applyDelete,
			ListManifest:             raftCB.listChunks,
			ApplyRaftRetentionPending: raftCB.applyRetPending,
			ListRetentionPending:     raftCB.listRetPending,
		}, nil
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

	return &TierInstance{
		TierID:          tierCfg.ID,
		Type:            string(tierCfg.Type),
		Chunks:          cm,
		Indexes:         im,
		Query:           qe,
		HasRaftLeader:             raftCB.hasLeader,
		ApplyRaftDelete:           raftCB.applyDelete,
		ListManifest:             raftCB.listChunks,
		ApplyRaftRetentionPending: raftCB.applyRetPending,
		ListRetentionPending:     raftCB.listRetPending,
	}, nil
}

// buildTierInstanceForStorage creates a TierInstance whose data directory is
// resolved from a specific file storage ID. Used for both primaries with
// explicit placements and secondaries (one per node per tier).
func (o *Orchestrator) buildTierInstanceForStorage(cfg *config.Config, vaultCfg config.VaultConfig, tierCfg config.TierConfig, factories Factories, storageID string, isFollower bool) (*TierInstance, error) {
	fs := findFileStorageByID(cfg, storageID)
	if fs == nil {
		return nil, fmt.Errorf("file storage %s not found", storageID)
	}

	// Build params normally, then override the dir with this storage's path.
	params := buildTierParams(cfg, vaultCfg, tierCfg, o.localNodeID)
	// Followers must NOT upload to cloud storage — the leader owns the cloud
	// blob. If the follower also uploads, it overwrites the leader's blob with
	// a different-sized version, corrupting diskBytes and breaking cloud reads.
	if isFollower {
		delete(params, "sealed_backing")
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

	// Same-node secondaries share the primary's tier Raft group.
	// Wire the manifest callbacks so they reconcile like any other follower.
	var raftCB tierRaftCallbacks
	if factories.GroupManager != nil {
		if g := factories.GroupManager.GetGroup(tierCfg.ID.String()); g != nil {
			r := g.Raft
			fsm, _ := g.FSM.(*multiraft.ChunkFSM)
			timeout := 10 * time.Second
			raftCB = tierRaftCallbacks{
				hasLeader: func() bool { return r.Leader() != "" },
		isLeader:  func() bool { return r.State() == hraft.Leader },
				applyDelete: func(id chunk.ChunkID) error {
					return r.Apply(multiraft.MarshalDeleteChunk(id), timeout).Error()
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
			}
		}
	}

	return &TierInstance{
		TierID:          tierCfg.ID,
		Type:            string(tierCfg.Type),
		Chunks:          cm,
		Indexes:         im,
		Query:           qe,
		HasRaftLeader:             raftCB.hasLeader,
		ApplyRaftDelete:           raftCB.applyDelete,
		ListManifest:             raftCB.listChunks,
		ApplyRaftRetentionPending: raftCB.applyRetPending,
		ListRetentionPending:     raftCB.listRetPending,
	}, nil
}

// findFileStorageByID resolves a file storage ID to its config across all nodes.
func findFileStorageByID(cfg *config.Config, storageID string) *config.FileStorage {
	for _, nsc := range cfg.NodeStorageConfigs {
		for i := range nsc.FileStorages {
			if nsc.FileStorages[i].ID.String() == storageID {
				return &nsc.FileStorages[i]
			}
		}
	}
	return nil
}

// mapTierTypeToFactory maps a TierType to the factory name used in Factories maps.
// wireTierRaftGroup creates a tier Raft group (or reuses existing) and injects
// a RaftAnnouncer into the chunk manager so chunk lifecycle events replicate to
// all nodes via consensus.
type tierRaftCallbacks struct {
	hasLeader       func() bool
	isLeader        func() bool
	applyDelete     func(id chunk.ChunkID) error
	applyRetPending func(id chunk.ChunkID) error
	listChunks      func() []chunk.ChunkID
	listRetPending  func() []chunk.ChunkID
}

func (o *Orchestrator) wireTierRaftGroup(cm chunk.ChunkManager, tierCfg config.TierConfig, nscs []config.NodeStorageConfig, factories Factories, isFollower bool) tierRaftCallbacks {
	if factories.GroupManager == nil {
		return tierRaftCallbacks{}
	}
	setter, ok := cm.(chunk.AnnouncerSetter)
	if !ok {
		return tierRaftCallbacks{}
	}

	groupID := tierCfg.ID.String()
	g := factories.GroupManager.GetGroup(groupID)
	if g == nil {
		members := o.buildTierRaftMembers(tierCfg, nscs, factories)
		leaderNodeID := tierCfg.LeaderNodeID(nscs)
		isLeader := leaderNodeID == "" || leaderNodeID == o.localNodeID
		var err error
		g, err = factories.GroupManager.CreateGroup(multiraft.GroupConfig{
			GroupID:   groupID,
			FSM:       multiraft.NewChunkFSM(),
			Bootstrap: isLeader,
			Members:   members,
		})
		if err != nil {
			o.logger.Warn("failed to create tier raft group",
				"tier", tierCfg.ID, "error", err)
			return tierRaftCallbacks{}
		}
	}
	setter.SetAnnouncer(multiraft.NewRaftAnnouncer(g.Raft, 10*time.Second, o.logger))

	r := g.Raft
	fsm, _ := g.FSM.(*multiraft.ChunkFSM)
	timeout := 10 * time.Second
	return tierRaftCallbacks{
		hasLeader: func() bool { return r.Leader() != "" },
		isLeader:  func() bool { return r.State() == hraft.Leader },
		applyDelete: func(id chunk.ChunkID) error {
			f := r.Apply(multiraft.MarshalDeleteChunk(id), timeout)
			return f.Error()
		},
		applyRetPending: func(id chunk.ChunkID) error {
			return r.Apply(multiraft.MarshalRetentionPending(id), timeout).Error()
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
		listRetPending: func() []chunk.ChunkID {
			if fsm == nil {
				return nil
			}
			var ids []chunk.ChunkID
			for _, e := range fsm.List() {
				if e.RetentionPending {
					ids = append(ids, e.ID)
				}
			}
			return ids
		},
	}
}

// buildTierRaftMembers builds the Raft member list from tier placement.
func (o *Orchestrator) buildTierRaftMembers(tierCfg config.TierConfig, nscs []config.NodeStorageConfig, factories Factories) []hraft.Server {
	if factories.NodeAddressResolver == nil {
		return nil
	}
	var members []hraft.Server
	nodeID := tierCfg.LeaderNodeID(nscs)
	if nodeID == "" {
		nodeID = o.localNodeID
	}
	if addr, ok := factories.NodeAddressResolver(nodeID); ok {
		members = append(members, hraft.Server{
			ID:      hraft.ServerID(nodeID),
			Address: hraft.ServerAddress(addr),
		})
	}
	for _, secID := range tierCfg.FollowerNodeIDs(nscs) {
		if addr, ok := factories.NodeAddressResolver(secID); ok {
			members = append(members, hraft.Server{
				ID:      hraft.ServerID(secID),
				Address: hraft.ServerAddress(addr),
			})
		}
	}
	return members
}

func mapTierTypeToFactory(t config.TierType) string {
	switch t {
	case config.TierTypeMemory:
		return "memory"
	case config.TierTypeFile:
		return "file"
	case config.TierTypeCloud:
		return "file"
	case config.TierTypeJSONL:
		return "jsonl"
	default:
		return string(t)
	}
}

// buildTierParams builds a params map from a TierConfig suitable for factory consumption.
func buildTierParams(cfg *config.Config, vaultCfg config.VaultConfig, tierCfg config.TierConfig, localNodeID string) map[string]string { //nolint:gocognit // flat type-switch mapping
	params := make(map[string]string)

	switch tierCfg.Type {
	case config.TierTypeMemory:
		if tierCfg.MemoryBudgetBytes > 0 {
			// Estimate ~1KB per record to derive maxRecords from budget.
			maxRecords := tierCfg.MemoryBudgetBytes / 1024
			if maxRecords == 0 {
				maxRecords = 10000
			}
			params["maxRecords"] = strconv.FormatUint(maxRecords, 10)
		} else {
			params["maxRecords"] = "10000"
		}

	case config.TierTypeFile:
		if fs := findLocalFileStorage(cfg, localNodeID, tierCfg.StorageClass); fs != nil {
			params["dir"] = filepath.Join(fs.Path, "vaults", vaultCfg.ID.String(), tierCfg.ID.String())
		}

	case config.TierTypeJSONL:
		if tierCfg.Path != "" {
			params["path"] = tierCfg.Path
		} else {
			// Default: jsonl/<vault-id>/<tier-id>.jsonl
			params["path"] = filepath.Join("jsonl", vaultCfg.ID.String(), tierCfg.ID.String()+".jsonl")
		}

	case config.TierTypeCloud:
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
		if fs := findLocalFileStorage(cfg, localNodeID, tierCfg.ActiveChunkClass); fs != nil {
			params["dir"] = filepath.Join(fs.Path, "vaults", vaultCfg.ID.String(), tierCfg.ID.String())
		}
		// Cache directory for locally-cached GLCB blobs (avoids cloud range requests).
		if fs := findLocalFileStorage(cfg, localNodeID, tierCfg.CacheClass); fs != nil {
			params["cache_dir"] = filepath.Join(fs.Path, "cache", vaultCfg.ID.String(), tierCfg.ID.String())
		}
	}

	return params
}

// findLocalFileStorage finds a FileStorage on the given node with the given storage class.
func findLocalFileStorage(cfg *config.Config, nodeID string, storageClass uint32) *config.FileStorage {
	if storageClass == 0 {
		return nil
	}
	for _, nsc := range cfg.NodeStorageConfigs {
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

// findCloudService finds a CloudService by ID in the config.
func findCloudService(cfg *config.Config, id uuid.UUID) *config.CloudService {
	for i := range cfg.CloudServices {
		if cfg.CloudServices[i].ID == id {
			return &cfg.CloudServices[i]
		}
	}
	return nil
}
