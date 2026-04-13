package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/system"
	"gastrolog/internal/index"
	"gastrolog/internal/lifecycle"
	"gastrolog/internal/query"
	"gastrolog/internal/raftgroup"
	tierfsm "gastrolog/internal/tier/raftfsm"

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
func findTierConfig(tiers []system.TierConfig, id uuid.UUID) *system.TierConfig {
	for i := range tiers {
		if tiers[i].ID == id {
			return &tiers[i]
		}
	}
	return nil
}

func findVaultConfig(vaults []system.VaultConfig, id uuid.UUID) *system.VaultConfig {
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
			n := o.sealAndDeleteAllChunks(tier, "delete tier data", tierID)
			o.logger.Info("tier data deleted", "tier", tierID, "chunks", n)
			return
		}
	}
}

// sealAndDeleteAllChunks seals the active chunk (if any), then deletes all
// chunks and their indexes. Returns the number of chunks found. Errors are
// logged with the given prefix but do not abort the cleanup.
func (o *Orchestrator) sealAndDeleteAllChunks(tier *TierInstance, op string, tierID uuid.UUID) int {
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
		if err := tier.Chunks.Delete(m.ID); err != nil {
			o.logger.Warn(op+": delete chunk failed", "tier", tierID, "chunk", m.ID, "error", err)
		}
	}
	return len(metas)
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
	o.sealAndDeleteAllChunks(tier, "remove tier", tierID)

	// Close managers.
	if err := tier.Chunks.Close(); err != nil {
		o.logger.Warn("remove tier: close chunk manager failed", "vault", vaultID, "tier", tierID, "error", err)
	}

	// Remove the tier's data directory entirely — not just its chunk subdirs.
	// Without this, leftover files (.lock, cloud.idx) and the tier dir itself
	// accumulate on disk forever. See gastrolog-42j4n.
	if remover, ok := tier.Chunks.(chunk.DirRemover); ok {
		if err := remover.RemoveDir(); err != nil {
			o.logger.Warn("remove tier: remove data directory failed", "vault", vaultID, "tier", tierID, "error", err)
		}
	}

	// Remove retention runner and cron rotation for this tier.
	delete(o.retention, retentionKey(tier.TierID, tier.StorageID))
	o.cronRotation.removeAllForVault(vaultID)

	// Stop the per-tier leader loop and clear its desired-members entry.
	o.tierLeaders.Stop(tier.TierID)

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

// StopTierLeaderLoop stops the per-tier leader reconciliation loop for a tier.
// Called by the dispatch handler during tier deletion cleanup so that non-storage
// nodes (which have no TierInstance but do participate in the tier Raft group)
// can cleanly stop the leader loop before the group is destroyed.
func (o *Orchestrator) StopTierLeaderLoop(tierID uuid.UUID) {
	o.tierLeaders.Stop(tierID)
}

// AddTierToVault builds a single tier instance and adds it to an existing vault
// without tearing down any other tiers. This is the incremental counterpart to
// RemoveTierFromVault.
func (o *Orchestrator) AddTierToVault(ctx context.Context, vaultID, tierID uuid.UUID, factories Factories) error {
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

	nscs := rt.NodeStorageConfigs
	leaderNodeID := system.LeaderNodeID(rt.TierPlacements[tierCfg.ID], nscs)
	followerNodeIDs := system.FollowerNodeIDs(rt.TierPlacements[tierCfg.ID], nscs)
	isLeader := leaderNodeID == "" || leaderNodeID == o.localNodeID
	isFollower := slices.Contains(followerNodeIDs, o.localNodeID)
	o.setDesiredTierLeader(tierCfg.ID, leaderNodeID, factories)
	if !isLeader && !isFollower {
		// No storage placement, but still join the tier Raft group as a
		// voter. See gastrolog-292yi.
		o.createTierRaftGroup(*tierCfg, rt.Nodes, factories)
		return nil
	}

	var ti *TierInstance
	if isLeader {
		t, err := o.buildPrimaryTierInstance(sys, *vaultCfg, tierCfg, factories)
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
func (o *Orchestrator) VaultConfig(id uuid.UUID) (system.VaultConfig, error) {
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
// Every node joins every tier's Raft group regardless of storage placement
// (gastrolog-292yi). Nodes with storage placements also get a TierInstance with
// a chunk manager; nodes without storage only participate in the Raft group.
func (o *Orchestrator) buildTierInstances(sys *system.System, vaultCfg system.VaultConfig, factories Factories) ([]*TierInstance, error) {
	cfg := &sys.Config
	rt := &sys.Runtime
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
		// Tell the tier leader manager which node should be the Raft leader.
		// Called for ALL paths (non-storage, leader, follower) since every
		// node runs a leader loop and may need to transfer leadership.
		o.setDesiredTierLeader(tierCfg.ID, leaderNodeID, factories)

		if !isLeader && !isFollower {
			// This node has no storage placement for this tier, but still
			// joins the tier Raft group as a voter. This decouples Raft
			// membership from storage — every node votes on every tier's
			// chunk metadata FSM. See gastrolog-292yi.
			o.createTierRaftGroup(*tierCfg, rt.Nodes, factories)
			continue
		}

		if isLeader {
			ti, err := o.buildPrimaryTierInstance(sys, vaultCfg, tierCfg, factories)
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
func (o *Orchestrator) alertTierInitFailed(tierID uuid.UUID, tierName string, err error) {
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

// buildPrimaryTierInstance creates the leader TierInstance using the placement's
// storage ID. This avoids directory collisions with same-node follower placements
// that would occur if findLocalFileStorage picked a different storage by class.
func (o *Orchestrator) buildPrimaryTierInstance(sys *system.System, vaultCfg system.VaultConfig, tierCfg *system.TierConfig, factories Factories) (*TierInstance, error) {
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

	// Create the tier Raft group BEFORE the chunk manager. Group creation is
	// fast (Raft log replay). Chunk manager creation is slow (scans disk for
	// existing chunks). Starting the Raft group early gives it time to elect
	// a leader and catch up while the chunk manager is loading.
	tierGroup, applier, raftCB := o.createTierRaftGroup(tierCfg, rt.Nodes, factories)

	// Build params from tier system.
	params := buildTierParams(sys, vaultCfg, tierCfg, o.localNodeID)

	// Followers keep cloud store access for reads (queries) but skip uploads.
	// The leader owns the blob; the follower adopts it via RegisterCloudChunk
	// when the tier Raft FSM propagates the upload announcement.
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
		wireTierFSMOnDelete(tierGroup, cm, nil, o.logger)
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
	wireTierFSMOnDelete(tierGroup, cm, im, o.logger)
	wireTierFSMOnUpload(tierGroup, cm, o.logger)
	return ti, nil
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

	// Create the tier Raft group BEFORE the chunk manager — same rationale
	// as buildTierInstance: start elections while chunk loading is in progress.
	tierGroup, applier, raftCB := o.createTierRaftGroup(tierCfg, rt.Nodes, factories)

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
	wireTierFSMOnDelete(tierGroup, cm, im, o.logger)
	wireTierFSMOnUpload(tierGroup, cm, o.logger)
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
func applyRotationPolicy(cm chunk.ChunkManager, policies []system.RotationPolicyConfig, policyID *uuid.UUID) error {
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

// tierRaftCallbacks holds the callbacks returned by createTierRaftGroup.
type tierRaftCallbacks struct {
	hasLeader              func() bool
	isLeader               func() bool
	isFSMReady             func() bool
	applyDelete            func(id chunk.ChunkID) error
	applyRetPending        func(id chunk.ChunkID) error
	applyTransitionStreamed  func(id chunk.ChunkID) error
	applyTransitionReceived func(sourceChunkID chunk.ChunkID) error
	hasTransitionReceipt    func(sourceChunkID chunk.ChunkID) bool
	listChunks              func() []chunk.ChunkID
	listRetPending         func() []chunk.ChunkID
	listTransitionStreamed  func() []chunk.ChunkID
	overlayFromFSM         func(chunk.ChunkMeta) chunk.ChunkMeta
}

// createTierRaftGroup creates or retrieves the tier Raft group for a tier.
// This is independent of the chunk manager — it only needs the tier config
// and group manager. Call this BEFORE creating the chunk manager so the Raft
// group starts elections early (while chunk loading is still in progress).
func (o *Orchestrator) createTierRaftGroup(tierCfg system.TierConfig, clusterNodes []system.NodeConfig, factories Factories) (*raftgroup.Group, tierfsm.Applier, tierRaftCallbacks) {
	if factories.GroupManager == nil {
		return nil, nil, tierRaftCallbacks{}
	}

	groupID := tierCfg.ID.String()
	members := o.buildTierRaftMembers(clusterNodes, factories)
	// Do not create the tier Raft group until ALL cluster nodes are
	// resolvable. A partial member list poisons the group's boltdb with a
	// wrong-size initial configuration, and we can't recover from that on
	// restart without losing data.
	//
	// Every cluster node participates in every tier Raft group (gastrolog-292yi),
	// so the expected count is len(clusterNodes). If some nodes aren't
	// resolvable yet (e.g. not yet announced via peer discovery), defer
	// creation until the next reconfigure sweep.
	if len(members) < len(clusterNodes) {
		o.logger.Debug("tier raft group: not all cluster nodes resolvable, deferring creation",
			"tier", tierCfg.ID,
			"have", len(members),
			"want", len(clusterNodes))
		return nil, nil, tierRaftCallbacks{}
	}
	// Safety net: verify this node is in the member list. With all cluster
	// nodes as members this should always be true, but guard against edge
	// cases like a node not yet in the cluster node list.
	isMember := false
	for _, srv := range members {
		if string(srv.ID) == o.localNodeID {
			isMember = true
			break
		}
	}
	if !isMember {
		return nil, nil, tierRaftCallbacks{}
	}

	g := factories.GroupManager.GetGroup(groupID)
	if g == nil {
		// Symmetric seeding: every assigned node calls CreateGroup with the
		// same member list. Raft elects a leader through normal election. No
		// node holds a special "bootstrap" role. The seed list is only used
		// when the local boltdb log is empty (first start of a brand-new tier);
		// on restart, the existing log already contains the configuration.
		var err error
		g, err = factories.GroupManager.CreateGroup(raftgroup.GroupConfig{
			GroupID:     groupID,
			FSM:         tierfsm.New(),
			SeedMembers: members,
		})
		if err != nil {
			o.logger.Warn("failed to create tier raft group",
				"tier", tierCfg.ID, "error", err)
			return nil, nil, tierRaftCallbacks{}
		}
	}

	// Update the desired-members view and ensure a leader loop is running
	// for this tier. The loop runs on every assigned node, but only the
	// node currently holding tier-Raft leadership runs the reconcile
	// callback (after raft.Barrier returns). Idempotent on re-call.
	o.tierLeaders.SetDesiredMembers(tierCfg.ID, members)
	o.tierLeaders.Start(tierCfg.ID, g)

	// Desired leader is set separately by setDesiredTierLeader after the
	// caller resolves the placement leader (createTierRaftGroup doesn't
	// have placement info).

	r := g.Raft
	fsm, _ := g.FSM.(*tierfsm.FSM)
	timeout := cluster.ReplicationTimeout

	// Create a forwarder that applies locally or forwards to the tier Raft
	// leader. This decouples the config placement leader from the tier Raft
	// leader — they may be on different nodes after a cluster restart.
	var applier tierfsm.Applier
	if factories.PeerConns != nil {
		applier = cluster.NewTierApplyForwarder(r, groupID, factories.PeerConns, timeout)
	} else {
		// Single-node mode: apply directly, no forwarding.
		applier = &directApplier{raft: r, timeout: timeout}
	}

	return g, applier, buildTierRaftCallbacks(r, fsm, applier)
}

// buildTierRaftCallbacks constructs the callback struct for a tier Raft group.
// Extracted from createTierRaftGroup to keep cognitive complexity within lint
// thresholds.
func buildTierRaftCallbacks(r *hraft.Raft, fsm *tierfsm.FSM, applier tierfsm.Applier) tierRaftCallbacks {
	return tierRaftCallbacks{
		hasLeader:  func() bool { return r.Leader() != "" },
		isLeader:   func() bool { return r.State() == hraft.Leader },
		isFSMReady: func() bool { return fsm != nil && fsm.Ready() },
		applyDelete: func(id chunk.ChunkID) error {
			return applier.Apply(tierfsm.MarshalDeleteChunk(id))
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
		listRetPending:        listFSMByFlag(fsm, func(e tierfsm.Entry) bool { return e.RetentionPending }),
		listTransitionStreamed: listFSMByFlag(fsm, func(e tierfsm.Entry) bool { return e.TransitionStreamed }),
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
func listFSMByFlag(fsm *tierfsm.FSM, pred func(tierfsm.Entry) bool) func() []chunk.ChunkID {
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
// routing to the tier Raft leader transparently. The phase parameter lets
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
func wireTierFSMOnDelete(g *raftgroup.Group, cm chunk.ChunkManager, im index.IndexManager, logger *slog.Logger) {
	if g == nil || cm == nil {
		return
	}
	fsm, ok := g.FSM.(*tierfsm.FSM)
	if !ok {
		return
	}
	silent, ok := cm.(chunk.SilentDeleter)
	if !ok {
		return
	}
	fsm.SetOnDelete(func(id chunk.ChunkID) {
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
			if errors.Is(err, chunk.ErrChunkNotFound) || errors.Is(err, chunk.ErrActiveChunk) {
				logger.Debug("FSM onDelete: DeleteSilent skipped",
					"chunk", id, "reason", err)
			} else {
				logger.Warn("FSM onDelete: DeleteSilent failed",
					"chunk", id, "error", err)
			}
		}
	})
}

// wireTierFSMOnUpload connects the tier Raft FSM's OnUpload callback to the
// chunk manager's RegisterCloudChunk method. When the FSM applies CmdUploadChunk
// (from the leader's AnnounceUpload), the follower's chunk manager registers
// the cloud chunk from metadata alone — no record streaming or S3 download.
func wireTierFSMOnUpload(g *raftgroup.Group, cm chunk.ChunkManager, logger *slog.Logger) {
	if g == nil || cm == nil {
		return
	}
	fsm, ok := g.FSM.(*tierfsm.FSM)
	if !ok {
		return
	}
	registrar, ok := cm.(chunk.CloudChunkRegistrar)
	if !ok {
		return
	}
	fsm.SetOnUpload(func(e tierfsm.Entry) {
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

// directApplier applies tier FSM commands locally via raft.Apply. Used in
// single-node mode where no forwarding is needed.
type directApplier struct {
	raft    *hraft.Raft
	timeout time.Duration
}

func (a *directApplier) Apply(data []byte) error {
	return a.raft.Apply(data, a.timeout).Error()
}

// buildTierRaftMembers returns ALL cluster nodes as Raft members.
// Every node participates in every tier Raft group regardless of storage
// placement. Nodes without storage for a tier vote and replicate the FSM
// but don't store chunk data. This decouples Raft membership from storage
// assignment — adding/removing storage never triggers AddVoter/RemoveVoter.
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

// setDesiredTierLeader tells the tier leader manager which node the placement
// manager assigned as the leader. Uses the factories' resolver.
func (o *Orchestrator) setDesiredTierLeader(tierID uuid.UUID, leaderNodeID string, factories Factories) {
	if leaderNodeID == "" || factories.NodeAddressResolver == nil {
		return
	}
	addr, ok := factories.NodeAddressResolver(leaderNodeID)
	if !ok {
		return
	}
	o.tierLeaders.SetDesiredLeader(tierID, &hraft.Server{
		ID:      hraft.ServerID(leaderNodeID),
		Address: hraft.ServerAddress(addr),
	})
}

// SetDesiredTierLeader is the exported version for use by the dispatch handler.
// Uses the stored node address resolver from ApplyConfig.
func (o *Orchestrator) SetDesiredTierLeader(tierID uuid.UUID, leaderNodeID string) {
	if leaderNodeID == "" || o.nodeAddrResolver == nil {
		return
	}
	addr, ok := o.nodeAddrResolver(leaderNodeID)
	if !ok {
		return
	}
	o.tierLeaders.SetDesiredLeader(tierID, &hraft.Server{
		ID:      hraft.ServerID(leaderNodeID),
		Address: hraft.ServerAddress(addr),
	})
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
func findCloudService(cfg *system.Config, id uuid.UUID) *system.CloudService {
	for i := range cfg.CloudServices {
		if cfg.CloudServices[i].ID == id {
			return &cfg.CloudServices[i]
		}
	}
	return nil
}
