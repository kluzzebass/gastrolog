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
func resolveVaultDir(params map[string]string, vaultsDir, vaultName string) map[string]string {
	dir := params["dir"]
	if dir == "" {
		dir = filepath.Join("vaults", vaultName)
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

	// Rebuild filter set from routes to include the new vault as a destination.
	if err := o.reloadFiltersFromRoutes(cfg); err != nil {
		// Rollback registration on filter error.
		delete(o.vaults, vaultCfg.ID)
		return err
	}

	// Set up retention and rotation from tier configs.
	if cfg != nil {
		o.applyTierPolicies(cfg, vaultCfg, vault)
	}

	o.logger.Info("vault added", "id", vaultCfg.ID, "name", vaultCfg.Name, "tiers", len(tiers))
	return nil
}

// applyTierPolicies applies rotation and retention policies for all tiers in a vault.
func (o *Orchestrator) applyTierPolicies(cfg *config.Config, vaultCfg config.VaultConfig, vault *Vault) {
	for _, tier := range vault.Tiers {
		tierCfg := findTierConfig(cfg.Tiers, tier.TierID)
		if tierCfg == nil {
			continue
		}
		o.applyTierRotation(cfg, vaultCfg, tier, tierCfg)
		o.applyTierRetention(cfg, vaultCfg, tier, tierCfg)
	}
}

// applyTierRotation applies a rotation policy from a tier config to its chunk manager.
func (o *Orchestrator) applyTierRotation(cfg *config.Config, vaultCfg config.VaultConfig, tier *TierInstance, tierCfg *config.TierConfig) {
	if tier.IsSecondary {
		tier.Chunks.SetRotationPolicy(chunk.NeverRotatePolicy{})
		return
	}
	if tierCfg.RotationPolicyID == nil {
		return
	}
	policyCfg := findRotationPolicy(cfg.RotationPolicies, *tierCfg.RotationPolicyID)
	if policyCfg == nil {
		return
	}
	policy, err := policyCfg.ToRotationPolicy()
	if err != nil {
		o.logger.Warn("invalid rotation policy for tier", "vault", vaultCfg.ID, "tier", tier.TierID, "error", err)
		return
	}
	if policy != nil {
		tier.Chunks.SetRotationPolicy(policy)
	}
	if policyCfg.Cron != nil && *policyCfg.Cron != "" {
		if err := o.cronRotation.addJob(vaultCfg.ID, tier.TierID, vaultCfg.Name, *policyCfg.Cron, tier.Chunks); err != nil {
			o.logger.Warn("failed to add cron rotation", "vault", vaultCfg.ID, "tier", tier.TierID, "error", err)
		}
	}
}

// applyTierRetention sets up a retention runner for a tier with retention rules.
func (o *Orchestrator) applyTierRetention(cfg *config.Config, vaultCfg config.VaultConfig, tier *TierInstance, tierCfg *config.TierConfig) {
	if len(tierCfg.RetentionRules) == 0 {
		return
	}
	rules, err := resolveRetentionRulesFromTier(cfg, vaultCfg, tierCfg)
	if err != nil {
		o.logger.Warn("invalid retention rules for tier", "vault", vaultCfg.ID, "tier", tier.TierID, "error", err)
		return
	}
	if len(rules) == 0 {
		return
	}
	key := tier.TierID
	runner := &retentionRunner{
		vaultID: vaultCfg.ID,
		tierID:  tier.TierID,
		cm:      tier.Chunks,
		im:      tier.Indexes,
		rules:   rules,
		orch:    o,
		now:     o.now,
		logger:  o.logger,
	}
	runner.isSecondary.Store(tier.IsSecondary)
	o.retention[key] = runner
	jobName := retentionJobName(tier.TierID)
	if err := o.scheduler.AddJob(jobName, defaultRetentionSchedule, runner.sweep); err != nil {
		o.logger.Warn("failed to add retention job", "vault", vaultCfg.ID, "tier", tier.TierID, "error", err)
	}
	o.scheduler.Describe(jobName, fmt.Sprintf("Retention sweep for '%s'", vaultCfg.Name))
}

// findTierConfig finds a TierConfig by ID in a slice.
func findTierConfig(tiers []config.TierConfig, id uuid.UUID) *config.TierConfig {
	for i := range tiers {
		if tiers[i].ID == id {
			return &tiers[i]
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

	// Remove per-tier retention and rotation jobs.
	o.removeVaultTierJobs(id, vault)

	// Remove from registry.
	delete(o.vaults, id)

	// Rebuild filter set without this vault.
	o.rebuildFilterSetLocked()

	o.logger.Info("vault removed", "id", id, "name", vault.Name, "type", vault.Type())
	return nil
}

// removeVaultTierJobs removes all per-tier retention runners and cron rotation jobs for a vault.
func (o *Orchestrator) removeVaultTierJobs(vaultID uuid.UUID, vault *Vault) {
	for _, tier := range vault.Tiers {
		key := tier.TierID
		o.scheduler.RemoveJob(retentionJobName(tier.TierID))
		delete(o.retention, key)
	}
	o.cronRotation.removeAllForVault(vaultID)
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

	// Cancel pending compress/index jobs before closing the chunk manager
	// to prevent use-after-close on the managers they capture.
	vaultPrefix := id.String()
	o.scheduler.RemoveJobsByPrefix("compress:" + vaultPrefix)
	o.scheduler.RemoveJobsByPrefix("index-build:" + vaultPrefix)

	// Close all tiers to release file locks.
	if err := vault.Close(); err != nil {
		o.logger.Warn("failed to close vault during force remove",
			"vault", id, "error", err)
	}

	// Remove per-tier retention and rotation jobs.
	o.removeVaultTierJobs(id, vault)

	// Remove from registry.
	delete(o.vaults, id)

	// Rebuild filter set without this vault.
	o.rebuildFilterSetLocked()

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

	// Cancel pending compress/index jobs before closing the chunk manager.
	vaultPrefix := id.String()
	o.scheduler.RemoveJobsByPrefix("compress:" + vaultPrefix)
	o.scheduler.RemoveJobsByPrefix("index-build:" + vaultPrefix)

	if err := vault.Close(); err != nil {
		o.logger.Warn("failed to close vault during unregister",
			"vault", id, "error", err)
	}

	// Remove per-tier retention and rotation jobs.
	o.removeVaultTierJobs(id, vault)

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
// Tiers whose primary/secondary storages don't resolve to the local node are skipped
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
			return nil, fmt.Errorf("tier %s not found in config", tierID)
		}

		// Determine if this node hosts this tier (as primary or secondary).
		primaryNodeID := tierCfg.PrimaryNodeID(nscs)
		secondaryNodeIDs := tierCfg.SecondaryNodeIDs(nscs)
		isPrimary := primaryNodeID == "" || primaryNodeID == o.localNodeID
		isSecondary := slices.Contains(secondaryNodeIDs, o.localNodeID)
		if !isPrimary && !isSecondary {
			continue
		}

		if isPrimary {
			ti, err := o.buildPrimaryTierInstance(cfg, vaultCfg, tierCfg, factories)
			if err != nil {
				closeTiers(tiers)
				return nil, fmt.Errorf("build tier %s: %w", tierID, err)
			}
			ti.SecondaryTargets = tierCfg.SecondaryTargets(nscs)
			tiers = append(tiers, ti)
		}

		// Secondary: build ALL instances with explicit storage resolution
		// to avoid directory/storage ID mismatch. Each local placement gets
		// its own TierInstance with its own chunk manager directory.
		if isSecondary {
			localTargets := tierCfg.SecondaryTargets(nscs)
			for _, tgt := range localTargets {
				if tgt.NodeID != o.localNodeID {
					continue
				}
				sti, err := o.buildTierInstanceForStorage(cfg, vaultCfg, *tierCfg, factories, tgt.StorageID)
				if err != nil {
					closeTiers(tiers)
					return nil, fmt.Errorf("build tier %s storage %s: %w", tierID, tgt.StorageID, err)
				}
				sti.IsSecondary = true
				sti.PrimaryNodeID = primaryNodeID
				sti.StorageID = tgt.StorageID
				sti.Chunks.SetRotationPolicy(chunk.NeverRotatePolicy{})
				tiers = append(tiers, sti)
			}
		}
	}
	return tiers, nil
}

// buildPrimaryTierInstance creates the primary TierInstance using the placement's
// storage ID. This avoids directory collisions with same-node secondary placements
// that would occur if findLocalFileStorage picked a different storage by class.
func (o *Orchestrator) buildPrimaryTierInstance(cfg *config.Config, vaultCfg config.VaultConfig, tierCfg *config.TierConfig, factories Factories) (*TierInstance, error) {
	storageID := tierCfg.PrimaryStorageID()
	if storageID != "" && !strings.HasPrefix(storageID, config.SyntheticStoragePrefix) {
		ti, err := o.buildTierInstanceForStorage(cfg, vaultCfg, *tierCfg, factories, storageID)
		if err != nil {
			return nil, err
		}
		ti.StorageID = storageID
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
// When isSecondary is true, cloud backing params are stripped so the secondary's
// PostSealProcess only runs compress + index without uploading to cloud storage.
// Cloud tiers use a shared blob key (vault-ID/chunk-ID.glcb) — if the secondary
// also uploads, it overwrites the primary's blob with a different-sized version,
// corrupting the primary's stored diskBytes and breaking all future cloud reads.
func (o *Orchestrator) buildTierInstance(cfg *config.Config, vaultCfg config.VaultConfig, tierCfg config.TierConfig, factories Factories, isSecondary bool) (*TierInstance, error) {
	// Map TierConfig.Type to factory name.
	factoryName := mapTierTypeToFactory(tierCfg.Type)

	// Build params from tier config.
	params := buildTierParams(cfg, vaultCfg, tierCfg, o.localNodeID)

	// Secondaries must NOT upload to cloud storage. The primary owns the
	// cloud blob; the secondary keeps a local compressed copy for queries.
	if isSecondary {
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

	cmParams := resolveVaultDir(params, factories.VaultsDir, vaultCfg.Name)
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
	o.wireTierRaftGroup(cm, tierCfg, cfg.NodeStorageConfigs, factories, isSecondary)

	// JSONL sinks are write-only — no query engine, no indexes.
	if tierCfg.Type == config.TierTypeJSONL {
		return &TierInstance{
			TierID: tierCfg.ID,
			Type:   string(tierCfg.Type),
			Chunks: cm,
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
		TierID:  tierCfg.ID,
		Type:    string(tierCfg.Type),
		Chunks:  cm,
		Indexes: im,
		Query:   qe,
	}, nil
}

// buildTierInstanceForStorage creates a secondary TierInstance whose data directory
// is resolved from a specific file storage ID rather than the default findLocalFileStorage
// lookup. Used for same-node replication where each secondary placement targets a
// different physical disk.
func (o *Orchestrator) buildTierInstanceForStorage(cfg *config.Config, vaultCfg config.VaultConfig, tierCfg config.TierConfig, factories Factories, storageID string) (*TierInstance, error) {
	fs := findFileStorageByID(cfg, storageID)
	if fs == nil {
		return nil, fmt.Errorf("file storage %s not found", storageID)
	}

	// Build params normally, then override the dir with this storage's path.
	params := buildTierParams(cfg, vaultCfg, tierCfg, o.localNodeID)
	delete(params, "sealed_backing") // always secondary
	params["dir"] = filepath.Join(fs.Path, tierCfg.ID.String())

	factoryName := mapTierTypeToFactory(tierCfg.Type)
	cmFactory, ok := factories.ChunkManagers[factoryName]
	if !ok {
		return nil, fmt.Errorf("unknown chunk manager type: %s", factoryName)
	}

	var cmLogger = factories.Logger
	if cmLogger != nil {
		cmLogger = cmLogger.With("vault", vaultCfg.ID, "tier", tierCfg.ID, "storage", storageID)
	}

	cmParams := resolveVaultDir(params, factories.VaultsDir, vaultCfg.Name)
	cmParams["_expect_existing"] = "true"
	cmParams["_vault_id"] = vaultCfg.ID.String()

	cm, err := cmFactory(cmParams, cmLogger)
	if err != nil {
		return nil, fmt.Errorf("create chunk manager: %w", err)
	}

	// Secondary replicas need index builders for local queries.
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

	return &TierInstance{
		TierID:  tierCfg.ID,
		Type:    string(tierCfg.Type),
		Chunks:  cm,
		Indexes: im,
		Query:   qe,
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
func (o *Orchestrator) wireTierRaftGroup(cm chunk.ChunkManager, tierCfg config.TierConfig, nscs []config.NodeStorageConfig, factories Factories, isSecondary bool) {
	if factories.GroupManager == nil {
		return
	}
	setter, ok := cm.(chunk.AnnouncerSetter)
	if !ok {
		return
	}

	groupID := tierCfg.ID.String()
	g := factories.GroupManager.GetGroup(groupID)
	if g == nil {
		members := o.buildTierRaftMembers(tierCfg, nscs, factories)
		primaryNodeID := tierCfg.PrimaryNodeID(nscs)
		isPrimary := primaryNodeID == "" || primaryNodeID == o.localNodeID
		var err error
		g, err = factories.GroupManager.CreateGroup(multiraft.GroupConfig{
			GroupID:   groupID,
			FSM:       multiraft.NewChunkFSM(),
			Bootstrap: isPrimary,
			Members:   members,
		})
		if err != nil {
			o.logger.Warn("failed to create tier raft group",
				"tier", tierCfg.ID, "error", err)
			return
		}
	}
	setter.SetAnnouncer(multiraft.NewRaftAnnouncer(g.Raft, 10*time.Second, o.logger))
}

// buildTierRaftMembers builds the Raft member list from tier placement.
func (o *Orchestrator) buildTierRaftMembers(tierCfg config.TierConfig, nscs []config.NodeStorageConfig, factories Factories) []hraft.Server {
	if factories.NodeAddressResolver == nil {
		return nil
	}
	var members []hraft.Server
	nodeID := tierCfg.PrimaryNodeID(nscs)
	if nodeID == "" {
		nodeID = o.localNodeID
	}
	if addr, ok := factories.NodeAddressResolver(nodeID); ok {
		members = append(members, hraft.Server{
			ID:      hraft.ServerID(nodeID),
			Address: hraft.ServerAddress(addr),
		})
	}
	for _, secID := range tierCfg.SecondaryNodeIDs(nscs) {
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
		// Find a FileStorage matching this tier's StorageClass on the local node.
		// Scope the directory per tier to prevent lock conflicts when multiple
		// file tiers share the same storage class.
		if fs := findLocalFileStorage(cfg, localNodeID, tierCfg.StorageClass); fs != nil {
			params["dir"] = filepath.Join(fs.Path, tierCfg.ID.String())
		}

	case config.TierTypeJSONL:
		if tierCfg.Path != "" {
			params["path"] = tierCfg.Path
		} else {
			// Default: jsonl/<vault-name>/sink_<tier-number>.jsonl
			tierNum := 1
			for i, tid := range vaultCfg.TierIDs {
				if tid == tierCfg.ID {
					tierNum = i + 1
					break
				}
			}
			vaultName := vaultCfg.Name
			if vaultName == "" {
				vaultName = vaultCfg.ID.String()
			}
			params["path"] = filepath.Join("jsonl", vaultName, fmt.Sprintf("sink_%d.jsonl", tierNum))
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
			params["dir"] = fs.Path
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
