package orchestrator

import (
	"context"
	"fmt"
	"maps"
	"path/filepath"

	"gastrolog/internal/config"
	"gastrolog/internal/query"

	"github.com/google/uuid"
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
// Loads the full config internally to resolve the vault's filter ID to a filter expression.
// Returns ErrDuplicateID if a vault with this ID already exists.
func (o *Orchestrator) AddVault(ctx context.Context, vaultCfg config.VaultConfig, factories Factories) error {
	cfg, err := o.loadConfig(ctx)
	if err != nil {
		return fmt.Errorf("load config for AddVault: %w", err)
	}

	// Migrate legacy cloud vaults → file vaults with sealed backing.
	migrateCloudVault(&vaultCfg)

	o.mu.Lock()
	defer o.mu.Unlock()

	if _, exists := o.vaults[vaultCfg.ID]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateID, vaultCfg.ID)
	}

	// Create chunk manager.
	cmFactory, ok := factories.ChunkManagers[vaultCfg.Type]
	if !ok {
		return fmt.Errorf("unknown chunk manager type: %s", vaultCfg.Type)
	}
	var cmLogger = factories.Logger
	if cmLogger != nil {
		cmLogger = cmLogger.With("vault", vaultCfg.ID)
	}
	cmParams := resolveVaultDir(vaultCfg.Params, factories.VaultsDir, vaultCfg.Name)
	cmParams["_vault_id"] = vaultCfg.ID.String()
	cm, err := cmFactory(cmParams, cmLogger)
	if err != nil {
		return fmt.Errorf("create chunk manager %s: %w", vaultCfg.ID, err)
	}

	// Create index manager.
	imFactory, ok := factories.IndexManagers[vaultCfg.Type]
	if !ok {
		return fmt.Errorf("unknown index manager type: %s", vaultCfg.Type)
	}
	var imLogger = factories.Logger
	if imLogger != nil {
		imLogger = imLogger.With("vault", vaultCfg.ID)
	}
	im, err := imFactory(cmParams, cm, imLogger)
	if err != nil {
		return fmt.Errorf("create index manager %s: %w", vaultCfg.ID, err)
	}

	// Create query engine.
	var qeLogger = factories.Logger
	if qeLogger != nil {
		qeLogger = qeLogger.With("vault", vaultCfg.ID)
	}
	qe := query.New(cm, im, qeLogger)

	// Register vault. AddVault does not apply disabled state (unlike ApplyConfig).
	vault := NewVault(vaultCfg.ID, cm, im, qe)
	vault.Name = vaultCfg.Name
	vault.Type = vaultCfg.Type
	o.vaults[vaultCfg.ID] = vault

	// Rebuild filter set from routes to include the new vault as a destination.
	if err := o.reloadFiltersFromRoutes(cfg); err != nil {
		// Rollback registration on filter error.
		delete(o.vaults, vaultCfg.ID)
		return err
	}

	// Set up retention job if applicable.
	if len(vaultCfg.RetentionRules) > 0 && cfg != nil {
		rules, err := resolveRetentionRules(cfg, vaultCfg)
		if err != nil {
			o.logger.Warn("invalid retention rules for new vault", "vault", vaultCfg.ID, "error", err)
		} else if len(rules) > 0 {
			runner := &retentionRunner{
				vaultID:  vaultCfg.ID,
				cm:       cm,
				im:       im,
				rules: rules,
				orch:     o,
				now:      o.now,
				logger:   o.logger,
			}
			o.retention[vaultCfg.ID] = runner
			if err := o.scheduler.AddJob(retentionJobName(vaultCfg.ID), defaultRetentionSchedule, runner.sweep); err != nil {
				o.logger.Warn("failed to add retention job for new vault", "vault", vaultCfg.ID, "error", err)
			}
			o.scheduler.Describe(retentionJobName(vaultCfg.ID), fmt.Sprintf("Retention sweep for '%s'", vaultCfg.Name))
		}
	}

	// Apply rotation policy (per-append threshold checks + cron schedule).
	if vaultCfg.Policy != nil && cfg != nil {
		if err := o.applyRotationPolicy(cfg, vaultCfg, cm); err != nil {
			o.logger.Warn("failed to apply rotation policy for new vault", "vault", vaultCfg.ID, "error", err)
		}
	}

	o.logger.Info("vault added", "id", vaultCfg.ID, "name", vaultCfg.Name, "type", vaultCfg.Type)
	return nil
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
	cm := vault.Chunks

	// Check if vault has any data.
	metas, err := cm.List()
	if err != nil {
		return fmt.Errorf("list chunks for vault %s: %w", id, err)
	}
	if len(metas) > 0 {
		return fmt.Errorf("%w: vault %s has %d chunk(s)", ErrVaultNotEmpty, id, len(metas))
	}

	// Also check if there's an active chunk with records.
	if active := cm.Active(); active != nil {
		return fmt.Errorf("%w: vault %s has active chunk", ErrVaultNotEmpty, id)
	}

	// Remove retention job if present.
	o.scheduler.RemoveJob(retentionJobName(id))
	delete(o.retention, id)

	// Remove cron rotation job if present.
	o.cronRotation.removeJob(id)

	// Remove from registry.
	delete(o.vaults, id)

	// Rebuild filter set without this vault.
	o.rebuildFilterSetLocked()

	o.logger.Info("vault removed", "id", id, "name", vault.Name, "type", vault.Type)
	return nil
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
	o.logger.Info("vault disabled", "id", id, "name", vault.Name, "type", vault.Type)
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
	o.logger.Info("vault enabled", "id", id, "name", vault.Name, "type", vault.Type)
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
	cm := vault.Chunks
	im := vault.Indexes

	// Seal active chunk if present.
	if active := cm.Active(); active != nil {
		if err := cm.Seal(); err != nil {
			return fmt.Errorf("seal active chunk for vault %s: %w", id, err)
		}
	}

	// Delete all indexes and chunks.
	metas, err := cm.List()
	if err != nil {
		return fmt.Errorf("list chunks for vault %s: %w", id, err)
	}
	for _, meta := range metas {
		if im != nil {
			// Best-effort index deletion; log and continue on error.
			if err := im.DeleteIndexes(meta.ID); err != nil {
				o.logger.Warn("failed to delete indexes during force remove",
					"vault", id, "chunk", meta.ID.String(), "error", err)
			}
		}
		if err := cm.Delete(meta.ID); err != nil {
			return fmt.Errorf("delete chunk %s in vault %s: %w", meta.ID.String(), id, err)
		}
	}

	// Cancel pending compress/index jobs before closing the chunk manager
	// to prevent use-after-close on the managers they capture.
	vaultPrefix := id.String()
	o.scheduler.RemoveJobsByPrefix("compress:" + vaultPrefix)
	o.scheduler.RemoveJobsByPrefix("index-build:" + vaultPrefix)

	// Close the chunk manager to release file locks.
	if err := cm.Close(); err != nil {
		o.logger.Warn("failed to close chunk manager during force remove",
			"vault", id, "error", err)
	}

	// Remove retention job if present.
	o.scheduler.RemoveJob(retentionJobName(id))
	delete(o.retention, id)

	// Remove cron rotation job if present.
	o.cronRotation.removeJob(id)

	// Remove from registry.
	delete(o.vaults, id)

	// Rebuild filter set without this vault.
	o.rebuildFilterSetLocked()

	o.logger.Info("vault force-removed", "id", id, "name", vault.Name, "type", vault.Type)
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

	// Cancel pending compress/index jobs before closing the chunk manager.
	vaultPrefix := id.String()
	o.scheduler.RemoveJobsByPrefix("compress:" + vaultPrefix)
	o.scheduler.RemoveJobsByPrefix("index-build:" + vaultPrefix)

	if err := vault.Chunks.Close(); err != nil {
		o.logger.Warn("failed to close chunk manager during unregister",
			"vault", id, "error", err)
	}

	// Remove retention and rotation jobs.
	o.scheduler.RemoveJob(retentionJobName(id))
	delete(o.retention, id)
	o.cronRotation.removeJob(id)

	// Remove from registry.
	delete(o.vaults, id)
	o.rebuildFilterSetLocked()

	o.logger.Info("vault unregistered (data preserved)", "id", id, "name", vault.Name, "type", vault.Type)
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
		// Type and Params are not tracked after creation.
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

// migrateCloudVault rewrites a legacy type:"cloud" vault config to type:"file"
// with the sealed_backing param set to the original provider. This is a
// backwards-compatible in-memory migration — the config store is updated on
// next save.
func migrateCloudVault(vc *config.VaultConfig) {
	if vc.Type != "cloud" {
		return
	}
	vc.Type = "file"
	if vc.Params == nil {
		vc.Params = make(map[string]string)
	}
	// Move provider → sealed_backing.
	if provider := vc.Params["provider"]; provider != "" {
		vc.Params["sealed_backing"] = provider
		delete(vc.Params, "provider")
	}
}
