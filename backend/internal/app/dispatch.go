package app

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"os"
	"slices"

	"github.com/google/uuid"

	"gastrolog/internal/cluster"
	"gastrolog/internal/config"
	"gastrolog/internal/config/raftfsm"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
)

// orchActions is the subset of orchestrator.Orchestrator methods used by the
// dispatcher. Defined at the consumer site so tests can supply a mock.
type orchActions interface {
	ListVaults() []uuid.UUID
	VaultType(id uuid.UUID) string
	AddVault(ctx context.Context, cfg config.VaultConfig, f orchestrator.Factories) error
	ReloadFilters(ctx context.Context) error
	ReloadRotationPolicies(ctx context.Context) error
	ReloadRetentionPolicies(ctx context.Context) error
	DisableVault(id uuid.UUID) error
	EnableVault(id uuid.UUID) error
	ForceRemoveVault(id uuid.UUID) error
	UnregisterVault(id uuid.UUID) error
	DrainVault(ctx context.Context, vaultID uuid.UUID, targetNodeID string) error
	IsDraining(vaultID uuid.UUID) bool
	CancelDrain(ctx context.Context, vaultID uuid.UUID) error
	ListIngesters() []uuid.UUID
	AddIngester(id uuid.UUID, name, ingType string, r orchestrator.Ingester) error
	RemoveIngester(id uuid.UUID) error
	UpdateMaxConcurrentJobs(n int) error
	FindLocalTierExported(vaultID, tierID uuid.UUID) *orchestrator.TierInstance
}

// ManagedFileHandler handles managed file lifecycle events from the FSM.
type ManagedFileHandler interface {
	// OnPut is called when a managed file's metadata is committed to Raft.
	// If the file isn't already on disk, it should be pulled from a peer.
	OnPut(ctx context.Context, fileID uuid.UUID)
	// OnDelete is called when a managed file is removed from Raft.
	// The handler should clean up the file from disk.
	OnDelete(fileID uuid.UUID)
}

// configDispatcher translates FSM notifications into orchestrator side effects.
// It is called synchronously from within FSM.Apply, so actions complete before
// the cfgStore write method returns to the server handler.
type configDispatcher struct {
	orch              orchActions
	cfgStore          config.Store
	factories         orchestrator.Factories
	localNodeID       string
	logger            *slog.Logger
	clusterTLS        *cluster.ClusterTLS // nil for single-node or memory mode
	tlsFilePath       string              // path to persist cluster TLS on rotation
	configSignal      *notify.Signal      // broadcasts config changes to WatchConfig streams
	managedFileHandler ManagedFileHandler   // nil for single-node or before wiring
	catchupScheduler   func(tierID uuid.UUID, secondaryNodeIDs []string) // nil until orch is wired
}

// Handle dispatches a single FSM notification to the appropriate orchestrator
// methods. Errors are logged but not propagated — the config mutation has
// already been committed to the FSM store and cannot be rolled back.
func (d *configDispatcher) Handle(n raftfsm.Notification) {
	if d.orch == nil {
		return // not wired yet (bootstrap phase)
	}

	ctx := context.Background()

	switch n.Kind {
	case raftfsm.NotifyVaultPut:
		d.handleVaultPut(ctx, n.ID)
	case raftfsm.NotifyVaultDeleted:
		d.handleVaultDeleted(n)
	case raftfsm.NotifyFilterPut, raftfsm.NotifyFilterDeleted:
		d.reloadFilters(ctx)
	case raftfsm.NotifyRoutePut, raftfsm.NotifyRouteDeleted:
		d.reloadFilters(ctx)
	case raftfsm.NotifyRotationPolicyPut, raftfsm.NotifyRotationPolicyDeleted:
		d.reloadRotationPolicies(ctx)
	case raftfsm.NotifyRetentionPolicyPut, raftfsm.NotifyRetentionPolicyDeleted:
		d.reloadRetentionPolicies(ctx)
	case raftfsm.NotifyIngesterPut:
		d.handleIngesterPut(ctx, n.ID)
	case raftfsm.NotifyIngesterDeleted:
		d.handleIngesterDeleted(n)
	case raftfsm.NotifySettingPut:
		d.handleSettingPut(ctx, n.Key)
	case raftfsm.NotifyClusterTLSPut:
		d.handleClusterTLSPut(ctx)
	case raftfsm.NotifyNodeConfigPut, raftfsm.NotifyNodeConfigDeleted:
		// No orchestrator side effects; configSignal fires below.
	case raftfsm.NotifyManagedFilePut:
		if d.managedFileHandler != nil {
			d.managedFileHandler.OnPut(ctx, n.ID)
		}
	case raftfsm.NotifyManagedFileDeleted:
		if d.managedFileHandler != nil {
			d.managedFileHandler.OnDelete(n.ID)
		}
	case raftfsm.NotifyTierPut:
		d.handleTierPut(ctx, n.ID)
	case raftfsm.NotifyTierDeleted:
		d.handleTierDeleted(ctx, n.ID)
	case raftfsm.NotifyCloudServicePut, raftfsm.NotifyCloudServiceDeleted,
		raftfsm.NotifyNodeStorageConfigSet:
		// No orchestrator side effects; configSignal fires below.
	}

	// Notify WatchConfig streams for all user-visible config changes.
	// Thread the Raft log index as the config version so the frontend can
	// skip stale refetches when it already holds a newer mutation response.
	if d.configSignal != nil && n.Kind != raftfsm.NotifyClusterTLSPut {
		d.configSignal.NotifyWithVersion(n.Index)
	}
}

func (d *configDispatcher) handleVaultPut(ctx context.Context, id uuid.UUID) {
	vaultCfg, err := d.cfgStore.GetVault(ctx, id)
	if err != nil || vaultCfg == nil {
		d.logger.Error("dispatch: read vault config", "id", id, "error", err)
		return
	}

	// With tiered storage, vaults no longer have a NodeID. Every node
	// instantiates all tiers it can serve.

	// Cancel any in-progress drain.
	if d.orch.IsDraining(id) {
		if err := d.orch.CancelDrain(ctx, id); err != nil {
			d.logger.Error("dispatch: cancel drain", "id", id, "error", err)
		}
		// Fall through to applyExistingVaultChanges to reconfigure.
	}

	if !slices.Contains(d.orch.ListVaults(), id) {
		if err := d.orch.AddVault(ctx, *vaultCfg, d.factories); err != nil {
			d.logger.Error("dispatch: add vault", "id", id, "name", vaultCfg.Name, "error", err)
		}
		return
	}

	d.applyExistingVaultChanges(ctx, id, vaultCfg)
}

// maybeStartDrain starts draining a vault to a remote node if the vault is
// locally registered and not already draining.
//
// Cloud vaults are exempt from drain — their data lives in shared object
// storage (S3/Azure/GCS) accessible from any node. Draining would wastefully
// download each chunk, send it over the internal network, and re-upload it.
// Instead, the vault is simply unregistered locally; the new node's
// AddVault creates a Manager pointing to the same bucket.
func (d *configDispatcher) maybeStartDrain(ctx context.Context, id uuid.UUID, targetNodeID string) {
	if !slices.Contains(d.orch.ListVaults(), id) {
		return
	}

	// Legacy: cloud vaults (type="cloud") were sealed-only and could be
	// reassigned by simply unregistering. With unified vault types, cloud-backed
	// file vaults have a local active chunk that needs draining, so they use
	// the normal drain path below.
	// NOTE: kept for backwards compatibility during rolling upgrades where some
	// nodes may still report type="cloud" for migrated vaults.
	if d.orch.VaultType(id) == "cloud" {
		if err := d.orch.UnregisterVault(id); err != nil && !errors.Is(err, orchestrator.ErrVaultNotFound) {
			d.logger.Error("dispatch: unregister cloud vault for reassignment", "id", id, "error", err)
		} else {
			d.logger.Info("dispatch: cloud vault reassigned, unregistered locally (no drain needed)", "id", id, "target_node", targetNodeID)
		}
		return
	}

	if d.orch.IsDraining(id) {
		return // drain already in progress
	}
	if err := d.orch.DrainVault(ctx, id, targetNodeID); err != nil {
		d.logger.Error("dispatch: drain vault", "id", id, "node", targetNodeID, "error", err)
	}
}

func (d *configDispatcher) applyExistingVaultChanges(ctx context.Context, id uuid.UUID, cfg *config.VaultConfig) {
	if err := d.orch.ReloadFilters(ctx); err != nil {
		d.logger.Error("dispatch: reload filters", "error", err)
	}
	if err := d.orch.ReloadRotationPolicies(ctx); err != nil {
		d.logger.Error("dispatch: reload rotation policies", "error", err)
	}
	if err := d.orch.ReloadRetentionPolicies(ctx); err != nil {
		d.logger.Error("dispatch: reload retention policies", "error", err)
	}
	if !cfg.Enabled {
		_ = d.orch.DisableVault(id)
	} else {
		_ = d.orch.EnableVault(id)
	}
}

func (d *configDispatcher) handleVaultDeleted(n raftfsm.Notification) {
	if err := d.orch.ForceRemoveVault(n.ID); err != nil && !errors.Is(err, orchestrator.ErrVaultNotFound) {
		d.logger.Error("dispatch: force remove vault", "id", n.ID, "name", n.Name, "error", err)
	}
	if n.DeleteData && n.Dir != "" {
		if err := os.RemoveAll(n.Dir); err != nil {
			d.logger.Error("dispatch: remove vault directory", "id", n.ID, "name", n.Name, "dir", n.Dir, "error", err)
		}
	}
}

func (d *configDispatcher) reloadFilters(ctx context.Context) {
	if err := d.orch.ReloadFilters(ctx); err != nil {
		d.logger.Error("dispatch: reload filters", "error", err)
	}
}

func (d *configDispatcher) reloadRotationPolicies(ctx context.Context) {
	if err := d.orch.ReloadRotationPolicies(ctx); err != nil {
		d.logger.Error("dispatch: reload rotation policies", "error", err)
	}
}

func (d *configDispatcher) reloadRetentionPolicies(ctx context.Context) {
	if err := d.orch.ReloadRetentionPolicies(ctx); err != nil {
		d.logger.Error("dispatch: reload retention policies", "error", err)
	}
}

func (d *configDispatcher) handleIngesterPut(ctx context.Context, id uuid.UUID) {
	ingCfg, err := d.cfgStore.GetIngester(ctx, id)
	if err != nil || ingCfg == nil {
		d.logger.Error("dispatch: read ingester config", "id", id, "error", err)
		return
	}

	if ingCfg.NodeID != "" && ingCfg.NodeID != d.localNodeID {
		// Ingester assigned to another node — stop it locally if running.
		if slices.Contains(d.orch.ListIngesters(), id) {
			if err := d.orch.RemoveIngester(id); err != nil && !errors.Is(err, orchestrator.ErrIngesterNotFound) {
				d.logger.Error("dispatch: remove ingester reassigned to remote node", "id", id, "name", ingCfg.Name, "node", ingCfg.NodeID, "error", err)
			} else {
				d.logger.Info("dispatch: ingester reassigned, stopped locally", "id", id, "name", ingCfg.Name, "target_node", ingCfg.NodeID)
			}
		}
		return
	}

	if slices.Contains(d.orch.ListIngesters(), id) {
		if err := d.orch.RemoveIngester(id); err != nil && !errors.Is(err, orchestrator.ErrIngesterNotFound) {
			d.logger.Error("dispatch: remove existing ingester", "id", id, "name", ingCfg.Name, "type", ingCfg.Type, "error", err)
		}
	}

	if !ingCfg.Enabled {
		return
	}

	reg, ok := d.factories.IngesterTypes[ingCfg.Type]
	if !ok {
		d.logger.Error("dispatch: unknown ingester type", "id", id, "name", ingCfg.Name, "type", ingCfg.Type)
		return
	}

	params := ingCfg.Params
	if d.factories.HomeDir != "" {
		params = make(map[string]string, len(ingCfg.Params)+1)
		maps.Copy(params, ingCfg.Params)
		params["_state_dir"] = d.factories.HomeDir
	}

	ingester, err := reg.Factory(ingCfg.ID, params, d.factories.Logger)
	if err != nil {
		d.logger.Error("dispatch: create ingester", "id", id, "name", ingCfg.Name, "type", ingCfg.Type, "error", err)
		return
	}

	if err := d.orch.AddIngester(ingCfg.ID, ingCfg.Name, ingCfg.Type, ingester); err != nil {
		d.logger.Error("dispatch: add ingester", "id", id, "name", ingCfg.Name, "type", ingCfg.Type, "error", err)
	}
}

func (d *configDispatcher) handleIngesterDeleted(n raftfsm.Notification) {
	if err := d.orch.RemoveIngester(n.ID); err != nil && !errors.Is(err, orchestrator.ErrIngesterNotFound) {
		d.logger.Error("dispatch: remove ingester", "id", n.ID, "name", n.Name, "error", err)
	}
}

func (d *configDispatcher) handleSettingPut(ctx context.Context, key string) {
	if key != "server" {
		return
	}

	ss, err := d.cfgStore.LoadServerSettings(ctx)
	if err != nil {
		d.logger.Error("dispatch: load server settings", "error", err)
		return
	}
	if ss.Scheduler.MaxConcurrentJobs > 0 {
		if err := d.orch.UpdateMaxConcurrentJobs(ss.Scheduler.MaxConcurrentJobs); err != nil {
			d.logger.Error("dispatch: update max concurrent jobs", "error", err)
		}
	}
}

// handleTierPut adjusts vault registration when a tier's NodeID changes.
// Runs on ALL nodes — each node independently decides whether it gained or lost
// ownership based on the tier's new NodeID vs localNodeID.
// handleTierPut adjusts vault registration when a tier's NodeID changes,
// and reloads rotation/retention policies when tier config changes.
func (d *configDispatcher) handleTierPut(ctx context.Context, tierID uuid.UUID) {
	tierCfg, err := d.cfgStore.GetTier(ctx, tierID)
	if err != nil || tierCfg == nil {
		d.logger.Error("dispatch: read tier config", "tier", tierID, "error", err)
		return
	}

	// Only react to placement changes. If the tier belongs here and the vault
	// is already registered, or if the tier doesn't belong here and the vault
	// isn't registered, there's nothing to do. The key question is: did this
	// tier JUST arrive or JUST leave this node?
	tierBelongsHere := tierCfg.NodeID == "" || tierCfg.NodeID == d.localNodeID || slices.Contains(tierCfg.SecondaryNodeIDs, d.localNodeID)

	vaults, err := d.cfgStore.ListVaults(ctx)
	if err != nil {
		d.logger.Error("dispatch: list vaults for tier change", "tier", tierID, "error", err)
		return
	}

	for _, v := range vaults {
		if !slices.Contains(v.TierIDs, tierID) {
			continue
		}

		if tierBelongsHere {
			d.rebuildVaultIfTierMissing(ctx, v, tierID)
		}
		// Note: we do NOT unregister on !tierBelongsHere && isLocalVault
		// because this vault may have OTHER tiers that DO belong here.
	}

	// Reload rotation and retention policies — tier config may have changed
	// policy references (rotation_policy_id, retention_rules).
	d.reloadRotationPolicies(ctx)
	d.reloadRetentionPolicies(ctx)

	// If this node is the primary and has secondaries, schedule catchup.
	if tierCfg.NodeID == d.localNodeID && len(tierCfg.SecondaryNodeIDs) > 0 && d.catchupScheduler != nil {
		d.catchupScheduler(tierID, tierCfg.SecondaryNodeIDs)
	}
}

func (d *configDispatcher) registerVault(ctx context.Context, v config.VaultConfig, tierID uuid.UUID) {
	if err := d.orch.AddVault(ctx, v, d.factories); err != nil {
		d.logger.Error("dispatch: add vault for gained tier",
			"vault", v.ID, "tier", tierID, "error", err)
	}
}

func (d *configDispatcher) rebuildVaultIfTierMissing(ctx context.Context, v config.VaultConfig, tierID uuid.UUID) {
	existing := d.orch.FindLocalTierExported(v.ID, tierID)
	if existing != nil {
		// Tier exists — check if its role changed (primary ↔ secondary).
		tierCfg, err := d.cfgStore.GetTier(ctx, tierID)
		if err != nil || tierCfg == nil {
			return
		}
		shouldBeSecondary := slices.Contains(tierCfg.SecondaryNodeIDs, d.localNodeID)
		if existing.IsSecondary == shouldBeSecondary {
			return // role unchanged, nothing to do
		}
		// Role changed — fall through to rebuild.
	}
	if err := d.orch.UnregisterVault(v.ID); err != nil && !errors.Is(err, orchestrator.ErrVaultNotFound) {
		d.logger.Error("dispatch: unregister vault for new tier",
			"vault", v.ID, "tier", tierID, "error", err)
	}
	if err := d.orch.AddVault(ctx, v, d.factories); err != nil {
		d.logger.Error("dispatch: re-add vault with new tier",
			"vault", v.ID, "tier", tierID, "error", err)
	}
}

// handleTierDeleted removes vaults that no longer have any local tiers.
func (d *configDispatcher) handleTierDeleted(ctx context.Context, tierID uuid.UUID) {
	vaults, err := d.cfgStore.ListVaults(ctx)
	if err != nil {
		d.logger.Error("dispatch: list vaults for tier deletion", "tier", tierID, "error", err)
		return
	}

	for _, v := range vaults {
		if !slices.Contains(v.TierIDs, tierID) {
			continue
		}
		if slices.Contains(d.orch.ListVaults(), v.ID) {
			// Vault references a deleted tier — rebuild to pick up the change.
			if err := d.orch.UnregisterVault(v.ID); err != nil && !errors.Is(err, orchestrator.ErrVaultNotFound) {
				d.logger.Error("dispatch: unregister vault for deleted tier", "vault", v.ID, "error", err)
			}
			if err := d.orch.AddVault(ctx, v, d.factories); err != nil {
				d.logger.Error("dispatch: re-add vault after tier deletion", "vault", v.ID, "error", err)
			}
		}
	}
}

func (d *configDispatcher) handleClusterTLSPut(ctx context.Context) {
	if d.clusterTLS == nil {
		return
	}
	cfg, err := d.cfgStore.Load(ctx)
	if err != nil || cfg == nil || cfg.ClusterTLS == nil {
		d.logger.Error("dispatch: read cluster TLS for reload", "error", err)
		return
	}
	tls := cfg.ClusterTLS
	if err := d.clusterTLS.Load([]byte(tls.ClusterCertPEM), []byte(tls.ClusterKeyPEM), []byte(tls.CACertPEM)); err != nil {
		d.logger.Error("dispatch: reload cluster TLS", "error", err)
		return
	}
	if d.tlsFilePath != "" {
		if err := cluster.SaveFile(d.tlsFilePath, []byte(tls.ClusterCertPEM), []byte(tls.ClusterKeyPEM), []byte(tls.CACertPEM)); err != nil {
			d.logger.Error("dispatch: save cluster TLS file", "error", err)
		}
	}
	d.logger.Info("dispatch: cluster TLS reloaded")
}
