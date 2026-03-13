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

	if vaultCfg.NodeID != "" && vaultCfg.NodeID != d.localNodeID {
		d.maybeStartDrain(ctx, id, vaultCfg.NodeID)
		// Always reload filters so forwarding targets point to the new node.
		// maybeStartDrain handles the source node (drain updates filters),
		// but on every OTHER node the filter set still has the old target.
		d.reloadFilters(ctx)
		return
	}

	// Vault assigned to this node — cancel any in-progress drain.
	if d.orch.IsDraining(id) {
		if err := d.orch.CancelDrain(ctx, id); err != nil {
			d.logger.Error("dispatch: cancel drain", "id", id, "error", err)
		}
		// Fall through to applyExistingVaultChanges to reconfigure.
	}

	if !slices.Contains(d.orch.ListVaults(), id) {
		if err := d.orch.AddVault(ctx, *vaultCfg, d.factories); err != nil {
			d.logger.Error("dispatch: add vault", "id", id, "name", vaultCfg.Name, "type", vaultCfg.Type, "error", err)
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
	if n.DeleteData && n.Dir != "" && (n.NodeID == "" || n.NodeID == d.localNodeID) {
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
