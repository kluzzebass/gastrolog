package main

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
	"gastrolog/internal/orchestrator"
)

// configDispatcher translates FSM notifications into orchestrator side effects.
// It is called synchronously from within FSM.Apply, so actions complete before
// the cfgStore write method returns to the server handler.
type configDispatcher struct {
	orch           *orchestrator.Orchestrator
	cfgStore       config.Store
	factories      orchestrator.Factories
	localNodeID    string
	logger         *slog.Logger
	clusterTLS     *cluster.ClusterTLS // nil for single-node or memory mode
	tlsFilePath    string              // path to persist cluster TLS on rotation
}

// Handle dispatches a single FSM notification to the appropriate orchestrator
// methods. Errors are logged but not propagated — the config mutation has
// already been committed to the FSM store and cannot be rolled back.
//
// Handle is a no-op until the orchestrator is wired (during bootstrap the
// orchestrator does not exist yet).
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
	}
}

func (d *configDispatcher) handleVaultPut(ctx context.Context, id uuid.UUID) {
	vaultCfg, err := d.cfgStore.GetVault(ctx, id)
	if err != nil || vaultCfg == nil {
		d.logger.Error("dispatch: read vault config", "id", id, "error", err)
		return
	}

	// Skip vaults belonging to another node.
	if vaultCfg.NodeID != "" && vaultCfg.NodeID != d.localNodeID {
		return
	}

	if !slices.Contains(d.orch.ListVaults(), id) {
		// New vault — add it to the orchestrator.
		if err := d.orch.AddVault(ctx, *vaultCfg, d.factories); err != nil {
			d.logger.Error("dispatch: add vault", "id", id, "name", vaultCfg.Name, "type", vaultCfg.Type, "error", err)
		}
		return
	}

	// Existing vault — reload dependent config and apply state changes.
	d.applyExistingVaultChanges(ctx, id, vaultCfg)
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
	if cfg.Type == "file" {
		_ = d.orch.SetVaultCompression(id, cfg.Params["compression"] == "zstd")
	}
}

func (d *configDispatcher) handleVaultDeleted(n raftfsm.Notification) {
	if err := d.orch.ForceRemoveVault(n.ID); err != nil && !errors.Is(err, orchestrator.ErrVaultNotFound) {
		d.logger.Error("dispatch: force remove vault", "id", n.ID, "name", n.Name, "error", err)
	}
	// Clean up the file vault directory only on the node that owns it.
	if n.Dir != "" && (n.NodeID == "" || n.NodeID == d.localNodeID) {
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

	// Skip ingesters belonging to another node.
	if ingCfg.NodeID != "" && ingCfg.NodeID != d.localNodeID {
		return
	}

	// Remove existing ingester if present (idempotent re-add).
	if slices.Contains(d.orch.ListIngesters(), id) {
		if err := d.orch.RemoveIngester(id); err != nil && !errors.Is(err, orchestrator.ErrIngesterNotFound) {
			d.logger.Error("dispatch: remove existing ingester", "id", id, "name", ingCfg.Name, "type", ingCfg.Type, "error", err)
		}
	}

	if !ingCfg.Enabled {
		return
	}

	factory, ok := d.factories.Ingesters[ingCfg.Type]
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

	ingester, err := factory(ingCfg.ID, params, d.factories.Logger)
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

// handleClusterTLSPut reloads cluster TLS material into the atomic holder.
// This enables hot-reload of certificates — new connections use the updated cert.
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
	// Persist updated TLS to local file for restart.
	if d.tlsFilePath != "" {
		if err := cluster.SaveFile(d.tlsFilePath, []byte(tls.ClusterCertPEM), []byte(tls.ClusterKeyPEM), []byte(tls.CACertPEM)); err != nil {
			d.logger.Error("dispatch: save cluster TLS file", "error", err)
		}
	}
	d.logger.Info("dispatch: cluster TLS reloaded")
}
