package app

import (
	"gastrolog/internal/glid"
	"context"
	"errors"
	"log/slog"
	"maps"
	"os"
	"slices"


	"gastrolog/internal/cluster"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
)

// orchActions is the subset of orchestrator.Orchestrator methods used by the
// dispatcher. Defined at the consumer site so tests can supply a mock.
type orchActions interface {
	ListVaults() []glid.GLID
	VaultType(id glid.GLID) string
	AddVault(ctx context.Context, cfg system.VaultConfig, f orchestrator.Factories) error
	ReloadFilters(ctx context.Context) error
	ReloadRotationPolicies(ctx context.Context) error
	ReloadRetentionPolicies(ctx context.Context) error
	DisableVault(id glid.GLID) error
	EnableVault(id glid.GLID) error
	ForceRemoveVault(id glid.GLID) error
	RemoveTierFromVault(vaultID, tierID glid.GLID) bool
	AddTierToVault(ctx context.Context, vaultID, tierID glid.GLID, f orchestrator.Factories) error
	DrainTier(ctx context.Context, vaultID, tierID glid.GLID, mode orchestrator.TierDrainMode, targetNodeID string) error
	UnregisterVault(id glid.GLID) error
	HasMissingTiers(vaultID glid.GLID, tierIDs []glid.GLID) bool
	LocalTierIDs(vaultID glid.GLID) []glid.GLID
	DrainVault(ctx context.Context, vaultID glid.GLID, targetNodeID string) error
	IsDraining(vaultID glid.GLID) bool
	CancelDrain(ctx context.Context, vaultID glid.GLID) error
	ListIngesters() []glid.GLID
	AddIngester(id glid.GLID, name, ingType string, passive bool, r orchestrator.Ingester) error
	RemoveIngester(id glid.GLID) error
	UpdateMaxConcurrentJobs(n int) error
	MaxConcurrentJobs() int
	FindLocalTierExported(vaultID, tierID glid.GLID) *orchestrator.TierInstance
	StopTierLeaderLoop(tierID glid.GLID)
	SetDesiredTierLeader(tierID glid.GLID, leaderNodeID string)
}

// ManagedFileHandler handles managed file lifecycle events from the FSM.
type ManagedFileHandler interface {
	// OnPut is called when a managed file's metadata is committed to Raft.
	// If the file isn't already on disk, it should be pulled from a peer.
	OnPut(ctx context.Context, fileID glid.GLID)
	// OnDelete is called when a managed file is removed from Raft.
	// The handler should clean up the file from disk.
	OnDelete(fileID glid.GLID)
}

// configDispatcher translates FSM notifications into orchestrator side effects.
// It is called synchronously from within FSM.Apply, so actions complete before
// the cfgStore write method returns to the server handler.
type configDispatcher struct {
	orch              orchActions
	cfgStore          system.Store
	factories         orchestrator.Factories
	localNodeID       string
	logger            *slog.Logger
	clusterTLS        *cluster.ClusterTLS // nil for single-node or memory mode
	tlsFilePath       string              // path to persist cluster TLS on rotation
	configSignal      *notify.Signal      // broadcasts config changes to WatchConfig streams
	managedFileHandler ManagedFileHandler   // nil for single-node or before wiring
	catchupScheduler   func(tierID glid.GLID, followerNodeIDs []string) // nil until orch is wired
	placementTrigger   func() // triggers immediate placement reconcile; nil for single-node
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
		d.handleTierDeleted(ctx, n.ID, n.Drain)
	case raftfsm.NotifyIngesterAssignmentSet:
		d.handleIngesterAssignment(ctx, n.ID)
	case raftfsm.NotifyTierPlacementsSet, raftfsm.NotifyCloudServicePut, raftfsm.NotifyCloudServiceDeleted,
		raftfsm.NotifyNodeStorageConfigSet, raftfsm.NotifySetupWizardDismissedSet,
		raftfsm.NotifyIngesterAliveSet,
		raftfsm.NotifyIngesterCheckpointSet:
		// No orchestrator side effects; configSignal fires below.
	}

	// Notify WatchConfig streams for all user-visible config changes.
	// Thread the Raft log index as the config version so the frontend can
	// skip stale refetches when it already holds a newer mutation response.
	if d.configSignal != nil && n.Kind != raftfsm.NotifyClusterTLSPut {
		d.configSignal.NotifyWithVersion(n.Index)
	}
}

func (d *configDispatcher) handleVaultPut(ctx context.Context, id glid.GLID) {
	vaultCfg, err := d.cfgStore.GetVault(ctx, id)
	if err != nil || vaultCfg == nil {
		d.logger.Error("dispatch: read vault config", "id", id, "error", err)
		return
	}

	tiers, err := d.cfgStore.ListTiers(ctx)
	if err != nil {
		d.logger.Error("dispatch: list tiers for vault put", "id", id, "error", err)
		return
	}
	tierIDs := system.VaultTierIDs(tiers, id)

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
		if d.placementTrigger != nil {
			d.placementTrigger()
		}
		return
	}

	// Incrementally add/remove tiers that changed. Never tear down the
	// entire vault — that causes cascading rebuilds and data warnings.
	if d.orch.HasMissingTiers(id, tierIDs) {
		d.reconcileVaultTiers(ctx, id, tierIDs)
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
func (d *configDispatcher) maybeStartDrain(ctx context.Context, id glid.GLID, targetNodeID string) {
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

// reconcileVaultTiers incrementally adds missing tiers and removes stale tiers
// from an existing vault, without tearing down any tiers that are unchanged.
func (d *configDispatcher) reconcileVaultTiers(ctx context.Context, vaultID glid.GLID, tierIDs []glid.GLID) {
	expected := make(map[glid.GLID]bool, len(tierIDs))
	for _, id := range tierIDs {
		expected[id] = true
	}

	// Remove tiers that are no longer in the config's tier list.
	for _, localTierID := range d.orch.LocalTierIDs(vaultID) {
		if !expected[localTierID] {
			d.orch.RemoveTierFromVault(vaultID, localTierID)
		}
	}

	// Add tiers that are in the config but not local.
	for _, tierID := range tierIDs {
		if err := d.orch.AddTierToVault(ctx, vaultID, tierID, d.factories); err != nil {
			d.logger.Error("dispatch: add tier to vault",
				"vault", vaultID, "tier", tierID, "error", err)
		}
	}

	vaultCfg, _ := d.cfgStore.GetVault(ctx, vaultID)
	d.applyExistingVaultChanges(ctx, vaultID, vaultCfg)

	if d.placementTrigger != nil {
		d.placementTrigger()
	}
}

func (d *configDispatcher) applyExistingVaultChanges(ctx context.Context, id glid.GLID, cfg *system.VaultConfig) {
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
		if err := d.orch.DisableVault(id); err != nil {
			d.logger.Error("dispatch: disable vault failed", "vault", id, "error", err)
		}
	} else {
		if err := d.orch.EnableVault(id); err != nil {
			d.logger.Error("dispatch: enable vault failed", "vault", id, "error", err)
		}
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

func (d *configDispatcher) handleIngesterPut(ctx context.Context, id glid.GLID) {
	ingCfg, err := d.cfgStore.GetIngester(ctx, id)
	if err != nil || ingCfg == nil {
		d.logger.Error("dispatch: read ingester config", "id", id, "error", err)
		return
	}

	reg, ok := d.factories.IngesterTypes[ingCfg.Type]
	isPassive := ok && reg.ListenAddrs != nil
	isSingleton := ok && reg.SingletonSupported && ingCfg.Singleton

	if !d.shouldRunIngester(ctx, *ingCfg, isSingleton) {
		if slices.Contains(d.orch.ListIngesters(), id) {
			if err := d.orch.RemoveIngester(id); err != nil && !errors.Is(err, orchestrator.ErrIngesterNotFound) {
				d.logger.Error("dispatch: remove ingester not assigned to this node", "id", id, "name", ingCfg.Name, "error", err)
			} else {
				d.logger.Info("dispatch: ingester removed, not assigned to this node", "id", id, "name", ingCfg.Name)
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

	ing, err := reg.Factory(ingCfg.ID, params, d.factories.Logger)
	if err != nil {
		d.logger.Error("dispatch: create ingester", "id", id, "name", ingCfg.Name, "type", ingCfg.Type, "error", err)
		return
	}

	// Restore Raft-replicated checkpoint if the ingester supports it.
	if cp, ok := ing.(orchestrator.Checkpointable); ok {
		data, cpErr := d.cfgStore.GetIngesterCheckpoint(ctx, ingCfg.ID)
		if cpErr == nil && len(data) > 0 {
			if loadErr := cp.LoadCheckpoint(data); loadErr != nil {
				d.logger.Warn("dispatch: checkpoint load failed, starting fresh", "id", id, "error", loadErr)
			}
		}
	}

	if err := d.orch.AddIngester(ingCfg.ID, ingCfg.Name, ingCfg.Type, isPassive, ing); err != nil {
		d.logger.Error("dispatch: add ingester", "id", id, "name", ingCfg.Name, "type", ingCfg.Type, "error", err)
	}
}

// shouldRunIngester checks whether this node should run the given ingester.
// Parallel ingesters: this node must be in NodeIDs (or NodeIDs empty).
// Singleton ingesters: this node must additionally be the Raft-assigned node.
func (d *configDispatcher) shouldRunIngester(ctx context.Context, cfg system.IngesterConfig, singleton bool) bool {
	if len(cfg.NodeIDs) > 0 && !slices.Contains(cfg.NodeIDs, d.localNodeID) {
		return false
	}
	if !singleton {
		return true
	}
	assigned, err := d.cfgStore.GetIngesterAssignment(ctx, cfg.ID)
	if err != nil {
		return false
	}
	// Empty assignment = not yet placed by the placement manager.
	// Allow local start — the placement manager will narrow it down on the
	// next reconcile cycle and cause the other nodes to stop via
	// NotifyIngesterAssignmentSet.
	return assigned == "" || assigned == d.localNodeID
}

// handleIngesterAssignment reacts to a Raft-replicated assignment change.
// Only meaningful for singleton ingesters — parallel ingesters ignore
// assignments (they run on every selected node). A stale assignment from
// a prior singleton era must not tear down a now-parallel ingester.
func (d *configDispatcher) handleIngesterAssignment(ctx context.Context, id glid.GLID) {
	ingCfg, err := d.cfgStore.GetIngester(ctx, id)
	if err != nil || ingCfg == nil {
		return
	}
	reg, ok := d.factories.IngesterTypes[ingCfg.Type]
	if !ok {
		return
	}
	isSingleton := reg.SingletonSupported && ingCfg.Singleton
	if !isSingleton {
		return // parallel — assignment is irrelevant
	}

	assigned, err := d.cfgStore.GetIngesterAssignment(ctx, id)
	if err != nil {
		d.logger.Error("dispatch: read ingester assignment", "id", id, "error", err)
		return
	}

	isRunningLocally := slices.Contains(d.orch.ListIngesters(), id)

	if assigned != d.localNodeID {
		// Not assigned to this node — stop it if running.
		if isRunningLocally {
			if err := d.orch.RemoveIngester(id); err != nil && !errors.Is(err, orchestrator.ErrIngesterNotFound) {
				d.logger.Error("dispatch: remove reassigned ingester", "id", id, "error", err)
			} else {
				d.logger.Info("dispatch: ingester reassigned away, stopped locally", "id", id, "new_node", assigned)
			}
		}
		return
	}

	// Assigned to this node — start it if not already running.
	if isRunningLocally {
		return
	}
	d.handleIngesterPut(ctx, id)
}

func (d *configDispatcher) handleIngesterDeleted(n raftfsm.Notification) {
	if err := d.orch.RemoveIngester(n.ID); err != nil && !errors.Is(err, orchestrator.ErrIngesterNotFound) {
		d.logger.Error("dispatch: remove ingester", "id", n.ID, "name", n.Name, "error", err)
	}
}

func (d *configDispatcher) handleSettingPut(ctx context.Context, key string) {
	switch key {
	case system.NotifyKeyServerSettingsRaftLegacy, system.NotifyKeyServiceSettings:
		// These paths may change scheduler limits.
	default:
		return
	}

	ss, err := d.cfgStore.LoadServerSettings(ctx)
	if err != nil {
		d.logger.Error("dispatch: load server settings", "error", err)
		return
	}
	// Only rebuild when MaxConcurrentJobs actually changed. Legacy
	// NotifySettingPut("server") and service saves share this path;
	// lookup-only saves use a different key — rebuilding the scheduler on every one of
	// those calls shuts down the whole scheduler and waits for in-flight
	// jobs, which causes gocron Shutdown timeouts on busy nodes.
	if ss.Scheduler.MaxConcurrentJobs > 0 && ss.Scheduler.MaxConcurrentJobs != d.orch.MaxConcurrentJobs() {
		if err := d.orch.UpdateMaxConcurrentJobs(ss.Scheduler.MaxConcurrentJobs); err != nil {
			d.logger.Error("dispatch: update max concurrent jobs", "error", err)
		}
	}
}

// handleTierPut adjusts vault registration when a tier's placements change.
// Runs on ALL nodes — each node independently decides whether it gained or lost
// ownership based on the tier's resolved node IDs vs localNodeID.
// Also reloads rotation/retention policies when tier config changes.
func (d *configDispatcher) handleTierPut(ctx context.Context, tierID glid.GLID) {
	tierCfg, err := d.cfgStore.GetTier(ctx, tierID)
	if err != nil || tierCfg == nil {
		d.logger.Error("dispatch: read tier config", "tier", tierID, "error", err)
		return
	}

	nscs, err := d.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		d.logger.Error("dispatch: list node storage configs for tier change", "tier", tierID, "error", err)
		return
	}

	// Each tier owns its vault reference directly.
	if tierCfg.VaultID == (glid.GLID{}) {
		return // tier not assigned to a vault
	}
	v, err := d.cfgStore.GetVault(ctx, tierCfg.VaultID)
	if err != nil || v == nil {
		d.logger.Error("dispatch: get vault for tier change", "tier", tierID, "vault", tierCfg.VaultID, "error", err)
		return
	}

	leaderNodeID := system.LeaderNodeID(func() []system.TierPlacement { p, _ := d.cfgStore.GetTierPlacements(ctx, tierCfg.ID); return p }(), nscs)
	followerNodeIDs := system.FollowerNodeIDs(func() []system.TierPlacement { p, _ := d.cfgStore.GetTierPlacements(ctx, tierCfg.ID); return p }(), nscs)

	// Only act on tier membership once placements are fully assigned. During
	// cluster-init the placement manager assigns placements one-at-a-time,
	// each firing its own CmdPutTier. Building the tier locally on a partial
	// placement state is wrong for two reasons: (1) we can't reliably answer
	// "does this tier belong here" with incomplete placements, and (2) it
	// would create the chunk manager (and tier Raft group) with a wrong-size
	// member list, which then persists in boltdb.
	//
	// Policy reloads (rotation/retention) still run below because they are
	// independent of placement state.
	d.applyTierMembershipChange(ctx, *v, tierID, leaderNodeID, followerNodeIDs)

	// Update the desired tier Raft leader so TransferLeadership aligns
	// the Raft leader with the placement leader.
	d.orch.SetDesiredTierLeader(tierID, leaderNodeID)

	// Tier Raft group membership is handled by the per-group leader loop
	// (see raftgroup.LeaderLoop). The placement leader does not push
	// membership changes — the tier Raft leader (whoever currently holds
	// leadership in the group) does, from inside its leader epoch after
	// raft.Barrier() returns. This avoids the divergence problem where the
	// placement leader and the tier Raft leader live on different nodes.

	// Reload filters so ingestion routing picks up the new placement leader
	// immediately. Without this, records are forwarded to the old (possibly
	// dead) node until the rotation sweep recompiles filters (up to 15s).
	if err := d.orch.ReloadFilters(ctx); err != nil {
		d.logger.Warn("dispatch: reload filters after tier change", "error", err)
	}

	// Reload rotation and retention policies — tier config may have changed
	// policy references (rotation_policy_id, retention_rules).
	d.reloadRotationPolicies(ctx)
	d.reloadRetentionPolicies(ctx)

	// Schedule catchup only for NEWLY added followers, not existing ones.
	// When a leader changes but followers stay the same (e.g. a node dies),
	// the surviving followers already have all chunks — no catchup needed.
	if leaderNodeID == d.localNodeID && len(followerNodeIDs) > 0 && d.catchupScheduler != nil {
		newFollowers := d.newFollowersForTier(v.ID, tierID, followerNodeIDs)
		if len(newFollowers) > 0 {
			d.catchupScheduler(tierID, newFollowers)
		}
	}

	// Trigger immediate placement reconcile so secondaries are assigned
	// without waiting for the 15-second ticker.
	if d.placementTrigger != nil {
		d.placementTrigger()
	}
}

// applyTierMembershipChange decides whether the tier belongs here based on
// the (complete) placement state, and either adds/rebuilds it locally or
// removes it if it no longer belongs. Deferred entirely when placements are
// incomplete — the next CmdPutTier from the placement manager will retry.
func (d *configDispatcher) applyTierMembershipChange(ctx context.Context, v system.VaultConfig, tierID glid.GLID, leaderNodeID string, followerNodeIDs []string) {
	// Placements are "complete" when they include a leader. We can't gate on
	// len(placements) >= RF because RF may be unsatisfiable when a node is
	// down — the placement manager writes the best it can with surviving
	// nodes. Gating on RF caused permanent deferral after node failure:
	// the role was never updated, rotation never ran, chunks never sealed.
	placements, _ := d.cfgStore.GetTierPlacements(ctx, tierID)
	hasLeader := false
	for _, p := range placements {
		if p.Leader {
			hasLeader = true
			break
		}
	}
	if !hasLeader {
		d.logger.Debug("dispatch: tier placements have no leader, deferring rebuild",
			"tier", tierID, "placements", len(placements))
		return
	}

	// Every node participates in every tier Raft group (gastrolog-292yi),
	// whether or not it has a storage placement for this tier. Non-storage
	// nodes still need to join as voters — without that, a tier with RF
	// smaller than the cluster size can't reach quorum because most nodes
	// never registered the group. AddTierToVault handles both cases: storage
	// nodes get a TierInstance, non-storage nodes only get a Raft group.
	tierBelongsHere := leaderNodeID == d.localNodeID || slices.Contains(followerNodeIDs, d.localNodeID)
	if !tierBelongsHere {
		if existing := d.orch.FindLocalTierExported(v.ID, tierID); existing != nil {
			// Tier moved away from this node — drop the storage instance.
			// The Raft group itself stays (symmetric voting).
			d.orch.RemoveTierFromVault(v.ID, tierID)
		}
	}
	d.rebuildVaultIfTierMissing(ctx, v, tierID)
}

func (d *configDispatcher) registerVault(ctx context.Context, v system.VaultConfig, tierID glid.GLID) {
	if err := d.orch.AddVault(ctx, v, d.factories); err != nil {
		d.logger.Error("dispatch: add vault for gained tier",
			"vault", v.ID, "tier", tierID, "error", err)
	}
}

func (d *configDispatcher) rebuildVaultIfTierMissing(ctx context.Context, v system.VaultConfig, tierID glid.GLID) {
	existing := d.orch.FindLocalTierExported(v.ID, tierID)
	if existing != nil {
		d.updateTierRoleIfNeeded(ctx, v.ID, tierID, existing)
		return
	}
	// Tier doesn't exist locally yet — add it incrementally.
	if err := d.orch.AddTierToVault(ctx, v.ID, tierID, d.factories); err != nil {
		d.logger.Error("dispatch: add tier to vault",
			"vault", v.ID, "tier", tierID, "error", err)
	}
}

// updateTierRoleIfNeeded checks whether a tier's role (leader ↔ follower) has changed
// and updates it in place — avoiding a full vault rebuild and file lock churn.
func (d *configDispatcher) updateTierRoleIfNeeded(ctx context.Context, vaultID, tierID glid.GLID, existing *orchestrator.TierInstance) {
	tierCfg, err := d.cfgStore.GetTier(ctx, tierID)
	if err != nil || tierCfg == nil {
		return
	}
	nscs, err := d.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return
	}
	leaderNodeID := system.LeaderNodeID(func() []system.TierPlacement { p, _ := d.cfgStore.GetTierPlacements(ctx, tierCfg.ID); return p }(), nscs)
	followerNodeIDs := system.FollowerNodeIDs(func() []system.TierPlacement { p, _ := d.cfgStore.GetTierPlacements(ctx, tierCfg.ID); return p }(), nscs)
	shouldBeFollower := slices.Contains(followerNodeIDs, d.localNodeID)
	if existing.IsFollower == shouldBeFollower {
		return // role unchanged
	}
	existing.IsFollower = shouldBeFollower
	// FollowerTargets are refreshed by the rotation sweep every 15s.
	if shouldBeFollower {
		existing.LeaderNodeID = leaderNodeID
	} else {
		existing.LeaderNodeID = ""
	}
	d.logger.Info("dispatch: tier role updated in place",
		"vault", vaultID, "tier", tierID,
		"isFollower", shouldBeFollower)
}

// newFollowersForTier returns follower node IDs that don't already have a
// local tier instance on this node's orchestrator. Existing followers already
// have all chunks from normal replication — only genuinely new followers need
// catchup. This prevents redundant chunk transfers on leader reassignment
// (e.g. when a node dies and the leader moves but followers stay the same).
func (d *configDispatcher) newFollowersForTier(vaultID, tierID glid.GLID, followerNodeIDs []string) []string {
	existing := d.orch.FindLocalTierExported(vaultID, tierID)
	if existing == nil {
		// Tier was just added to this node — all followers are new.
		return followerNodeIDs
	}
	// Build set of follower node IDs that were already being replicated to.
	prev := make(map[string]bool, len(existing.FollowerTargets))
	for _, tgt := range existing.FollowerTargets {
		prev[tgt.NodeID] = true
	}
	var added []string
	for _, nid := range followerNodeIDs {
		if !prev[nid] {
			added = append(added, nid)
		}
	}
	return added
}

// handleTierDeleted removes vaults that no longer have any local tiers.
func (d *configDispatcher) handleTierDeleted(ctx context.Context, tierID glid.GLID, drain bool) {
	d.logger.Info("dispatch: handleTierDeleted", "tier", tierID, "drain", drain)

	// The tier config is already deleted from the store, so we can't look up
	// VaultID. Instead, scan locally registered vaults for this tier instance.
	for _, vaultID := range d.orch.ListVaults() {
		tier := d.orch.FindLocalTierExported(vaultID, tierID)
		if tier == nil {
			continue // this node doesn't host the tier in this vault
		}

		if drain && tier.IsLeader() {
			// Only the config leader should drain — it owns the data.
			if err := d.orch.DrainTier(ctx, vaultID, tierID, orchestrator.TierDrainDecommission, ""); err != nil {
				d.logger.Warn("dispatch: tier drain failed, removing immediately",
					"vault", vaultID, "tier", tierID, "error", err)
				d.orch.RemoveTierFromVault(vaultID, tierID)
			} else {
				// Don't destroy Raft group yet — drain needs the tier instance.
				// finishTierDrain will clean up after completion.
				return
			}
		} else {
			// Non-leader or non-drain: remove local instance immediately.
			d.orch.RemoveTierFromVault(vaultID, tierID)
		}
	}

	// Stop the leader loop before destroying the group. This is especially
	// important for non-storage nodes that joined the tier Raft group
	// (gastrolog-292yi) but have no TierInstance — RemoveTierFromVault
	// above is a no-op for them, so the leader loop would otherwise leak.
	d.orch.StopTierLeaderLoop(tierID)

	// Destroy the tier's Raft group (safe for non-leader or non-drain paths).
	if d.factories.GroupManager != nil {
		if err := d.factories.GroupManager.DestroyGroup(tierID.String()); err != nil {
			d.logger.Debug("dispatch: destroy tier raft group (may not exist)", "tier", tierID, "error", err)
		}
	}
}

func (d *configDispatcher) handleClusterTLSPut(ctx context.Context) {
	if d.clusterTLS == nil {
		return
	}
	cfg, err := d.cfgStore.Load(ctx)
	if err != nil || cfg == nil || cfg.Runtime.ClusterTLS == nil {
		d.logger.Error("dispatch: read cluster TLS for reload", "error", err)
		return
	}
	tls := cfg.Runtime.ClusterTLS
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
