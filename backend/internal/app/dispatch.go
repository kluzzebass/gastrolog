package app

import (
	"context"
	"errors"
	"gastrolog/internal/glid"
	"log/slog"
	"maps"
	"os"
	"slices"

	"gastrolog/internal/cluster"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
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
	ApplyRotationPolicyForRole(ctx context.Context, vaultID glid.GLID) error
	DisableVault(id glid.GLID) error
	EnableVault(id glid.GLID) error
	ForceRemoveVault(id glid.GLID) error
	RemoveVaultInstance(vaultID glid.GLID) bool
	DeleteVaultInstance(vaultID glid.GLID) bool
	AddVaultInstance(ctx context.Context, vaultID glid.GLID, f orchestrator.Factories) error
	DrainInstance(ctx context.Context, vaultID glid.GLID, mode orchestrator.DrainMode, targetNodeID string) error
	UnregisterVault(id glid.GLID) error
	MissingVaultInstance(vaultID glid.GLID, vaultIDs []glid.GLID) bool
	LocalInstanceIDs(vaultID glid.GLID) []glid.GLID
	DrainVault(ctx context.Context, vaultID glid.GLID, targetNodeID string) error
	IsDraining(vaultID glid.GLID) bool
	CancelDrain(ctx context.Context, vaultID glid.GLID) error
	ListIngesters() []glid.GLID
	AddIngester(id glid.GLID, name, ingType string, passive bool, r orchestrator.Ingester) error
	RemoveIngester(id glid.GLID) error
	UpdateMaxConcurrentJobs(n int) error
	MaxConcurrentJobs() int
	FindLocalVaultInstance(vaultID glid.GLID) *orchestrator.VaultInstance
	RefreshVaultCtlMembers(clusterNodes []system.NodeConfig, f orchestrator.Factories)
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
	orch               orchActions
	cfgStore           system.Store
	factories          orchestrator.Factories
	localNodeID        string
	logger             *slog.Logger
	clusterTLS         *cluster.ClusterTLS                              // nil for single-node or memory mode
	tlsFilePath        string                                           // path to persist cluster TLS on rotation
	configSignal       *notify.Signal                                   // broadcasts config changes to WatchConfig streams
	managedFileHandler ManagedFileHandler                               // nil for single-node or before wiring
	catchupScheduler   func(vaultID glid.GLID, followerNodeIDs []string) // nil until orch is wired
	placementTrigger   func()                                           // triggers immediate placement reconcile; nil for single-node
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
		// Cluster membership changed — refresh every local vault's
		// vault-ctl Raft group desired-member set. The vault-ctl leader's
		// reconcile pass (next tick, or sooner on a leader transition)
		// then issues AddVoter / RemoveServer to converge the per-group
		// configuration. Without this, vault-ctl groups stay frozen at
		// bootstrap membership and a scaled-in node loops forever in
		// pre-vote campaigns. See gastrolog-4zy8a.
		d.handleNodeConfigChange(ctx)
	case raftfsm.NotifyNodeStateChanged:
		// Node lifecycle state transitioned (e.g., Live → Unreachable,
		// Maintenance → Live, etc.). The placement guard (gastrolog-slc6l)
		// gates rotation on the soft-offline states, so a state change
		// can flip a node from "rotation-permitted" to "rotation-gated"
		// or vice versa. Wake the placement reconciler so the gate
		// re-evaluates without waiting for the 15s ticker.
		if d.placementTrigger != nil {
			d.placementTrigger()
		}
	case raftfsm.NotifyManagedFilePut:
		if d.managedFileHandler != nil {
			d.managedFileHandler.OnPut(ctx, n.ID)
		}
	case raftfsm.NotifyManagedFileDeleted:
		if d.managedFileHandler != nil {
			d.managedFileHandler.OnDelete(n.ID)
		}
	case raftfsm.NotifyIngesterAssignmentSet:
		d.handleIngesterAssignment(ctx, n.ID)
	case raftfsm.NotifyVaultPlacementsSet:
		// Re-fire handleInstancePut so applyInstanceMembershipChange can pick
		// up the now-complete placements. Without this re-trigger, a
		// rejoining node that replays VaultPut before
		// CmdSetVaultPlacements never builds the missing instance — the
		// failure mode that left node3 with only 1 of 3 instances after
		// a snapshot/replication catchup. See gastrolog-51gme.
		d.handleInstancePut(ctx, n.ID)
	case raftfsm.NotifyNodeStorageConfigSet:
		// NSC changes the universe of eligible placement candidates —
		// either by adding a storage (new follower slot opens up) or
		// removing one (existing placements may need to migrate). The
		// 15s placement ticker would eventually pick this up, but
		// operators expect `gastrolog config node add-storage` to take
		// effect immediately; without a trigger the vault stays at its
		// stale placement count for up to 15s and looks broken. See
		// gastrolog-2yeie.
		if d.placementTrigger != nil {
			d.placementTrigger()
		}
	case raftfsm.NotifyCloudServicePut, raftfsm.NotifyCloudServiceDeleted,
		raftfsm.NotifySetupWizardDismissedSet,
		raftfsm.NotifyIngesterAliveSet,
		raftfsm.NotifyIngesterCheckpointSet,
		raftfsm.NotifyLogLevelsSet:
		// No orchestrator side effects; configSignal fires below.
		// For LogLevelsSet the ComponentFilterHandler picks up the
		// new rule set via its configSignal subscription (Phase 2c).
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

	// The vault has exactly one vault whose ID equals the vault's ID.
	// Every node instantiates the vault if it can serve it.
	vaultIDs := []glid.GLID{id}

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

	// Incrementally add/remove instances that changed. Never tear down
	// the entire vault — that causes cascading rebuilds and data warnings.
	if d.orch.MissingVaultInstance(id, vaultIDs) {
		d.reconcileVaultInstance(ctx, id, vaultIDs)
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

// reconcileVaultInstance incrementally adds missing instances and removes
// stale ones from an existing vault, without tearing down any instances
// that are unchanged.
func (d *configDispatcher) reconcileVaultInstance(ctx context.Context, vaultID glid.GLID, vaultIDs []glid.GLID) {
	expected := make(map[glid.GLID]bool, len(vaultIDs))
	for _, id := range vaultIDs {
		expected[id] = true
	}

	// Remove instances that are no longer expected.
	for _, localVaultID := range d.orch.LocalInstanceIDs(vaultID) {
		if !expected[localVaultID] {
			d.orch.RemoveVaultInstance(vaultID)
		}
	}

	// Add instances that are expected but not local.
	for range vaultIDs {
		if err := d.orch.AddVaultInstance(ctx, vaultID, d.factories); err != nil {
			d.logger.Error("dispatch: add vault instance",
				"vault", vaultID, "error", err)
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
//
// Eligibility:
//   - AllNodes=true: every node in the current cluster is eligible — this
//     node included, regardless of NodeIDs. Joiners pick up the ingester
//     automatically; the dispatcher consults membership state on every
//     call rather than snapshotting at config-write time.
//   - AllNodes=false, NodeIDs non-empty: only listed nodes eligible.
//   - AllNodes=false, NodeIDs empty: every node eligible (legacy
//     "empty list = match all" semantic, preserved for backwards compat
//     with configs created before AllNodes existed).
//
// Parallel ingesters run on every eligible node; singleton ingesters run
// on the Raft-assigned eligible node.
func (d *configDispatcher) shouldRunIngester(ctx context.Context, cfg system.IngesterConfig, singleton bool) bool {
	if !cfg.AllNodes && len(cfg.NodeIDs) > 0 && !slices.Contains(cfg.NodeIDs, d.localNodeID) {
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

// handleInstancePut adjusts vault registration when an instance's placements change.
// Runs on ALL nodes — each node independently decides whether it gained or lost
// ownership based on the vault's resolved node IDs vs localNodeID.
// Also reloads rotation/retention policies when instance config changes.
func (d *configDispatcher) handleInstancePut(ctx context.Context, vaultID glid.GLID) {
	// The vault's ID equals the instance's ID.
	v, err := d.cfgStore.GetVault(ctx, vaultID)
	if err != nil || v == nil {
		d.logger.Error("dispatch: get vault for vault change", "vault", vaultID, "error", err)
		return
	}

	nscs, err := d.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		d.logger.Error("dispatch: list node storage configs for vault change", "vault", vaultID, "error", err)
		return
	}

	leaderNodeID := system.LeaderNodeID(func() []system.VaultPlacement { p, _ := d.cfgStore.GetVaultPlacements(ctx, vaultID); return p }(), nscs)
	followerNodeIDs := system.FollowerNodeIDs(func() []system.VaultPlacement { p, _ := d.cfgStore.GetVaultPlacements(ctx, vaultID); return p }(), nscs)

	// Only act on vault membership once placements are fully assigned. During
	// cluster-init the placement manager assigns placements one-at-a-time,
	// each firing its own CmdPutVault. Building the vault locally on a partial
	// placement state is wrong for two reasons: (1) we can't reliably answer
	// "does this vault belong here" with incomplete placements, and (2) it
	// would create the chunk manager (and vault-ctl Raft group) with a wrong-size
	// member list, which then persists in boltdb.
	//
	// Policy reloads (rotation/retention) still run below because they are
	// independent of placement state.
	d.applyInstanceMembershipChange(ctx, *v, vaultID, leaderNodeID, followerNodeIDs)

	// Reload filters so ingestion routing picks up the new placement leader
	// immediately. Without this, records are forwarded to the old (possibly
	// dead) node until the rotation sweep recompiles filters (up to 15s).
	if err := d.orch.ReloadFilters(ctx); err != nil {
		d.logger.Warn("dispatch: reload filters after vault change", "error", err)
	}

	// Reload rotation and retention policies — vault config may have changed
	// policy references (rotation_policy_id, retention_rules).
	d.reloadRotationPolicies(ctx)
	d.reloadRetentionPolicies(ctx)

	// Schedule catchup only for NEWLY added followers, not existing ones.
	// When a leader changes but followers stay the same (e.g. a node dies),
	// the surviving followers already have all chunks — no catchup needed.
	if leaderNodeID == d.localNodeID && len(followerNodeIDs) > 0 && d.catchupScheduler != nil {
		newFollowers := d.newFollowersForInstance(v.ID, followerNodeIDs)
		if len(newFollowers) > 0 {
			d.catchupScheduler(v.ID, newFollowers)
		}
	}

	// Trigger immediate placement reconcile so secondaries are assigned
	// without waiting for the 15-second ticker.
	if d.placementTrigger != nil {
		d.placementTrigger()
	}
}

// applyInstanceMembershipChange decides whether the vault belongs here based on
// the (complete) placement state, and either adds/rebuilds it locally or
// removes it if it no longer belongs. Deferred entirely when placements are
// incomplete — the next CmdPutVault from the placement manager will retry.
func (d *configDispatcher) applyInstanceMembershipChange(ctx context.Context, v system.VaultConfig, vaultID glid.GLID, leaderNodeID string, followerNodeIDs []string) {
	// Placements are "complete" when they include a leader. We can't gate on
	// len(placements) >= RF because RF may be unsatisfiable when a node is
	// down — the placement manager writes the best it can with surviving
	// nodes. Gating on RF caused permanent deferral after node failure:
	// the role was never updated, rotation never ran, chunks never sealed.
	placements, _ := d.cfgStore.GetVaultPlacements(ctx, vaultID)
	hasLeader := false
	for _, p := range placements {
		if p.Leader {
			hasLeader = true
			break
		}
	}
	if !hasLeader {
		d.logger.Debug("dispatch: vault placements have no leader, deferring rebuild",
			"vault", vaultID, "placements", len(placements))
		return
	}

	// Every node participates in every vault-ctl Raft group (gastrolog-292yi),
	// whether or not it has a storage placement for this vault. Non-storage
	// nodes still need to join as voters — without that, a vault with RF
	// smaller than the cluster size can't reach quorum because most nodes
	// never registered the group. AddVaultInstance handles both cases: storage
	// nodes get a VaultInstance, non-storage nodes only get a Raft group.
	vaultBelongsHere := leaderNodeID == d.localNodeID || slices.Contains(followerNodeIDs, d.localNodeID)
	if !vaultBelongsHere {
		if existing := d.orch.FindLocalVaultInstance(v.ID); existing != nil {
			// Instance moved away from this node — drop the storage
			// instance. The Raft group itself stays (symmetric voting).
			d.orch.RemoveVaultInstance(v.ID)
		}
	}
	d.rebuildVaultIfInstanceMissing(ctx, v, vaultID)
}

func (d *configDispatcher) registerVault(ctx context.Context, v system.VaultConfig, vaultID glid.GLID) {
	if err := d.orch.AddVault(ctx, v, d.factories); err != nil {
		d.logger.Error("dispatch: add vault for gained vault",
			"vault", v.ID, "error", err)
	}
}

func (d *configDispatcher) rebuildVaultIfInstanceMissing(ctx context.Context, v system.VaultConfig, vaultID glid.GLID) {
	_ = vaultID // legacy parameter; instance is identified by v.ID under 1:1 collapse
	existing := d.orch.FindLocalVaultInstance(v.ID)
	if existing != nil {
		d.updateInstanceRoleIfNeeded(ctx, v.ID, existing)
		return
	}
	// The vault may not be registered in the orchestrator yet — typically
	// because we got here via a NotifyVaultPlacementsSet that came in over
	// post-snapshot Raft log replay while the corresponding NotifyVaultPut
	// was inside the snapshot itself. Snapshot restore (fsm.Restore) does
	// NOT fire onApply notifications, so the dispatcher never saw the
	// vault-put for this vault. Without this guard, AddVaultInstance below
	// fails with ErrVaultNotFound and the orchestrator permanently lacks
	// the vault until the operator forces a config write. See gastrolog-3idjc.
	if !slices.Contains(d.orch.ListVaults(), v.ID) {
		if err := d.orch.AddVault(ctx, v, d.factories); err != nil &&
			!errors.Is(err, orchestrator.ErrDuplicateID) {
			d.logger.Error("dispatch: add vault before instance",
				"vault", v.ID, "error", err)
			return
		}
	}
	// Instance doesn't exist locally yet — add it incrementally.
	if err := d.orch.AddVaultInstance(ctx, v.ID, d.factories); err != nil {
		d.logger.Error("dispatch: add vault instance",
			"vault", v.ID, "error", err)
	}
}

// updateInstanceRoleIfNeeded checks whether a vault instance's role
// has changed and refreshes the LeaderNodeID pointer in place —
// avoiding a full vault rebuild and file lock churn.
//
// Under fan-out (gastrolog-hshgl) the leader/follower distinction at
// the data-path level is gone, so the leader/follower role flip no
// longer triggers a rotation-policy change. LeaderNodeID survives as
// a routing hint for the lifecycle reconciler's RequestReplicaCatchup
// peer set.
func (d *configDispatcher) updateInstanceRoleIfNeeded(ctx context.Context, vaultID glid.GLID, existing *orchestrator.VaultInstance) {
	v, err := d.cfgStore.GetVault(ctx, vaultID)
	if err != nil || v == nil {
		return
	}
	nscs, err := d.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return
	}
	leaderNodeID := system.LeaderNodeID(func() []system.VaultPlacement { p, _ := d.cfgStore.GetVaultPlacements(ctx, vaultID); return p }(), nscs)
	followerNodeIDs := system.FollowerNodeIDs(func() []system.VaultPlacement { p, _ := d.cfgStore.GetVaultPlacements(ctx, vaultID); return p }(), nscs)
	shouldBeFollower := slices.Contains(followerNodeIDs, d.localNodeID)

	// Refresh LeaderNodeID for the reconciler's peer-set construction.
	// The placement leader can transfer between two other nodes while
	// this node's placement membership doesn't change; the local
	// pointer must follow. See gastrolog-4zy8a.
	if shouldBeFollower {
		existing.LeaderNodeID = leaderNodeID
	} else {
		existing.LeaderNodeID = ""
	}
}

// newFollowersForInstance returns follower node IDs that don't already have a
// local vault instance on this node's orchestrator. Existing followers already
// have all chunks from normal replication — only genuinely new followers need
// catchup. This prevents redundant chunk transfers on leader reassignment
// (e.g. when a node dies and the leader moves but followers stay the same).
func (d *configDispatcher) newFollowersForInstance(vaultID glid.GLID, followerNodeIDs []string) []string {
	existing := d.orch.FindLocalVaultInstance(vaultID)
	if existing == nil {
		// Instance was just added to this node — all followers are new.
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

// handleNodeConfigChange reads the current cluster node list and refreshes
// every local vault's vault-ctl Raft group desired-member set so the leader
// manager's reconcile pass can AddVoter / RemoveServer the diff.
func (d *configDispatcher) handleNodeConfigChange(ctx context.Context) {
	nodes, err := d.cfgStore.ListNodes(ctx)
	if err != nil {
		d.logger.Error("dispatch: list nodes for vault-ctl membership refresh", "error", err)
		return
	}
	d.orch.RefreshVaultCtlMembers(nodes, d.factories)
}

// ReplayConfigFromStore walks the FSM-backed config store and registers
// any vault or ingester the orchestrator is missing. Use after a fresh
// joiner has finished snapshot replication: FSM.Restore does not fire
// onApply notifications (only FSM.Apply does), so a joiner whose state
// arrived purely via snapshot would otherwise see an empty
// vault/ingester registry. See gastrolog-3hcfm.
//
// Crucially, we only call the per-entity handler for entities the
// orchestrator does NOT already have. A joiner whose state arrived via
// post-snapshot Apply (chatterbox PUT after a snapshot, but log entries
// covered the rest) ALREADY received the notification through the live
// dispatcher; re-firing handleIngesterPut for it would trip the
// orchestrator's remove+re-add idempotent-replace path. That path
// races the new ingester goroutine's setIngesterAlive(true) against
// the old goroutine's setIngesterAlive(false): when the stale false
// lands in Raft after the new true, the FSM ends up showing the node
// as not-alive even though chatterbox is happily running. The
// dashboard then displays "7/10" instead of "10/10".
//
// Routes / rotation / retention reload as a whole set per call —
// idempotent and goroutine-free — so they run unconditionally.
func (d *configDispatcher) ReplayConfigFromStore(ctx context.Context) {
	if d.orch == nil || d.cfgStore == nil {
		return
	}

	registeredVaults := make(map[glid.GLID]bool)
	for _, id := range d.orch.ListVaults() {
		registeredVaults[id] = true
	}
	vaults, err := d.cfgStore.ListVaults(ctx)
	if err != nil {
		d.logger.Error("dispatch: list vaults for replay", "error", err)
	}
	for _, v := range vaults {
		if registeredVaults[v.ID] {
			continue
		}
		d.handleVaultPut(ctx, v.ID)
	}

	registeredIngesters := make(map[glid.GLID]bool)
	for _, id := range d.orch.ListIngesters() {
		registeredIngesters[id] = true
	}
	ingesters, err := d.cfgStore.ListIngesters(ctx)
	if err != nil {
		d.logger.Error("dispatch: list ingesters for replay", "error", err)
	}
	for _, ing := range ingesters {
		if registeredIngesters[ing.ID] {
			continue
		}
		d.handleIngesterPut(ctx, ing.ID)
	}

	d.reloadFilters(ctx)
	d.reloadRotationPolicies(ctx)
	d.reloadRetentionPolicies(ctx)
}
