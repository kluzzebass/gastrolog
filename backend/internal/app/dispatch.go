package app

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"log/slog"
	"maps"
	"os"
	"slices"
	"sync"

	"gastrolog/internal/alert"
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
	IsIngesterRunning(id glid.GLID) bool
	ReconcileIngesters(desired []orchestrator.IngesterDesired) error
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
	clusterTLS         *cluster.ClusterTLS                               // nil for single-node or memory mode
	tlsFilePath        string                                            // path to persist cluster TLS on rotation
	configSignal       *notify.Signal                                    // broadcasts config changes to WatchConfig streams
	managedFileHandler ManagedFileHandler                                // nil for single-node or before wiring
	catchupScheduler   func(vaultID glid.GLID, followerNodeIDs []string) // nil until orch is wired
	placementTrigger   func()                                            // triggers immediate placement reconcile; nil for single-node
	alerts             alert.Sink                                        // raises/clears config-side-effect-failed; nil disables alerting

	// obligations tracks per-entity config side effects that failed to
	// apply to the local orchestrator. Keyed by obligationKey(entityType,
	// entityID). It is a standing reconcile debt: retried event-driven by
	// the next dispatch touching the entity (Handle) and by startup replay
	// (ReplayConfigFromStore), and cleared when that later apply succeeds.
	// In-memory only — the mutation is durable in Raft and startup reconcile
	// rebuilds any divergence, and alarms do not persist across restart. The
	// mutex guards the map because Handle can fire from more than one FSM
	// (cluster-ctl store variants) over a node's lifetime.
	mu          sync.Mutex
	obligations map[string]obligation

	// lastIngesterDivergence is the previously logged not-running set
	// (sorted, comma-joined; "" = converged), so the convergence sweep logs
	// once per state change rather than every 15s tick. Only the sweep
	// goroutine touches it.
	lastIngesterDivergence string
}

// obligation is a config side effect that failed to apply to this node's
// orchestrator: a standing reconcile debt for one entity, cleared when a
// later dispatch (or startup replay) applies the entity cleanly.
type obligation struct {
	entityType string // "vault", "ingester", "route", "setting", …
	entityID   string // GLID / key, or "" for whole-set operations
	op         string // the failed operation, e.g. "vault-put"
	err        error
}

// configSideEffectAlarmType is the catalog ID for the standing alarm raised
// when a committed config mutation cannot be applied to the local
// orchestrator. The instance key is the obligation key, so each diverged
// entity annunciates and clears independently.
const configSideEffectAlarmType = "config-side-effect-failed"

// Entity-type labels for reconcile obligations. Whole-set operations (route
// filters, policy reloads, the ingester reconcile, membership refresh) carry
// an empty entity ID and key on the bare label.
const (
	entVault      = "vault"
	entIngester   = "ingester"
	entRoute      = "route"
	entRotation   = "rotation-policy"
	entRetention  = "retention-policy"
	entSetting    = "setting"
	entClusterTLS = "cluster-tls"
	entNodeConfig = "node-config"
)

// obligationKey composes the stable dedup key for an entity's reconcile
// obligation (and the alarm instance key). Whole-set operations have no
// entity ID and key on the bare type label.
func obligationKey(entityType, entityID string) string {
	if entityID == "" {
		return entityType
	}
	return entityType + ":" + entityID
}

// settle records or clears the reconcile obligation and standing alarm for
// one entity from the outcome of applying its config side effect. A non-nil
// err records the obligation and raises the alarm (both keyed by the entity);
// a nil err clears any standing obligation and its alarm.
//
// Called inline from Handle: recording is O(1) in-memory bookkeeping plus an
// alarm Raise/Clear (also in-memory), so FSM.Apply stays fast and
// non-blocking. There is no dedicated retry worker — the mutation already
// executes inline in Apply today, and the only correctness-relevant retry
// triggers (the next dispatch of the entity, leadership reload, startup
// replay) already flow through Handle / ReplayConfigFromStore inline. A
// worker would either duplicate those or need a periodic poll, which is
// forbidden; true async execution is deferred to the config-store redesign.
//
// Obligation transitions are logged (events are logs): one line on the raise
// edge and one on the clear edge, not on every retry that re-observes the
// same standing obligation.
func (d *configDispatcher) settle(entityType, entityID, op string, err error) {
	key := obligationKey(entityType, entityID)

	d.mu.Lock()
	_, existed := d.obligations[key]
	switch {
	case err != nil:
		if d.obligations == nil {
			d.obligations = make(map[string]obligation)
		}
		d.obligations[key] = obligation{entityType: entityType, entityID: entityID, op: op, err: err}
	case existed:
		delete(d.obligations, key)
	}
	d.mu.Unlock()

	if err != nil {
		if !existed {
			d.logger.Error("dispatch: config side effect failed — reconcile obligation recorded",
				"entity_type", entityType, "entity", entityID, "op", op, "error", err)
		}
		if d.alerts != nil {
			d.alerts.Raise(configSideEffectAlarmType, key,
				fmt.Sprintf("config side-effect failed: %s %s (%s): %v", entityType, entityID, op, err))
		}
		return
	}
	if existed {
		d.logger.Info("dispatch: config side effect recovered — reconcile obligation cleared",
			"entity_type", entityType, "entity", entityID, "op", op)
	}
	if d.alerts != nil {
		d.alerts.Clear(configSideEffectAlarmType, key)
	}
}

// obligationCount returns the number of standing reconcile obligations. Used
// by tests and, potentially, inspector surfaces.
func (d *configDispatcher) obligationCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.obligations)
}

// Handle dispatches a single FSM notification to the appropriate orchestrator
// methods. It runs inline from within FSM.Apply, so it stays fast: the
// mutation is already durable in Raft and cannot be rolled back. Failures are
// no longer swallowed — each side effect that can fail returns an error, and
// Handle records it as a per-entity reconcile obligation and raises a standing
// alarm via settle(). The obligation is retried event-driven: the next
// dispatch touching the same entity re-runs the handler (and settle clears the
// obligation on success), as does startup replay in ReplayConfigFromStore.
func (d *configDispatcher) Handle(n raftfsm.Notification) {
	if d.orch == nil {
		return // not wired yet (bootstrap phase)
	}

	ctx := context.Background()

	switch n.Kind {
	case raftfsm.NotifyVaultPut:
		d.settle(entVault, n.ID.String(), "vault-put", d.handleVaultPut(ctx, n.ID))
	case raftfsm.NotifyVaultDeleted:
		d.settle(entVault, n.ID.String(), "vault-delete", d.handleVaultDeleted(n))
	case raftfsm.NotifyRoutePut, raftfsm.NotifyRouteDeleted:
		d.settle(entRoute, "", "reload-filters", d.reloadFilters(ctx))
	case raftfsm.NotifyRotationPolicyPut, raftfsm.NotifyRotationPolicyDeleted:
		d.settle(entRotation, "", "reload-rotation-policies", d.reloadRotationPolicies(ctx))
	case raftfsm.NotifyRetentionPolicyPut, raftfsm.NotifyRetentionPolicyDeleted:
		d.settle(entRetention, "", "reload-retention-policies", d.reloadRetentionPolicies(ctx))
	case raftfsm.NotifyIngesterPut:
		d.settle(entIngester, "", "reconcile-ingesters", d.reconcileIngesters(ctx))
	case raftfsm.NotifyIngesterDeleted:
		d.settle(entIngester, "", "reconcile-ingesters", d.reconcileIngesters(ctx))
	case raftfsm.NotifySettingPut:
		d.settle(entSetting, n.Key, "apply-setting", d.handleSettingPut(ctx, n.Key))
	case raftfsm.NotifyClusterTLSPut:
		d.settle(entClusterTLS, "", "reload-cluster-tls", d.handleClusterTLSPut(ctx))
	case raftfsm.NotifyNodeConfigPut, raftfsm.NotifyNodeConfigDeleted:
		// Cluster membership changed — refresh every local vault's
		// vault-ctl Raft group desired-member set. The vault-ctl leader's
		// reconcile pass (next tick, or sooner on a leader transition)
		// then issues AddVoter / RemoveServer to converge the per-group
		// configuration. Without this, vault-ctl groups stay frozen at
		// bootstrap membership and a scaled-in node loops forever in
		// pre-vote campaigns. See gastrolog-4zy8a.
		d.settle(entNodeConfig, "", "refresh-vault-ctl-members", d.handleNodeConfigChange(ctx))
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
		d.settle(entIngester, "", "reconcile-ingesters", d.reconcileIngesters(ctx))
	case raftfsm.NotifyVaultPlacementsSet:
		// Re-fire handleInstancePut so applyInstanceMembershipChange can pick
		// up the now-complete placements. Without this re-trigger, a
		// rejoining node that replays VaultPut before
		// CmdSetVaultPlacements never builds the missing instance — the
		// failure mode that left node3 with only 1 of 3 instances after
		// a snapshot/replication catchup. See gastrolog-51gme.
		d.settle(entVault, n.ID.String(), "instance-put", d.handleInstancePut(ctx, n.ID))
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

// handleVaultPut applies a committed vault-put to the local orchestrator. It
// attempts every step (drain-cancel, add-or-reconfigure) and returns the
// joined error of whatever failed, so the caller can record one reconcile
// obligation for the vault.
func (d *configDispatcher) handleVaultPut(ctx context.Context, id glid.GLID) error {
	vaultCfg, err := d.cfgStore.GetVault(ctx, id)
	if err != nil {
		return fmt.Errorf("read vault config %s: %w", id, err)
	}
	if vaultCfg == nil {
		return fmt.Errorf("read vault config %s: config not found", id)
	}

	// The vault has exactly one vault whose ID equals the vault's ID.
	// Every node instantiates the vault if it can serve it.
	vaultIDs := []glid.GLID{id}

	var errs []error

	// Cancel any in-progress drain.
	if d.orch.IsDraining(id) {
		if err := d.orch.CancelDrain(ctx, id); err != nil {
			errs = append(errs, fmt.Errorf("cancel drain: %w", err))
		}
		// Fall through to applyExistingVaultChanges to reconfigure.
	}

	if !slices.Contains(d.orch.ListVaults(), id) {
		if err := d.orch.AddVault(ctx, *vaultCfg, d.factories); err != nil {
			errs = append(errs, fmt.Errorf("add vault %q: %w", vaultCfg.Name, err))
		}
		if d.placementTrigger != nil {
			d.placementTrigger()
		}
		return errors.Join(errs...)
	}

	// Incrementally add/remove instances that changed. Never tear down
	// the entire vault — that causes cascading rebuilds and data warnings.
	if d.orch.MissingVaultInstance(id, vaultIDs) {
		errs = append(errs, d.reconcileVaultInstance(ctx, id, vaultIDs))
		return errors.Join(errs...)
	}

	errs = append(errs, d.applyExistingVaultChanges(ctx, id, vaultCfg))
	return errors.Join(errs...)
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
func (d *configDispatcher) reconcileVaultInstance(ctx context.Context, vaultID glid.GLID, vaultIDs []glid.GLID) error {
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

	var errs []error

	// Add instances that are expected but not local.
	for range vaultIDs {
		if err := d.orch.AddVaultInstance(ctx, vaultID, d.factories); err != nil {
			errs = append(errs, fmt.Errorf("add vault instance %s: %w", vaultID, err))
		}
	}

	vaultCfg, _ := d.cfgStore.GetVault(ctx, vaultID)
	if vaultCfg != nil {
		errs = append(errs, d.applyExistingVaultChanges(ctx, vaultID, vaultCfg))
	}

	if d.placementTrigger != nil {
		d.placementTrigger()
	}
	return errors.Join(errs...)
}

// applyExistingVaultChanges reloads policies and re-applies enabled state for
// a vault already registered locally. It attempts every step and returns the
// joined error of whatever failed rather than bailing on the first — a reload
// failure must not skip the enable/disable step.
func (d *configDispatcher) applyExistingVaultChanges(ctx context.Context, id glid.GLID, cfg *system.VaultConfig) error {
	var errs []error
	if err := d.orch.ReloadFilters(ctx); err != nil {
		errs = append(errs, fmt.Errorf("reload filters: %w", err))
	}
	if err := d.orch.ReloadRotationPolicies(ctx); err != nil {
		errs = append(errs, fmt.Errorf("reload rotation policies: %w", err))
	}
	if err := d.orch.ReloadRetentionPolicies(ctx); err != nil {
		errs = append(errs, fmt.Errorf("reload retention policies: %w", err))
	}
	if !cfg.Enabled {
		if err := d.orch.DisableVault(id); err != nil {
			errs = append(errs, fmt.Errorf("disable vault: %w", err))
		}
	} else {
		if err := d.orch.EnableVault(id); err != nil {
			errs = append(errs, fmt.Errorf("enable vault: %w", err))
		}
	}
	return errors.Join(errs...)
}

func (d *configDispatcher) handleVaultDeleted(n raftfsm.Notification) error {
	var errs []error
	if err := d.orch.ForceRemoveVault(n.ID); err != nil && !errors.Is(err, orchestrator.ErrVaultNotFound) {
		errs = append(errs, fmt.Errorf("force remove vault %q: %w", n.Name, err))
	}
	if n.DeleteData && n.Dir != "" {
		if err := os.RemoveAll(n.Dir); err != nil {
			errs = append(errs, fmt.Errorf("remove vault directory %q: %w", n.Dir, err))
		}
	}
	return errors.Join(errs...)
}

func (d *configDispatcher) reloadFilters(ctx context.Context) error {
	if err := d.orch.ReloadFilters(ctx); err != nil {
		return fmt.Errorf("reload filters: %w", err)
	}
	return nil
}

func (d *configDispatcher) reloadRotationPolicies(ctx context.Context) error {
	if err := d.orch.ReloadRotationPolicies(ctx); err != nil {
		return fmt.Errorf("reload rotation policies: %w", err)
	}
	return nil
}

func (d *configDispatcher) reloadRetentionPolicies(ctx context.Context) error {
	if err := d.orch.ReloadRetentionPolicies(ctx); err != nil {
		return fmt.Errorf("reload retention policies: %w", err)
	}
	return nil
}

// reconcileIngesters recomputes the full set of ingesters that should run on
// this node from the config store and drives the orchestrator toward it. It is
// the single entry point for every ingester-related FSM notification (put,
// delete, singleton assignment): the orchestrator diffs the snapshot and only
// (re)builds changed ingesters, so unchanged ingesters never flap their alive
// state.
func (d *configDispatcher) reconcileIngesters(ctx context.Context) error {
	cfgs, err := d.cfgStore.ListIngesters(ctx)
	if err != nil {
		return fmt.Errorf("list ingesters: %w", err)
	}

	desired := make([]orchestrator.IngesterDesired, 0, len(cfgs))
	for _, cfg := range cfgs {
		if !cfg.Enabled {
			continue
		}
		reg, ok := d.factories.IngesterTypes[cfg.Type]
		if !ok {
			// An unknown ingester type is a config-level defect, not a
			// transient side-effect failure: retrying the same reconcile
			// will never resolve it, so it is logged and the ingester
			// skipped rather than folded into the reconcile obligation.
			d.logger.Error("dispatch: unknown ingester type", "id", cfg.ID, "name", cfg.Name, "type", cfg.Type)
			continue
		}
		isSingleton := reg.SingletonSupported && cfg.Singleton
		if !d.shouldRunIngester(ctx, cfg, isSingleton) {
			continue
		}
		desired = append(desired, d.ingesterDesired(ctx, cfg, reg))
	}

	if err := d.orch.ReconcileIngesters(desired); err != nil {
		return fmt.Errorf("reconcile ingesters: %w", err)
	}
	return nil
}

// ingesterDesired builds the desired-ingester entry for cfg. The Build closure
// is invoked lazily by the orchestrator — only when the ingester must be
// (re)constructed — and restores any Raft-replicated checkpoint before
// returning so a resuming ingester continues where it left off.
func (d *configDispatcher) ingesterDesired(ctx context.Context, cfg system.IngesterConfig, reg orchestrator.IngesterRegistration) orchestrator.IngesterDesired {
	return orchestrator.IngesterDesired{
		ID:      cfg.ID,
		Name:    cfg.Name,
		Type:    cfg.Type,
		Passive: reg.ListenAddrs != nil,
		Params:  maps.Clone(cfg.Params),
		Build: func() (orchestrator.Ingester, error) {
			params := cfg.Params
			if d.factories.HomeDir != "" {
				params = make(map[string]string, len(cfg.Params)+1)
				maps.Copy(params, cfg.Params)
				params["_state_dir"] = d.factories.HomeDir
			}
			ing, err := reg.Factory(cfg.ID, params, d.factories.Logger)
			if err != nil {
				return nil, err
			}
			// Restore Raft-replicated checkpoint if the ingester supports it.
			if cp, ok := ing.(orchestrator.Checkpointable); ok {
				if data, cpErr := d.cfgStore.GetIngesterCheckpoint(ctx, cfg.ID); cpErr == nil && len(data) > 0 {
					if loadErr := cp.LoadCheckpoint(data); loadErr != nil {
						d.logger.Warn("dispatch: checkpoint load failed, starting fresh", "id", cfg.ID, "error", loadErr)
					}
				}
			}
			return ing, nil
		},
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

func (d *configDispatcher) handleSettingPut(ctx context.Context, key string) error {
	switch key {
	case system.NotifyKeyServerSettingsRaftLegacy, system.NotifyKeyServiceSettings:
		// These paths may change scheduler limits.
	default:
		return nil
	}

	ss, err := d.cfgStore.LoadServerSettings(ctx)
	if err != nil {
		return fmt.Errorf("load server settings: %w", err)
	}
	// Only rebuild when MaxConcurrentJobs actually changed. Legacy
	// NotifySettingPut("server") and service saves share this path;
	// lookup-only saves use a different key — rebuilding the scheduler on every one of
	// those calls shuts down the whole scheduler and waits for in-flight
	// jobs, which causes gocron Shutdown timeouts on busy nodes.
	if ss.Scheduler.MaxConcurrentJobs > 0 && ss.Scheduler.MaxConcurrentJobs != d.orch.MaxConcurrentJobs() {
		if err := d.orch.UpdateMaxConcurrentJobs(ss.Scheduler.MaxConcurrentJobs); err != nil {
			return fmt.Errorf("update max concurrent jobs: %w", err)
		}
	}
	return nil
}

// handleInstancePut adjusts vault registration when an instance's placements change.
// Runs on ALL nodes — each node independently decides whether it gained or lost
// ownership based on the vault's resolved node IDs vs localNodeID.
// Also reloads rotation/retention policies when instance config changes.
func (d *configDispatcher) handleInstancePut(ctx context.Context, vaultID glid.GLID) error {
	// The vault's ID equals the instance's ID.
	v, err := d.cfgStore.GetVault(ctx, vaultID)
	if err != nil {
		return fmt.Errorf("get vault for vault change %s: %w", vaultID, err)
	}
	if v == nil {
		return fmt.Errorf("get vault for vault change %s: config not found", vaultID)
	}

	nscs, err := d.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return fmt.Errorf("list node storage configs for vault change %s: %w", vaultID, err)
	}

	leaderNodeID := system.LeaderNodeID(func() []system.VaultPlacement { p, _ := d.cfgStore.GetVaultPlacements(ctx, vaultID); return p }(), nscs)
	followerNodeIDs := system.FollowerNodeIDs(func() []system.VaultPlacement { p, _ := d.cfgStore.GetVaultPlacements(ctx, vaultID); return p }(), nscs)

	var errs []error

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
	if err := d.applyInstanceMembershipChange(ctx, *v, vaultID, leaderNodeID, followerNodeIDs); err != nil {
		errs = append(errs, err)
	}

	// Reload filters so ingestion routing picks up the new placement leader
	// immediately. Without this, records are forwarded to the old (possibly
	// dead) node until the rotation sweep recompiles filters (up to 15s).
	if err := d.orch.ReloadFilters(ctx); err != nil {
		errs = append(errs, fmt.Errorf("reload filters after vault change: %w", err))
	}

	// Reload rotation and retention policies — vault config may have changed
	// policy references (rotation_policy_id, retention_rules).
	if err := d.reloadRotationPolicies(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := d.reloadRetentionPolicies(ctx); err != nil {
		errs = append(errs, err)
	}

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
	return errors.Join(errs...)
}

// applyInstanceMembershipChange decides whether the vault belongs here based on
// the (complete) placement state, and either adds/rebuilds it locally or
// removes it if it no longer belongs. Deferred entirely when placements are
// incomplete — the next CmdPutVault from the placement manager will retry.
func (d *configDispatcher) applyInstanceMembershipChange(ctx context.Context, v system.VaultConfig, vaultID glid.GLID, leaderNodeID string, followerNodeIDs []string) error {
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
		// Deferred, not failed: the next CmdPutVault from the placement
		// manager retries. No obligation — this is expected mid-init state.
		d.logger.Debug("dispatch: vault placements have no leader, deferring rebuild",
			"vault", vaultID, "placements", len(placements))
		return nil
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
	return d.rebuildVaultIfInstanceMissing(ctx, v, vaultID)
}

func (d *configDispatcher) registerVault(ctx context.Context, v system.VaultConfig, vaultID glid.GLID) {
	if err := d.orch.AddVault(ctx, v, d.factories); err != nil {
		d.logger.Error("dispatch: add vault for gained vault",
			"vault", v.ID, "error", err)
	}
}

func (d *configDispatcher) rebuildVaultIfInstanceMissing(ctx context.Context, v system.VaultConfig, vaultID glid.GLID) error {
	_ = vaultID // legacy parameter; instance is identified by v.ID under 1:1 collapse
	existing := d.orch.FindLocalVaultInstance(v.ID)
	if existing != nil {
		return d.updateInstanceRoleIfNeeded(ctx, v.ID, existing)
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
			return fmt.Errorf("add vault before instance %s: %w", v.ID, err)
		}
	}
	// Instance doesn't exist locally yet — add it incrementally.
	if err := d.orch.AddVaultInstance(ctx, v.ID, d.factories); err != nil {
		return fmt.Errorf("add vault instance %s: %w", v.ID, err)
	}
	return nil
}

// updateInstanceRoleIfNeeded checks whether a vault instance's role
// (leader ↔ follower) has changed and updates it in place — avoiding a full
// vault rebuild and file lock churn.
//
// On a role transition, also re-applies the role-appropriate rotation
// policy to the chunk manager: NeverRotatePolicy on follower, user policy
// on leader. Without this, the chunk manager's policy lags the role flip
// by up to 15 s (the rotationSweep interval). On a follower→leader flip,
// the new leader carries NeverRotatePolicy during the gap and records pile
// up without rotation; on a leader→follower flip, the new follower briefly
// keeps the user policy and could rotate independently. See
// gastrolog-50n4b.
func (d *configDispatcher) updateInstanceRoleIfNeeded(ctx context.Context, vaultID glid.GLID, existing *orchestrator.VaultInstance) error {
	v, err := d.cfgStore.GetVault(ctx, vaultID)
	if err != nil {
		return fmt.Errorf("get vault for role update %s: %w", vaultID, err)
	}
	if v == nil {
		return nil
	}
	nscs, err := d.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return fmt.Errorf("list node storage configs for role update %s: %w", vaultID, err)
	}
	leaderNodeID := system.LeaderNodeID(func() []system.VaultPlacement { p, _ := d.cfgStore.GetVaultPlacements(ctx, vaultID); return p }(), nscs)
	followerNodeIDs := system.FollowerNodeIDs(func() []system.VaultPlacement { p, _ := d.cfgStore.GetVaultPlacements(ctx, vaultID); return p }(), nscs)
	shouldBeFollower := slices.Contains(followerNodeIDs, d.localNodeID)
	roleChanged := existing.IsFollower != shouldBeFollower

	// Always refresh LeaderNodeID, even when this node's role is unchanged.
	// The placement leader can transfer between two other nodes (e.g. an
	// existing leader fails over to a different node) while this follower
	// stays a follower. Without this refresh, the local instance's leader
	// pointer freezes at the original leader and the lifecycle reconciler's
	// RequestReplicaCatchup loops forever against a stale target that
	// rejects every request with "not placement leader". See gastrolog-4zy8a.
	if shouldBeFollower {
		existing.LeaderNodeID = leaderNodeID
	} else {
		existing.LeaderNodeID = ""
	}

	if !roleChanged {
		return nil
	}
	existing.IsFollower = shouldBeFollower
	// FollowerTargets are refreshed by the rotation sweep every 15s.
	d.logger.Info("dispatch: vault role updated in place",
		"vault", vaultID,
		"isFollower", shouldBeFollower)
	if err := d.orch.ApplyRotationPolicyForRole(ctx, vaultID); err != nil {
		return fmt.Errorf("apply rotation policy after role change %s: %w", vaultID, err)
	}
	return nil
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

func (d *configDispatcher) handleClusterTLSPut(ctx context.Context) error {
	if d.clusterTLS == nil {
		return nil
	}
	cfg, err := d.cfgStore.Load(ctx)
	if err != nil {
		return fmt.Errorf("read cluster TLS for reload: %w", err)
	}
	if cfg == nil || cfg.Runtime.ClusterTLS == nil {
		return errors.New("read cluster TLS for reload: cluster TLS absent from config")
	}
	tls := cfg.Runtime.ClusterTLS
	if err := d.clusterTLS.Load([]byte(tls.ClusterCertPEM), []byte(tls.ClusterKeyPEM), []byte(tls.CACertPEM)); err != nil {
		return fmt.Errorf("reload cluster TLS: %w", err)
	}
	if d.tlsFilePath != "" {
		if err := cluster.SaveFile(d.tlsFilePath, []byte(tls.ClusterCertPEM), []byte(tls.ClusterKeyPEM), []byte(tls.CACertPEM)); err != nil {
			return fmt.Errorf("save cluster TLS file: %w", err)
		}
	}
	d.logger.Info("dispatch: cluster TLS reloaded")
	return nil
}

// handleNodeConfigChange reads the current cluster node list and refreshes
// every local vault's vault-ctl Raft group desired-member set so the leader
// manager's reconcile pass can AddVoter / RemoveServer the diff.
func (d *configDispatcher) handleNodeConfigChange(ctx context.Context) error {
	nodes, err := d.cfgStore.ListNodes(ctx)
	if err != nil {
		return fmt.Errorf("list nodes for vault-ctl membership refresh: %w", err)
	}
	d.orch.RefreshVaultCtlMembers(nodes, d.factories)
	return nil
}

// ReplayConfigFromStore walks the FSM-backed config store and registers
// any vault or ingester the orchestrator is missing. Use after a fresh
// joiner has finished snapshot replication: FSM.Restore does not fire
// onApply notifications (only FSM.Apply does), so a joiner whose state
// arrived purely via snapshot would otherwise see an empty
// vault/ingester registry. See gastrolog-3hcfm.
//
// For vaults we only call the per-entity handler for those the orchestrator
// does NOT already have (a joiner whose state arrived via post-snapshot Apply
// already received the live notification). Ingesters reconcile as a whole set
// via reconcileIngesters, which is idempotent and does not flap the alive state
// of an already-running ingester (the orchestrator keeps unchanged ingester
// instances untouched), so it runs unconditionally.
//
// Routes / rotation / retention reload as a whole set per call — idempotent and
// goroutine-free — so they run unconditionally.
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
	// Replay is a reload trigger: route each side effect through settle so a
	// clean re-apply clears any standing obligation/alarm from a prior failed
	// dispatch (and a still-failing one keeps the obligation standing). After
	// a restart the obligation map starts empty, so a now-successful replay
	// simply records nothing — startup reconcile heals the divergence.
	for _, v := range vaults {
		if registeredVaults[v.ID] {
			continue
		}
		d.settle(entVault, v.ID.String(), "vault-put", d.handleVaultPut(ctx, v.ID))
	}

	d.settle(entIngester, "", "reconcile-ingesters", d.reconcileIngesters(ctx))

	d.settle(entRoute, "", "reload-filters", d.reloadFilters(ctx))
	d.settle(entRotation, "", "reload-rotation-policies", d.reloadRotationPolicies(ctx))
	d.settle(entRetention, "", "reload-retention-policies", d.reloadRetentionPolicies(ctx))
}
