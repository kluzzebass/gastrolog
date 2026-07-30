package orchestrator

import (
	"context"
	"fmt"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/ingestion"
	"log/slog"
	"maps"
	"slices"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/home"
	"gastrolog/internal/index"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/system"
)

// Factories holds factory functions for creating components from configuration.
// The orchestrator uses these to instantiate components without knowing
// about concrete implementation types.
//
// Factory maps are keyed by type name (e.g., "file", "memory", "syslog-udp").
// The caller (typically main or a bootstrap package) populates these maps
// by importing concrete implementation packages and calling their NewFactory()
// functions.
//
// Logging:
//   - Logger is passed to factories that support it
//   - Factories create child loggers scoped to their component
//   - If Logger is nil, components use discard loggers
//
// ConnectionTester validates connectivity for an ingester configuration
// without saving or starting it. Returns a human-readable success message
// or an error describing the failure.
type ConnectionTester func(ctx context.Context, params map[string]string) (string, error)

// IngesterRegistration bundles an ingester's factory, default parameters,
// and optional connection tester into a single registration unit.
// This prevents the factory, defaults, and tester maps from diverging
// when new ingester types are added.
type IngesterRegistration struct {
	Factory     ingestion.IngesterFactory
	Defaults    func() map[string]string
	Tester      ConnectionTester                                      // nil if not supported
	ListenAddrs func(params map[string]string) []ingestion.ListenAddr // nil for non-listeners

	// SingletonSupported indicates whether it is meaningful to run this
	// ingester type in singleton (Raft-assigned, one-node) mode. When false,
	// the per-instance IngesterConfig.Singleton flag is ignored — the ingester
	// always runs in parallel on every node in NodeIDs. Set this to false for
	// per-node-local data sources (docker, self, tail, metrics) and for
	// listeners (OS-level port coordination handles the singleton case).
	SingletonSupported bool
}

type Factories struct {
	IngesterTypes map[string]IngesterRegistration
	ChunkManagers map[string]chunk.ManagerFactory
	IndexManagers map[string]index.ManagerFactory

	// Logger is the base logger passed to component factories.
	// Components derive child loggers with their own scope.
	// If nil, components use discard loggers.
	Logger *slog.Logger

	// HomeDir is the gastrolog home directory. When non-empty, it is injected as
	// the "_state_dir" param so that ingesters can persist state (e.g. bookmarks).
	HomeDir string

	// VaultsDir overrides the base directory for vault storage. When non-empty,
	// relative vault paths are resolved against this directory instead of HomeDir.
	// Defaults to HomeDir if not set.
	VaultsDir string

	// GroupManager, when non-nil, manages vault-ctl Raft groups for chunk metadata
	// replication. buildInstance creates a Raft group per instance and wires
	// a RaftAnnouncer to the chunk manager.
	GroupManager *raftgroup.GroupManager

	// NodeAddressResolver maps a node ID to its Raft server address.
	// Used to build vault-ctl Raft group membership from instance config's node assignments.
	// When nil, instance groups bootstrap as single-node (no cross-node replication).
	NodeAddressResolver func(nodeID string) (string, bool)

	// PeerConns provides cached gRPC connections to cluster peers.
	// Used by the instance apply forwarder to forward Raft applies when
	// the config placement leader is not the vault-ctl Raft leader.
	// Nil in single-node mode.
	PeerConns *cluster.PeerConnManager

	// Note: No QueryEngineFactory is needed because QueryEngine construction
	// is trivial and uniform (query.New(cm, im, logger)). If QueryEngine ever
	// requires configuration, add a factory here.
}

// ApplyConfig creates and registers components based on the provided configuration.
// It uses the factory maps to look up the appropriate factory for each component type.
//
// Atomicity: ApplyConfig is NOT atomic. On error, some components may have
// been constructed and registered while others were not. Callers must discard
// the orchestrator on error and create a fresh one. Do not attempt to recover
// or retry with the same orchestrator instance.
func (o *Orchestrator) ApplyConfig(sys *system.System, factories Factories) error {
	if sys == nil {
		return nil
	}

	o.groupMgr = factories.GroupManager
	o.peerConns = factories.PeerConns
	if factories.PeerConns != nil && o.segmentPuller == nil {
		o.segmentPuller = cluster.NewSegmentPuller(factories.PeerConns)
		o.chunkGLCBPuller = cluster.NewChunkGLCBPuller(factories.PeerConns)
	}

	// Root the per-vault segment areas under the node home unless already
	// configured. Origin vaults are registered during applyVaults→route reload,
	// so this must be set first. See originRoot.
	if o.segmentsDir == "" && factories.HomeDir != "" {
		o.homeDir = factories.HomeDir
		o.segmentsDir = home.New(factories.HomeDir).SegmentsDir()
	} else if factories.HomeDir != "" {
		o.homeDir = factories.HomeDir
	}
	// Cache vaultsDir the same way resolveVaultDir resolves it at each call
	// site: VaultsDir when the operator set one, else HomeDir. Periodic
	// passes with no per-call Factories (refreshStorageGuards) read this
	// cached value instead of re-deriving it; re-deriving it there was a real
	// regression when the periodic guard refresh landed.
	if factories.VaultsDir != "" {
		o.vaultsDir = factories.VaultsDir
	} else if factories.HomeDir != "" {
		o.vaultsDir = factories.HomeDir
	}

	if err := o.applyVaults(sys, factories); err != nil {
		return err
	}
	// Retention and rotation are now applied per-vault inside initVault
	// via applyVaultPolicies. No separate pass needed.
	if err := o.applyIngesters(sys, factories); err != nil {
		return err
	}

	// The placement ROLE / FollowerTargets refresh legs of the retired 15s
	// placement sweep are now event-driven (see ReconcileVaultPlacement /
	// ReconcilePlacements). The routing-table + pipeline-registration leg
	// (reloadPipelineFromConfig) is NOT: it also aligns each vault-ctl Raft
	// group's desired leader with the placement leader (SetDesiredLeaderID)
	// and re-registers the pipeline vault as the vault-ctl handle/leadership
	// converges — async Raft convergence with no config event to hang off.
	// Without a periodic pass, a vault-ctl election that lands leadership on a
	// non-home node leaves the chunking planner (home ∧ vault-ctl leader)
	// running nowhere, stalling manifest planning until the next unrelated
	// config change. Keep that one leg on a narrowed scheduler job — narrowing
	// it away regressed once, with a home node down — it is the sibling of
	// vault-ctl-membership-reconcile, not the retired placement sweep.
	if err := o.startPipelineConfigReconcile(); err != nil {
		o.logger.Warn("failed to add pipeline-config-reconcile job", "error", err)
	}

	return nil
}

// applyVaults creates vault instances for each vault in the config,
// compiles filters, and registers vaults.
func (o *Orchestrator) applyVaults(sys *system.System, factories Factories) error {
	cfg := &sys.Config
	vaultIDs := make(map[glid.GLID]bool)

	for _, vaultCfg := range cfg.Vaults {
		if vaultIDs[vaultCfg.ID] {
			return fmt.Errorf("duplicate vault ID: %s", vaultCfg.ID)
		}
		vaultIDs[vaultCfg.ID] = true

		if err := o.initVault(sys, vaultCfg, factories); err != nil {
			return err
		}
	}

	// Compile filters at startup so vaults can receive records immediately.
	// The rotation sweep also reconciles every 15s as a safety net.
	//
	// Under o.mu like every other reloadRoutesFromConfig caller: startup is
	// NOT single-threaded — raft config replay drives the dispatcher
	// (handleInstancePut → ReloadFilters, locked) concurrently with
	// ApplyConfig, and the unlocked reload here raced it into a fatal
	// concurrent map write on o.pipelineVaults that crashed a node during a
	// rolling restart with live placement changes.
	o.mu.Lock()
	err := o.reloadRoutesFromConfig(sys)
	o.mu.Unlock()
	if err != nil {
		return err
	}
	return nil
}

// initVault creates vault instances for a single vault and registers it.
// Returns nil on success and on recoverable init failures (vault is skipped).
// Returns an error only for structural config problems.
func (o *Orchestrator) initVault(sys *system.System, vaultCfg system.VaultConfig, factories Factories) error {
	instance, err := o.buildVaultInstance(sys, vaultCfg, factories)
	if err != nil {
		o.logger.Error("vault failed to initialize, skipping",
			"id", vaultCfg.ID, "name", vaultCfg.Name, "error", err)
		if o.alerts != nil {
			o.alerts.Raise("vault-init", vaultCfg.ID.String(),
				fmt.Sprintf("Vault %q failed to initialize: %v", vaultCfg.Name, err))
		}
		return nil
	}

	// Register the vault even when no instance is built locally (this node
	// has no placement). Matches AddVault's runtime behaviour: the vault
	// shell is registered so a later placement-driven AddVaultInstance can
	// hydrate it. On cluster snapshot restore there's no NotifyVaultPut for
	// bulk-loaded state, so initVault must register here or subsequent
	// AddVaultInstance fires "vault not found" in a loop.
	vault := NewVault(vaultCfg.ID, instance)
	vault.Name = vaultCfg.Name
	vault.Enabled = vaultCfg.Enabled
	vault.StorageType = string(vaultCfg.Type)
	o.RegisterVault(vault)
	if o.alerts != nil {
		o.alerts.Clear("vault-init", vaultCfg.ID.String())
	}
	o.logger.Info("vault registered", "id", vaultCfg.ID, "name", vaultCfg.Name, "enabled", vaultCfg.Enabled)
	return nil
}

// startRetentionSweep registers the single retention sweep job that discovers
// and evaluates all vault instances each tick. No per-vault lifecycle needed.
func (o *Orchestrator) startRetentionSweep() error {
	if err := o.scheduler.AddJob(retentionJobName, sweepCron(defaultRetentionSchedule), o.retentionSweepAll); err != nil {
		return fmt.Errorf("retention sweep job: %w", err)
	}
	o.scheduler.Describe(retentionJobName, "Retention sweep (all vaults)")
	return nil
}

// startInstanceCatchupSweep registers the periodic per-node reconcile
// BACKSTOP that guarantees local-state convergence on every vault
// instance. Every 20 seconds (cron 13/33/53s, phase-offset from the
// retention sweep at second 0) every node walks its OWN vault-ctl FSM and
// runs the reconcile categories — see vaultCatchupSweepAll for the
// per-category invariants. The primary convergence is event-driven (delete
// obligations on CmdRequestDelete, orphans on snapshot install, leader-only
// categories on lead-gained, and stale pending-delete acks additionally on a
// placement move under a stable leader); this tick is the safety net for
// dropped/raced events plus the two categories that are periodic-by-nature
// (idle-active inactivity, grace-period GCs).
func (o *Orchestrator) startInstanceCatchupSweep() error {
	if err := o.scheduler.AddJob(vaultCatchupSweepJobName, sweepCron(vaultCatchupSweepSchedule), o.vaultCatchupSweepAll); err != nil {
		return fmt.Errorf("vault catchup sweep job: %w", err)
	}
	o.scheduler.Describe(vaultCatchupSweepJobName, "Vault reconcile backstop (event-driven primary; catches missed delete/orphan/replica events + periodic idle-active & grace GCs)")
	return nil
}

// applyIngesters creates and registers ingesters from the system.
func (o *Orchestrator) applyIngesters(sys *system.System, factories Factories) error {
	cfg := &sys.Config
	ingesterIDs := make(map[glid.GLID]bool)

	for _, recvCfg := range cfg.Ingesters {
		if ingesterIDs[recvCfg.ID] {
			return fmt.Errorf("duplicate ingester ID: %s", recvCfg.ID)
		}
		ingesterIDs[recvCfg.ID] = true

		if !recvCfg.Enabled {
			continue
		}
		if err := o.applyIngester(recvCfg, sys.Runtime.IngesterAssignment, sys.Runtime.IngesterCheckpoints, factories); err != nil {
			return err
		}
	}

	return nil
}

// applyIngester creates and registers a single ingester if it should run on this node.
func (o *Orchestrator) applyIngester(recvCfg system.IngesterConfig, assignments map[glid.GLID]string, checkpoints map[glid.GLID][]byte, factories Factories) error {
	reg, ok := factories.IngesterTypes[recvCfg.Type]
	if !ok {
		return fmt.Errorf("unknown ingester type: %s", recvCfg.Type)
	}

	// Selected-node gate: NodeIDs only restricts placement when AllNodes is
	// false. AllNodes=true means every cluster node is eligible regardless of
	// the (legacy) NodeIDs list. Mirrors shouldRunIngester in app/dispatch.go;
	// without the AllNodes short-circuit, cold restart only starts the
	// ingester on whichever node happens to be in NodeIDs.
	if !recvCfg.AllNodes && len(recvCfg.NodeIDs) > 0 && !slices.Contains(recvCfg.NodeIDs, o.localNodeID) {
		return nil
	}

	// Singleton gate: only applies when the type supports singleton mode
	// and the instance is configured for it. Everything else is parallel —
	// runs on every selected node with no central coordination.
	isSingleton := reg.SingletonSupported && recvCfg.Singleton
	if isSingleton {
		// Raft-assigned singleton. Empty assignment = placement manager hasn't
		// run yet — allow local start; it'll be narrowed down on the next
		// reconcile via NotifyIngesterAssignmentSet.
		assigned := assignments[recvCfg.ID]
		if assigned != "" && assigned != o.localNodeID {
			return nil
		}
	}

	params := maps.Clone(recvCfg.Params)
	if params == nil {
		params = make(map[string]string)
	}
	if factories.HomeDir != "" {
		params["_state_dir"] = factories.HomeDir
	}

	var recvLogger *slog.Logger
	if factories.Logger != nil {
		recvLogger = factories.Logger.With("ingester_id", recvCfg.ID)
	}
	recv, err := reg.Factory(recvCfg.ID, params, recvLogger)
	if err != nil {
		return fmt.Errorf("create ingester %s: %w", recvCfg.ID, err)
	}

	// Restore checkpoint if available (active ingesters resuming after failover).
	if cp, ok := recv.(ingestion.Checkpointable); ok {
		if data := checkpoints[recvCfg.ID]; len(data) > 0 {
			if err := cp.LoadCheckpoint(data); err != nil {
				o.logger.Warn("ingester checkpoint load failed, starting fresh", "id", recvCfg.ID, "error", err)
			}
		}
	}

	o.registerIngester(recvCfg.ID, recvCfg.Name, recvCfg.Type, reg.ListenAddrs != nil, recv)
	return nil
}
