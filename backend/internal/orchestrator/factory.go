package orchestrator

import (
	"context"
	"fmt"
	"gastrolog/internal/glid"
	"log/slog"
	"maps"
	"slices"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
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

// ListenAddr describes a network address that a listener ingester will bind to.
type ListenAddr struct {
	Network string // "tcp", "udp"
	Address string
}

// IngesterRegistration bundles an ingester's factory, default parameters,
// and optional connection tester into a single registration unit.
// This prevents the factory, defaults, and tester maps from diverging
// when new ingester types are added.
type IngesterRegistration struct {
	Factory     IngesterFactory
	Defaults    func() map[string]string
	Tester      ConnectionTester                            // nil if not supported
	ListenAddrs func(params map[string]string) []ListenAddr // nil for non-listeners

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

	// GroupManager, when non-nil, manages tier Raft groups for chunk metadata
	// replication. buildTierInstance creates a Raft group per tier and wires
	// a RaftAnnouncer to the chunk manager.
	GroupManager *raftgroup.GroupManager

	// NodeAddressResolver maps a node ID to its Raft server address.
	// Used to build tier Raft group membership from tier config's node assignments.
	// When nil, tier groups bootstrap as single-node (no cross-node replication).
	NodeAddressResolver func(nodeID string) (string, bool)

	// PeerConns provides cached gRPC connections to cluster peers.
	// Used by the tier apply forwarder to forward Raft applies when
	// the config placement leader is not the tier Raft leader.
	// Nil in single-node mode.
	PeerConns *cluster.PeerConns

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

	// Store the address resolver for SetDesiredTierLeader (used by dispatch).
	if factories.NodeAddressResolver != nil {
		o.nodeAddrResolver = factories.NodeAddressResolver
	}
	o.groupMgr = factories.GroupManager

	if err := o.applyVaults(sys, factories); err != nil {
		return err
	}
	// Retention and rotation are now applied per-vault inside initVault
	// via applyTierPolicies. No separate pass needed.
	if err := o.applyIngesters(sys, factories); err != nil {
		return err
	}

	// Schedule the rotation sweep so time-based policies (e.g., maxAge)
	// trigger even when no records are flowing to a vault.
	if !o.scheduler.HasJob(rotationSweepJobName) {
		if err := o.scheduler.AddJob(rotationSweepJobName, rotationSweepSchedule, o.rotationSweep); err != nil {
			o.logger.Warn("failed to add rotation sweep job", "error", err)
		}
		o.scheduler.Describe(rotationSweepJobName, "Check active chunks for time-based rotation")
	}

	return nil
}

// applyVaults creates tier instances for each vault in the config,
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
	if err := o.reloadFiltersFromRoutes(sys); err != nil {
		return err
	}
	return nil
}

// initVault creates tier instances for a single vault and registers it.
// Returns nil on success and on recoverable init failures (vault is skipped).
// Returns an error only for structural config problems.
func (o *Orchestrator) initVault(sys *system.System, vaultCfg system.VaultConfig, factories Factories) error {
	alertKey := fmt.Sprintf("vault-init:%s", vaultCfg.ID)

	tiers, err := o.buildTierInstances(sys, vaultCfg, factories)
	if err != nil {
		o.logger.Error("vault failed to initialize, skipping",
			"id", vaultCfg.ID, "name", vaultCfg.Name, "error", err)
		if o.alerts != nil {
			o.alerts.Set(alertKey, alert.Error, "orchestrator",
				fmt.Sprintf("Vault %q failed to initialize: %v", vaultCfg.Name, err))
		}
		return nil
	}

	// Register the vault even when it has zero local tiers, matching
	// AddVault's runtime behaviour (see reconfig_vaults.go:65). Tiers arrive
	// incrementally via handleTierPut, which requires the vault to already
	// be in the orchestrator so AddTierToVault can find it. On a cluster
	// restart from a snapshot no NotifyVaultPut fires for bulk-loaded state,
	// so without this initVault must do the registration itself or the
	// subsequent handleTierPut fires "vault not found" in an unrecoverable
	// loop. See gastrolog-264pk.
	vault := NewVault(vaultCfg.ID, tiers...)
	vault.Name = vaultCfg.Name
	vault.Enabled = vaultCfg.Enabled
	o.RegisterVault(vault)
	if o.alerts != nil {
		o.alerts.Clear(alertKey)
	}
	o.logger.Info("vault registered", "id", vaultCfg.ID, "name", vaultCfg.Name, "enabled", vaultCfg.Enabled)
	return nil
}

// startRetentionSweep registers the single retention sweep job that discovers
// and evaluates all tier instances each tick. No per-tier lifecycle needed.
func (o *Orchestrator) startRetentionSweep() error {
	if err := o.scheduler.AddJob(retentionJobName, defaultRetentionSchedule, o.retentionSweepAll); err != nil {
		return fmt.Errorf("retention sweep job: %w", err)
	}
	o.scheduler.Describe(retentionJobName, "Retention sweep (all tiers)")
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

	// Selected-node gate: if NodeIDs is non-empty, this node must be in it.
	if len(recvCfg.NodeIDs) > 0 && !slices.Contains(recvCfg.NodeIDs, o.localNodeID) {
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
	if cp, ok := recv.(Checkpointable); ok {
		if data := checkpoints[recvCfg.ID]; len(data) > 0 {
			if err := cp.LoadCheckpoint(data); err != nil {
				o.logger.Warn("ingester checkpoint load failed, starting fresh", "id", recvCfg.ID, "error", err)
			}
		}
	}

	o.registerIngester(recvCfg.ID, recvCfg.Name, recvCfg.Type, reg.ListenAddrs != nil, recv)
	return nil
}
