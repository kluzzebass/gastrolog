package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"maps"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/config"
	"gastrolog/internal/index"
	"gastrolog/internal/multiraft"

	"github.com/google/uuid"
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
	Tester      ConnectionTester                      // nil if not supported
	ListenAddrs func(params map[string]string) []ListenAddr // nil for non-listeners
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
	GroupManager *multiraft.GroupManager

	// NodeAddressResolver maps a node ID to its Raft server address.
	// Used to build tier Raft group membership from tier config's node assignments.
	// When nil, tier groups bootstrap as single-node (no cross-node replication).
	NodeAddressResolver func(nodeID string) (string, bool)

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
func (o *Orchestrator) ApplyConfig(cfg *config.Config, factories Factories) error {
	if cfg == nil {
		return nil
	}

	if err := o.applyVaults(cfg, factories); err != nil {
		return err
	}
	if err := o.applyRetention(cfg); err != nil {
		return err
	}
	if err := o.applyIngesters(cfg, factories); err != nil {
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
func (o *Orchestrator) applyVaults(cfg *config.Config, factories Factories) error {
	vaultIDs := make(map[uuid.UUID]bool)

	for _, vaultCfg := range cfg.Vaults {
		if vaultIDs[vaultCfg.ID] {
			return fmt.Errorf("duplicate vault ID: %s", vaultCfg.ID)
		}
		vaultIDs[vaultCfg.ID] = true

		if err := o.initVault(cfg, vaultCfg, factories); err != nil {
			return err
		}
	}

	// Build filter set from routes.
	if err := o.reloadFiltersFromRoutes(cfg); err != nil {
		return err
	}

	return nil
}

// initVault creates tier instances for a single vault and registers it.
// Returns nil on success and on recoverable init failures (vault is skipped).
// Returns an error only for structural config problems.
func (o *Orchestrator) initVault(cfg *config.Config, vaultCfg config.VaultConfig, factories Factories) error {
	alertKey := fmt.Sprintf("vault-init:%s", vaultCfg.ID)

	if len(vaultCfg.TierIDs) == 0 {
		o.logger.Warn("vault has no tier IDs, skipping", "id", vaultCfg.ID, "name", vaultCfg.Name)
		return nil
	}

	tiers, err := o.buildTierInstances(cfg, vaultCfg, factories)
	if err != nil {
		o.logger.Error("vault failed to initialize, skipping",
			"id", vaultCfg.ID, "name", vaultCfg.Name, "error", err)
		if o.alerts != nil {
			o.alerts.Set(alertKey, alert.Error, "orchestrator",
				fmt.Sprintf("Vault %q failed to initialize: %v", vaultCfg.Name, err))
		}
		return nil
	}

	// temporary: if all tiers are assigned to other nodes, skip this vault locally (until tier election)
	if len(tiers) == 0 {
		o.logger.Info("vault has no local tiers, skipping", "id", vaultCfg.ID, "name", vaultCfg.Name)
		return nil
	}

	vault := NewVault(vaultCfg.ID, tiers...)
	vault.Name = vaultCfg.Name
	vault.Enabled = vaultCfg.Enabled
	o.RegisterVault(vault)
	o.applyTierPolicies(cfg, vaultCfg, vault)
	if o.alerts != nil {
		o.alerts.Clear(alertKey)
	}
	o.logger.Info("vault registered", "id", vaultCfg.ID, "name", vaultCfg.Name, "enabled", vaultCfg.Enabled)
	return nil
}

// applyRetention sets up retention jobs for all tiers that have retention rules.
func (o *Orchestrator) applyRetention(cfg *config.Config) error {
	for _, vaultCfg := range cfg.Vaults {
		vault := o.vaults[vaultCfg.ID]
		if vault == nil {
			continue
		}

		for _, tier := range vault.Tiers {
			if tier.IsSecondary {
				continue
			}
			tierCfg := findTierConfig(cfg.Tiers, tier.TierID)
			if tierCfg == nil || len(tierCfg.RetentionRules) == 0 {
				continue
			}

			rules, err := resolveRetentionRulesFromTier(cfg, vaultCfg, tierCfg)
			if err != nil {
				return err
			}
			if len(rules) == 0 {
				continue
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
				return fmt.Errorf("retention job for vault %s tier %s: %w", vaultCfg.ID, tier.TierID, err)
			}
			o.scheduler.Describe(jobName, fmt.Sprintf("Retention sweep for '%s'", vaultCfg.Name))
		}
	}

	return nil
}

// applyIngesters creates and registers ingesters from the config.
func (o *Orchestrator) applyIngesters(cfg *config.Config, factories Factories) error {
	ingesterIDs := make(map[uuid.UUID]bool)

	for _, recvCfg := range cfg.Ingesters {
		if ingesterIDs[recvCfg.ID] {
			return fmt.Errorf("duplicate ingester ID: %s", recvCfg.ID)
		}
		ingesterIDs[recvCfg.ID] = true

		// Skip ingesters belonging to another node.
		if recvCfg.NodeID != "" && recvCfg.NodeID != o.localNodeID {
			continue
		}

		if !recvCfg.Enabled {
			continue
		}

		// Look up ingester factory.
		reg, ok := factories.IngesterTypes[recvCfg.Type]
		if !ok {
			return fmt.Errorf("unknown ingester type: %s", recvCfg.Type)
		}

		// Inject _state_dir so ingesters can persist state.
		params := maps.Clone(recvCfg.Params)
		if params == nil {
			params = make(map[string]string)
		}
		if factories.HomeDir != "" {
			params["_state_dir"] = factories.HomeDir
		}

		// Create ingester with scoped logger.
		var recvLogger *slog.Logger
		if factories.Logger != nil {
			recvLogger = factories.Logger.With("ingester_id", recvCfg.ID)
		}
		recv, err := reg.Factory(recvCfg.ID, params, recvLogger)
		if err != nil {
			return fmt.Errorf("create ingester %s: %w", recvCfg.ID, err)
		}

		o.RegisterIngester(recvCfg.ID, recvCfg.Name, recvCfg.Type, recv)
	}

	return nil
}
