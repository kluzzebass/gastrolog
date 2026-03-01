package orchestrator

import (
	"fmt"
	"log/slog"
	"maps"

	"gastrolog/internal/chunk"
	"gastrolog/internal/config"
	"gastrolog/internal/index"
	"gastrolog/internal/query"

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
type Factories struct {
	Ingesters        map[string]IngesterFactory
	IngesterDefaults map[string]func() map[string]string
	ChunkManagers    map[string]chunk.ManagerFactory
	IndexManagers    map[string]index.ManagerFactory

	// Logger is the base logger passed to component factories.
	// Components derive child loggers with their own scope.
	// If nil, components use discard loggers.
	Logger *slog.Logger

	// HomeDir is the gastrolog home directory. When non-empty, it is injected as
	// the "_state_dir" param so that ingesters can persist state (e.g. bookmarks).
	HomeDir string

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

// applyVaults creates chunk/index/query managers for each vault in the config,
// compiles filters, applies rotation policies, and registers vaults.
func (o *Orchestrator) applyVaults(cfg *config.Config, factories Factories) error {
	vaultIDs := make(map[uuid.UUID]bool)

	for _, vaultCfg := range cfg.Vaults {
		if vaultIDs[vaultCfg.ID] {
			return fmt.Errorf("duplicate vault ID: %s", vaultCfg.ID)
		}
		vaultIDs[vaultCfg.ID] = true

		// Skip vaults belonging to another node.
		if vaultCfg.NodeID != "" && vaultCfg.NodeID != o.localNodeID {
			continue
		}

		// Routes are compiled separately — vault-level filter was removed.
		// Legacy: compile a catch-all for vaults not covered by any route,
		// or leave the filter set to be built from routes below.

		// Look up chunk manager factory.
		cmFactory, ok := factories.ChunkManagers[vaultCfg.Type]
		if !ok {
			return fmt.Errorf("unknown chunk manager type: %s", vaultCfg.Type)
		}

		// Create chunk manager with vault-scoped logger.
		var cmLogger *slog.Logger
		if factories.Logger != nil {
			cmLogger = factories.Logger.With("vault", vaultCfg.ID)
		}
		// Inject _expect_existing so file vaults can warn about missing directories.
		cmParams := maps.Clone(vaultCfg.Params)
		if cmParams == nil {
			cmParams = make(map[string]string)
		}
		cmParams["_expect_existing"] = "true"
		cm, err := cmFactory(cmParams, cmLogger)
		if err != nil {
			return fmt.Errorf("create chunk manager %s: %w", vaultCfg.ID, err)
		}

		// Apply rotation policy if specified.
		if vaultCfg.Policy != nil {
			if err := o.applyRotationPolicy(cfg, vaultCfg, cm); err != nil {
				return err
			}
		}

		// Look up index manager factory.
		imFactory, ok := factories.IndexManagers[vaultCfg.Type]
		if !ok {
			return fmt.Errorf("unknown index manager type: %s", vaultCfg.Type)
		}

		// Create index manager (needs chunk manager for reading data).
		var imLogger *slog.Logger
		if factories.Logger != nil {
			imLogger = factories.Logger.With("vault", vaultCfg.ID)
		}
		im, err := imFactory(vaultCfg.Params, cm, imLogger)
		if err != nil {
			return fmt.Errorf("create index manager %s: %w", vaultCfg.ID, err)
		}

		// Create query engine with scoped logger.
		var qeLogger *slog.Logger
		if factories.Logger != nil {
			qeLogger = factories.Logger.With("vault", vaultCfg.ID)
		}
		qe := query.New(cm, im, qeLogger)

		// Register vault.
		vault := NewVault(vaultCfg.ID, cm, im, qe)
		vault.Name = vaultCfg.Name
		vault.Type = vaultCfg.Type
		vault.Enabled = vaultCfg.Enabled
		o.RegisterVault(vault)
	}

	// Build filter set from routes.
	if err := o.reloadFiltersFromRoutes(cfg); err != nil {
		return err
	}

	return nil
}

// applyRotationPolicy resolves and applies a rotation policy (and optional cron schedule)
// to a chunk manager.
func (o *Orchestrator) applyRotationPolicy(cfg *config.Config, vaultCfg config.VaultConfig, cm chunk.ChunkManager) error {
	policyCfg := findRotationPolicy(cfg.RotationPolicies, *vaultCfg.Policy)
	if policyCfg == nil {
		return fmt.Errorf("vault %s references unknown policy: %s", vaultCfg.ID, *vaultCfg.Policy)
	}
	policy, err := policyCfg.ToRotationPolicy()
	if err != nil {
		return fmt.Errorf("invalid policy %s for vault %s: %w", *vaultCfg.Policy, vaultCfg.ID, err)
	}
	if policy != nil {
		cm.SetRotationPolicy(policy)
	}

	// Set up cron rotation if configured.
	if policyCfg.Cron != nil && *policyCfg.Cron != "" {
		if err := o.cronRotation.addJob(vaultCfg.ID, vaultCfg.Name, *policyCfg.Cron, cm); err != nil {
			return fmt.Errorf("cron rotation for vault %s: %w", vaultCfg.ID, err)
		}
	}

	return nil
}

// applyRetention sets up retention jobs for vaults that have retention rules.
func (o *Orchestrator) applyRetention(cfg *config.Config) error {
	for _, vaultCfg := range cfg.Vaults {
		if len(vaultCfg.RetentionRules) == 0 {
			continue
		}

		vault := o.vaults[vaultCfg.ID]
		if vault == nil {
			continue
		}

		rules, err := resolveRetentionRules(cfg, vaultCfg)
		if err != nil {
			return err
		}
		if len(rules) == 0 {
			continue
		}

		runner := &retentionRunner{
			vaultID:  vaultCfg.ID,
			cm:       vault.Chunks,
			im:       vault.Indexes,
			rules: rules,
			orch:     o,
			now:      o.now,
			logger:   o.logger,
		}
		o.retention[vaultCfg.ID] = runner
		if err := o.scheduler.AddJob(retentionJobName(vaultCfg.ID), defaultRetentionSchedule, runner.sweep); err != nil {
			return fmt.Errorf("retention job for vault %s: %w", vaultCfg.ID, err)
		}
		o.scheduler.Describe(retentionJobName(vaultCfg.ID), fmt.Sprintf("Retention sweep for '%s'", vaultCfg.Name))
	}

	return nil
}

// resolveRetentionRules converts config rules to resolved retentionRule objects.
func resolveRetentionRules(cfg *config.Config, vaultCfg config.VaultConfig) ([]retentionRule, error) {
	var rules []retentionRule
	for _, b := range vaultCfg.RetentionRules {
		retCfg := findRetentionPolicy(cfg.RetentionPolicies, b.RetentionPolicyID)
		if retCfg == nil {
			return nil, fmt.Errorf("vault %s references unknown retention policy: %s", vaultCfg.ID, b.RetentionPolicyID)
		}
		policy, err := retCfg.ToRetentionPolicy()
		if err != nil {
			return nil, fmt.Errorf("invalid retention policy %s for vault %s: %w", b.RetentionPolicyID, vaultCfg.ID, err)
		}
		if policy == nil {
			continue
		}
		rb := retentionRule{
			policy: policy,
			action: b.Action,
		}
		if b.Destination != nil {
			rb.destination = *b.Destination
		}
		rules = append(rules, rb)
	}
	return rules, nil
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
		recvFactory, ok := factories.Ingesters[recvCfg.Type]
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
		recv, err := recvFactory(recvCfg.ID, params, recvLogger)
		if err != nil {
			return fmt.Errorf("create ingester %s: %w", recvCfg.ID, err)
		}

		o.RegisterIngester(recvCfg.ID, recvCfg.Name, recvCfg.Type, recv)
	}

	return nil
}
