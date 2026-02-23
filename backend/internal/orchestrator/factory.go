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

	if err := o.applyStores(cfg, factories); err != nil {
		return err
	}
	if err := o.applyRetention(cfg); err != nil {
		return err
	}
	if err := o.applyIngesters(cfg, factories); err != nil {
		return err
	}

	// Schedule the rotation sweep so time-based policies (e.g., maxAge)
	// trigger even when no records are flowing to a store.
	if !o.scheduler.HasJob(rotationSweepJobName) {
		if err := o.scheduler.AddJob(rotationSweepJobName, rotationSweepSchedule, o.rotationSweep); err != nil {
			o.logger.Warn("failed to add rotation sweep job", "error", err)
		}
		o.scheduler.Describe(rotationSweepJobName, "Check active chunks for time-based rotation")
	}

	return nil
}

// applyStores creates chunk/index/query managers for each store in the config,
// compiles filters, applies rotation policies, and registers stores.
func (o *Orchestrator) applyStores(cfg *config.Config, factories Factories) error {
	storeIDs := make(map[uuid.UUID]bool)
	var compiledFilters []*CompiledFilter

	for _, storeCfg := range cfg.Stores {
		if storeIDs[storeCfg.ID] {
			return fmt.Errorf("duplicate store ID: %s", storeCfg.ID)
		}
		storeIDs[storeCfg.ID] = true

		// Resolve filter ID to expression and compile.
		var filterID uuid.UUID
		if storeCfg.Filter != nil {
			filterID = *storeCfg.Filter
		}
		filterExpr := resolveFilterExpr(cfg, filterID)
		f, err := CompileFilter(storeCfg.ID, filterExpr)
		if err != nil {
			return fmt.Errorf("invalid filter for store %s: %w", storeCfg.ID, err)
		}
		compiledFilters = append(compiledFilters, f)

		// Look up chunk manager factory.
		cmFactory, ok := factories.ChunkManagers[storeCfg.Type]
		if !ok {
			return fmt.Errorf("unknown chunk manager type: %s", storeCfg.Type)
		}

		// Create chunk manager with store-scoped logger.
		var cmLogger *slog.Logger
		if factories.Logger != nil {
			cmLogger = factories.Logger.With("store", storeCfg.ID)
		}
		// Inject _expect_existing so file stores can warn about missing directories.
		cmParams := maps.Clone(storeCfg.Params)
		if cmParams == nil {
			cmParams = make(map[string]string)
		}
		cmParams["_expect_existing"] = "true"
		cm, err := cmFactory(cmParams, cmLogger)
		if err != nil {
			return fmt.Errorf("create chunk manager %s: %w", storeCfg.ID, err)
		}

		// Apply rotation policy if specified.
		if storeCfg.Policy != nil {
			if err := o.applyRotationPolicy(cfg, storeCfg, cm); err != nil {
				return err
			}
		}

		// Look up index manager factory.
		imFactory, ok := factories.IndexManagers[storeCfg.Type]
		if !ok {
			return fmt.Errorf("unknown index manager type: %s", storeCfg.Type)
		}

		// Create index manager (needs chunk manager for reading data).
		var imLogger *slog.Logger
		if factories.Logger != nil {
			imLogger = factories.Logger.With("store", storeCfg.ID)
		}
		im, err := imFactory(storeCfg.Params, cm, imLogger)
		if err != nil {
			return fmt.Errorf("create index manager %s: %w", storeCfg.ID, err)
		}

		// Create query engine with scoped logger.
		var qeLogger *slog.Logger
		if factories.Logger != nil {
			qeLogger = factories.Logger.With("store", storeCfg.ID)
		}
		qe := query.New(cm, im, qeLogger)

		// Register store.
		store := NewStore(storeCfg.ID, cm, im, qe)
		store.Enabled = storeCfg.Enabled
		o.RegisterStore(store)
	}

	// Set filter set if any filters were compiled.
	if len(compiledFilters) > 0 {
		o.SetFilterSet(NewFilterSet(compiledFilters))
	}

	return nil
}

// applyRotationPolicy resolves and applies a rotation policy (and optional cron schedule)
// to a chunk manager.
func (o *Orchestrator) applyRotationPolicy(cfg *config.Config, storeCfg config.StoreConfig, cm chunk.ChunkManager) error {
	policyCfg := findRotationPolicy(cfg.RotationPolicies, *storeCfg.Policy)
	if policyCfg == nil {
		return fmt.Errorf("store %s references unknown policy: %s", storeCfg.ID, *storeCfg.Policy)
	}
	policy, err := policyCfg.ToRotationPolicy()
	if err != nil {
		return fmt.Errorf("invalid policy %s for store %s: %w", *storeCfg.Policy, storeCfg.ID, err)
	}
	if policy != nil {
		cm.SetRotationPolicy(policy)
	}

	// Set up cron rotation if configured.
	if policyCfg.Cron != nil && *policyCfg.Cron != "" {
		if err := o.cronRotation.addJob(storeCfg.ID, storeCfg.Name, *policyCfg.Cron, cm); err != nil {
			return fmt.Errorf("cron rotation for store %s: %w", storeCfg.ID, err)
		}
	}

	return nil
}

// applyRetention sets up retention jobs for stores that have retention rules.
func (o *Orchestrator) applyRetention(cfg *config.Config) error {
	for _, storeCfg := range cfg.Stores {
		if len(storeCfg.RetentionRules) == 0 {
			continue
		}

		store := o.stores[storeCfg.ID]
		if store == nil {
			continue
		}

		rules, err := resolveRetentionRules(cfg, storeCfg)
		if err != nil {
			return err
		}
		if len(rules) == 0 {
			continue
		}

		runner := &retentionRunner{
			storeID:  storeCfg.ID,
			cm:       store.Chunks,
			im:       store.Indexes,
			rules: rules,
			orch:     o,
			now:      o.now,
			logger:   o.logger,
		}
		o.retention[storeCfg.ID] = runner
		if err := o.scheduler.AddJob(retentionJobName(storeCfg.ID), defaultRetentionSchedule, runner.sweep); err != nil {
			return fmt.Errorf("retention job for store %s: %w", storeCfg.ID, err)
		}
		o.scheduler.Describe(retentionJobName(storeCfg.ID), fmt.Sprintf("Retention sweep for '%s'", storeCfg.Name))
	}

	return nil
}

// resolveRetentionRules converts config rules to resolved retentionRule objects.
func resolveRetentionRules(cfg *config.Config, storeCfg config.StoreConfig) ([]retentionRule, error) {
	var rules []retentionRule
	for _, b := range storeCfg.RetentionRules {
		retCfg := findRetentionPolicy(cfg.RetentionPolicies, b.RetentionPolicyID)
		if retCfg == nil {
			return nil, fmt.Errorf("store %s references unknown retention policy: %s", storeCfg.ID, b.RetentionPolicyID)
		}
		policy, err := retCfg.ToRetentionPolicy()
		if err != nil {
			return nil, fmt.Errorf("invalid retention policy %s for store %s: %w", b.RetentionPolicyID, storeCfg.ID, err)
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

		o.RegisterIngester(recvCfg.ID, recv)
	}

	return nil
}
