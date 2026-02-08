package orchestrator

import (
	"fmt"
	"log/slog"

	"gastrolog/internal/chunk"
	"gastrolog/internal/config"
	"gastrolog/internal/index"
	"gastrolog/internal/query"
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
	Ingesters     map[string]IngesterFactory
	ChunkManagers map[string]chunk.ManagerFactory
	IndexManagers map[string]index.ManagerFactory

	// Logger is the base logger passed to component factories.
	// Components derive child loggers with their own scope.
	// If nil, components use discard loggers.
	Logger *slog.Logger

	// Note: No QueryEngineFactory is needed because QueryEngine construction
	// is trivial and uniform (query.New(cm, im, logger)). If QueryEngine ever
	// requires configuration, add a factory here.
}

// ApplyConfig creates and registers components based on the provided configuration.
// It uses the factory maps to look up the appropriate factory for each component type.
//
// For each store in the config:
//   - Creates a ChunkManager using the matching factory
//   - Creates an IndexManager using the matching factory (same type as ChunkManager)
//   - Creates a QueryEngine wiring the ChunkManager and IndexManager
//   - Registers all three under the store's ID
//
// For each ingester in the config:
//   - Creates a Ingester using the matching factory
//   - Registers it under the ingester's ID
//
// Returns an error if:
//   - A required factory is not found for a given type
//   - A factory returns an error during construction
//   - Duplicate IDs are encountered
//
// Atomicity: ApplyConfig is NOT atomic. On error, some components may have
// been constructed and registered while others were not. Callers must discard
// the orchestrator on error and create a fresh one. Do not attempt to recover
// or retry with the same orchestrator instance.
func (o *Orchestrator) ApplyConfig(cfg *config.Config, factories Factories) error {
	if cfg == nil {
		return nil
	}

	// Track IDs to detect duplicates.
	storeIDs := make(map[string]bool)
	ingesterIDs := make(map[string]bool)

	// Compile filters and create stores (chunk manager + index manager + query engine).
	var compiledFilters []*CompiledFilter

	for _, storeCfg := range cfg.Stores {
		if storeIDs[storeCfg.ID] {
			return fmt.Errorf("duplicate store ID: %s", storeCfg.ID)
		}
		storeIDs[storeCfg.ID] = true

		// Resolve filter ID to expression and compile.
		var filterID string
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

		// Create chunk manager.
		cm, err := cmFactory(storeCfg.Params)
		if err != nil {
			return fmt.Errorf("create chunk manager %s: %w", storeCfg.ID, err)
		}

		// Apply rotation policy if specified.
		if storeCfg.Policy != nil {
			policyCfg, ok := cfg.RotationPolicies[*storeCfg.Policy]
			if !ok {
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
				if err := o.cronRotation.addJob(storeCfg.ID, *policyCfg.Cron, cm); err != nil {
					return fmt.Errorf("cron rotation for store %s: %w", storeCfg.ID, err)
				}
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

		// Register all components.
		o.RegisterChunkManager(storeCfg.ID, cm)
		o.RegisterIndexManager(storeCfg.ID, im)
		o.RegisterQueryEngine(storeCfg.ID, qe)
	}

	// Set filter set if any filters were compiled.
	if len(compiledFilters) > 0 {
		o.SetFilterSet(NewFilterSet(compiledFilters))
	}

	// Set up retention runners for stores with retention policies.
	for _, storeCfg := range cfg.Stores {
		cm, ok := o.chunks[storeCfg.ID]
		if !ok {
			continue
		}
		im, ok := o.indexes[storeCfg.ID]
		if !ok {
			continue
		}

		var policy chunk.RetentionPolicy

		if storeCfg.Retention != nil {
			retCfg, ok := cfg.RetentionPolicies[*storeCfg.Retention]
			if !ok {
				return fmt.Errorf("store %s references unknown retention policy: %s", storeCfg.ID, *storeCfg.Retention)
			}
			p, err := retCfg.ToRetentionPolicy()
			if err != nil {
				return fmt.Errorf("invalid retention policy %s for store %s: %w", *storeCfg.Retention, storeCfg.ID, err)
			}
			policy = p
		}

		if policy != nil {
			runner := &retentionRunner{
				storeID:  storeCfg.ID,
				cm:       cm,
				im:       im,
				policy:   policy,
				interval: defaultRetentionInterval,
				now:      o.now,
				logger:   o.logger,
			}
			o.retention[storeCfg.ID] = runner
		}
	}

	// Create ingesters.
	for _, recvCfg := range cfg.Ingesters {
		if ingesterIDs[recvCfg.ID] {
			return fmt.Errorf("duplicate ingester ID: %s", recvCfg.ID)
		}
		ingesterIDs[recvCfg.ID] = true

		// Look up ingester factory.
		recvFactory, ok := factories.Ingesters[recvCfg.Type]
		if !ok {
			return fmt.Errorf("unknown ingester type: %s", recvCfg.Type)
		}

		// Create ingester with scoped logger.
		var recvLogger *slog.Logger
		if factories.Logger != nil {
			recvLogger = factories.Logger.With("ingester_id", recvCfg.ID)
		}
		recv, err := recvFactory(recvCfg.ID, recvCfg.Params, recvLogger)
		if err != nil {
			return fmt.Errorf("create ingester %s: %w", recvCfg.ID, err)
		}

		o.RegisterIngester(recvCfg.ID, recv)
	}

	return nil
}
