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

	// Compile routes and create stores (chunk manager + index manager + query engine).
	var compiledRoutes []*CompiledRoute

	for _, storeCfg := range cfg.Stores {
		if storeIDs[storeCfg.ID] {
			return fmt.Errorf("duplicate store ID: %s", storeCfg.ID)
		}
		storeIDs[storeCfg.ID] = true

		// Compile route expression.
		route, err := CompileRoute(storeCfg.ID, storeCfg.Route)
		if err != nil {
			return fmt.Errorf("invalid route for store %s: %w", storeCfg.ID, err)
		}
		compiledRoutes = append(compiledRoutes, route)

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
		if storeCfg.Policy != "" {
			policyCfg, ok := cfg.RotationPolicies[storeCfg.Policy]
			if !ok {
				return fmt.Errorf("store %s references unknown policy: %s", storeCfg.ID, storeCfg.Policy)
			}
			policy, err := policyCfg.ToRotationPolicy()
			if err != nil {
				return fmt.Errorf("invalid policy %s for store %s: %w", storeCfg.Policy, storeCfg.ID, err)
			}
			if policy != nil {
				cm.SetRotationPolicy(policy)
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

	// Set router if any routes were compiled.
	if len(compiledRoutes) > 0 {
		o.SetRouter(NewRouter(compiledRoutes))
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
		recv, err := recvFactory(recvCfg.Params, recvLogger)
		if err != nil {
			return fmt.Errorf("create ingester %s: %w", recvCfg.ID, err)
		}

		o.RegisterIngester(recvCfg.ID, recv)
	}

	return nil
}
