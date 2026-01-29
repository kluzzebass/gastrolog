package orchestrator

import (
	"fmt"

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
type Factories struct {
	Receivers     map[string]ReceiverFactory
	ChunkManagers map[string]chunk.ManagerFactory
	IndexManagers map[string]index.ManagerFactory

	// Note: No QueryEngineFactory is needed because QueryEngine construction
	// is trivial and uniform (query.New(cm, im)). If QueryEngine ever requires
	// configuration, add a factory here.
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
// For each receiver in the config:
//   - Creates a Receiver using the matching factory
//   - Registers it under the receiver's ID
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
	receiverIDs := make(map[string]bool)

	// Create stores (chunk manager + index manager + query engine).
	for _, storeCfg := range cfg.Stores {
		if storeIDs[storeCfg.ID] {
			return fmt.Errorf("duplicate store ID: %s", storeCfg.ID)
		}
		storeIDs[storeCfg.ID] = true

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

		// Look up index manager factory.
		imFactory, ok := factories.IndexManagers[storeCfg.Type]
		if !ok {
			return fmt.Errorf("unknown index manager type: %s", storeCfg.Type)
		}

		// Create index manager (needs chunk manager for reading data).
		im, err := imFactory(storeCfg.Params, cm)
		if err != nil {
			return fmt.Errorf("create index manager %s: %w", storeCfg.ID, err)
		}

		// Create query engine.
		qe := query.New(cm, im)

		// Register all components.
		o.RegisterChunkManager(storeCfg.ID, cm)
		o.RegisterIndexManager(storeCfg.ID, im)
		o.RegisterQueryEngine(storeCfg.ID, qe)
	}

	// Create receivers.
	for _, recvCfg := range cfg.Receivers {
		if receiverIDs[recvCfg.ID] {
			return fmt.Errorf("duplicate receiver ID: %s", recvCfg.ID)
		}
		receiverIDs[recvCfg.ID] = true

		// Look up receiver factory.
		recvFactory, ok := factories.Receivers[recvCfg.Type]
		if !ok {
			return fmt.Errorf("unknown receiver type: %s", recvCfg.Type)
		}

		// Create receiver.
		recv, err := recvFactory(recvCfg.Params)
		if err != nil {
			return fmt.Errorf("create receiver %s: %w", recvCfg.ID, err)
		}

		o.RegisterReceiver(recvCfg.ID, recv)
	}

	return nil
}
