// roots.go declares the small set of cross-cutting component roots
// that multiple packages want to attach children to.
//
// Single-package roots (Orchestrator, Server, Cluster, etc.) live in
// the packages that own them — there's no reason for the comp package
// to know about them, and keeping them local avoids manifest drift.
//
// This file is intentionally small. Resist adding leaf paths here:
// per-ingester-type, per-indexer-type, per-chunk-manager-backend
// paths are constructed inline at their construction sites, and
// auto-register via comp.Root / comp.Sub so they show up in All()
// the moment the binary's wiring runs them.
package comp

// Cross-cutting roots — used by 2+ packages to namespace their leaves.
var (
	// Ingester is the namespace under which every ingester type lives:
	// ingester.relp, ingester.http, ingester.mqtt, etc.
	Ingester = Root("ingester")

	// Indexer is the namespace for the format-specific indexers
	// (attr, json, kv, token) inside the index pipeline.
	Indexer = Root("indexer")

	// IndexManager is the namespace for the per-backend index managers
	// (index-manager.file, index-manager.memory).
	IndexManager = Root("index-manager")

	// ChunkManager is the namespace for the per-backend chunk managers
	// (chunk-manager.file, chunk-manager.memory).
	ChunkManager = Root("chunk-manager")
)
