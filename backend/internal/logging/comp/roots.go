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
	Ingester = Root("ingester").Desc(
		"Inbound log receivers. Children name the protocol (relp, http, mqtt, syslog, otlp, kafka, fluentfwd, chatterbox, scatterbox, metrics, self, tail, docker); grandchildren are per-instance scopes keyed on the ingester config ID.")

	// Indexer is the namespace for the format-specific indexers
	// (attr, json, kv, token) inside the index pipeline.
	Indexer = Root("indexer").Desc(
		"Format-specific indexers run during sealing. Children: attr (per-attribute string index), json (JSON-path index), kv (key/value index), token (full-text token index).")

	// IndexManager is the namespace for the per-backend index managers
	// (index-manager.file, index-manager.memory).
	IndexManager = Root("index-manager").Desc(
		"Top-level index lifecycle owners. Children name the backend: file (on-disk mmap-backed indexes) and memory (transient in-memory indexes).")

	// ChunkManager is the namespace for the per-backend chunk managers
	// (chunk-manager.file, chunk-manager.memory).
	ChunkManager = Root("chunk-manager").Desc(
		"Top-level chunk store managers — open, seal, query, and retire chunks. Children name the backend: file (on-disk GLCB) and memory (transient in-memory).")
)
