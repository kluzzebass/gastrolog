package orchestrator

import "gastrolog/internal/logging/comp"

// Component paths for the orchestrator package and its sub-systems.
var (
	compOrchestrator = comp.Root("orchestrator").Desc(
		"Top-level orchestrator — vault lifecycle, scheduling, drain, replication, retention. Most operator filters target one of the subsystem sub-paths below rather than this root.")

	compVaultCtlLeader = compOrchestrator.Sub("vault-ctl-leader-manager").Desc(
		"Per-vault control-plane Raft leader epoch supervision: membership reconcile and optional leadership transfer.")

	compVaultLifecycle = compOrchestrator.Sub("vault-lifecycle-reconciler").Desc(
		"Per-vault FSM-driven reconciler — applies snapshot state, runs the receipt protocol, sweeps missing replicas, drives PostSealProcess.")

	compReplication = compOrchestrator.Sub("replication").Desc(
		"Sealed-chunk replication to RF followers: target selection, fan-out, retry, partial-failure aggregation.")

	compDrain = compOrchestrator.Sub("drain").Desc(
		"Vault drain — moving chunks off a vault that's leaving this node, plus inter-vault chunk transfers.")

	compRetention = compOrchestrator.Sub("retention").Desc(
		"Retention sweep + archival sweep — applies policy-driven eviction and archival to sealed chunks.")

	compRotation = compOrchestrator.Sub("rotation").Desc(
		"Active-chunk rotation: timer-driven sealing on schedule plus cron-fired forced rotations.")

	compScheduler = compOrchestrator.Sub("scheduler").Desc(
		"Async job scheduler: queueing, slot allocation, job-event broker, persistence.")

	compVaultOps = compOrchestrator.Sub("vault-ops").Desc(
		"Raw vault CRUD apply paths invoked from the FSM dispatcher when a vault command commits.")

	compCacheEviction = compOrchestrator.Sub("cache-eviction").Desc(
		"Local-vault cache eviction loop — drops in-memory chunk/index entries once they're committed to the storage backend.")

	compCloudHealth = compOrchestrator.Sub("cloud-health").Desc(
		"Cloud-backed vault health probes — periodic reachability checks against the configured cloud store.")
)
