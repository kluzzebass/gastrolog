package orchestrator

import "gastrolog/internal/logging/comp"

// Component paths for the orchestrator package and its sub-systems.
//
// The orchestrator package is large enough that a single "orchestrator"
// component would cover dozens of unrelated concerns. Each major
// subsystem gets its own sub-path so operators can target verbosity at
// the right granularity:
//
//   orchestrator.replication    chunk replication + follower catchup
//   orchestrator.drain          vault drain + chunk transfer to new homes
//   orchestrator.retention      retention sweep + archival sweep
//   orchestrator.rotation       sealing on schedule + cron rotation
//   orchestrator.scheduler      job scheduler + job-event broker
//   orchestrator.vault-ops      raw vault CRUD apply paths
//   orchestrator.cache-eviction local-vault cache eviction loop
//   orchestrator.cloud-health   cloud-backed vault health probes
//
// The two paths that already had explicit literal-component logging
// (vault-ctl-leader-manager, vault-lifecycle-reconciler) keep their
// previously-established names so existing operator scripts continue to
// match.
//
// See gastrolog-3flfp.
var (
	compOrchestrator   = comp.Root("orchestrator")
	compVaultCtlLeader = compOrchestrator.Sub("vault-ctl-leader-manager")
	compVaultLifecycle = compOrchestrator.Sub("vault-lifecycle-reconciler")
	compReplication    = compOrchestrator.Sub("replication")
	compDrain          = compOrchestrator.Sub("drain")
	compRetention      = compOrchestrator.Sub("retention")
	compRotation       = compOrchestrator.Sub("rotation")
	compScheduler      = compOrchestrator.Sub("scheduler")
	compVaultOps       = compOrchestrator.Sub("vault-ops")
	compCacheEviction  = compOrchestrator.Sub("cache-eviction")
	compCloudHealth    = compOrchestrator.Sub("cloud-health")
)
