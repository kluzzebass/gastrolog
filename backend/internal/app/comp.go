package app

import "gastrolog/internal/logging/comp"

// Component paths for the app package's wiring-time sub-systems.
// See gastrolog-3flfp for the design.
var (
	compCluster         = comp.Root("cluster")
	compRaft            = comp.Root("raft")
	compDispatch        = comp.Root("dispatch")
	compPlacement       = comp.Root("placement")
	compRecordForwarder = comp.Root("record-forwarder")
	compVaultReplicator = comp.Root("vault-replicator")
	compManagedFiles    = comp.Root("managed-files")
	compBroadcast       = comp.Root("broadcast")
	compStatsCollector  = comp.Root("stats-collector")
)
