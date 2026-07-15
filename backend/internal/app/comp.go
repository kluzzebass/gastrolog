package app

import "gastrolog/internal/logging/comp"

// Component paths for the app package's wiring-time sub-systems.
// See gastrolog-3flfp.
var (
	compCluster = comp.Root("cluster").Desc(
		"Cluster server — peer connection management, broadcast/heartbeat tick, cross-node RPC forwarding.")

	compRaft = comp.Root("raft").Desc(
		"Cluster-ctl Raft group — log replication, snapshots, leader elections for cluster-wide config state.")

	compDispatch = comp.Root("dispatch").Desc(
		"Config FSM dispatcher — fires orchestrator side effects + configSignal broadcasts after every committed mutation.")

	compPlacement = comp.Root("placement").Desc(
		"Vault placement reconciler — observes config + cluster topology and assigns vault homes to nodes.")

	compVaultReplicator = comp.Root("vault-replicator").Desc(
		"Per-vault chunk replicator — pushes sealed chunks to follower nodes to meet the configured RF.")

	compManagedFiles = comp.Root("managed-files").Desc(
		"Managed file transfer + repair — distributes uploaded files (e.g. lookup tables) across the cluster.")

	compBroadcast = comp.Root("broadcast").Desc(
		"Periodic cluster-wide stats/state broadcast — each node publishes its NodeStats to all peers on every tick.")

	compStatsCollector = comp.Root("stats-collector").Desc(
		"Local stats aggregation — assembles NodeStats from orchestrator + vault sources on every broadcast tick.")
)
