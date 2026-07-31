package server

import "gastrolog/internal/logging/comp"

// Component paths for the server package.
var (
	compServer = comp.Root("server").Desc(
		"HTTP/Connect RPC server — listener lifecycle, request routing, middleware, TLS reconfiguration.")

	compQuery = comp.Root("query").Desc(
		"Query RPC handlers — Search, Follow, Histogram, GetContext, Explain entry points.")

	compVaultServer = comp.Root("vault-server").Desc(
		"Cluster-side per-vault RPC handlers used for inter-node replication and chunk transfer.")

	compRPCErrors = comp.Root("rpc-errors").Desc(
		"RPC interceptor that logs error responses returned to clients — useful for surfacing handler failures.")

	compLifecycle = comp.Root("lifecycle").Desc(
		"Lifecycle RPC handlers — Health, Shutdown, GetClusterStatus, JoinCluster, RemoveNode.")
)
