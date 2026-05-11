package server

import "gastrolog/internal/logging/comp"

// Component paths for the server package. See gastrolog-3flfp.
var (
	compServer      = comp.Root("server")
	compQuery       = comp.Root("query")
	compVaultServer = comp.Root("vault-server")
	compRPCErrors   = comp.Root("rpc-errors")
	compLifecycle   = comp.Root("lifecycle")
)
