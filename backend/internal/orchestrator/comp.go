package orchestrator

import "gastrolog/internal/logging/comp"

// Component paths for the orchestrator package and its sub-systems.
// See gastrolog-3flfp for the design.
var (
	compOrchestrator   = comp.Root("orchestrator")
	compVaultCtlLeader = compOrchestrator.Sub("vault-ctl-leader-manager")
	compVaultLifecycle = compOrchestrator.Sub("vault-lifecycle-reconciler")
)
