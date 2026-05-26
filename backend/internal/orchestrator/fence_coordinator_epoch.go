package orchestrator

import (
	"gastrolog/internal/glid"
)

func (o *Orchestrator) onVaultCtlLeaderEpoch(vaultID glid.GLID) {
	o.submitLocalFenceHint(vaultID, o.now())
	_ = o.evaluateVaultFence(vaultID, o.now(), false)
}
