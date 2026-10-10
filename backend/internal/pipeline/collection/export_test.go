package collection

import "gastrolog/internal/glid"

// PendingCollectWaiters reports how many CollectOnce callers are queued for a
// worker pass on vaultID that has not started yet.
func (m *Manager) PendingCollectWaiters(vaultID glid.GLID) int {
	m.mu.Lock()
	v := m.vaults[vaultID]
	m.mu.Unlock()
	if v == nil {
		return 0
	}
	v.collectWaitMu.Lock()
	defer v.collectWaitMu.Unlock()
	return len(v.collectWaiters)
}
