package memory

import "gastrolog/internal/chunk"

// ChunkContainsVaultSeq reports whether the chunk holds a record with vault_seq.
func (m *Manager) ChunkContainsVaultSeq(id chunk.ChunkID, seq uint64) (bool, error) {
	if seq == 0 {
		return false, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active != nil && m.active.meta.ID == id {
		for _, rec := range m.active.records {
			if rec.VaultSeq == seq {
				return true, nil
			}
		}
	}
	for _, state := range m.chunks {
		if state.meta.ID != id {
			continue
		}
		for _, rec := range state.records {
			if rec.VaultSeq == seq {
				return true, nil
			}
		}
		return false, nil
	}
	return false, nil
}
