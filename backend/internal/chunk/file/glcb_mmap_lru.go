package file

import (
	"slices"

	"gastrolog/internal/chunk"
)

// glcbMappedCap bounds how many sealed chunks may keep a whole-file GLCB mmap
// resident at once. Cold chunks are munmapped when over cap and unpinned.
var glcbMappedCap = 64

func (m *Manager) touchGLCBMapped(id chunk.ChunkID) {
	m.glcbMapMu.Lock()
	defer m.glcbMapMu.Unlock()

	m.glcbMapLRU = slices.DeleteFunc(m.glcbMapLRU, func(c chunk.ChunkID) bool {
		return c == id
	})
	m.glcbMapLRU = append([]chunk.ChunkID{id}, m.glcbMapLRU...)
}

func (m *Manager) noteGLCBMapped(id chunk.ChunkID) {
	m.glcbMapMu.Lock()
	defer m.glcbMapMu.Unlock()

	m.glcbMapLRU = slices.DeleteFunc(m.glcbMapLRU, func(c chunk.ChunkID) bool {
		return c == id
	})
	m.glcbMapLRU = append([]chunk.ChunkID{id}, m.glcbMapLRU...)
	m.enforceGLCBMapLRULocked()
}

func (m *Manager) dropGLCBMapEntry(id chunk.ChunkID) {
	m.glcbMapMu.Lock()
	m.dropGLCBMapEntryLocked(id)
	m.glcbMapMu.Unlock()
}

func (m *Manager) dropGLCBMapEntryLocked(id chunk.ChunkID) {
	m.glcbMapLRU = slices.DeleteFunc(m.glcbMapLRU, func(c chunk.ChunkID) bool {
		return c == id
	})
}

func (m *Manager) enforceGLCBMapLRULocked() {
	for len(m.glcbMapLRU) > glcbMappedCap {
		evicted := false
		for i := len(m.glcbMapLRU) - 1; i >= 0; i-- {
			evictID := m.glcbMapLRU[i]
			blob := m.mappedGLCBBlob(evictID)
			if blob == nil {
				m.glcbMapLRU = append(m.glcbMapLRU[:i], m.glcbMapLRU[i+1:]...)
				evicted = true
				break
			}
			if blob.PinCount() > 0 {
				continue
			}
			m.glcbMapLRU = append(m.glcbMapLRU[:i], m.glcbMapLRU[i+1:]...)
			m.closeMappedGLCB(evictID)
			evicted = true
			break
		}
		if !evicted {
			return
		}
	}
}
