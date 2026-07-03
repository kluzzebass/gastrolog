package file

import (
	"slices"

	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
)

// defaultGLCBDecodedTablesCap bounds how many sealed chunks may hold
// heap-decoded GLCB dict+record index at once. Mmaps stay in glcbMapped; LRU
// eviction and cursor close drop decode state only (gastrolog-2o9e9 histogram
// attr scan). Per-Manager state (glcbDecodeCap), not a package global — tests
// tune it per instance without racing parallel readers (gastrolog-1woee2).
const defaultGLCBDecodedTablesCap = 32

// noteGLCBDecoded records that id's GLCB dict/index are loaded and enforces
// the decode-table LRU cap across chunks.
func (m *Manager) noteGLCBDecoded(id chunk.ChunkID) {
	m.glcbDecodeMu.Lock()
	defer m.glcbDecodeMu.Unlock()

	m.glcbDecodeLRU = slices.DeleteFunc(m.glcbDecodeLRU, func(c chunk.ChunkID) bool {
		return c == id
	})
	m.glcbDecodeLRU = append([]chunk.ChunkID{id}, m.glcbDecodeLRU...)
	m.enforceGLCBDecodeLRULocked()
}

// releaseGLCBDecodeTables drops decode state after the last cursor pin on id
// closes and removes id from the decode LRU.
func (m *Manager) releaseGLCBDecodeTables(id chunk.ChunkID, blob *chunkcloud.MappedBlob) {
	blob.TryReleaseRecordTables()
	m.glcbDecodeMu.Lock()
	m.glcbDecodeLRU = slices.DeleteFunc(m.glcbDecodeLRU, func(c chunk.ChunkID) bool {
		return c == id
	})
	m.glcbDecodeMu.Unlock()
}

func (m *Manager) dropGLCBDecodeEntry(id chunk.ChunkID) {
	m.glcbDecodeMu.Lock()
	m.glcbDecodeLRU = slices.DeleteFunc(m.glcbDecodeLRU, func(c chunk.ChunkID) bool {
		return c == id
	})
	m.glcbDecodeMu.Unlock()
}

func (m *Manager) enforceGLCBDecodeLRULocked() {
	for len(m.glcbDecodeLRU) > m.glcbDecodeCap {
		evicted := false
		for i := len(m.glcbDecodeLRU) - 1; i >= 0; i-- {
			evictID := m.glcbDecodeLRU[i]
			blob := m.mappedGLCBBlob(evictID)
			if blob == nil || !blob.TryReleaseRecordTables() {
				continue
			}
			m.glcbDecodeLRU = append(m.glcbDecodeLRU[:i], m.glcbDecodeLRU[i+1:]...)
			evicted = true
			break
		}
		if !evicted {
			return
		}
	}
}

func (m *Manager) mappedGLCBBlob(id chunk.ChunkID) *chunkcloud.MappedBlob {
	v, ok := m.glcbMapped.Load(id)
	if !ok {
		return nil
	}
	return v.(*mappedGLCBEntry).blob
}
