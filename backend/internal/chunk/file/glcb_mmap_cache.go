package file

import (
	"fmt"
	"os"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
)

type mappedGLCBEntry struct {
	path string
	blob *glcb.MappedBlob
}

var _ chunk.GLCBSectionReader = (*Manager)(nil)

// mappedGLCB returns the chunk's whole-file mmap with one pin already held
// for the caller, who must Release it. The pin is taken under glcbMapMu, the
// lock the LRU enforcer runs under, so a mapping can never be closed between
// a caller receiving it and pinning it. A pinned mapping is never evicted, so
// a search holding cursors on more chunks than glcbMappedCap simply keeps
// them all mapped instead of losing the newest one.
func (m *Manager) mappedGLCB(id chunk.ChunkID) (*glcb.MappedBlob, error) {
	path := m.glcbPath(id)
	if _, err := os.Stat(path); err != nil {
		m.evictMappedGLCB(id)
		return nil, err
	}
	if blob := m.pinMappedGLCB(id, path); blob != nil {
		return blob, nil
	}
	fresh, err := glcb.OpenMappedBlob(path)
	if err != nil {
		return nil, err
	}
	m.glcbMapMu.Lock()
	defer m.glcbMapMu.Unlock()
	if blob := m.pinMappedGLCBLocked(id, path); blob != nil {
		// Another caller mapped the same file first; share that mapping.
		_ = fresh.Close()
		return blob, nil
	}
	fresh.Retain()
	m.glcbMapped.Store(id, &mappedGLCBEntry{path: path, blob: fresh})
	m.noteGLCBMappedLocked(id)
	return fresh, nil
}

func (m *Manager) pinMappedGLCB(id chunk.ChunkID, path string) *glcb.MappedBlob {
	m.glcbMapMu.Lock()
	defer m.glcbMapMu.Unlock()
	return m.pinMappedGLCBLocked(id, path)
}

// pinMappedGLCBLocked pins and returns the cached mapping for id when it is
// for the given path; a mapping of a stale path is closed and dropped. Caller
// holds glcbMapMu.
func (m *Manager) pinMappedGLCBLocked(id chunk.ChunkID, path string) *glcb.MappedBlob {
	v, ok := m.glcbMapped.Load(id)
	if !ok {
		return nil
	}
	e := v.(*mappedGLCBEntry)
	if e.path != path {
		m.closeMappedGLCB(id)
		m.dropGLCBMapEntryLocked(id)
		return nil
	}
	e.blob.Retain()
	m.touchGLCBMappedLocked(id)
	return e.blob
}

func (m *Manager) evictMappedGLCB(id chunk.ChunkID) {
	m.closeMappedGLCB(id)
	m.dropGLCBMapEntry(id)
}

func (m *Manager) closeMappedGLCB(id chunk.ChunkID) {
	if v, ok := m.glcbMapped.LoadAndDelete(id); ok {
		_ = v.(*mappedGLCBEntry).blob.Close()
	}
	m.dropGLCBDecodeEntry(id)
}

// WithGLCBSection implements chunk.GLCBSectionReader.
func (m *Manager) WithGLCBSection(id chunk.ChunkID, sectionType byte, fn func(version uint8, section []byte) error) error {
	chunkLock := m.chunkLockFor(id)
	chunkLock.RLock()
	defer chunkLock.RUnlock()

	blob, err := m.mappedGLCB(id)
	if err != nil {
		return err
	}
	defer blob.Release()

	entry, section, ok := blob.Section(sectionType)
	if !ok || len(section) == 0 {
		return fmt.Errorf("%w: type=0x%02x in %s", glcb.ErrSectionNotFound, sectionType, blob.Path())
	}
	return fn(entry.Version, section)
}
