package file

import (
	"fmt"
	"os"

	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
)

type mappedGLCBEntry struct {
	path string
	blob *chunkcloud.MappedBlob
}

var _ chunk.GLCBSectionReader = (*Manager)(nil)

func (m *Manager) mappedGLCB(id chunk.ChunkID) (*chunkcloud.MappedBlob, error) {
	path := m.glcbPath(id)
	if v, ok := m.glcbMapped.Load(id); ok {
		e := v.(*mappedGLCBEntry)
		if e.path == path {
			if _, err := os.Stat(path); err == nil {
				return e.blob, nil
			}
			m.evictMappedGLCB(id)
		} else {
			m.evictMappedGLCB(id)
		}
	}
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	blob, err := chunkcloud.OpenMappedBlob(path)
	if err != nil {
		return nil, err
	}
	m.glcbMapped.Store(id, &mappedGLCBEntry{path: path, blob: blob})
	return blob, nil
}

func (m *Manager) evictMappedGLCB(id chunk.ChunkID) {
	if v, ok := m.glcbMapped.LoadAndDelete(id); ok {
		_ = v.(*mappedGLCBEntry).blob.Close()
	}
	m.dropGLCBDecodeEntry(id)
}

// WithGLCBSection implements chunk.GLCBSectionReader.
func (m *Manager) WithGLCBSection(id chunk.ChunkID, sectionType byte, fn func([]byte) error) error {
	chunkLock := m.chunkLockFor(id)
	chunkLock.RLock()
	defer chunkLock.RUnlock()

	blob, err := m.mappedGLCB(id)
	if err != nil {
		return err
	}
	blob.Retain()
	defer blob.Release()

	section, ok := blob.Section(sectionType)
	if !ok || len(section) == 0 {
		return fmt.Errorf("%w: type=0x%02x in %s", chunkcloud.ErrSectionNotFound, sectionType, blob.Path())
	}
	return fn(section)
}
