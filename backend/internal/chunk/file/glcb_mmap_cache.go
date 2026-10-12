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
//
// A cached mapping is served only while it maps the file now on the path: a
// copy quarantined aside and replaced by another keeps its path, so the path
// alone would keep serving the bytes that left it.
func (m *Manager) mappedGLCB(id chunk.ChunkID) (*glcb.MappedBlob, error) {
	path := m.glcbPath(id)
	onPath, err := os.Stat(path)
	if err != nil {
		m.evictMappedGLCB(id)
		return nil, err
	}
	if blob := m.pinMappedGLCB(id, path, onPath); blob != nil {
		return blob, nil
	}
	fresh, err := m.openGLCBBlob(path)
	if err != nil {
		return nil, err
	}
	m.glcbMapMu.Lock()
	defer m.glcbMapMu.Unlock()
	if blob := m.pinMappedGLCBLocked(id, path, fresh.FileInfo()); blob != nil {
		// Another caller mapped the same file first; share that mapping.
		_ = fresh.Close()
		return blob, nil
	}
	fresh.Retain()
	m.glcbMapped.Store(id, &mappedGLCBEntry{path: path, blob: fresh})
	if !fresh.Held() {
		m.noteGLCBMappedLocked(id)
	}
	return fresh, nil
}

// openGLCBBlob maps the GLCB at path, or, for a memory vault, reads it into
// RAM. A held GLCB stays out of the mapping LRU: it is resident until its
// chunk is deleted or its copy is forgotten, and the memory budget, not the
// mapping cap, bounds how many there are.
func (m *Manager) openGLCBBlob(path string) (*glcb.MappedBlob, error) {
	if m.cfg.MemoryBudgetBytes > 0 {
		return glcb.OpenHeldBlob(path)
	}
	return glcb.OpenMappedBlob(path)
}

func (m *Manager) pinMappedGLCB(id chunk.ChunkID, path string, onPath os.FileInfo) *glcb.MappedBlob {
	m.glcbMapMu.Lock()
	defer m.glcbMapMu.Unlock()
	return m.pinMappedGLCBLocked(id, path, onPath)
}

// pinMappedGLCBLocked pins and returns the cached mapping for id when it maps
// the file onPath describes at path; a mapping of any other file is closed
// and dropped. Caller holds glcbMapMu.
func (m *Manager) pinMappedGLCBLocked(id chunk.ChunkID, path string, onPath os.FileInfo) *glcb.MappedBlob {
	v, ok := m.glcbMapped.Load(id)
	if !ok {
		return nil
	}
	e := v.(*mappedGLCBEntry)
	if e.path != path || !e.blob.IsFile(onPath) {
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
//
// It must not take the per-chunk lock: a search reads sections while holding
// an open cursor on the same chunk, which already holds that lock for
// reading, and Go's RWMutex deadlocks a recursive read against any writer
// queued in between (delete, upload, eviction). The pin from mappedGLCB is
// what keeps the mapping valid for fn; writers replace data.glcb only by
// atomic rename, and mappedGLCB refuses a path that no longer exists.
func (m *Manager) WithGLCBSection(id chunk.ChunkID, sectionType byte, fn func(version uint8, section []byte) error) error {
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

var _ chunk.ChunkBudgetMonitor = (*Manager)(nil)

// BudgetExceeded implements chunk.ChunkBudgetMonitor: for a memory vault, the
// bytes by which its sealed chunks exceed the memory budget. Every sealed
// chunk counts, held yet or not, since each is held once it is read. A vault
// without a memory budget is never over it.
func (m *Manager) BudgetExceeded() int64 {
	if m.cfg.MemoryBudgetBytes == 0 {
		return 0
	}
	metas, err := m.List()
	if err != nil {
		return 0
	}
	var sealed int64
	for _, meta := range metas {
		if meta.Sealed {
			sealed += meta.Bytes
		}
	}
	budget := int64(m.cfg.MemoryBudgetBytes) //nolint:gosec // G115: a byte budget is far below int64's range
	return max(sealed-budget, 0)
}
