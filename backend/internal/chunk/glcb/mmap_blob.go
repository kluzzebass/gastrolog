package glcb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

// MappedBlob is a read-only mmap of an entire data.glcb file. Dict, record
// index, record frames, embedded sections, and TOC are all accessed as slices
// into the mapping — one open, one mmap, OS page cache owns the bytes.
//
// Dict and record index are parsed lazily on first Reader() call so histogram
// and TS-index lookups (Section only) do not heap-load every chunk in a window.
type MappedBlob struct {
	path           string
	data           []byte
	layout         blobLayoutMeta
	meta           BlobMeta
	dict           chunk.DictReader
	indexBytes     []byte
	recordsBaseOff int64
	toc            BlobTOC
	recordMu       sync.Mutex
	recordLoaded   bool
	recordInitErr  error
	pins           atomic.Int32
	closed         atomic.Bool
}

// OpenMappedBlob memory-maps path and parses the GLCB in place.
func OpenMappedBlob(path string) (*MappedBlob, error) {
	path = filepath.Clean(path)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	size := info.Size()
	if size == 0 {
		_ = f.Close()
		return nil, fmt.Errorf("empty GLCB: %s", path)
	}
	if size > int64(^uint(0)>>1) {
		_ = f.Close()
		return nil, fmt.Errorf("GLCB too large to mmap: %s", path)
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115: bounded by check above
	_ = f.Close()
	if err != nil {
		return nil, fmt.Errorf("mmap %s: %w", path, err)
	}
	blob, err := parseMappedBlob(data)
	if err != nil {
		_ = syscall.Munmap(data)
		return nil, err
	}
	blob.path = path
	blob.data = data
	return blob, nil
}

// PinCount returns active Retain pins (tests and cache eviction).
func (b *MappedBlob) PinCount() int32 { return b.pins.Load() }

// RecordTablesLoaded reports whether dict and record index are parsed for reads.
func (b *MappedBlob) RecordTablesLoaded() bool {
	b.recordMu.Lock()
	loaded := b.recordLoaded
	b.recordMu.Unlock()
	return loaded
}

// TryReleaseRecordTables drops heap-decoded dict and record index while
// keeping the mmap. No-op when retain pins are held or tables were never
// loaded. Safe to call after the last cursor / section reader closes.
func (b *MappedBlob) TryReleaseRecordTables() bool {
	if b.pins.Load() > 0 {
		return false
	}
	b.recordMu.Lock()
	defer b.recordMu.Unlock()
	if !b.recordLoaded {
		return false
	}
	b.dict = nil
	b.indexBytes = nil
	b.recordsBaseOff = 0
	b.recordLoaded = false
	b.recordInitErr = nil
	return true
}

// Retain pins the mapping for an in-flight cursor or TS-index lookup.
func (b *MappedBlob) Retain() { b.pins.Add(1) }

// Release drops a Retain pin; munmaps when the blob was evicted and no pins remain.
func (b *MappedBlob) Release() {
	if b.pins.Add(-1) == 0 && b.closed.Load() {
		_ = b.unmap()
	}
}

// Close requests eviction of the whole-file mapping. The mmap is released only
// after all retain pins drain (active cursors / locked section reads).
func (b *MappedBlob) Close() error {
	b.closed.Store(true)
	if b.pins.Load() == 0 {
		return b.unmap()
	}
	return nil
}

func (b *MappedBlob) unmap() error {
	if b.data == nil {
		return nil
	}
	err := syscall.Munmap(b.data)
	b.data = nil
	return err
}

// Path returns the file this mapping was opened from.
func (b *MappedBlob) Path() string { return b.path }

// Meta returns parsed blob metadata.
func (b *MappedBlob) Meta() BlobMeta { return b.meta }

// Section returns the TOC entry and a sub-slice of the mapping for a TOC
// section type. The entry carries the section's recorded version so decode
// dispatch (Registry.NewView) can honor it. The slice aliases b.data and is
// invalid after Close.
func (b *MappedBlob) Section(sectionType byte) (TOCEntry, []byte, bool) {
	entry, ok := b.toc.Find(sectionType)
	if !ok || entry.Size <= 0 {
		return TOCEntry{}, nil, false
	}
	start := entry.Offset
	end := start + entry.Size
	if start < 0 || end > int64(len(b.data)) {
		return TOCEntry{}, nil, false
	}
	return entry, b.data[start:end], true
}

// Reader returns a record cursor backend that reads frames from this mapping.
func (b *MappedBlob) Reader() (*Reader, error) {
	if err := b.ensureRecordTables(); err != nil {
		return nil, err
	}
	b.recordMu.Lock()
	dict := b.dict
	indexBytes := b.indexBytes
	base := b.recordsBaseOff
	data := b.data
	meta := b.meta
	b.recordMu.Unlock()
	return &Reader{
		meta:           meta,
		dict:           dict,
		indexBytes:     indexBytes,
		recordsBaseOff: base,
		mmapData:       data,
	}, nil
}

func (b *MappedBlob) ensureRecordTables() error {
	b.recordMu.Lock()
	if b.recordLoaded {
		err := b.recordInitErr
		b.recordMu.Unlock()
		return err
	}
	err := b.loadRecordTablesLocked()
	b.recordInitErr = err
	b.recordLoaded = true
	b.recordMu.Unlock()
	return err
}

func (b *MappedBlob) loadRecordTablesLocked() error {
	layout := b.layout
	if int(layout.DictOff)+int(layout.DictSize) > len(b.data) {
		return errors.New("dict out of range")
	}
	dictRegion := b.data[int(layout.DictOff) : int(layout.DictOff)+int(layout.DictSize)]
	dict, err := chunk.NewMmapStringDict(dictRegion, layout.DictEntries)
	if err != nil {
		return err
	}
	if int(layout.IndexOff)+int(layout.IndexSize) > len(b.data) {
		return errors.New("record index out of range")
	}
	b.dict = dict
	b.indexBytes = b.data[int(layout.IndexOff) : int(layout.IndexOff)+int(layout.IndexSize)]
	b.recordsBaseOff = int64(layout.RecordsOff)
	return nil
}

func parseMappedBlob(data []byte) (*MappedBlob, error) {
	if len(data) < headerSize+int(tocFooterSize) {
		return nil, fmt.Errorf("GLCB too small: %d bytes", len(data))
	}
	if _, err := format.DecodeAndValidate(data[:preambleSize], format.TypeGLCB, formatVersion); err != nil {
		return nil, fmt.Errorf("GLCB preamble: %w", err)
	}
	layout, err := decodeBlobLayoutMeta(data[preambleSize:headerSize])
	if err != nil {
		return nil, err
	}
	if int(layout.DictOff)+int(layout.DictSize) > len(data) {
		return nil, errors.New("dict out of range")
	}
	if int(layout.IndexOff)+int(layout.IndexSize) > len(data) {
		return nil, errors.New("record index out of range")
	}
	// The whole mapping ends with the TOC tail, so the shared byte-slice
	// parser applies directly — no mmap-specific bounds math.
	toc, err := ParseTOC(data)
	if err != nil {
		return nil, fmt.Errorf("read TOC: %w", err)
	}
	return &MappedBlob{
		layout: layout,
		meta:   layoutMetaToBlobMeta(layout, toc),
		toc:    toc,
	}, nil
}
