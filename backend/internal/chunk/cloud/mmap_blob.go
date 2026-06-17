package cloud

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

// MappedBlob is a read-only mmap of an entire data.glcb file. Dict, record
// index, record frames, embedded sections, and TOC are all accessed as slices
// into the mapping — one open, one mmap, OS page cache owns the bytes.
type MappedBlob struct {
	path           string
	data           []byte
	meta           BlobMeta
	dict           *chunk.StringDict
	index          []recordIndex
	recordsBaseOff int64
	toc            BlobTOC
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

// Section returns a sub-slice of the mapping for a TOC section type.
// The slice aliases b.data and is invalid after Close.
func (b *MappedBlob) Section(sectionType byte) ([]byte, bool) {
	entry, ok := b.toc.Find(sectionType)
	if !ok || entry.Size <= 0 {
		return nil, false
	}
	start := entry.Offset
	end := start + entry.Size
	if start < 0 || end > int64(len(b.data)) {
		return nil, false
	}
	return b.data[start:end], true
}

// Reader returns a record cursor backend that reads frames from this mapping.
func (b *MappedBlob) Reader() *Reader {
	return &Reader{
		meta:           b.meta,
		dict:           b.dict,
		index:          b.index,
		recordsBaseOff: b.recordsBaseOff,
		mmapData:       b.data,
		keepFile:       true,
	}
}

func parseMappedBlob(data []byte) (*MappedBlob, error) {
	if len(data) < headerSize+int(tocFooterSize) {
		return nil, fmt.Errorf("GLCB too small: %d bytes", len(data))
	}
	if _, err := format.DecodeAndValidate(data[:preambleSize], format.TypeCloudBlob, formatVersion); err != nil {
		return nil, fmt.Errorf("GLCB preamble: %w", err)
	}
	layout, err := decodeBlobLayoutMeta(data[preambleSize:headerSize])
	if err != nil {
		return nil, err
	}
	if int(layout.DictOff)+int(layout.DictSize) > len(data) {
		return nil, errors.New("dict out of range")
	}
	dict, err := decodeDictFromBuf(data[int(layout.DictOff):int(layout.DictOff)+int(layout.DictSize)], layout.DictEntries)
	if err != nil {
		return nil, err
	}
	if int(layout.IndexOff)+int(layout.IndexSize) > len(data) {
		return nil, errors.New("record index out of range")
	}
	index := make([]recordIndex, layout.RecordCount)
	indexBytes := data[int(layout.IndexOff) : int(layout.IndexOff)+int(layout.IndexSize)]
	for i := range layout.RecordCount {
		off := int(i) * indexEntrySize
		index[i] = recordIndex{
			Offset: binary.LittleEndian.Uint64(indexBytes[off:]),
			Size:   binary.LittleEndian.Uint32(indexBytes[off+8:]),
		}
	}
	toc, err := parseTOCFromMapped(data)
	if err != nil {
		return nil, fmt.Errorf("read TOC: %w", err)
	}
	return &MappedBlob{
		meta:           layoutMetaToBlobMeta(layout, toc),
		dict:           dict,
		index:          index,
		recordsBaseOff: int64(layout.RecordsOff),
		toc:            toc,
	}, nil
}

func parseTOCFromMapped(data []byte) (BlobTOC, error) {
	if len(data) < tocFooterSize {
		return BlobTOC{}, errors.New("blob too small for TOC footer")
	}
	footer := data[len(data)-tocFooterSize:]
	count, _, err := parseTOCFooter(footer)
	if err != nil {
		return BlobTOC{}, err
	}
	entryBytes := int(count) * tocEntrySize
	entriesStart := len(data) - tocFooterSize - entryBytes
	if entriesStart < 0 {
		return BlobTOC{}, errors.New("blob too small for TOC entries")
	}
	return parseTOCRegion(data[entriesStart:len(data)-tocFooterSize], footer)
}
