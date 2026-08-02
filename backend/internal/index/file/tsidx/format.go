package tsidx

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/format"
	"gastrolog/internal/tsindex"
)

var (
	ErrIndexTooSmall   = errors.New("timestamp index too small")
	ErrIndexIncomplete = errors.New("timestamp index incomplete (missing complete flag)")
)

// Entry is a (timestamp, position) pair for binary search. It is a type
// alias for tsindex.Entry — tsidx carries no semantics beyond the shared
// wire-layout entry, so there is nothing to convert at the boundary.
type Entry = tsindex.Entry

// blobPath returns the GLCB blob path for the given chunk under dir.
func blobPath(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), glcb.BlobFilename)
}

// FindStartRank returns the rank (index in the IngestTS-sorted slice) of
// the first entry with TS >= ts. Distinct from FindStartPosition: this
// returns the index in the sorted slice, FindStartPosition returns the
// physical record position from the entry's Pos field. The two differ on
// non-monotonic chunks built via ImportRecords.
//
// The search reuses tsindex.Compare's ordering via slices.BinarySearchFunc:
// probing for {TS: ts, Pos: 0} finds the first entry with TS >= ts, since
// Pos is unsigned and 0 is its minimum value, so any entry at the same TS
// always compares >= the probe.
func FindStartRank(entries []Entry, ts int64) (uint64, bool) {
	idx, _ := slices.BinarySearchFunc(entries, Entry{TS: ts}, tsindex.Compare)
	if idx == len(entries) {
		return 0, false
	}
	return uint64(idx), true //nolint:gosec // G115: entry count bounded by chunk record count (< 2^32)
}

// FindStartPosition returns the position of the first entry with TS >= ts.
// Returns (pos, true) if found, (0, false) if ts is after all entries.
func FindStartPosition(entries []Entry, ts int64) (uint64, bool) {
	rank, ok := FindStartRank(entries, ts)
	if !ok {
		return 0, false
	}
	return uint64(entries[rank].Pos), true
}

// MmapView is a TS-index section view plus the lifetime of the bytes it
// reads from. The view itself comes from the GLCB section registry, so the
// section's recorded format version picks the decode — this wrapper only
// owns releasing the underlying mapping (Close is a no-op for views that
// alias a parent whole-file mapping, which the parent MappedBlob owns).
type MmapView struct {
	view  tsindex.View
	close func() error
}

// GLCBPath returns the canonical data.glcb path for a chunk under dir.
func GLCBPath(dir string, chunkID chunk.ChunkID) string {
	return blobPath(dir, chunkID)
}

// OpenIngestMmapAt opens the ingest TS index section inside the given GLCB.
func OpenIngestMmapAt(glcbPath string) (MmapView, error) {
	return openSectionMmap(glcbPath, format.TypeIngestIndex)
}

// OpenSourceMmapAt opens the source TS index section inside the given GLCB.
func OpenSourceMmapAt(glcbPath string) (MmapView, error) {
	return openSectionMmap(glcbPath, format.TypeSourceIndex)
}

// OpenIngestMmap opens the chunk's ingest TS index section inside
// data.glcb, resolves its view through the section registry, and returns
// it for repeated lookups. Returns ErrIndexTooSmall if the section is empty.
func OpenIngestMmap(dir string, chunkID chunk.ChunkID) (MmapView, error) {
	return openSectionMmap(blobPath(dir, chunkID), format.TypeIngestIndex)
}

// OpenSourceMmap is the SourceTS counterpart to OpenIngestMmap.
func OpenSourceMmap(dir string, chunkID chunk.ChunkID) (MmapView, error) {
	return openSectionMmap(blobPath(dir, chunkID), format.TypeSourceIndex)
}

// ViewFromSection wraps raw TS-index section bytes that alias a parent
// whole-file GLCB mapping, dispatching decode on the section's recorded
// type and version. Close is a no-op — the parent MappedBlob owns the
// munmap.
func ViewFromSection(sectionType byte, version uint8, data []byte) (MmapView, error) {
	view, err := registryView(sectionType, version, data)
	if err != nil {
		return MmapView{}, err
	}
	if view.Len() == 0 {
		return MmapView{}, ErrIndexTooSmall
	}
	return MmapView{view: view}, nil
}

func openSectionMmap(path string, sectionType byte) (MmapView, error) {
	entry, data, closer, err := glcb.MapSection(path, sectionType)
	if err != nil {
		return MmapView{}, err
	}
	view, err := registryView(entry.Type, entry.Version, data)
	if err != nil {
		_ = closer()
		return MmapView{}, err
	}
	if view.Len() == 0 {
		_ = closer()
		return MmapView{}, ErrIndexTooSmall
	}
	return MmapView{view: view, close: closer}, nil
}

// registryView narrows the section registry's decode to the TS-index view
// interface — the one place tsidx asserts what kind of view a TS section
// yields.
func registryView(sectionType byte, version uint8, data []byte) (tsindex.View, error) {
	v, err := glcb.DefaultRegistry().NewView(glcb.TOCEntry{Type: sectionType, Version: version}, data)
	if err != nil {
		return nil, err
	}
	view, ok := v.(tsindex.View)
	if !ok {
		return nil, fmt.Errorf("tsidx: section type 0x%02x version %d decoded to %T, not a TS-index view", sectionType, version, v)
	}
	return view, nil
}

// Close releases the underlying mmap region.
func (v MmapView) Close() error {
	if v.close == nil {
		return nil
	}
	return v.close()
}

// SearchTS binary-searches the section for the first entry with TS >=
// tsNano. Returns (rank, pos, true) if found, (0, 0, false) if past all
// entries.
func (v MmapView) SearchTS(tsNano int64) (rank uint32, pos uint32, ok bool) {
	if v.view == nil {
		return 0, 0, false
	}
	return v.view.SearchTS(tsNano)
}

// Len returns the number of (timestamp, position) entries in the section.
func (v MmapView) Len() uint32 {
	if v.view == nil {
		return 0
	}
	return v.view.Len()
}

// EntryAt returns the entry at rank i. Caller must ensure i < Len().
func (v MmapView) EntryAt(i uint32) Entry { return v.view.EntryAt(i) }
