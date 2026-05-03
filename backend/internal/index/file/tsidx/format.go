package tsidx

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/cloud"
	"gastrolog/internal/format"
)

const (
	currentVersion = 0x01

	entrySize = 12 // ts int64 + pos uint32
)

var (
	ErrIndexTooSmall   = errors.New("timestamp index too small")
	ErrIndexIncomplete = errors.New("timestamp index incomplete (missing complete flag)")
)

// Entry is a (timestamp, position) pair for binary search.
type Entry struct {
	TS  int64
	Pos uint32
}

// blobPath returns the GLCB blob path for the given chunk under dir.
func blobPath(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), cloud.BlobFilename)
}

// decodeRawEntries decodes a raw tail of `[ts:i64][pos:u32] × N`
// entries — the layout used by the embedded ITSI/STSI sections in the
// GLCB. There is no header: the section's type is recorded in the TOC
// entry, and the count derives from the section size.
func decodeRawEntries(data []byte) ([]Entry, error) {
	if len(data)%entrySize != 0 {
		return nil, fmt.Errorf("tsidx: section length %d not a multiple of %d", len(data), entrySize)
	}
	n := len(data) / entrySize
	entries := make([]Entry, n)
	for i := range n {
		off := i * entrySize
		entries[i].TS = int64(binary.LittleEndian.Uint64(data[off : off+8])) //nolint:gosec // G115: nanosecond timestamps fit in int64
		entries[i].Pos = binary.LittleEndian.Uint32(data[off+8 : off+entrySize])
	}
	return entries, nil
}

// FindStartRank returns the rank (index in the IngestTS-sorted slice) of
// the first entry with TS >= ts. Distinct from FindStartPosition: this
// returns the index in the sorted slice, FindStartPosition returns the
// physical record position from the entry's Pos field. The two differ on
// non-monotonic chunks built via ImportRecords. See gastrolog-66b7x.
func FindStartRank(entries []Entry, ts int64) (uint64, bool) {
	n := uint32(len(entries)) //nolint:gosec // G115: entry count bounded by chunk record count (< 2^32)
	if n == 0 {
		return 0, false
	}
	if ts > entries[n-1].TS {
		return 0, false
	}
	if ts <= entries[0].TS {
		return 0, true
	}
	lo, hi := uint32(0), n
	for lo < hi {
		mid := lo + (hi-lo)/2
		if entries[mid].TS < ts {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return uint64(lo), true
}

// FindStartPosition returns the position of the first entry with TS >= ts.
// Returns (pos, true) if found, (0, false) if ts is after all entries.
func FindStartPosition(entries []Entry, ts int64) (uint64, bool) {
	n := uint32(len(entries)) //nolint:gosec // G115: entry count bounded by chunk record count (< 2^32)
	if n == 0 {
		return 0, false
	}
	if ts > entries[n-1].TS {
		return 0, false
	}
	if ts <= entries[0].TS {
		return uint64(entries[0].Pos), true
	}
	// Binary search: first index i where entries[i].TS >= ts
	lo, hi := uint32(0), n
	for lo < hi {
		mid := lo + (hi-lo)/2
		if entries[mid].TS < ts {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return uint64(entries[lo].Pos), true
}

// LoadIngestIndex loads the ingest TS index for a sealed chunk by reading
// the embedded ITSI section from the chunk's data.glcb blob. Returns
// ErrIndexNotFound-equivalent (cloud.ErrSectionNotFound or os.IsNotExist)
// when the chunk has no embedded ITSI section.
func LoadIngestIndex(dir string, chunkID chunk.ChunkID) ([]Entry, error) {
	return loadFromBlob(blobPath(dir, chunkID), format.TypeIngestIndex)
}

// LoadSourceIndex is the source-TS counterpart to LoadIngestIndex.
func LoadSourceIndex(dir string, chunkID chunk.ChunkID) ([]Entry, error) {
	return loadFromBlob(blobPath(dir, chunkID), format.TypeSourceIndex)
}

func loadFromBlob(path string, sectionType byte) ([]Entry, error) {
	entries, err := cloud.LoadSection(path, sectionType, decodeRawEntries)
	if err != nil {
		// Surface "the chunk's blob isn't here" as os.IsNotExist so callers
		// can distinguish "no chunk" from "decode failure".
		return nil, err
	}
	if len(entries) == 0 {
		return nil, ErrIndexTooSmall
	}
	return entries, nil
}

// MmapView holds a long-lived mmap of an embedded TS index section
// inside a chunk's GLCB blob. data points at the section's raw entry
// bytes only — no header prefix. Binary search reads 12-byte entries
// directly from the mmap region, no heap-allocated entry slice. The
// mapping is released via Close (called from the manager's evictTSMmap
// on DeleteIndexes).
type MmapView struct {
	data  []byte // section bytes only (raw entries, no header)
	n     uint32
	close func() error
}

// OpenIngestMmap opens the chunk's ingest TS index section inside
// data.glcb, validates the section size, and returns a MmapView for
// repeated lookups. Returns ErrIndexTooSmall if the section is empty.
func OpenIngestMmap(dir string, chunkID chunk.ChunkID) (MmapView, error) {
	return openSectionMmap(blobPath(dir, chunkID), format.TypeIngestIndex)
}

// OpenSourceMmap is the SourceTS counterpart to OpenIngestMmap.
func OpenSourceMmap(dir string, chunkID chunk.ChunkID) (MmapView, error) {
	return openSectionMmap(blobPath(dir, chunkID), format.TypeSourceIndex)
}

func openSectionMmap(path string, sectionType byte) (MmapView, error) {
	data, closer, err := cloud.MapSection(path, sectionType)
	if err != nil {
		return MmapView{}, err
	}
	if len(data)%entrySize != 0 {
		_ = closer()
		return MmapView{}, fmt.Errorf("tsidx: section length %d not a multiple of %d", len(data), entrySize)
	}
	n := len(data) / entrySize
	if n == 0 {
		_ = closer()
		return MmapView{}, ErrIndexTooSmall
	}
	return MmapView{
		data:  data,
		n:     uint32(n), //nolint:gosec // G115: entry count bounded by chunk record count
		close: closer,
	}, nil
}

// Close releases the underlying mmap region.
func (v MmapView) Close() error {
	if v.close == nil {
		return nil
	}
	return v.close()
}

// SearchTS binary-searches the mmap'd index for the first entry with TS >=
// tsNano. Returns (rank, pos, true) if found, (0, 0, false) if past all
// entries. Operates directly on the mmap'd bytes via binary.LittleEndian
// — no heap allocation. See gastrolog-66b7x.
func (v MmapView) SearchTS(tsNano int64) (rank uint32, pos uint32, ok bool) {
	if v.n == 0 {
		return 0, 0, false
	}
	last := v.entryAt(v.n - 1)
	if tsNano > last.TS {
		return 0, 0, false
	}
	lo, hi := uint32(0), v.n
	for lo < hi {
		mid := lo + (hi-lo)/2
		if v.entryTS(mid) < tsNano {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	e := v.entryAt(lo)
	return lo, e.Pos, true
}

func (v MmapView) entryAt(i uint32) Entry {
	off := int(i) * entrySize
	return Entry{
		TS:  int64(binary.LittleEndian.Uint64(v.data[off : off+8])), //nolint:gosec // G115
		Pos: binary.LittleEndian.Uint32(v.data[off+8 : off+entrySize]),
	}
}

func (v MmapView) entryTS(i uint32) int64 {
	off := int(i) * entrySize
	return int64(binary.LittleEndian.Uint64(v.data[off : off+8])) //nolint:gosec // G115
}

// Suppress unused-import warning when this file is the only consumer of os.
var _ = os.ErrNotExist
