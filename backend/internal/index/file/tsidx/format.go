package tsidx

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
	"gastrolog/internal/index/idxmmap"
)

const (
	currentVersion = 0x01

	countSize  = 4
	headerSize = format.HeaderSize + countSize
	entrySize  = 12 // ts int64 + pos uint32
	ingestFile = "ingest.idx"
	sourceFile = "source.idx"
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

// Layout: Header (4) | count (4) | (ts int64, pos uint32) * count, sorted by ts.
func encodeIndex(entries []Entry, typ byte) []byte {
	sorted := make([]Entry, len(entries))
	copy(sorted, entries)
	slices.SortFunc(sorted, func(a, b Entry) int {
		if a.TS != b.TS {
			return int(a.TS - b.TS)
		}
		return int(a.Pos) - int(b.Pos)
	})

	buf := make([]byte, headerSize+len(sorted)*entrySize)
	cursor := 0
	h := format.Header{Type: typ, Version: currentVersion, Flags: format.FlagComplete}
	cursor += h.EncodeInto(buf[cursor:])
	binary.LittleEndian.PutUint32(buf[cursor:cursor+countSize], uint32(len(sorted))) //nolint:gosec // G115: entry count bounded by chunk record count
	cursor += countSize

	for _, e := range sorted {
		binary.LittleEndian.PutUint64(buf[cursor:cursor+8], uint64(e.TS)) //nolint:gosec // G115: timestamp nanoseconds stored as uint64 for binary encoding
		cursor += 8
		binary.LittleEndian.PutUint32(buf[cursor:cursor+4], e.Pos)
		cursor += 4
	}
	return buf
}

func decodeIndex(data []byte, expectedType byte) ([]Entry, error) {
	if len(data) < headerSize {
		return nil, ErrIndexTooSmall
	}
	h, err := format.DecodeAndValidate(data, expectedType, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("tsidx: %w", err)
	}
	if h.Flags&format.FlagComplete == 0 {
		return nil, ErrIndexIncomplete
	}
	cursor := format.HeaderSize
	n := binary.LittleEndian.Uint32(data[cursor : cursor+countSize])
	cursor += countSize

	entries := make([]Entry, n)
	for i := range n {
		if cursor+entrySize > len(data) {
			return nil, ErrIndexTooSmall
		}
		entries[i].TS = int64(binary.LittleEndian.Uint64(data[cursor : cursor+8])) //nolint:gosec // G115: nanosecond timestamps fit in int64
		entries[i].Pos = binary.LittleEndian.Uint32(data[cursor+8 : cursor+entrySize])
		cursor += entrySize
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

// SearchIngestFile binary-searches the on-disk ingest index for the first
// entry with TS >= tsNano. O(log n) with fixed 12-byte reads, no heap
// allocation beyond a single entry buffer.
// Returns (recordPos, true) if found, (0, false) otherwise.
func SearchIngestFile(dir string, chunkID chunk.ChunkID, tsNano int64) (uint64, bool, error) {
	_, pos, ok, err := searchTSIndexFile(IngestIndexPath(dir, chunkID), format.TypeIngestIndex, tsNano)
	return uint64(pos), ok, err
}

// SearchIngestFileRank binary-searches the on-disk ingest index and returns
// the **entry index** (rank in IngestTS-sorted order) of the first entry with
// TS >= tsNano. Distinct from SearchIngestFile, which returns the physical
// record position from the entry's pos field — the two differ for chunks
// where physical layout doesn't match IngestTS order (notably ImportRecords-
// built chunks). Used by histogram counting where rank arithmetic gives
// correct per-bucket counts. See gastrolog-66b7x.
func SearchIngestFileRank(dir string, chunkID chunk.ChunkID, tsNano int64) (uint64, bool, error) {
	rank, _, ok, err := searchTSIndexFile(IngestIndexPath(dir, chunkID), format.TypeIngestIndex, tsNano)
	return uint64(rank), ok, err
}

// SearchSourceFile is the source-TS equivalent of SearchIngestFile.
func SearchSourceFile(dir string, chunkID chunk.ChunkID, tsNano int64) (uint64, bool, error) {
	_, pos, ok, err := searchTSIndexFile(SourceIndexPath(dir, chunkID), format.TypeSourceIndex, tsNano)
	return uint64(pos), ok, err
}

// SearchSourceFileRank is the source-TS equivalent of SearchIngestFileRank.
func SearchSourceFileRank(dir string, chunkID chunk.ChunkID, tsNano int64) (uint64, bool, error) {
	rank, _, ok, err := searchTSIndexFile(SourceIndexPath(dir, chunkID), format.TypeSourceIndex, tsNano)
	return uint64(rank), ok, err
}

// searchTSIndexFile binary-searches a TS index file for the first entry
// with TS >= tsNano. Returns (rank, pos, ok, err) so callers can pick the
// appropriate value: rank is the entry's index in the IngestTS-sorted
// index (correct for histogram counting), pos is the physical record
// position in the chunk file (correct for cursor positioning).
func searchTSIndexFile(path string, expectedType byte, tsNano int64) (uint32, uint32, bool, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return 0, 0, false, err
	}
	defer func() { _ = f.Close() }()

	// Read and validate header + count.
	var hdr [headerSize]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		return 0, 0, false, fmt.Errorf("read tsidx header: %w", err)
	}
	h, err := format.DecodeAndValidate(hdr[:format.HeaderSize], expectedType, currentVersion)
	if err != nil {
		return 0, 0, false, fmt.Errorf("tsidx: %w", err)
	}
	if h.Flags&format.FlagComplete == 0 {
		return 0, 0, false, ErrIndexIncomplete
	}
	n := binary.LittleEndian.Uint32(hdr[format.HeaderSize:])
	if n == 0 {
		return 0, 0, false, nil
	}

	// Binary search: first index i where TS[i] >= tsNano.
	var buf [entrySize]byte
	readEntry := func(i uint32) (int64, uint32, error) {
		off := int64(headerSize) + int64(i)*int64(entrySize)
		if _, err := f.ReadAt(buf[:], off); err != nil {
			return 0, 0, err
		}
		ts := int64(binary.LittleEndian.Uint64(buf[:8])) //nolint:gosec // G115
		pos := binary.LittleEndian.Uint32(buf[8:])
		return ts, pos, nil
	}

	// Quick bounds check.
	lastTS, _, err := readEntry(n - 1)
	if err != nil {
		return 0, 0, false, err
	}
	if tsNano > lastTS {
		return 0, 0, false, nil // past all entries
	}

	lo, hi := uint32(0), n
	for lo < hi {
		mid := lo + (hi-lo)/2
		midTS, _, err := readEntry(mid)
		if err != nil {
			return 0, 0, false, err
		}
		if midTS < tsNano {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	_, pos, err := readEntry(lo)
	if err != nil {
		return 0, 0, false, err
	}
	return lo, pos, true, nil
}

// IngestIndexPath returns the path to the ingest index file.
func IngestIndexPath(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), ingestFile)
}

// SourceIndexPath returns the path to the source index file.
func SourceIndexPath(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), sourceFile)
}

// LoadIngestIndex loads the ingest index from disk via mmap. The decoder
// copies all field values out of the mmap region into the returned []Entry,
// so the mmap is released immediately on return — no heap allocation for
// the raw file bytes. See gastrolog-3rvws.
func LoadIngestIndex(dir string, chunkID chunk.ChunkID) ([]Entry, error) {
	entries, err := idxmmap.Load(IngestIndexPath(dir, chunkID), func(data []byte) ([]Entry, error) {
		return decodeIndex(data, format.TypeIngestIndex)
	})
	if errors.Is(err, idxmmap.ErrEmpty) {
		return nil, ErrIndexTooSmall
	}
	return entries, err
}

// LoadSourceIndex loads the source index from disk via mmap. See LoadIngestIndex.
func LoadSourceIndex(dir string, chunkID chunk.ChunkID) ([]Entry, error) {
	entries, err := idxmmap.Load(SourceIndexPath(dir, chunkID), func(data []byte) ([]Entry, error) {
		return decodeIndex(data, format.TypeSourceIndex)
	})
	if errors.Is(err, idxmmap.ErrEmpty) {
		return nil, ErrIndexTooSmall
	}
	return entries, err
}

// IngestTempFilePattern returns the glob pattern for ingest index temp files.
func IngestTempFilePattern(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), ingestFile+".tmp.*")
}

// SourceTempFilePattern returns the glob pattern for source index temp files.
func SourceTempFilePattern(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), sourceFile+".tmp.*")
}

// MmapView holds a long-lived mmap of an on-disk TS index file plus the
// decoded entry count. Binary search reads 12-byte entries directly from
// the mmap region — no heap-allocated entry slice. The mmap stays valid
// for the cached lifetime of the view; release via Close (called from
// the manager's evictTSMmap on DeleteIndexes). See gastrolog-66b7x.
type MmapView struct {
	data []byte // full file mmap (header + entries)
	n    uint32 // entry count from header
}

// OpenIngestMmap opens the chunk's ingest TS index file, validates the
// header, and returns a MmapView for repeated lookups. Returns
// ErrIndexTooSmall if the file is empty.
func OpenIngestMmap(dir string, chunkID chunk.ChunkID) (MmapView, error) {
	return openTSMmap(IngestIndexPath(dir, chunkID), format.TypeIngestIndex)
}

// OpenSourceMmap is the SourceTS counterpart to OpenIngestMmap.
func OpenSourceMmap(dir string, chunkID chunk.ChunkID) (MmapView, error) {
	return openTSMmap(SourceIndexPath(dir, chunkID), format.TypeSourceIndex)
}

func openTSMmap(path string, expectedType byte) (MmapView, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return MmapView{}, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return MmapView{}, fmt.Errorf("stat tsidx: %w", err)
	}
	size := info.Size()
	if size < int64(headerSize) {
		return MmapView{}, ErrIndexTooSmall
	}
	data, err := mmapReadOnly(int(f.Fd()), int(size)) //nolint:gosec // G115
	if err != nil {
		return MmapView{}, fmt.Errorf("mmap tsidx: %w", err)
	}
	h, err := format.DecodeAndValidate(data[:format.HeaderSize], expectedType, currentVersion)
	if err != nil {
		_ = munmap(data)
		return MmapView{}, fmt.Errorf("tsidx: %w", err)
	}
	if h.Flags&format.FlagComplete == 0 {
		_ = munmap(data)
		return MmapView{}, ErrIndexIncomplete
	}
	n := binary.LittleEndian.Uint32(data[format.HeaderSize : format.HeaderSize+countSize])
	return MmapView{data: data, n: n}, nil
}

// Close releases the underlying mmap region.
func (v MmapView) Close() error {
	if v.data == nil {
		return nil
	}
	return munmap(v.data)
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
	off := headerSize + int(i)*entrySize
	return Entry{
		TS:  int64(binary.LittleEndian.Uint64(v.data[off : off+8])), //nolint:gosec // G115
		Pos: binary.LittleEndian.Uint32(v.data[off+8 : off+entrySize]),
	}
}

func (v MmapView) entryTS(i uint32) int64 {
	off := headerSize + int(i)*entrySize
	return int64(binary.LittleEndian.Uint64(v.data[off : off+8])) //nolint:gosec // G115
}
