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
)

const (
	currentVersion = 0x01

	countSize  = 4
	headerSize = format.HeaderSize + countSize
	entrySize  = 12 // ts int64 + pos uint32
	ingestFile = "_ingest.idx"
	sourceFile = "_source.idx"
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

// IngestIndexPath returns the path to the ingest index file.
func IngestIndexPath(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), ingestFile)
}

// SourceIndexPath returns the path to the source index file.
func SourceIndexPath(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), sourceFile)
}

// LoadIngestIndex loads the ingest index from disk.
func LoadIngestIndex(dir string, chunkID chunk.ChunkID) ([]Entry, error) {
	path := IngestIndexPath(dir, chunkID)
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("read ingest index: %w", err)
	}
	return decodeIndex(data, format.TypeIngestIndex)
}

// LoadSourceIndex loads the source index from disk.
func LoadSourceIndex(dir string, chunkID chunk.ChunkID) ([]Entry, error) {
	path := SourceIndexPath(dir, chunkID)
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("read source index: %w", err)
	}
	return decodeIndex(data, format.TypeSourceIndex)
}

// IngestTempFilePattern returns the glob pattern for ingest index temp files.
func IngestTempFilePattern(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), ingestFile+".tmp.*")
}

// SourceTempFilePattern returns the glob pattern for source index temp files.
func SourceTempFilePattern(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), sourceFile+".tmp.*")
}
