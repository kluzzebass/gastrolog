// Package cloud defines the single-blob format for cloud-archived chunks.
//
// Header, dictionary, and record index are stored uncompressed.
// Record data uses seekable zstd (256KB independent frames), enabling
// O(1) random access to any record via the offset index.
//
//	Uncompressed prefix:
//	  Header (98 bytes):
//	    [magic:4]          "GLCB" (GastroLog Cloud Blob)
//	    [version:1]        format version (0x01)
//	    [flags:1]          reserved
//	    [chunkID:16]       raw UUIDv7 bytes
//	    [vaultID:16]       raw UUID bytes
//	    [recordCount:u32]  total records
//	    [startTS:i64]      min WriteTS (nanos)
//	    [endTS:i64]        max WriteTS (nanos)
//	    [ingestStart:i64]  min IngestTS (nanos)
//	    [ingestEnd:i64]    max IngestTS (nanos)
//	    [sourceStart:i64]  min SourceTS, 0 = none (nanos)
//	    [sourceEnd:i64]    max SourceTS, 0 = none (nanos)
//	    [dictEntries:u32]  string dictionary size
//	    [dictSize:u32]     total bytes of dictionary section
//
//	  Dictionary (dictSize bytes):
//	    [len:u16][bytes] × dictEntries
//
//	  Record Index (recordCount × 12 bytes):
//	    [offset:u64]       byte offset into decompressed record data
//	    [size:u32]         frame size (excluding the u32 frameLen prefix)
//
//	Seekable zstd section:
//	  Record data compressed in ~256KB independent frames with an appended
//	  seek table for random access via ReadAt.
//
// Record frame encoding:
//
//	[frameLen:u32]     frame size excluding this field
//	[sourceTS:i64]
//	[ingestTS:i64]
//	[writeTS:i64]
//	[ingesterID:16]
//	[ingestSeq:u32]
//	[attrCount:u16]
//	[keyID:u32][valID:u32] × attrCount
//	[rawLen:u32]
//	[raw bytes]
package cloud

import (
	"time"

	"github.com/google/uuid"

	"gastrolog/internal/chunk"
)

var magic = [4]byte{'G', 'L', 'C', 'B'}

const (
	formatVersion = 0x01
	headerSize    = 98 // fixed header before dictionary

	// Record index entry: byte offset (u64) + frame size (u32).
	indexEntrySize = 12

	// Seekable zstd frame size — each frame is independently compressed,
	// enabling random access at frame granularity. Matches the file vault.
	seekableFrameSize = 256 << 10 // 256 KB

	// Minimum record frame: timestamps (3×8) + ingesterID (16) + ingestSeq (4)
	// + attrCount (2) + rawLen (4) = 58 bytes.
	minFrameSize = 58
)

// tsNanos converts a time.Time to nanoseconds, using 0 for the zero value.
// Go's zero time (year 1 CE) predates Unix epoch and doesn't round-trip
// through UnixNano, so we use 0 as a sentinel for "no timestamp."
func tsNanos(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixNano())
}

// tsFromNanos converts nanoseconds back to time.Time, returning the zero
// value for the 0 sentinel.
func tsFromNanos(n uint64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(n)) //nolint:gosec // G115: nanosecond timestamps are always positive in practice
}

// BlobMeta holds the metadata decoded from a cloud blob header.
type BlobMeta struct {
	ChunkID     chunk.ChunkID
	VaultID     uuid.UUID
	RecordCount uint32
	StartTS     time.Time
	EndTS       time.Time
	IngestStart time.Time
	IngestEnd   time.Time
	SourceStart time.Time // zero = no source timestamps
	SourceEnd   time.Time
}

// recordIndex is one entry in the record offset index.
type recordIndex struct {
	Offset uint64 // byte offset into decompressed record data
	Size   uint32 // frame size (excluding the u32 frameLen prefix)
}
