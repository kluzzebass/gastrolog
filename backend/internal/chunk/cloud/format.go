// Package cloud defines the single-blob format for cloud-archived chunks.
//
// Header, dictionary, and record index are stored uncompressed.
// Record data uses seekable zstd (256KB independent frames), enabling
// O(1) random access to any record via the offset index.
//
//	Uncompressed prefix:
//	  Header (96 bytes):
//	    [signature:1]      0x69 ('i') — common header
//	    [type:1]           0x67 ('g') — cloud blob
//	    [version:1]        format version (0x01)
//	    [flags:1]          reserved
//	    [chunkID:16]       raw UUIDv7 bytes
//	    [vaultID:16]       raw UUID bytes
//	    [recordCount:u32]  total records
//	    [writeStart:i64]   min WriteTS (nanos)
//	    [writeEnd:i64]     max WriteTS (nanos)
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
//	TS indexes + TOC (after seekable zstd):
//	  IngestTS Index: [tsNano:i64][pos:u32] × recordCount, sorted by ts
//	  SourceTS Index: [tsNano:i64][pos:u32] × N (excludes zero-TS records), sorted by ts
//
//	TOC entries (56 bytes each, one per section pointed to from the TOC):
//	    [magic:4]           section type (e.g. "ITSI" for ingest TS index)
//	    [version:u32]       per-section version
//	    [offset:u64]        byte offset from blob start
//	    [size:u64]          byte count
//	    [hash:32]           SHA-256 of the section's bytes
//
//	TOC footer (44 bytes, at the very end of the blob):
//	    [entryCount:u32]    number of entries above
//	    [blobDigest:32]     SHA-256 of bytes [0, fileSize - footerSize)
//	                        — every byte of the blob except this footer
//	    [footerVersion:u32] footer schema version
//	    [magic:4]           "GTOC"
//
//	Read protocol: read the last 44 bytes as the footer, validate the
//	magic, then read entryCount × 56 bytes immediately preceding the
//	footer to recover all section pointers + hashes. The blob digest
//	covers everything from the start of the file through the last
//	entry — readers verifying integrity hash that range and compare.
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
	"crypto/sha256"
	"gastrolog/internal/glid"
	"time"

	"gastrolog/internal/chunk"
)

const (
	formatVersion = 0x01
	headerSize    = 96 // fixed header before dictionary

	// Record index entry: byte offset (u64) + frame size (u32).
	indexEntrySize = 12

	// Seekable zstd frame size — each frame is independently compressed,
	// enabling random access at frame granularity. Matches the file vault.
	seekableFrameSize = 256 << 10 // 256 KB

	// Minimum record frame: 3×8 (timestamps) + 16 (ingesterID) + 16
	// (nodeID) + 4 (ingestSeq) + 2 (attrCount) + 4 (rawLen) = 66 bytes,
	// before any attrs (0×8) and raw payload (0). The previous constant
	// was off-by-8 and rejected valid small-record frames; latent because
	// no read path ever decoded a multi-file-sealed chunk through the
	// chunkcloud reader until step 7c stage 2b (gastrolog-24m1t).
	minFrameSize = 66

	// TS index entry: [tsNano:i64][pos:u32] = 12 bytes, sorted by TS.
	tsIndexEntrySize = 12

	// TOC entry: [magic:4][version:u32][offset:u64][size:u64][hash:32].
	tocEntrySize = 56

	// TOC footer: [entryCount:u32][blobDigest:32][footerVersion:u32][magic:4].
	tocFooterSize = 44
	TOCFooterSize = tocFooterSize // exported for byte-range readers

	tocFooterMagic   = "GTOC"
	tocFooterVersion = uint32(1)
)

// Section magics for entries in the TOC. Each magic identifies a kind of
// section the blob can carry; readers look up entries by magic to find
// the section's offset+size+hash without caring about positional order.
const (
	SectionIngestTSIndex = "ITSI"
	SectionSourceTSIndex = "STSI"
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
	VaultID     glid.GLID
	RecordCount uint32
	RawBytes    int64 // uncompressed record data size (0 if unknown)
	WriteStart  time.Time
	WriteEnd    time.Time
	IngestStart time.Time
	IngestEnd   time.Time
	SourceStart time.Time // zero = no source timestamps
	SourceEnd   time.Time

	// TOC fields — populated for v2 blobs with embedded TS indexes.
	IngestIdxOffset int64 // byte offset from blob start (0 = none)
	IngestIdxSize   int64
	SourceIdxOffset int64 // byte offset from blob start (0 = none)
	SourceIdxSize   int64
}

// BlobTOC holds section pointers, per-section hashes, and a whole-blob
// digest decoded from the blob's TOC footer + entries.
//
// Convenience fields (IngestIdxOffset / SourceIdxSize / etc.) are populated
// from Entries during parse for the common section magics (ITSI, STSI).
// Callers that need to read sections introduced after this commit should
// look them up via Entries directly.
type BlobTOC struct {
	Entries    []TOCEntry
	BlobDigest [32]byte
	Version    uint32

	// Convenience accessors for the well-known sections; zero-valued
	// when the section isn't present.
	IngestIdxOffset int64
	IngestIdxSize   int64
	IngestIdxHash   [32]byte
	SourceIdxOffset int64
	SourceIdxSize   int64
	SourceIdxHash   [32]byte
}

// TOCEntry describes one section within a GLCB blob: its type (Magic),
// per-section version, byte range (Offset, Size), and content hash.
type TOCEntry struct {
	Magic   [4]byte
	Version uint32
	Offset  int64
	Size    int64
	Hash    [32]byte
}

// VerifyHash reports whether the given bytes hash to this entry's
// recorded SHA-256. Used by callers (cache fills, byte-range downloads)
// to detect corruption against the FSM-replicated truth.
func (e *TOCEntry) VerifyHash(data []byte) bool {
	return sha256.Sum256(data) == e.Hash
}

// Find returns the entry with the given section magic, or false if no
// entry of that kind is present.
func (t *BlobTOC) Find(magic string) (TOCEntry, bool) {
	for _, e := range t.Entries {
		if string(e.Magic[:]) == magic {
			return e, true
		}
	}
	return TOCEntry{}, false
}

// recordIndex is one entry in the record offset index.
type recordIndex struct {
	Offset uint64 // byte offset into decompressed record data
	Size   uint32 // frame size (excluding the u32 frameLen prefix)
}
