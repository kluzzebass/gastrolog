// Package cloud defines the GLCB (GastroLog Chunk Blob) on-disk format —
// a self-describing single-file representation of a sealed chunk.
//
// GLCB is used universally — local-only vaults store a `data.glcb` file
// per sealed chunk; cloud-backed vaults upload the same file (zstd-wrapped
// for transport) to object storage and keep a local cache copy with the
// same `data.glcb` name. The format itself is silent on compression: it
// defines only the on-disk layout, and every section is directly
// readable without a decompression step. Compression, when applied, is
// a generic file-level wrapper produced by the cloud-upload pipeline
// (see ../../chunk/file/manager.go's uploadToCloud). See
// docs/obsoleted/vault_redesign.md decisions 6 and 9.
//
//	Layout (offsets are absolute from the start of the file):
//
//	  Preamble (4 bytes): common header, version 0x01
//	  Layout metadata (128 bytes, fixed offset 4): IDs, bounds, section offsets
//	  Records section: [frameLen:u32][frame] × recordCount
//	  Dictionary: [len:u16][bytes] × dictEntries
//	  Record index: recordCount × 12 bytes
//	  TS indexes, TOC entries, 44-byte footer (magic "GTOC")
//
//	Write order during build: record frames accumulate in a work file in the
//	same directory as the finished GLCB; finalize writes the fixed prefix,
//	copies records, then appends variable sections. Atomic rename to the final
//	name therefore never crosses filesystems.
//
//	Read protocol: preamble + layout at bytes 0–131, then ReadAt using
//	offsets from layout; TOC tail supplies TS index locations and digest.
//
// Record frame encoding (each frame's bytes, NOT including the u32
// frameLen prefix that precedes it in the records section):
//
//	[sourceTS:i64]
//	[ingestTS:i64]
//	[writeTS:i64]
//	[ingesterID:16]
//	[nodeID:16]
//	[ingestSeq:u32]
//	[attrCount:u16]
//	[keyID:u32][valID:u32] × attrCount
//	[rawLen:u32]
//	[raw bytes]
//
// Random access: ReadRecord(pos) is one ReadAt against the file at
// recordsSectionOffset + recordIndex[pos].Offset for recordIndex[pos].Size
// bytes. No decompression step.
package cloud

import (
	"gastrolog/internal/glid"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

// BlobFilename is the on-disk filename of a sealed chunk's GLCB blob,
// always located at <chunkDir>/data.glcb.
const BlobFilename = "data.glcb"

const (
	formatVersion = 0x01

	// Record index entry: byte offset (u64) + frame size (u32).
	indexEntrySize = 12

	// TOC entry: [type:u8][version:u8][offset:u32][size:u32][hash:32].
	tocEntrySize = 42

	// TOC footer: [entryCount:u32][blobDigest:32][footerVersion:u32][magic:4].
	tocFooterSize = 44

	tocFooterMagic   = "GTOC"
	tocFooterVersion = uint32(1)
)

// Record frame layout (see the package comment for the full frame shape).
// The encoders (appendRecordFrame / appendRecordFrameView, encode.go) and
// the decoder (decodeFrame, reader.go) both derive sizes and offsets from
// these constants so the durable byte layout is named exactly once.
const (
	// frameLenSize is the u32 frame-length prefix that precedes each
	// frame in the records section; it is not part of the frame itself.
	frameLenSize = 4

	frameTSSize        = 8  // sourceTS / ingestTS / writeTS: i64 nanoseconds
	frameGLIDSize      = 16 // ingesterID / nodeID
	frameIngestSeqSize = 4  // ingestSeq: u32

	// frameFixedHeaderSize covers every fixed-width field before the
	// attrs block: three timestamps, two GLIDs, and ingestSeq.
	frameFixedHeaderSize = 3*frameTSSize + 2*frameGLIDSize + frameIngestSeqSize

	// Attrs block wire form, mirroring chunk.EncodeWithDict:
	// [attrCount:u16] then attrCount × [keyID:u32][valID:u32].
	frameAttrCountSize = 2
	frameAttrPairSize  = 8

	// frameRawLenSize is the u32 raw-payload length following the attrs.
	frameRawLenSize = 4
)

// Section type bytes for entries in the TOC. Each type identifies a kind
// of section the blob can carry; readers look up entries by type to find
// the section's offset+size+hash without caring about positional order.
// Reuses format.Type so a section's type byte matches the type byte that
// would appear in the same kind of standalone file.
//
// Only the two TS-index sections are emitted by the writer today (emitTail,
// writer.go). The remaining type bytes are forward declarations for the
// standalone index kinds under internal/index/file (token, JSON, KV, attr)
// that are planned to embed as blob sections; they stay declared so the
// type-byte ↔ index-kind mapping is pinned in one place.
const (
	SectionIngestTSIndex  = format.TypeIngestIndex
	SectionSourceTSIndex  = format.TypeSourceIndex
	SectionTokenIndex     = format.TypeTokenIndex
	SectionJSONIndex      = format.TypeJSONIndex
	SectionKVKeyIndex     = format.TypeKVKeyIndex
	SectionKVValueIndex   = format.TypeKVValueIndex
	SectionKVKVIndex      = format.TypeKVIndex
	SectionAttrKeyIndex   = format.TypeAttrKeyIndex
	SectionAttrValueIndex = format.TypeAttrValueIndex
	SectionAttrKVIndex    = format.TypeAttrKVIndex
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
// value for the 0 sentinel. The result is in UTC so records read from
// cloud-backed chunks compare equal (as Go struct values, which is what
// map keys use) to records arriving via proto-wire deserialization
// (timestamppb.AsTime() also yields UTC). The two paths must agree on
// time.Location pointer or EventID-keyed map lookups silently miss.
func tsFromNanos(n uint64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(n)).UTC() //nolint:gosec // G115: nanosecond timestamps are always positive in practice
}

// BlobMeta holds metadata decoded from a GLCB: the front layout block
// (IDs, bounds, record/dict/index offsets) plus TS index locations from
// the TOC tail.
type BlobMeta struct {
	ChunkID     chunk.ChunkID
	VaultID     glid.GLID
	RecordCount uint32
	RawBytes    int64 // uncompressed record data size (0 if unknown)
	// IngestTSMonotonic is the build-time fact that ingest timestamps are
	// non-decreasing in merge order, persisted in the layout meta — never
	// derived by touching record frames (gastrolog-699s7p).
	IngestTSMonotonic bool
	WriteStart        time.Time
	WriteEnd          time.Time
	IngestStart       time.Time
	IngestEnd         time.Time
	SourceStart       time.Time // zero = no source timestamps
	SourceEnd         time.Time

	// TS index section locations from the TOC tail (0 = section absent).
	IngestIdxOffset int64 // byte offset from blob start
	IngestIdxSize   int64
	SourceIdxOffset int64 // byte offset from blob start
	SourceIdxSize   int64
}

// BlobTOC holds section pointers, per-section hashes, and a whole-blob
// digest decoded from the blob's TOC footer + entries.
//
// Convenience fields (IngestIdxOffset / SourceIdxSize / etc.) are populated
// from Entries during parse for the common section magics (ITSI, STSI).
// Callers that need other section types should look them up via Entries.
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

// TOCEntry describes one section within a GLCB blob: its type byte
// (from format.Type), per-section version, byte range (Offset, Size),
// and content hash.
//
// On disk, Offset and Size are encoded as u32 (a single GLCB blob is
// bounded well below 4 GB by chunk policy). In memory we keep them as
// int64 so callers can pass them directly to io.NewSectionReader,
// f.ReadAt, etc., without per-call conversions.
type TOCEntry struct {
	Type    byte
	Version uint8
	Offset  int64
	Size    int64
	Hash    [32]byte
}

// newBlobTOC assembles a BlobTOC from its entries and whole-blob digest,
// populating the convenience accessors for the well-known sections. The
// writer (finalizeTOC) and the readers (parseTOCRegion) both construct
// TOCs through here so the well-known-section population cannot drift.
func newBlobTOC(entries []TOCEntry, digest [32]byte) BlobTOC {
	toc := BlobTOC{
		Entries:    entries,
		BlobDigest: digest,
		Version:    tocFooterVersion,
	}
	if e, ok := toc.Find(SectionIngestTSIndex); ok {
		toc.IngestIdxOffset = e.Offset
		toc.IngestIdxSize = e.Size
		toc.IngestIdxHash = e.Hash
	}
	if e, ok := toc.Find(SectionSourceTSIndex); ok {
		toc.SourceIdxOffset = e.Offset
		toc.SourceIdxSize = e.Size
		toc.SourceIdxHash = e.Hash
	}
	return toc
}

// Find returns the entry with the given section type, or false if no
// entry of that kind is present.
func (t *BlobTOC) Find(sectionType byte) (TOCEntry, bool) {
	for _, e := range t.Entries {
		if e.Type == sectionType {
			return e, true
		}
	}
	return TOCEntry{}, false
}

// recordIndexEntry is one entry in the record offset index.
type recordIndexEntry struct {
	Offset uint64 // byte offset into decompressed record data
	Size   uint32 // frame size (excluding the u32 frameLen prefix)
}
