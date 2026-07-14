package glcb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"

	"gastrolog/internal/chunk"
)

// Reader provides random-access record reads from a GLCB. Record frames are
// sliced straight from the whole-file mapping of the owning MappedBlob —
// every GLCB open path (warm cache, cloud-download promote, pipeline
// cursors) goes through OpenMappedBlob + MappedBlob.Reader(); there is no
// file-descriptor read path (gastrolog-2v9d67).
type Reader struct {
	meta           BlobMeta
	dict           chunk.DictReader
	indexBytes     []byte // record index slice into the mapping
	recordsBaseOff int64  // absolute offset of the records section in the file
	mmapData       []byte // record frames are sliced from this mapping
}

// ReadTOC reads the TOC footer + entries from the tail of an open blob
// file. The footer is a fixed tocFooterSize (44) bytes at the very end;
// it announces how many entries precede it. Each entry is tocEntrySize
// (42) bytes. Exported for callers that need to verify a downloaded
// blob's whole-blob digest without constructing a full Reader (e.g.
// cache-populate integrity checks — gastrolog-grnc3).
//
// Every entry's section range is validated against fileSize: an entry
// with Offset+Size past EOF (corrupt or truncated blob) is rejected here
// so downstream mmap windows (MapSection, LoadSection) never map past
// EOF and SIGBUS on first access.
func ReadTOC(f *os.File, fileSize int64) (BlobTOC, error) {
	if fileSize < int64(tocFooterSize) {
		return BlobTOC{}, errors.New("blob too small for TOC footer")
	}
	var footer [tocFooterSize]byte
	if _, err := f.ReadAt(footer[:], fileSize-int64(tocFooterSize)); err != nil {
		return BlobTOC{}, fmt.Errorf("read TOC footer: %w", err)
	}
	count, _, err := parseTOCFooter(footer[:])
	if err != nil {
		return BlobTOC{}, err
	}
	entriesEnd := fileSize - int64(tocFooterSize)
	entriesStart := entriesEnd - int64(count)*int64(tocEntrySize)
	if entriesStart < 0 {
		return BlobTOC{}, errors.New("blob too small for TOC entries")
	}
	entryBuf := make([]byte, entriesEnd-entriesStart)
	if _, err := f.ReadAt(entryBuf, entriesStart); err != nil {
		return BlobTOC{}, fmt.Errorf("read TOC entries: %w", err)
	}
	toc, err := parseTOCRegion(entryBuf, footer[:])
	if err != nil {
		return BlobTOC{}, err
	}
	// Reject sections that extend past EOF. Offset and Size decode from
	// u32 so both are non-negative and the sum cannot overflow int64.
	for _, e := range toc.Entries {
		if e.Offset+e.Size > fileSize {
			return BlobTOC{}, fmt.Errorf(
				"TOC entry type 0x%02x: section [%d, %d) exceeds blob size %d",
				e.Type, e.Offset, e.Offset+e.Size, fileSize)
		}
	}
	return toc, nil
}

// ParseTOC parses a contiguous tail buffer that includes both the TOC
// entries and the tocFooterSize (44) byte footer. Exported for use by
// remote readers that download the blob's tail by byte range. The buffer
// must be at least `entryCount × tocEntrySize + tocFooterSize` bytes
// long; the entry count is read from the footer.
func ParseTOC(buf []byte) (BlobTOC, error) {
	if len(buf) < tocFooterSize {
		return BlobTOC{}, errors.New("TOC buffer too small for footer")
	}
	footer := buf[len(buf)-tocFooterSize:]
	count, _, err := parseTOCFooter(footer)
	if err != nil {
		return BlobTOC{}, err
	}
	entryBytes := int64(count) * int64(tocEntrySize)
	if int64(len(buf)) < entryBytes+int64(tocFooterSize) {
		return BlobTOC{}, errors.New("TOC buffer too small for declared entry count")
	}
	entries := buf[len(buf)-int(entryBytes)-tocFooterSize : len(buf)-tocFooterSize]
	return parseTOCRegion(entries, footer)
}

// parseTOCFooter validates the magic + version and returns the entry count
// and blob digest from a 44-byte footer.
func parseTOCFooter(buf []byte) (count uint32, digest [32]byte, err error) {
	if len(buf) < tocFooterSize {
		return 0, digest, errors.New("TOC footer buffer too small")
	}
	if string(buf[40:44]) != tocFooterMagic {
		return 0, digest, errors.New("TOC magic mismatch")
	}
	footerVersion := binary.LittleEndian.Uint32(buf[36:40])
	if footerVersion != tocFooterVersion {
		return 0, digest, fmt.Errorf("unsupported TOC footer version %d (want %d)", footerVersion, tocFooterVersion)
	}
	count = binary.LittleEndian.Uint32(buf[0:4])
	copy(digest[:], buf[4:36])
	return count, digest, nil
}

// parseTOCRegion decodes the entry array + footer into a BlobTOC, populating
// both the structured Entries slice and the convenience fields for the
// well-known section magics.
func parseTOCRegion(entryBuf, footerBuf []byte) (BlobTOC, error) {
	count, digest, err := parseTOCFooter(footerBuf)
	if err != nil {
		return BlobTOC{}, err
	}
	if int64(len(entryBuf)) != int64(count)*int64(tocEntrySize) {
		return BlobTOC{}, fmt.Errorf("TOC entry buffer is %d bytes, expected %d", len(entryBuf), int64(count)*int64(tocEntrySize))
	}
	entries := make([]TOCEntry, count)
	for i := range entries {
		off := i * tocEntrySize
		entries[i] = decodeTOCEntry(entryBuf[off : off+tocEntrySize])
	}
	return newBlobTOC(entries, digest), nil
}

// decodeTOCEntry deserializes one tocEntrySize-byte on-disk TOC entry;
// the inverse of encodeTOCEntry (writer.go). raw must be exactly
// tocEntrySize bytes.
func decodeTOCEntry(raw []byte) TOCEntry {
	var e TOCEntry
	e.Type = raw[0]
	e.Version = raw[1]
	e.Offset = int64(binary.LittleEndian.Uint32(raw[2:6]))
	e.Size = int64(binary.LittleEndian.Uint32(raw[6:10]))
	copy(e.Hash[:], raw[10:42])
	return e
}

// Meta returns the blob metadata.
func (rd *Reader) Meta() BlobMeta { return rd.meta }

// ReadRecord reads a single record by position (0-based). The frame is
// decoded straight from the mapping; the payload is detached so the record
// may outlive the cursor.
func (rd *Reader) ReadRecord(pos uint32) (chunk.Record, error) {
	return rd.readRecordAt(pos)
}

// PrewarmSequential was meant to warm the page cache for the whole GLCB
// with pread-style syscalls before a full scan, so cold mmap faults do not
// pin scheduler Ps inside non-preemptible kernel fault handlers under disk
// saturation (gastrolog-1io54g). The syscall warm needs a file descriptor,
// and no mmap-backed Reader ever carried one — MappedBlob closes its fd
// right after mmap — so the warm loop has been unreachable since it landed.
// The dead fd plumbing was removed in gastrolog-2v9d67; re-wiring the warm
// (e.g. reopening MappedBlob.Path()) is a deliberate behavior change that
// belongs to its own issue.
func (rd *Reader) PrewarmSequential() {}

// ReadFanOutRecord reads a record for immediate hand-off to another
// goroutine (retention fan-out). It is the chunk.RecordFanOutSource
// interface seam; the read itself is identical to ReadRecord — the payload
// is always detached from the mapping via cloneMmapRecord.
func (rd *Reader) ReadFanOutRecord(pos uint32) (chunk.Record, error) {
	return rd.readRecordAt(pos)
}

func (rd *Reader) readRecordAt(pos uint32) (chunk.Record, error) {
	if pos >= rd.meta.RecordCount {
		return chunk.Record{}, chunk.ErrNoMoreRecords
	}

	idx, err := recordIndexAt(rd.indexBytes, pos)
	if err != nil {
		return chunk.Record{}, err
	}
	if idx.Offset > math.MaxInt64 {
		return chunk.Record{}, fmt.Errorf("record %d: offset %d overflows int64", pos, idx.Offset)
	}
	absOff := rd.recordsBaseOff + int64(idx.Offset)
	frameEnd := absOff + int64(idx.Size)
	if absOff < 0 || frameEnd > int64(len(rd.mmapData)) {
		return chunk.Record{}, fmt.Errorf("record %d: frame [%d,%d) out of mmap bounds %d", pos, absOff, frameEnd, len(rd.mmapData))
	}
	rec, err := decodeFrame(rd.mmapData[absOff:frameEnd], rd.dict)
	if err != nil {
		return chunk.Record{}, err
	}
	// Records may outlive this cursor (search batching, export queues).
	// Detach payload bytes from the GLCB mmap before the mapping is evicted.
	return cloneMmapRecord(rec), nil
}

// Close drops the Reader's references into the mapping. The mmap's lifetime
// is owned by the MappedBlob that produced this Reader.
func (rd *Reader) Close() error {
	rd.mmapData = nil
	rd.dict = nil
	rd.indexBytes = nil
	return nil
}

// --- Shared helpers ---

// cloneMmapRecord detaches the record's payload from the GLCB mapping. Only
// Raw aliases frame bytes: the Attrs map and its strings come from
// DecodeWithDict, and both DictReader implementations return heap strings
// (MmapStringDict interns; StringDict stores), so cloning them re-copied
// memory that never touched the mmap (gastrolog-11y2iv).
func cloneMmapRecord(rec chunk.Record) chunk.Record {
	if len(rec.Raw) > 0 {
		rec.Raw = slices.Clone(rec.Raw)
	}
	return rec
}

// decodeFrame decodes a record frame into a Record using the given dictionary.
// Every field read is bounds-checked at its own site so the layout and the
// guard never drift. No upstream magic-number length check.
func decodeFrame(frame []byte, dict chunk.DictReader) (chunk.Record, error) {
	off := 0
	var rec chunk.Record

	if off+frameTSSize > len(frame) {
		return chunk.Record{}, errors.New("truncated sourceTS")
	}
	rec.SourceTS = tsFromNanos(binary.LittleEndian.Uint64(frame[off:]))
	off += frameTSSize

	if off+frameTSSize > len(frame) {
		return chunk.Record{}, errors.New("truncated ingestTS")
	}
	rec.IngestTS = tsFromNanos(binary.LittleEndian.Uint64(frame[off:]))
	off += frameTSSize

	if off+frameTSSize > len(frame) {
		return chunk.Record{}, errors.New("truncated writeTS")
	}
	rec.WriteTS = tsFromNanos(binary.LittleEndian.Uint64(frame[off:]))
	off += frameTSSize

	if off+frameGLIDSize > len(frame) {
		return chunk.Record{}, errors.New("truncated ingesterID")
	}
	copy(rec.EventID.IngesterID[:], frame[off:off+frameGLIDSize])
	off += frameGLIDSize

	if off+frameGLIDSize > len(frame) {
		return chunk.Record{}, errors.New("truncated nodeID")
	}
	copy(rec.EventID.NodeID[:], frame[off:off+frameGLIDSize])
	off += frameGLIDSize

	if off+frameIngestSeqSize > len(frame) {
		return chunk.Record{}, errors.New("truncated ingestSeq")
	}
	rec.EventID.IngestSeq = binary.LittleEndian.Uint32(frame[off:])
	off += frameIngestSeqSize
	rec.EventID.IngestTS = rec.IngestTS

	if off+frameAttrCountSize > len(frame) {
		return chunk.Record{}, errors.New("truncated attr count")
	}
	attrCount := int(binary.LittleEndian.Uint16(frame[off:]))
	attrDataLen := frameAttrCountSize + attrCount*frameAttrPairSize
	if off+attrDataLen > len(frame) {
		return chunk.Record{}, errors.New("truncated attrs")
	}
	attrs, err := chunk.DecodeWithDict(frame[off:off+attrDataLen], dict)
	if err != nil {
		return chunk.Record{}, fmt.Errorf("decode attrs: %w", err)
	}
	rec.Attrs = attrs
	off += attrDataLen

	if off+frameRawLenSize > len(frame) {
		return chunk.Record{}, errors.New("truncated raw length")
	}
	rawLen := binary.LittleEndian.Uint32(frame[off:])
	off += frameRawLenSize
	if off+int(rawLen) > len(frame) {
		return chunk.Record{}, errors.New("truncated raw body")
	}
	rec.Raw = frame[off : off+int(rawLen)]
	return rec, nil
}
