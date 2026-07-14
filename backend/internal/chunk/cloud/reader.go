package cloud

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

// Reader provides random-access record reads from a GLCB on local disk.
// Records are read directly via file.ReadAt — no decompression step.
type Reader struct {
	meta           BlobMeta
	dict           chunk.DictReader
	index          []recordIndex // heap path when indexBytes is nil
	indexBytes     []byte
	indexCount     uint32
	recordsBaseOff int64    // absolute offset of the records section in the file
	mmapData       []byte   // when set, record frames are sliced from this mapping
	dictBuf        []byte   // keeps dict bytes alive for MmapStringDict when not mmap'd
	file           *os.File // GLCB file; closed (and removed unless keepFile) on Close()
	mappedOwner    *MappedBlob
	keepFile       bool // if true, Close() does not remove the file (local cache)
}

// NewCacheReader opens a GLCB from a local cache file.
// Unlike NewReader, Close() does NOT remove the file — the cache
// manages the file's lifecycle.
func NewCacheReader(f *os.File) (*Reader, error) {
	path := f.Name()
	if path != "" {
		if blob, err := OpenMappedBlob(path); err == nil {
			_ = f.Close()
			rd, err := blob.Reader()
			if err != nil {
				_ = blob.Close()
				return nil, err
			}
			rd.mappedOwner = blob
			rd.keepFile = true
			return rd, nil
		}
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind GLCB: %w", err)
	}
	rd, err := NewReader(f)
	if err != nil {
		return nil, err
	}
	rd.keepFile = true
	return rd, nil
}

// NewReader opens a GLCB from a local file.
func NewReader(f *os.File) (*Reader, error) {
	var pre [preambleSize]byte
	if _, err := io.ReadFull(f, pre[:]); err != nil {
		return nil, fmt.Errorf("read preamble: %w", err)
	}
	if _, err := format.DecodeAndValidate(pre[:], format.TypeCloudBlob, formatVersion); err != nil {
		return nil, fmt.Errorf("GLCB preamble: %w", err)
	}

	layoutBuf := make([]byte, layoutMetaSize)
	if _, err := f.ReadAt(layoutBuf, preambleSize); err != nil {
		return nil, fmt.Errorf("read layout meta: %w", err)
	}
	layout, err := decodeBlobLayoutMeta(layoutBuf)
	if err != nil {
		return nil, err
	}

	dictBuf := make([]byte, layout.DictSize)
	if _, err := f.ReadAt(dictBuf, int64(layout.DictOff)); err != nil {
		return nil, fmt.Errorf("read dict: %w", err)
	}
	dict, err := chunk.NewMmapStringDict(dictBuf, layout.DictEntries)
	if err != nil {
		return nil, fmt.Errorf("decode dict: %w", err)
	}

	indexBuf := make([]byte, layout.IndexSize)
	if _, err := f.ReadAt(indexBuf, int64(layout.IndexOff)); err != nil {
		return nil, fmt.Errorf("read index: %w", err)
	}

	fileInfo, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat GLCB: %w", err)
	}
	toc, err := ReadTOC(f, fileInfo.Size())
	if err != nil {
		return nil, fmt.Errorf("read TOC: %w", err)
	}

	return &Reader{
		meta:           layoutMetaToBlobMeta(layout, toc),
		dict:           dict,
		indexBytes:     indexBuf,
		indexCount:     layout.RecordCount,
		dictBuf:        dictBuf,
		recordsBaseOff: int64(layout.RecordsOff),
		file:           f,
	}, nil
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
		raw := entryBuf[off : off+tocEntrySize]
		var e TOCEntry
		e.Type = raw[0]
		e.Version = raw[1]
		e.Offset = int64(binary.LittleEndian.Uint32(raw[2:6]))
		e.Size = int64(binary.LittleEndian.Uint32(raw[6:10]))
		copy(e.Hash[:], raw[10:42])
		entries[i] = e
	}
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
	return toc, nil
}

// Meta returns the blob metadata.
func (rd *Reader) Meta() BlobMeta { return rd.meta }

// ReadRecord reads a single record by position (0-based).
// One file.ReadAt — no decompression step.
func (rd *Reader) ReadRecord(pos uint32) (chunk.Record, error) {
	return rd.readRecordAt(pos, true)
}

// ReadFanOutRecord reads a record for immediate hand-off to another goroutine
// (retention fan-out). On the mmap path the payload is detached once via
// cloneMmapRecord; callers must not retain references across cursor close.
// PrewarmSequential warms the page cache for the whole GLCB with
// pread-style syscalls before a full scan. Frames served from the mmap
// otherwise cold-fault in scan order, pinning scheduler Ps inside
// non-preemptible kernel fault handlers — under disk saturation that
// stalls the entire runtime and manufactures Raft elections
// (gastrolog-1io54g). Syscall reads release their P, and the scan then
// hits warm pages as minor faults. No-op for non-mmap readers;
// best-effort on errors (real I/O problems surface on access).
func (rd *Reader) PrewarmSequential() {
	if rd.mmapData == nil || rd.file == nil {
		return
	}
	info, err := rd.file.Stat()
	if err != nil {
		return
	}
	buf := make([]byte, 1<<20)
	for off := int64(0); off < info.Size(); off += int64(len(buf)) {
		n, err := rd.file.ReadAt(buf, off)
		if n == 0 && err != nil {
			return
		}
	}
}

func (rd *Reader) ReadFanOutRecord(pos uint32) (chunk.Record, error) {
	return rd.readRecordAt(pos, true)
}

func (rd *Reader) readRecordAt(pos uint32, detach bool) (chunk.Record, error) {
	if pos >= rd.meta.RecordCount {
		return chunk.Record{}, chunk.ErrNoMoreRecords
	}

	idx, err := rd.recordIndexAt(pos)
	if err != nil {
		return chunk.Record{}, err
	}
	if idx.Offset > math.MaxInt64 {
		return chunk.Record{}, fmt.Errorf("record %d: offset %d overflows int64", pos, idx.Offset)
	}
	absOff := rd.recordsBaseOff + int64(idx.Offset)
	frameEnd := absOff + int64(idx.Size)
	if rd.mmapData != nil {
		if absOff < 0 || frameEnd > int64(len(rd.mmapData)) {
			return chunk.Record{}, fmt.Errorf("record %d: frame [%d,%d) out of mmap bounds %d", pos, absOff, frameEnd, len(rd.mmapData))
		}
		rec, err := decodeFrame(rd.mmapData[absOff:frameEnd], rd.dict)
		if err != nil {
			return chunk.Record{}, err
		}
		// Records may outlive this cursor (search batching, export queues).
		// Detach payload bytes from the GLCB mmap before the mapping is evicted.
		if detach {
			return cloneMmapRecord(rec), nil
		}
		return rec, nil
	}
	buf := make([]byte, idx.Size)
	if _, err := rd.file.ReadAt(buf, absOff); err != nil {
		return chunk.Record{}, fmt.Errorf("read record %d: %w", pos, err)
	}

	return decodeFrame(buf, rd.dict)
}

// Close closes the file and (unless keepFile is set) removes it.
func (rd *Reader) Close() error {
	if rd.mappedOwner != nil {
		owner := rd.mappedOwner
		rd.mappedOwner = nil
		rd.mmapData = nil
		rd.dict = nil
		rd.indexBytes = nil
		return owner.Close()
	}
	if rd.mmapData != nil {
		// Mapping lifetime is owned by MappedBlob, not this Reader.
		rd.mmapData = nil
		return nil
	}
	var errs []error
	if rd.file != nil {
		name := rd.file.Name()
		if err := rd.file.Close(); err != nil {
			errs = append(errs, err)
		}
		if !rd.keepFile {
			_ = os.Remove(name) //nolint:gosec // name is from os.CreateTemp via rd.file
		}
	}
	return errors.Join(errs...)
}

// --- Shared helpers ---

// decodeDictFromBuf decodes dictionary entries from a byte buffer.
func decodeDictFromBuf(buf []byte, dictEntries uint32) (*chunk.StringDict, error) {
	dict := chunk.NewStringDict()
	off := 0
	for range dictEntries {
		if off+2 > len(buf) {
			return nil, errors.New("truncated dict buffer")
		}
		strLen := int(binary.LittleEndian.Uint16(buf[off:]))
		off += 2
		if off+strLen > len(buf) {
			return nil, errors.New("truncated dict entry")
		}
		if _, err := dict.Add(string(buf[off : off+strLen])); err != nil {
			return nil, fmt.Errorf("add dict entry: %w", err)
		}
		off += strLen
	}
	return dict, nil
}

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

	if off+8 > len(frame) {
		return chunk.Record{}, errors.New("truncated sourceTS")
	}
	rec.SourceTS = tsFromNanos(binary.LittleEndian.Uint64(frame[off:]))
	off += 8

	if off+8 > len(frame) {
		return chunk.Record{}, errors.New("truncated ingestTS")
	}
	rec.IngestTS = tsFromNanos(binary.LittleEndian.Uint64(frame[off:]))
	off += 8

	if off+8 > len(frame) {
		return chunk.Record{}, errors.New("truncated writeTS")
	}
	rec.WriteTS = tsFromNanos(binary.LittleEndian.Uint64(frame[off:]))
	off += 8

	if off+16 > len(frame) {
		return chunk.Record{}, errors.New("truncated ingesterID")
	}
	copy(rec.EventID.IngesterID[:], frame[off:off+16])
	off += 16

	if off+16 > len(frame) {
		return chunk.Record{}, errors.New("truncated nodeID")
	}
	copy(rec.EventID.NodeID[:], frame[off:off+16])
	off += 16

	if off+4 > len(frame) {
		return chunk.Record{}, errors.New("truncated ingestSeq")
	}
	rec.EventID.IngestSeq = binary.LittleEndian.Uint32(frame[off:])
	off += 4
	rec.EventID.IngestTS = rec.IngestTS

	if off+2 > len(frame) {
		return chunk.Record{}, errors.New("truncated attr count")
	}
	attrCount := int(binary.LittleEndian.Uint16(frame[off:]))
	attrDataLen := 2 + attrCount*8
	if off+attrDataLen > len(frame) {
		return chunk.Record{}, errors.New("truncated attrs")
	}
	attrs, err := chunk.DecodeWithDict(frame[off:off+attrDataLen], dict)
	if err != nil {
		return chunk.Record{}, fmt.Errorf("decode attrs: %w", err)
	}
	rec.Attrs = attrs
	off += attrDataLen

	if off+4 > len(frame) {
		return chunk.Record{}, errors.New("truncated raw length")
	}
	rawLen := binary.LittleEndian.Uint32(frame[off:])
	off += 4
	if off+int(rawLen) > len(frame) {
		return chunk.Record{}, errors.New("truncated raw body")
	}
	rec.Raw = frame[off : off+int(rawLen)]
	return rec, nil
}
