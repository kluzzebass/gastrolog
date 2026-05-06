package cloud

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

// Reader provides random-access record reads from a GLCB on local disk.
// Records are read directly via file.ReadAt — no decompression step.
type Reader struct {
	meta            BlobMeta
	dict            *chunk.StringDict
	index           []recordIndex
	recordsBaseOff  int64    // absolute offset of the records section in the file
	file            *os.File // GLCB file; closed (and removed unless keepFile) on Close()
	keepFile        bool     // if true, Close() does not remove the file (local cache)
}

// NewCacheReader opens a GLCB from a local cache file.
// Unlike NewReader, Close() does NOT remove the file — the cache
// manages the file's lifecycle.
func NewCacheReader(f *os.File) (*Reader, error) {
	rd, err := NewReader(f)
	if err != nil {
		return nil, err
	}
	rd.keepFile = true
	return rd, nil
}

// NewReader opens a GLCB from a local file. Used for local-only vaults
// (the file is the canonical sealed chunk on disk) and for cloud-backed
// vaults after a cloud download has unwrapped the zstd transport layer
// and produced a plain GLCB.
func NewReader(f *os.File) (*Reader, error) {
	// --- Header ---
	var hdr [headerSize]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if _, err := format.DecodeAndValidate(hdr[:format.HeaderSize], format.TypeCloudBlob, formatVersion); err != nil {
		return nil, fmt.Errorf("GLCB header: %w", err)
	}

	meta, dictEntries := decodeHeaderCommon(hdr[:])
	dictSize := binary.LittleEndian.Uint32(hdr[92:96])

	// --- Dictionary ---
	dictBuf := make([]byte, dictSize)
	if _, err := io.ReadFull(f, dictBuf); err != nil {
		return nil, fmt.Errorf("read dict: %w", err)
	}
	dict, err := decodeDictFromBuf(dictBuf, dictEntries)
	if err != nil {
		return nil, err
	}

	// --- Record Index ---
	indexBuf := make([]byte, int(meta.RecordCount)*indexEntrySize)
	if _, err := io.ReadFull(f, indexBuf); err != nil {
		return nil, fmt.Errorf("read index: %w", err)
	}
	index := make([]recordIndex, meta.RecordCount)
	for i := range meta.RecordCount {
		off := int(i) * indexEntrySize
		index[i] = recordIndex{
			Offset: binary.LittleEndian.Uint64(indexBuf[off:]),
			Size:   binary.LittleEndian.Uint32(indexBuf[off+8:]),
		}
	}

	// --- Read TOC from end of blob to populate convenience offsets ---
	recordsBaseOff := int64(headerSize) + int64(dictSize) + int64(meta.RecordCount)*int64(indexEntrySize)
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat GLCB: %w", err)
	}

	toc, err := ReadTOC(f, fileInfo.Size())
	if err != nil {
		return nil, fmt.Errorf("read TOC: %w", err)
	}
	meta.IngestIdxOffset = toc.IngestIdxOffset
	meta.IngestIdxSize = toc.IngestIdxSize
	meta.SourceIdxOffset = toc.SourceIdxOffset
	meta.SourceIdxSize = toc.SourceIdxSize

	return &Reader{
		meta:           meta,
		dict:           dict,
		index:          index,
		recordsBaseOff: recordsBaseOff,
		file:           f,
	}, nil
}

// ReadTOC reads the TOC footer + entries from the tail of an open blob
// file. The footer is a fixed 44 bytes at the very end; it announces how
// many entries precede it. Each entry is 56 bytes. Exported for callers
// that need to verify a downloaded blob's whole-blob digest without
// constructing a full Reader (e.g. cache-populate integrity checks —
// gastrolog-grnc3).
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
	return parseTOCRegion(entryBuf, footer[:])
}

// ParseTOC parses a contiguous tail buffer that includes both the TOC
// entries and the 44-byte footer. Exported for use by remote readers that
// download the blob's tail by byte range. The buffer must be exactly
// `entryCount × 56 + 44` bytes long; the entry count is read from the
// footer.
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
	if pos >= rd.meta.RecordCount {
		return chunk.Record{}, chunk.ErrNoMoreRecords
	}

	idx := rd.index[pos]
	if idx.Offset > math.MaxInt64 {
		return chunk.Record{}, fmt.Errorf("record %d: offset %d overflows int64", pos, idx.Offset)
	}
	buf := make([]byte, idx.Size)
	if _, err := rd.file.ReadAt(buf, rd.recordsBaseOff+int64(idx.Offset)); err != nil {
		return chunk.Record{}, fmt.Errorf("read record %d: %w", pos, err)
	}

	return decodeFrame(buf, rd.dict)
}

// Close closes the file and (unless keepFile is set) removes it.
func (rd *Reader) Close() error {
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

// decodeHeaderCommon parses the 96-byte header (after common header validation).
func decodeHeaderCommon(hdr []byte) (BlobMeta, uint32) {
	var meta BlobMeta
	copy(meta.ChunkID[:], hdr[4:20])
	copy(meta.VaultID[:], hdr[20:36])
	meta.RecordCount = binary.LittleEndian.Uint32(hdr[36:40])
	meta.WriteStart = tsFromNanos(binary.LittleEndian.Uint64(hdr[40:48]))
	meta.WriteEnd = tsFromNanos(binary.LittleEndian.Uint64(hdr[48:56]))
	meta.IngestStart = tsFromNanos(binary.LittleEndian.Uint64(hdr[56:64]))
	meta.IngestEnd = tsFromNanos(binary.LittleEndian.Uint64(hdr[64:72]))
	meta.SourceStart = tsFromNanos(binary.LittleEndian.Uint64(hdr[72:80]))
	meta.SourceEnd = tsFromNanos(binary.LittleEndian.Uint64(hdr[80:88]))
	dictEntries := binary.LittleEndian.Uint32(hdr[88:92])
	return meta, dictEntries
}

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

// decodeFrame decodes a record frame into a Record using the given dictionary.
// Every field read is bounds-checked at its own site so the layout and the
// guard never drift. No upstream magic-number length check.
func decodeFrame(frame []byte, dict *chunk.StringDict) (chunk.Record, error) {
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
