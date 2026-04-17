package cloud

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/klauspost/compress/zstd"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

// zstdDec is a package-level decoder, concurrent-safe, always available for reads.
var zstdDec *zstd.Decoder

func init() {
	var err error
	zstdDec, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	if err != nil {
		panic("zstd: init decoder: " + err.Error())
	}
}

// Reader provides random-access record reads from a cloud blob.
// The blob must be on local disk (temp file) so seekable zstd can use ReadAt.
type Reader struct {
	meta     BlobMeta
	dict     *chunk.StringDict
	index    []recordIndex
	seek     seekable.Reader
	file     *os.File // temp file; closed and removed on Close()
	keepFile bool     // if true, Close() does not remove the file (cache)
}

// NewCacheReader opens a cloud blob from a local cache file.
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

// NewReader opens a cloud blob from a local file.
// The file is typically a temp file created from an S3 download.
// Accepts both v1 (no TS indexes) and v2 (embedded TS indexes + TOC).
func NewReader(f *os.File) (*Reader, error) {
	// --- Header ---
	var hdr [headerSize]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if _, err := format.DecodeAndValidate(hdr[:format.HeaderSize], format.TypeCloudBlob, formatVersion); err != nil {
		return nil, fmt.Errorf("cloud blob header: %w", err)
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

	// --- Read TOC from end of blob to get TS index section boundaries ---
	dataOffset := int64(headerSize) + int64(dictSize) + int64(meta.RecordCount)*int64(indexEntrySize)
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat blob file: %w", err)
	}

	toc, err := readTOC(f, fileInfo.Size())
	if err != nil {
		return nil, fmt.Errorf("read TOC: %w", err)
	}
	meta.IngestIdxOffset = toc.IngestIdxOffset
	meta.IngestIdxSize = toc.IngestIdxSize
	meta.SourceIdxOffset = toc.SourceIdxOffset
	meta.SourceIdxSize = toc.SourceIdxSize

	// Seekable zstd ends where the ingest TS index section starts.
	dataEnd := toc.IngestIdxOffset
	section := io.NewSectionReader(f, dataOffset, dataEnd-dataOffset)
	seek, err := seekable.NewReader(section, zstdDec)
	if err != nil {
		return nil, fmt.Errorf("open seekable reader: %w", err)
	}

	return &Reader{
		meta:  meta,
		dict:  dict,
		index: index,
		seek:  seek,
		file:  f,
	}, nil
}

// readTOC reads the 48-byte TOC footer from the end of the blob file.
func readTOC(f *os.File, fileSize int64) (BlobTOC, error) {
	var buf [tocSize]byte
	if _, err := f.ReadAt(buf[:], fileSize-tocSize); err != nil {
		return BlobTOC{}, err
	}
	if string(buf[0:4]) != tocMagic {
		return BlobTOC{}, errors.New("TOC magic mismatch")
	}
	// tocVersion at buf[4:8] — reserved for future use
	return BlobTOC{
		IngestIdxOffset: int64(binary.LittleEndian.Uint64(buf[8:16])),  //nolint:gosec // round-trip
		IngestIdxSize:   int64(binary.LittleEndian.Uint64(buf[16:24])), //nolint:gosec // round-trip
		SourceIdxOffset: int64(binary.LittleEndian.Uint64(buf[24:32])), //nolint:gosec // round-trip
		SourceIdxSize:   int64(binary.LittleEndian.Uint64(buf[32:40])), //nolint:gosec // round-trip
	}, nil
}

// ParseTOC parses a 48-byte TOC buffer. Exported for use by the chunk manager
// during backfill of pre-existing blobs.
func ParseTOC(buf []byte) (BlobTOC, error) {
	if len(buf) < tocSize {
		return BlobTOC{}, errors.New("TOC buffer too small")
	}
	if string(buf[0:4]) != tocMagic {
		return BlobTOC{}, errors.New("TOC magic mismatch")
	}
	return BlobTOC{
		IngestIdxOffset: int64(binary.LittleEndian.Uint64(buf[8:16])),  //nolint:gosec // round-trip
		IngestIdxSize:   int64(binary.LittleEndian.Uint64(buf[16:24])), //nolint:gosec // round-trip
		SourceIdxOffset: int64(binary.LittleEndian.Uint64(buf[24:32])), //nolint:gosec // round-trip
		SourceIdxSize:   int64(binary.LittleEndian.Uint64(buf[32:40])), //nolint:gosec // round-trip
	}, nil
}

// Meta returns the blob metadata.
func (rd *Reader) Meta() BlobMeta { return rd.meta }

// ReadRecord reads a single record by position (0-based).
func (rd *Reader) ReadRecord(pos uint32) (chunk.Record, error) {
	if pos >= rd.meta.RecordCount {
		return chunk.Record{}, chunk.ErrNoMoreRecords
	}

	idx := rd.index[pos]
	if idx.Offset > math.MaxInt64 {
		return chunk.Record{}, fmt.Errorf("record %d: offset %d overflows int64", pos, idx.Offset)
	}
	buf := make([]byte, idx.Size)
	if _, err := rd.seek.ReadAt(buf, int64(idx.Offset)); err != nil {
		return chunk.Record{}, fmt.Errorf("read record %d: %w", pos, err)
	}

	return decodeFrame(buf, rd.dict)
}

// Close releases the seekable reader and removes the temp file.
func (rd *Reader) Close() error {
	var errs []error
	if rd.seek != nil {
		if err := rd.seek.Close(); err != nil {
			errs = append(errs, err)
		}
	}
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
func decodeFrame(frame []byte, dict *chunk.StringDict) (chunk.Record, error) {
	if len(frame) < minFrameSize {
		return chunk.Record{}, fmt.Errorf("frame too small: %d < %d", len(frame), minFrameSize)
	}
	off := 0
	var rec chunk.Record

	rec.SourceTS = tsFromNanos(binary.LittleEndian.Uint64(frame[off:]))
	off += 8
	rec.IngestTS = tsFromNanos(binary.LittleEndian.Uint64(frame[off:]))
	off += 8
	rec.WriteTS = tsFromNanos(binary.LittleEndian.Uint64(frame[off:]))
	off += 8
	copy(rec.EventID.IngesterID[:], frame[off:off+16])
	off += 16
	copy(rec.EventID.NodeID[:], frame[off:off+16])
	off += 16
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
