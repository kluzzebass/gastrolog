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
	meta  BlobMeta
	dict  *chunk.StringDict
	index []recordIndex
	seek  seekable.Reader
	file  *os.File // temp file; closed and removed on Close()
}

// NewReader opens a cloud blob from a local file.
// The file is typically a temp file created from an S3 download.
func NewReader(f *os.File) (*Reader, error) {
	// --- Header ---
	var hdr [headerSize]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if hdr[0] != magic[0] || hdr[1] != magic[1] || hdr[2] != magic[2] || hdr[3] != magic[3] {
		return nil, fmt.Errorf("invalid magic: %x", hdr[0:4])
	}
	if hdr[4] != formatVersion {
		return nil, fmt.Errorf("unsupported version: %d", hdr[4])
	}

	meta, dictEntries := decodeHeaderCommon(hdr[:])
	dictSize := binary.LittleEndian.Uint32(hdr[94:98])

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

	// --- Seekable Zstd Section ---
	dataOffset := int64(headerSize) + int64(dictSize) + int64(meta.RecordCount)*int64(indexEntrySize)
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat blob file: %w", err)
	}
	section := io.NewSectionReader(f, dataOffset, fileInfo.Size()-dataOffset)

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
		_ = os.Remove(name) //nolint:gosec // name is from os.CreateTemp via rd.file
	}
	return errors.Join(errs...)
}

// --- Shared helpers ---

// decodeHeaderCommon parses the 98-byte header.
func decodeHeaderCommon(hdr []byte) (BlobMeta, uint32) {
	var meta BlobMeta
	copy(meta.ChunkID[:], hdr[6:22])
	copy(meta.VaultID[:], hdr[22:38])
	meta.RecordCount = binary.LittleEndian.Uint32(hdr[38:42])
	meta.StartTS = tsFromNanos(binary.LittleEndian.Uint64(hdr[42:50]))
	meta.EndTS = tsFromNanos(binary.LittleEndian.Uint64(hdr[50:58]))
	meta.IngestStart = tsFromNanos(binary.LittleEndian.Uint64(hdr[58:66]))
	meta.IngestEnd = tsFromNanos(binary.LittleEndian.Uint64(hdr[66:74]))
	meta.SourceStart = tsFromNanos(binary.LittleEndian.Uint64(hdr[74:82]))
	meta.SourceEnd = tsFromNanos(binary.LittleEndian.Uint64(hdr[82:90]))
	dictEntries := binary.LittleEndian.Uint32(hdr[90:94])
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
