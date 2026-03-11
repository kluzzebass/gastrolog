package cloud

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"

	"gastrolog/internal/chunk"
)

// Reader streams records from a cloud blob.
// It decompresses and parses the blob lazily — records are decoded
// one at a time via Next().
type Reader struct {
	dec  *zstd.Decoder
	r    io.Reader // decompressed stream
	meta BlobMeta
	dict *chunk.StringDict
	pos  uint32 // records read so far
}

// NewReader opens a cloud blob for reading.
// It reads the header and dictionary eagerly, then yields records
// via Next(). The caller must call Close() when done.
func NewReader(src io.Reader) (*Reader, error) {
	dec, err := zstd.NewReader(src)
	if err != nil {
		return nil, fmt.Errorf("create zstd reader: %w", err)
	}
	r := dec.IOReadCloser()

	// --- Header ---
	var hdr [headerSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		dec.Close()
		return nil, fmt.Errorf("read header: %w", err)
	}
	if hdr[0] != magic[0] || hdr[1] != magic[1] || hdr[2] != magic[2] || hdr[3] != magic[3] {
		dec.Close()
		return nil, fmt.Errorf("invalid magic: %x", hdr[0:4])
	}
	if hdr[4] != formatVersion {
		dec.Close()
		return nil, fmt.Errorf("unsupported version: %d", hdr[4])
	}

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

	// --- Dictionary ---
	dict := chunk.NewStringDict()
	var lenBuf [2]byte
	for range dictEntries {
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			dec.Close()
			return nil, fmt.Errorf("read dict entry length: %w", err)
		}
		strLen := binary.LittleEndian.Uint16(lenBuf[:])
		strBuf := make([]byte, strLen)
		if _, err := io.ReadFull(r, strBuf); err != nil {
			dec.Close()
			return nil, fmt.Errorf("read dict entry: %w", err)
		}
		if _, err := dict.Add(string(strBuf)); err != nil {
			dec.Close()
			return nil, fmt.Errorf("add dict entry: %w", err)
		}
	}

	return &Reader{
		dec:  dec,
		r:    r,
		meta: meta,
		dict: dict,
	}, nil
}

// Meta returns the blob metadata from the header.
func (rd *Reader) Meta() BlobMeta {
	return rd.meta
}

// Next reads the next record. Returns chunk.ErrNoMoreRecords when
// all records have been read.
func (rd *Reader) Next() (chunk.Record, error) {
	if rd.pos >= rd.meta.RecordCount {
		return chunk.Record{}, chunk.ErrNoMoreRecords
	}

	// Read frame length.
	var frameLenBuf [4]byte
	if _, err := io.ReadFull(rd.r, frameLenBuf[:]); err != nil {
		return chunk.Record{}, fmt.Errorf("read frame length: %w", err)
	}
	frameLen := binary.LittleEndian.Uint32(frameLenBuf[:])
	if frameLen < minFrameSize {
		return chunk.Record{}, fmt.Errorf("frame too small: %d", frameLen)
	}

	frame := make([]byte, frameLen)
	if _, err := io.ReadFull(rd.r, frame); err != nil {
		return chunk.Record{}, fmt.Errorf("read frame: %w", err)
	}

	rec, err := rd.decodeFrame(frame)
	if err != nil {
		return chunk.Record{}, fmt.Errorf("record %d: %w", rd.pos, err)
	}
	rd.pos++
	return rec, nil
}

func (rd *Reader) decodeFrame(frame []byte) (chunk.Record, error) {
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

	// Decode attributes via dictionary.
	if off+2 > len(frame) {
		return chunk.Record{}, errors.New("truncated attr count")
	}
	attrCount := int(binary.LittleEndian.Uint16(frame[off:]))
	attrDataLen := 2 + attrCount*8
	if off+attrDataLen > len(frame) {
		return chunk.Record{}, errors.New("truncated attrs")
	}
	attrs, err := chunk.DecodeWithDict(frame[off:off+attrDataLen], rd.dict)
	if err != nil {
		return chunk.Record{}, fmt.Errorf("decode attrs: %w", err)
	}
	rec.Attrs = attrs
	off += attrDataLen

	// Raw body.
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

// Close releases decompressor resources.
func (rd *Reader) Close() error {
	rd.dec.Close()
	return nil
}
