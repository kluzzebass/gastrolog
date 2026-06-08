package segment

import (
	"encoding/binary"
	"errors"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	"gastrolog/internal/format"
	"gastrolog/internal/record"
)

var errSegmentFinalized = errors.New("segment is finalized")

// File is a durable segment on disk. The fixed header is rewritten after
// each append; record data follows the header as [frameLen:u32][frame body] frames.
type File struct {
	f         *os.File
	hdr       Header
	hdrBuf    [HeaderSize]byte
	recordCRC hash.Hash32 // rolling CRC32/IEEE over [HeaderSize:recordsEnd)
	dataEnd   uint32      // exclusive end of committed record bytes (hot-path append anchor)
}

// Create initializes a new empty segment file at path.
func Create(path string, meta Meta) (*File, error) {
	f, err := os.OpenFile(filepath.Clean(path), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	sf := &File{
		f: f,
		hdr: Header{
			ID:      meta.ID,
			VaultID: meta.VaultID,
			DataEnd: HeaderSize,
		},
		recordCRC: crc32.NewIEEE(),
		dataEnd:   HeaderSize,
	}
	if err := sf.writeHeader(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return sf, nil
}

// Open opens an existing segment and reconciles the header against on-disk frames.
func Open(path string) (*File, error) {
	f, err := os.OpenFile(filepath.Clean(path), os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	sf := &File{f: f}
	if err := sf.readHeader(); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := sf.reconcileOnOpen(); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := sf.verifyChecksum(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return sf, nil
}

// Header returns the current decoded header.
func (sf *File) Header() Header {
	return sf.hdr
}

// EncodeFrame builds the frame body for a record (excluding the u32 length prefix).
func EncodeFrame(rec *record.Record, writeTS time.Time) ([]byte, error) {
	return encodeFrame(rec, writeTS)
}

// Append encodes and appends a record frame, then rewrites the header.
// DataEnd is the byte offset where the record starts (recovery anchor).
func (sf *File) Append(rec *record.Record, writeTS time.Time) error {
	if writeTS.IsZero() {
		writeTS = time.Now().UTC()
	}
	body, err := encodeFrame(rec, writeTS)
	if err != nil {
		return err
	}
	return sf.AppendFrame(rec, writeTS, body)
}

// AppendFrame appends a pre-encoded frame body and rewrites the header.
func (sf *File) AppendFrame(rec *record.Record, _ time.Time, body []byte) error {
	if rec == nil {
		return errors.New("nil record")
	}
	if sf.hdr.IndexOffset > 0 {
		return errSegmentFinalized
	}

	writeOff, err := sf.appendOffset()
	if err != nil {
		return err
	}

	var lenPrefix [frameLenPrefixSize]byte
	binary.LittleEndian.PutUint32(lenPrefix[:], uint32(len(body))) //nolint:gosec // G115: frame bounded by encode

	if _, err := sf.f.WriteAt(lenPrefix[:], int64(writeOff)); err != nil {
		return err
	}
	if _, err := sf.f.WriteAt(body, int64(writeOff+frameLenPrefixSize)); err != nil {
		return err
	}

	if sf.recordCRC == nil {
		sf.recordCRC = crc32.NewIEEE()
	}
	if _, err := sf.recordCRC.Write(lenPrefix[:]); err != nil {
		return err
	}
	if _, err := sf.recordCRC.Write(body); err != nil {
		return err
	}

	frameLen := frameLenPrefixSize + len(body)

	sf.hdr.RecordCount++
	if sf.hdr.RecordCount == 1 {
		sf.hdr.FirstIngestTS = rec.EventID.IngestTS
	}
	sf.hdr.LastIngestTS = rec.EventID.IngestTS
	sf.hdr.DataEnd = writeOff
	sf.hdr.SegmentChecksum = sf.recordCRC.Sum32()
	sf.dataEnd = writeOff + uint32(frameLen) //nolint:gosec // G115: frame bounded by encode
	return sf.writeHeader()
}

// Sync persists appended frames and header rewrites to stable storage.
func (sf *File) Sync() error {
	return sf.f.Sync()
}

// Size returns the current on-disk file length.
func (sf *File) Size() (int64, error) {
	info, err := sf.f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// MarkComplete sets the completed flag in the header (working→completed rename
// is the caller's responsibility).
func (sf *File) MarkComplete() error {
	sf.hdr.Flags |= FlagComplete
	return sf.writeHeader()
}

// Close closes the underlying file.
func (sf *File) Close() error {
	return sf.f.Close()
}

// ReadAll decodes every valid record frame in the segment record region.
func (sf *File) ReadAll() ([]record.Record, error) {
	recEnd, err := sf.recordsEnd()
	if err != nil {
		return nil, err
	}
	var out []record.Record
	if sf.hdr.RecordCount > 0 {
		out = make([]record.Record, 0, sf.hdr.RecordCount)
	}
	off := uint32(HeaderSize)
	for off < recEnd {
		rec, n, err := readFrameAt(sf.f, int64(off), recEnd-off)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
		off += n
	}
	return out, nil
}

func (sf *File) readHeader() error {
	info, err := sf.f.Stat()
	if err != nil {
		return err
	}
	readLen := int64(HeaderSize)
	if info.Size() < readLen {
		if info.Size() < int64(HeaderSizeV1) {
			return ErrHeaderTooSmall
		}
		readLen = int64(HeaderSizeV1)
	}
	n, err := sf.f.ReadAt(sf.hdrBuf[:readLen], 0)
	if err != nil {
		return err
	}
	if int64(n) < readLen {
		return ErrHeaderTooSmall
	}
	hdr, err := decodeHeader(sf.hdrBuf[:n])
	if err != nil {
		return err
	}
	sf.hdr = hdr
	return nil
}

func (sf *File) writeHeader() error {
	sf.hdr.Type = format.TypeSegment
	sf.hdr.Version = formatVersion
	encodeHeader(sf.hdr, sf.hdrBuf[:])
	_, err := sf.f.WriteAt(sf.hdrBuf[:], 0)
	return err
}

// appendOffset is the byte offset where the next frame write starts.
func (sf *File) appendOffset() (uint32, error) {
	return sf.dataEnd, nil
}

// validDataEnd is the exclusive end of committed record bytes on disk.
func (sf *File) validDataEnd() (uint32, error) {
	if sf.hdr.RecordCount == 0 {
		return HeaderSize, nil
	}
	if sf.dataEnd > sf.hdr.DataEnd {
		return sf.dataEnd, nil
	}
	_, n, err := sf.frameAt(sf.hdr.DataEnd)
	if err != nil {
		return 0, err
	}
	return sf.hdr.DataEnd + n, nil
}

func (sf *File) frameAt(off uint32) (record.Record, uint32, error) {
	info, err := sf.f.Stat()
	if err != nil {
		return record.Record{}, 0, err
	}
	if int64(off) >= info.Size() {
		return record.Record{}, 0, ErrFrameLength
	}
	remaining := uint32(info.Size() - int64(off)) //nolint:gosec // G115: segment file size bounded
	return readFrameAt(sf.f, int64(off), remaining)
}

// reconcileOnOpen rebuilds header fields and truncates any torn tail. The
// common case uses DataEnd as an O(1) anchor: read the frame there, valid data
// ends at the end of that frame, truncate anything after. A short forward scan
// picks up header lag (frame written before header rewrite). If the anchor
// frame is bad, fall back to scanning from the front.
func (sf *File) reconcileOnOpen() error {
	if sf.hdr.IndexOffset > 0 {
		if err := sf.verifyIndexedLayout(); err != nil {
			return err
		}
		sf.dataEnd = sf.hdr.IndexOffset
		return nil
	}

	info, err := sf.f.Stat()
	if err != nil {
		return err
	}
	fileSize := uint32(info.Size()) //nolint:gosec // G115: segment file size bounded

	validEnd, lastStart, count, firstTS, lastTS, err := sf.reconcileFromAnchor(fileSize)
	if err != nil {
		validEnd, lastStart, count, firstTS, lastTS = sf.resyncFromFront(fileSize)
	}

	if int64(validEnd) < info.Size() {
		if err := sf.f.Truncate(int64(validEnd)); err != nil {
			return err
		}
	}

	sf.hdr.RecordCount = count
	sf.hdr.FirstIngestTS = firstTS
	sf.hdr.LastIngestTS = lastTS
	if count == 0 {
		sf.hdr.DataEnd = HeaderSize
	} else {
		sf.hdr.DataEnd = lastStart
	}

	sum, err := sf.initRecordCRC(validEnd)
	if err != nil {
		return err
	}
	sf.hdr.SegmentChecksum = sum
	sf.dataEnd = validEnd
	return sf.writeHeader()
}

func (sf *File) reconcileFromAnchor(fileSize uint32) (validEnd, lastStart, count uint32, firstTS, lastTS time.Time, err error) {
	if sf.hdr.RecordCount == 0 {
		validEnd, lastStart, count, firstTS, lastTS = sf.scanForward(HeaderSize, fileSize, 0, time.Time{}, time.Time{})
		return validEnd, lastStart, count, firstTS, lastTS, nil
	}

	rec, n, err := sf.frameAt(sf.hdr.DataEnd)
	if err != nil {
		return 0, 0, 0, time.Time{}, time.Time{}, err
	}

	validEnd = sf.hdr.DataEnd + n
	count = sf.hdr.RecordCount
	firstTS = sf.hdr.FirstIngestTS
	lastTS = rec.EventID.IngestTS
	validEnd, lastStart, count, firstTS, lastTS = sf.scanForward(validEnd, fileSize, count, firstTS, lastTS)
	return validEnd, lastStart, count, firstTS, lastTS, nil
}

func (sf *File) scanForward(off, fileSize, count uint32, firstTS, lastTS time.Time) (validEnd, lastStart, outCount uint32, outFirst, outLast time.Time) {
	validEnd = off
	if count == 0 {
		lastStart = HeaderSize
	} else {
		lastStart = sf.hdr.DataEnd
	}
	outCount = count
	outFirst = firstTS
	outLast = lastTS

	for off < fileSize {
		rec, n, readErr := readFrameAt(sf.f, int64(off), fileSize-off)
		if readErr != nil {
			break
		}
		outCount++
		if outCount == 1 {
			outFirst = rec.EventID.IngestTS
		}
		lastStart = off
		outLast = rec.EventID.IngestTS
		validEnd = off + n
		off += n
	}
	return validEnd, lastStart, outCount, outFirst, outLast
}

func (sf *File) resyncFromFront(fileSize uint32) (validEnd, lastStart, count uint32, firstTS, lastTS time.Time) {
	return sf.scanForward(HeaderSize, fileSize, 0, time.Time{}, time.Time{})
}

func (sf *File) initRecordCRC(recEnd uint32) (uint32, error) {
	h := crc32.NewIEEE()
	if recEnd > HeaderSize {
		if err := crc32IEEEFeed(h, sf.f, HeaderSize, recEnd); err != nil {
			return 0, err
		}
	}
	sf.recordCRC = h
	return h.Sum32(), nil
}

func (sf *File) verifyChecksum() error {
	if sf.hdr.IndexOffset > 0 {
		return nil // verifyIndexedLayout in reconcileOnOpen already checked both regions.
	}
	if sf.recordCRC == nil {
		return errors.New("segment record CRC not initialized")
	}
	if sf.recordCRC.Sum32() != sf.hdr.SegmentChecksum {
		return errors.New("segment checksum mismatch")
	}
	return nil
}

func readFrameAt(r io.ReaderAt, offset int64, limit uint32) (record.Record, uint32, error) {
	var lenBuf [4]byte
	if _, err := r.ReadAt(lenBuf[:], offset); err != nil {
		return record.Record{}, 0, err
	}
	bodyLen := binary.LittleEndian.Uint32(lenBuf[:])
	if bodyLen == 0 || bodyLen > limit-frameLenPrefixSize {
		return record.Record{}, 0, ErrFrameLength
	}
	body := make([]byte, bodyLen)
	if _, err := r.ReadAt(body, offset+frameLenPrefixSize); err != nil {
		return record.Record{}, 0, err
	}
	rec, err := decodeFrameBody(body)
	if err != nil {
		return record.Record{}, 0, err
	}
	return rec, frameLenPrefixSize + bodyLen, nil
}
