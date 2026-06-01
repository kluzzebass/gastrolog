package segment

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	"gastrolog/internal/record"
)

// File is a durable V3 segment on disk. The fixed header is rewritten after
// each append; record data follows the header as [frameLen:u32][frame body] frames.
type File struct {
	f      *os.File
	hdr    Header
	hdrBuf [HeaderSize]byte
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

	writeOff, err := sf.appendOffset()
	if err != nil {
		return err
	}

	frameLen := frameLenPrefixSize + len(body)
	buf := make([]byte, frameLen)
	binary.LittleEndian.PutUint32(buf[0:frameLenPrefixSize], uint32(len(body))) //nolint:gosec // G115: frame bounded by encode
	copy(buf[frameLenPrefixSize:], body)

	if _, err := sf.f.WriteAt(buf, int64(writeOff)); err != nil {
		return err
	}

	sf.hdr.RecordCount++
	if sf.hdr.RecordCount == 1 {
		sf.hdr.FirstIngestTS = rec.EventID.IngestTS
	}
	sf.hdr.LastIngestTS = rec.EventID.IngestTS
	sf.hdr.DataEnd = writeOff

	validEnd := writeOff + uint32(frameLen) //nolint:gosec // G115: segment size bounded in practice
	sum, err := sf.checksumOver(validEnd)
	if err != nil {
		return err
	}
	sf.hdr.SegmentChecksum = sum
	return sf.writeHeader()
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

// ReadAll decodes every valid record frame in the segment.
func (sf *File) ReadAll() ([]record.Record, error) {
	validEnd, err := sf.validDataEnd()
	if err != nil {
		return nil, err
	}
	var out []record.Record
	off := uint32(HeaderSize)
	for off < validEnd {
		rec, n, err := readFrameAt(sf.f, int64(off), validEnd-off)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
		off += n
	}
	return out, nil
}

func (sf *File) readHeader() error {
	n, err := sf.f.ReadAt(sf.hdrBuf[:], 0)
	if err != nil {
		return err
	}
	if n < HeaderSize {
		return ErrHeaderTooSmall
	}
	hdr, err := decodeHeader(sf.hdrBuf[:])
	if err != nil {
		return err
	}
	sf.hdr = hdr
	return nil
}

func (sf *File) writeHeader() error {
	encodeHeader(sf.hdr, sf.hdrBuf[:])
	_, err := sf.f.WriteAt(sf.hdrBuf[:], 0)
	return err
}

// appendOffset is the byte offset where the next frame write starts.
func (sf *File) appendOffset() (uint32, error) {
	end, err := sf.validDataEnd()
	if err != nil {
		return 0, err
	}
	return end, nil
}

// validDataEnd is the exclusive end of committed record bytes on disk.
func (sf *File) validDataEnd() (uint32, error) {
	if sf.hdr.RecordCount == 0 {
		return HeaderSize, nil
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

	sum, err := sf.checksumOver(validEnd)
	if err != nil {
		return err
	}
	sf.hdr.SegmentChecksum = sum
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

func (sf *File) checksumOver(validEnd uint32) (uint32, error) {
	if validEnd <= HeaderSize {
		return 0, nil
	}
	n := int64(validEnd - HeaderSize)
	buf := make([]byte, n)
	if _, err := sf.f.ReadAt(buf, HeaderSize); err != nil {
		return 0, err
	}
	return crc32.ChecksumIEEE(buf), nil
}

func (sf *File) verifyChecksum() error {
	validEnd, err := sf.validDataEnd()
	if err != nil {
		return err
	}
	sum, err := sf.checksumOver(validEnd)
	if err != nil {
		return err
	}
	if sum != sf.hdr.SegmentChecksum {
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
