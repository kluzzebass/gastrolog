package segment

import (
	"encoding/binary"
	"errors"
	"slices"
	"syscall"

	"gastrolog/internal/format"
	"gastrolog/internal/record"
)

const (
	// IndexEntrySize is the on-disk byte length of one EventID index entry.
	IndexEntrySize = eventIDWireSize + format.SizeU32
)

var (
	// ErrIndexAlreadyBuilt is returned when BuildIndex is called twice.
	ErrIndexAlreadyBuilt = errors.New("segment index already built")
	// ErrNoIndex is returned when EventID-order access needs a built index.
	ErrNoIndex = errors.New("segment has no EventID index")
	// ErrIndexBounds is returned when an EventID-order position is out of range.
	ErrIndexBounds = errors.New("EventID-order index position out of range")
)

// IndexEntry maps one record's canonical EventID to its frame offset on disk.
type IndexEntry struct {
	EventID record.EventID
	FilePos uint32 // byte offset of the frame length prefix
}

func encodeIndexEntry(buf []byte, e IndexEntry) {
	encodeEventID(buf[0:eventIDWireSize], e.EventID)
	binary.LittleEndian.PutUint32(buf[eventIDWireSize:], e.FilePos)
}

func decodeIndexEntry(buf []byte) (IndexEntry, error) {
	if len(buf) < IndexEntrySize {
		return IndexEntry{}, ErrFrameTooSmall
	}
	id, err := decodeEventID(buf[0:eventIDWireSize])
	if err != nil {
		return IndexEntry{}, err
	}
	return IndexEntry{
		EventID: id,
		FilePos: binary.LittleEndian.Uint32(buf[eventIDWireSize:]),
	}, nil
}

func compareIndexEntries(a, b IndexEntry) int {
	if c := a.EventID.Compare(b.EventID); c != 0 {
		return c
	}
	if a.FilePos < b.FilePos {
		return -1
	}
	if a.FilePos > b.FilePos {
		return 1
	}
	return 0
}

func sortIndexRegion(data []byte) {
	n := len(data) / IndexEntrySize
	if n == 0 {
		return
	}
	entries := make([]IndexEntry, n)
	for i := range n {
		e, err := decodeIndexEntry(data[i*IndexEntrySize : (i+1)*IndexEntrySize])
		if err != nil {
			return
		}
		entries[i] = e
	}
	slices.SortFunc(entries, compareIndexEntries)
	for i, e := range entries {
		encodeIndexEntry(data[i*IndexEntrySize:(i+1)*IndexEntrySize], e)
	}
}

// BuildIndex scans the record region, appends a sorted (EventID, filepos) tail,
// and updates IndexOffset and checksums. The segment must not already be indexed.
func (sf *File) BuildIndex() error {
	if sf.hdr.RecordCount == 0 {
		return errors.New("empty segment")
	}
	if sf.hdr.IndexOffset != 0 {
		return ErrIndexAlreadyBuilt
	}

	recordEnd, err := sf.validDataEnd()
	if err != nil {
		return err
	}

	indexBytes := int64(sf.hdr.RecordCount) * IndexEntrySize
	newSize := int64(recordEnd) + indexBytes
	if err := sf.f.Truncate(newSize); err != nil {
		return err
	}

	indexBase := recordEnd
	off := uint32(HeaderSize)
	for i := range sf.hdr.RecordCount {
		id, frameLen, err := eventIDAtFrame(sf.f, off, recordEnd-off)
		if err != nil {
			return err
		}
		var buf [IndexEntrySize]byte
		encodeIndexEntry(buf[:], IndexEntry{EventID: id, FilePos: off})
		entryOff := int64(indexBase) + int64(i)*IndexEntrySize
		if _, err := sf.f.WriteAt(buf[:], entryOff); err != nil {
			return err
		}
		off += frameLen
	}
	if off != recordEnd {
		return errors.New("record scan length mismatch")
	}

	data, err := syscall.Mmap(int(sf.f.Fd()), 0, int(newSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED) //nolint:gosec // G115: file size bounded
	if err != nil {
		return err
	}
	sortIndexRegion(data[indexBase:newSize])
	if err := syscall.Munmap(data); err != nil {
		return err
	}

	sum, err := sf.checksumRange(indexBase, uint32(newSize)) //nolint:gosec // G115: newSize from bounded segment file
	if err != nil {
		return err
	}

	sf.hdr.IndexOffset = recordEnd
	sf.hdr.IndexChecksum = sum
	sf.dataEnd = recordEnd
	var recSum uint32
	if sf.recordCRC != nil {
		recSum = sf.recordCRC.Sum32()
	} else {
		var err error
		recSum, err = sf.initRecordCRC(recordEnd)
		if err != nil {
			return err
		}
	}
	sf.hdr.SegmentChecksum = recSum
	return sf.writeHeader()
}

// Finalize builds the EventID index (when non-empty) and marks the segment complete.
func (sf *File) Finalize() error {
	if sf.hdr.RecordCount > 0 {
		if err := sf.BuildIndex(); err != nil {
			return err
		}
	}
	return sf.MarkComplete()
}

// FrameByteLen returns the on-disk byte length of the frame starting at filePos.
func (sf *File) FrameByteLen(filePos uint32) (uint32, error) {
	recEnd, err := sf.recordsEnd()
	if err != nil {
		return 0, err
	}
	if filePos < HeaderSize || filePos >= recEnd {
		return 0, ErrFrameLength
	}
	var lenBuf [frameLenPrefixSize]byte
	if _, err := sf.f.ReadAt(lenBuf[:], int64(filePos)); err != nil {
		return 0, err
	}
	bodyLen := binary.LittleEndian.Uint32(lenBuf[:])
	if bodyLen == 0 || filePos+frameLenPrefixSize+bodyLen > recEnd {
		return 0, ErrFrameLength
	}
	return frameLenPrefixSize + bodyLen, nil
}

// IndexEntryAt returns the index entry at position pos in EventID order.
func (sf *File) IndexEntryAt(pos uint32) (IndexEntry, error) {
	if sf.hdr.IndexOffset == 0 {
		return IndexEntry{}, ErrNoIndex
	}
	if pos >= sf.hdr.RecordCount {
		return IndexEntry{}, ErrIndexBounds
	}
	return sf.readIndexEntry(pos)
}

// ReadRecordAtFilePos decodes the record frame starting at filePos.
func (sf *File) ReadRecordAtFilePos(filePos uint32) (record.Record, error) {
	recEnd, err := sf.recordsEnd()
	if err != nil {
		return record.Record{}, err
	}
	if filePos < HeaderSize || filePos >= recEnd {
		return record.Record{}, ErrFrameLength
	}
	rec, _, err := readFrameAt(sf.f, int64(filePos), recEnd-filePos)
	return rec, err
}

// RecordAtEventOrder returns the record at position pos in canonical EventID order.
func (sf *File) RecordAtEventOrder(pos uint32) (record.Record, error) {
	entry, err := sf.IndexEntryAt(pos)
	if err != nil {
		return record.Record{}, err
	}
	return sf.ReadRecordAtFilePos(entry.FilePos)
}

func (sf *File) readIndexEntry(pos uint32) (IndexEntry, error) {
	off := int64(sf.hdr.IndexOffset) + int64(pos)*IndexEntrySize
	var buf [IndexEntrySize]byte
	if _, err := sf.f.ReadAt(buf[:], off); err != nil {
		return IndexEntry{}, err
	}
	return decodeIndexEntry(buf[:])
}

func (sf *File) recordsEnd() (uint32, error) {
	if sf.hdr.IndexOffset > 0 {
		return sf.hdr.IndexOffset, nil
	}
	return sf.validDataEnd()
}

func (sf *File) verifyIndexedLayout() error {
	info, err := sf.f.Stat()
	if err != nil {
		return err
	}
	indexEnd := int64(sf.hdr.IndexOffset) + int64(sf.hdr.RecordCount)*IndexEntrySize
	if info.Size() < indexEnd {
		return errors.New("segment index tail truncated")
	}
	recSum, err := sf.initRecordCRC(sf.hdr.IndexOffset)
	if err != nil {
		return err
	}
	if recSum != sf.hdr.SegmentChecksum {
		return errors.New("segment checksum mismatch")
	}
	idxSum, err := sf.checksumRange(sf.hdr.IndexOffset, uint32(indexEnd)) //nolint:gosec // G115: indexEnd from bounded segment file
	if err != nil {
		return err
	}
	if idxSum != sf.hdr.IndexChecksum {
		return errors.New("segment index checksum mismatch")
	}
	return nil
}

func (sf *File) checksumRange(start, end uint32) (uint32, error) {
	return crc32IEEEOver(sf.f, start, end)
}

func eventIDAtFrame(r interface {
	ReadAt([]byte, int64) (int, error)
}, off, limit uint32) (record.EventID, uint32, error) {
	var lenBuf [frameLenPrefixSize]byte
	if _, err := r.ReadAt(lenBuf[:], int64(off)); err != nil {
		return record.EventID{}, 0, err
	}
	bodyLen := binary.LittleEndian.Uint32(lenBuf[:])
	if bodyLen == 0 || bodyLen > limit-frameLenPrefixSize {
		return record.EventID{}, 0, ErrFrameLength
	}
	var idBuf [eventIDWireSize]byte
	if _, err := r.ReadAt(idBuf[:], int64(off+frameLenPrefixSize)); err != nil {
		return record.EventID{}, 0, err
	}
	id, err := decodeEventID(idBuf[:])
	if err != nil {
		return record.EventID{}, 0, err
	}
	return id, frameLenPrefixSize + bodyLen, nil
}
