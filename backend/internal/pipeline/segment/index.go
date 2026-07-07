package segment

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"slices"
	"syscall"
	"time"

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

// BuildIndex appends a sorted (EventID, filepos) tail and updates IndexOffset
// and checksums. Writer-created segments build both index tails from the
// in-memory per-frame capture — sort in memory, ONE pwrite per tail, CRCs
// over the in-memory buffers (gastrolog-oin19g). The disk-scan path below
// remains for Open-path (crash-recovered) segments, whose capture is absent:
// it re-reads the file with ~8 preads and issues 2 pwrites per record, which
// stalled the writer's record loop for the whole rebuild at rotation time.
// The segment must not already be indexed.
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

	if len(sf.memEntries) == int(sf.hdr.RecordCount) {
		return sf.buildIndexFromMemory(recordEnd)
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
	if err := sf.buildSourceIndex(recordEnd); err != nil {
		return err
	}
	return sf.writeHeader()
}

// buildIndexFromMemory writes both index tails from the per-append capture:
// in-memory sorts, ONE pwrite per tail, checksums over the in-memory buffers.
// Output is byte-identical to the disk-scan path (same comparators, same
// entry encodings, same header fields) — enforced by test (gastrolog-oin19g).
func (sf *File) buildIndexFromMemory(recordEnd uint32) error {
	entries := make([]memIndexEntry, len(sf.memEntries))
	copy(entries, sf.memEntries)
	slices.SortFunc(entries, func(a, b memIndexEntry) int {
		return compareIndexEntries(a.entry, b.entry)
	})

	idxBuf := make([]byte, len(entries)*IndexEntrySize)
	for i := range entries {
		encodeIndexEntry(idxBuf[i*IndexEntrySize:], entries[i].entry)
	}
	if _, err := sf.f.WriteAt(idxBuf, int64(recordEnd)); err != nil {
		return err
	}
	eventIndexEnd := recordEnd + sf.hdr.RecordCount*IndexEntrySize

	// Source index entries reference EventID-order positions; walk the sorted
	// entries so pos matches what readIndexEntry(pos) would return.
	var srcEntries []sourceIndexEntry
	var first, last time.Time
	for pos, e := range entries {
		if e.sourceNS == 0 {
			continue // zero SourceTS is excluded, as in the disk scan
		}
		ts := tsFromNanos(e.sourceNS)
		if len(srcEntries) == 0 {
			first, last = ts, ts
		} else {
			if ts.Before(first) {
				first = ts
			}
			if ts.After(last) {
				last = ts
			}
		}
		srcEntries = append(srcEntries, sourceIndexEntry{ts: int64(e.sourceNS), pos: uint32(pos)}) //nolint:gosec // G115: pos bounded by RecordCount; nanos fit int64
	}
	slices.SortStableFunc(srcEntries, compareSourceIndexEntries)

	sf.hdr.SourceIndexOffset = eventIndexEnd
	sf.hdr.SourceIndexCount = uint32(len(srcEntries)) //nolint:gosec // G115: bounded by RecordCount
	sf.hdr.FirstSourceTS = first
	sf.hdr.LastSourceTS = last
	sf.hdr.SourceIndexChecksum = 0
	fileEnd := int64(eventIndexEnd)
	if len(srcEntries) > 0 {
		srcBuf := make([]byte, len(srcEntries)*SourceIndexEntrySize)
		for i, e := range srcEntries {
			encodeSourceIndexEntry(srcBuf[i*SourceIndexEntrySize:], e)
		}
		if _, err := sf.f.WriteAt(srcBuf, int64(eventIndexEnd)); err != nil {
			return err
		}
		sf.hdr.SourceIndexChecksum = crc32.ChecksumIEEE(srcBuf)
		fileEnd += int64(len(srcBuf))
	}
	// Drop anything beyond the tails (a lagging header cannot leave trailing
	// bytes for writer-created files, but verifyIndexedLayout requires the
	// exact size, so enforce it the way the disk path's Truncate does).
	if err := sf.f.Truncate(fileEnd); err != nil {
		return err
	}

	sf.hdr.IndexOffset = recordEnd
	sf.hdr.IndexChecksum = crc32.ChecksumIEEE(idxBuf)
	sf.dataEnd = recordEnd
	if sf.recordCRC != nil {
		sf.hdr.SegmentChecksum = sf.recordCRC.Sum32()
	}
	sf.memEntries = nil
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

// ReadViewAtFilePos decodes the frame at filePos into a record.View aliasing
// scratch — no attrs map, no Raw copy, same CRC verification as the Record
// path. The returned buffer replaces the caller's scratch (it may have
// grown); the view is valid only until the buffer's next reuse. For scan
// loops that read many records but consume only fixed fields — the planner's
// bounds pass fully materialized every record for three timestamps, 24GB of
// cumulative garbage on a loaded home (gastrolog-11y2iv).
func (sf *File) ReadViewAtFilePos(filePos uint32, scratch []byte) (record.View, []byte, error) {
	recEnd, err := sf.recordsEnd()
	if err != nil {
		return record.View{}, scratch, err
	}
	if filePos < HeaderSize || filePos >= recEnd {
		return record.View{}, scratch, ErrFrameLength
	}
	return readFrameViewAtBuf(sf.f, int64(filePos), recEnd-filePos, scratch)
}

// ViewAtEventOrder is ReadViewAtFilePos addressed by canonical EventID-order
// position. Same aliasing contract.
func (sf *File) ViewAtEventOrder(pos uint32, scratch []byte) (record.View, []byte, error) {
	entry, err := sf.IndexEntryAt(pos)
	if err != nil {
		return record.View{}, scratch, err
	}
	return sf.ReadViewAtFilePos(entry.FilePos, scratch)
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
	if sf.hdr.Version == formatVersionV2 || sf.hdr.SourceIndexCount > 0 {
		return sf.verifySourceIndexLayout(info.Size())
	}
	if sf.hdr.IndexOffset > 0 {
		eventIndexEnd := int64(sf.hdr.IndexOffset) + int64(sf.hdr.RecordCount)*IndexEntrySize
		if info.Size() != eventIndexEnd {
			return errors.New("segment trailing bytes after EventID index")
		}
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
