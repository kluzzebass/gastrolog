package segment

import (
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"syscall"
	"time"

	"gastrolog/internal/format"
)

const (
	// SourceIndexEntrySize is the on-disk byte length of one SourceTS index entry.
	SourceIndexEntrySize = format.SizeU64 + format.SizeU32

	formatVersionV1 = 0x01
	formatVersionV2 = 0x02
	formatVersion   = formatVersionV2
)

type sourceIndexEntry struct {
	ts  int64
	pos uint32
}

func compareSourceIndexEntries(a, b sourceIndexEntry) int {
	if a.ts != b.ts {
		if a.ts < b.ts {
			return -1
		}
		return 1
	}
	if a.pos < b.pos {
		return -1
	}
	if a.pos > b.pos {
		return 1
	}
	return 0
}

func decodeSourceIndexEntry(buf []byte) (sourceIndexEntry, error) {
	if len(buf) < SourceIndexEntrySize {
		return sourceIndexEntry{}, ErrFrameTooSmall
	}
	return sourceIndexEntry{
		ts:  int64(binary.LittleEndian.Uint64(buf[0:])), //nolint:gosec // G115: nanosecond timestamps fit in int64
		pos: binary.LittleEndian.Uint32(buf[format.SizeU64:]),
	}, nil
}

func encodeSourceIndexEntry(buf []byte, e sourceIndexEntry) {
	binary.LittleEndian.PutUint64(buf[0:], uint64(e.ts)) //nolint:gosec // G115: nanosecond timestamps stored as uint64
	binary.LittleEndian.PutUint32(buf[format.SizeU64:], e.pos)
}

// sortSourceIndexRegion sorts a mmap'd on-disk source index tail in place.
func sortSourceIndexRegion(data []byte) {
	n := len(data) / SourceIndexEntrySize
	if n == 0 {
		return
	}
	entries := make([]sourceIndexEntry, n)
	for i := range n {
		e, err := decodeSourceIndexEntry(data[i*SourceIndexEntrySize : (i+1)*SourceIndexEntrySize])
		if err != nil {
			return
		}
		entries[i] = e
	}
	slices.SortStableFunc(entries, compareSourceIndexEntries)
	for i, e := range entries {
		encodeSourceIndexEntry(data[i*SourceIndexEntrySize:(i+1)*SourceIndexEntrySize], e)
	}
}

type sourceIndexReader struct {
	r    io.ReaderAt
	base int64
}

func (sir sourceIndexReader) readEntry(i int) (sourceIndexEntry, error) {
	var buf [SourceIndexEntrySize]byte
	off := sir.base + int64(i)*SourceIndexEntrySize
	if _, err := sir.r.ReadAt(buf[:], off); err != nil {
		return sourceIndexEntry{}, err
	}
	return decodeSourceIndexEntry(buf[:])
}

// FindSourceStartPosition binary-searches the on-disk source index for the first
// entry with SourceTS >= start. Probes the file directly; does not load the tail.
func (sf *File) FindSourceStartPosition(start time.Time) (uint32, bool, error) {
	if sf.hdr.IndexOffset == 0 {
		return 0, false, ErrNoIndex
	}
	if sf.hdr.SourceIndexCount == 0 {
		return 0, false, nil
	}
	pos, ok, err := findSourceStartOnDisk(sf.f, int64(sf.hdr.SourceIndexOffset), sf.hdr.SourceIndexCount, start.UnixNano())
	if err != nil {
		return 0, false, err
	}
	return uint32(pos), ok, nil //nolint:gosec // G115: positions bounded by RecordCount
}

func findSourceStartOnDisk(r io.ReaderAt, base int64, count uint32, tsNano int64) (uint64, bool, error) {
	n := int(count)
	if n == 0 {
		return 0, false, nil
	}
	sir := sourceIndexReader{r: r, base: base}

	first, err := sir.readEntry(0)
	if err != nil {
		return 0, false, err
	}
	last, err := sir.readEntry(n - 1)
	if err != nil {
		return 0, false, err
	}
	if tsNano > last.ts {
		return 0, false, nil
	}
	if tsNano <= first.ts {
		return uint64(first.pos), true, nil
	}

	lo, hi := 0, n
	for lo < hi {
		mid := lo + (hi-lo)/2
		entry, err := sir.readEntry(mid)
		if err != nil {
			return 0, false, err
		}
		if entry.ts < tsNano {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	entry, err := sir.readEntry(lo)
	if err != nil {
		return 0, false, err
	}
	return uint64(entry.pos), true, nil
}

func (sf *File) buildSourceIndex(recordEnd uint32) error {
	eventIndexEnd := sf.hdr.IndexOffset + sf.hdr.RecordCount*IndexEntrySize

	var count uint32
	var first, last time.Time
	for pos := range sf.hdr.RecordCount {
		entry, err := sf.readIndexEntry(pos)
		if err != nil {
			return err
		}
		ts, err := sourceTSAtFrame(sf.f, entry.FilePos, recordEnd)
		if err != nil {
			return err
		}
		if ts.IsZero() {
			continue
		}
		count++
		if count == 1 {
			first, last = ts, ts
		} else {
			if ts.Before(first) {
				first = ts
			}
			if ts.After(last) {
				last = ts
			}
		}
	}

	sf.hdr.SourceIndexOffset = eventIndexEnd
	sf.hdr.SourceIndexCount = count
	sf.hdr.FirstSourceTS = first
	sf.hdr.LastSourceTS = last

	if count == 0 {
		sf.hdr.SourceIndexChecksum = 0
		return nil
	}

	sourceEnd := eventIndexEnd + count*SourceIndexEntrySize
	if err := sf.f.Truncate(int64(sourceEnd)); err != nil {
		return err
	}

	var writeIdx uint32
	for pos := range sf.hdr.RecordCount {
		entry, err := sf.readIndexEntry(pos)
		if err != nil {
			return err
		}
		ts, err := sourceTSAtFrame(sf.f, entry.FilePos, recordEnd)
		if err != nil {
			return err
		}
		if ts.IsZero() {
			continue
		}
		var buf [SourceIndexEntrySize]byte
		encodeSourceIndexEntry(buf[:], sourceIndexEntry{ts: ts.UnixNano(), pos: pos})
		off := int64(eventIndexEnd) + int64(writeIdx)*SourceIndexEntrySize
		if _, err := sf.f.WriteAt(buf[:], off); err != nil {
			return err
		}
		writeIdx++
	}

	data, err := syscall.Mmap(int(sf.f.Fd()), 0, int(sourceEnd), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED) //nolint:gosec // G115: file size bounded
	if err != nil {
		return err
	}
	sortSourceIndexRegion(data[eventIndexEnd:sourceEnd])
	if err := syscall.Munmap(data); err != nil {
		return err
	}

	sum, err := sf.checksumRange(eventIndexEnd, sourceEnd)
	if err != nil {
		return err
	}
	sf.hdr.SourceIndexChecksum = sum
	return nil
}

func (sf *File) verifySourceIndexLayout(fileSize int64) error {
	eventIndexEnd := int64(sf.hdr.IndexOffset) + int64(sf.hdr.RecordCount)*IndexEntrySize
	if int64(sf.hdr.SourceIndexOffset) != eventIndexEnd {
		return errors.New("segment source index offset mismatch")
	}
	sourceEnd := int64(sf.hdr.SourceIndexOffset) + int64(sf.hdr.SourceIndexCount)*SourceIndexEntrySize
	if sf.hdr.SourceIndexCount == 0 {
		if fileSize != eventIndexEnd {
			return errors.New("segment trailing bytes after empty source index")
		}
		return nil
	}
	if fileSize < sourceEnd {
		return errors.New("segment source index tail truncated")
	}
	if fileSize > sourceEnd {
		return errors.New("segment trailing bytes after source index")
	}
	sum, err := sf.checksumRange(sf.hdr.SourceIndexOffset, uint32(sourceEnd)) //nolint:gosec // G115: sourceEnd from bounded segment file
	if err != nil {
		return err
	}
	if sum != sf.hdr.SourceIndexChecksum {
		return errors.New("segment source index checksum mismatch")
	}
	return nil
}

func sourceTSAtFrame(r io.ReaderAt, off, recEnd uint32) (time.Time, error) {
	var lenBuf [frameLenPrefixSize]byte
	if _, err := r.ReadAt(lenBuf[:], int64(off)); err != nil {
		return time.Time{}, err
	}
	bodyLen := binary.LittleEndian.Uint32(lenBuf[:])
	if bodyLen == 0 || off+frameLenPrefixSize+bodyLen > recEnd {
		return time.Time{}, ErrFrameLength
	}
	var tsBuf [format.SizeU64]byte
	tsOff := int64(off + frameLenPrefixSize + eventIDWireSize)
	if _, err := r.ReadAt(tsBuf[:], tsOff); err != nil {
		return time.Time{}, err
	}
	return tsFromNanos(binary.LittleEndian.Uint64(tsBuf[:])), nil
}
