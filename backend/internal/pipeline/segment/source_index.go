package segment

import (
	"encoding/binary"
	"errors"
	"io"
	"slices"
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

func sortSourceIndexEntries(entries []sourceIndexEntry) {
	slices.SortStableFunc(entries, compareSourceIndexEntries)
}

func encodeSourceIndexEntry(buf []byte, e sourceIndexEntry) {
	binary.LittleEndian.PutUint64(buf[0:], uint64(e.ts)) //nolint:gosec // G115: nanosecond timestamps stored as uint64
	binary.LittleEndian.PutUint32(buf[format.SizeU64:], e.pos)
}

func writeSourceIndexRegion(w io.WriterAt, base int64, entries []sourceIndexEntry) error {
	for i, e := range entries {
		var buf [SourceIndexEntrySize]byte
		encodeSourceIndexEntry(buf[:], e)
		off := base + int64(i)*SourceIndexEntrySize
		if _, err := w.WriteAt(buf[:], off); err != nil {
			return err
		}
	}
	return nil
}

// FindSourceStartPosition binary-searches the source index for the first entry with
// SourceTS >= start. Returns (event-order position, true) when found; (0, false) when
// start is after all indexed sources or the index is empty.
func (sf *File) FindSourceStartPosition(start time.Time) (uint32, bool, error) {
	if sf.hdr.IndexOffset == 0 {
		return 0, false, ErrNoIndex
	}
	if sf.hdr.SourceIndexCount == 0 {
		return 0, false, nil
	}
	n := int(sf.hdr.SourceIndexCount)
	data := make([]byte, n*SourceIndexEntrySize)
	if _, err := sf.f.ReadAt(data, int64(sf.hdr.SourceIndexOffset)); err != nil {
		return 0, false, err
	}
	pos, ok := findSourceStartPosition(data, start.UnixNano())
	return uint32(pos), ok, nil //nolint:gosec // G115: positions bounded by RecordCount
}

// findSourceStartPosition binary-searches raw sorted source index bytes.
func findSourceStartPosition(data []byte, tsNano int64) (uint64, bool) {
	n := len(data) / SourceIndexEntrySize
	if n == 0 {
		return 0, false
	}

	readTS := func(i int) int64 {
		off := i * SourceIndexEntrySize
		return int64(binary.LittleEndian.Uint64(data[off:])) //nolint:gosec // G115: nanosecond timestamps fit in int64
	}
	readPos := func(i int) uint32 {
		off := i*SourceIndexEntrySize + format.SizeU64
		return binary.LittleEndian.Uint32(data[off:])
	}

	if tsNano > readTS(n-1) {
		return 0, false
	}
	if tsNano <= readTS(0) {
		return uint64(readPos(0)), true
	}

	lo, hi := 0, n
	for lo < hi {
		mid := lo + (hi-lo)/2
		if readTS(mid) < tsNano {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return uint64(readPos(lo)), true
}

func (sf *File) buildSourceIndex(recordEnd uint32) error {
	eventIndexEnd := sf.hdr.IndexOffset + sf.hdr.RecordCount*IndexEntrySize

	var entries []sourceIndexEntry
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
		entries = append(entries, sourceIndexEntry{ts: ts.UnixNano(), pos: pos})
		if len(entries) == 1 {
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

	sortSourceIndexEntries(entries)

	sf.hdr.SourceIndexOffset = eventIndexEnd
	sf.hdr.SourceIndexCount = uint32(len(entries)) //nolint:gosec // G115: bounded by RecordCount
	sf.hdr.FirstSourceTS = first
	sf.hdr.LastSourceTS = last

	if len(entries) == 0 {
		sf.hdr.SourceIndexChecksum = 0
		return nil
	}

	sourceBytes := int64(len(entries)) * SourceIndexEntrySize
	newSize := int64(eventIndexEnd) + sourceBytes
	if err := sf.f.Truncate(newSize); err != nil {
		return err
	}
	if err := writeSourceIndexRegion(sf.f, int64(eventIndexEnd), entries); err != nil {
		return err
	}
	sum, err := sf.checksumRange(eventIndexEnd, uint32(newSize)) //nolint:gosec // G115: newSize from bounded segment file
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
