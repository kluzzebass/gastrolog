package segment

import (
	"encoding/binary"
	"errors"
	"io"
	"syscall"
	"time"

	"gastrolog/internal/format"
	"gastrolog/internal/tsindex"
)

const (
	// SourceIndexEntrySize is the on-disk byte length of one SourceTS index entry.
	SourceIndexEntrySize = tsindex.EntrySize

	formatVersion = 0x01
)

// FindSourceStartPosition binary-searches the on-disk source index for the first
// entry with SourceTS >= start. Probes the file directly; does not load the tail.
func (sf *File) FindSourceStartPosition(start time.Time) (uint32, bool, error) {
	if sf.hdr.IndexOffset == 0 {
		return 0, false, ErrNoIndex
	}
	if sf.hdr.SourceIndexCount == 0 {
		return 0, false, nil
	}
	return tsindex.FindStartAt(sf.f, int64(sf.hdr.SourceIndexOffset), int(sf.hdr.SourceIndexCount), start.UnixNano())
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
		tsindex.Encode(buf[:], tsindex.Entry{TS: ts.UnixNano(), Pos: pos})
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
	tsindex.SortRegion(data[eventIndexEnd:sourceEnd])
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
