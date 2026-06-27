package cloud

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"gastrolog/internal/chunk"
)

func recordIndexAt(indexBytes []byte, pos uint32) (recordIndex, error) {
	off := int(pos) * indexEntrySize
	if off+indexEntrySize > len(indexBytes) {
		return recordIndex{}, chunk.ErrNoMoreRecords
	}
	return recordIndex{
		Offset: binary.LittleEndian.Uint64(indexBytes[off:]),
		Size:   binary.LittleEndian.Uint32(indexBytes[off+8:]),
	}, nil
}

func ingestMonotonicFromIndex(indexBytes []byte, recordsBase int64, recordCount uint32, frameData []byte, readAt func(off int64, buf []byte) error) (bool, error) {
	if recordCount == 0 {
		return true, nil
	}
	var prev uint64
	var ingestBuf [8]byte
	for i := range recordCount {
		idx, err := recordIndexAt(indexBytes, i)
		if err != nil {
			return false, err
		}
		if idx.Offset > math.MaxInt64 {
			return false, fmt.Errorf("record %d: offset overflows int64", i)
		}
		absOff := recordsBase + int64(idx.Offset)
		ingestOff := absOff + 8 // after sourceTS
		frameEnd := absOff + int64(idx.Size)
		if ingestOff+8 > frameEnd {
			return false, fmt.Errorf("record %d: frame too small for ingestTS", i)
		}
		var ingest uint64
		switch {
		case frameData != nil:
			if ingestOff < 0 || ingestOff+8 > int64(len(frameData)) {
				return false, fmt.Errorf("record %d: ingestTS out of mmap bounds", i)
			}
			ingest = binary.LittleEndian.Uint64(frameData[ingestOff : ingestOff+8])
		case readAt != nil:
			if err := readAt(ingestOff, ingestBuf[:]); err != nil {
				return false, fmt.Errorf("record %d ingestTS: %w", i, err)
			}
			ingest = binary.LittleEndian.Uint64(ingestBuf[:])
		default:
			return false, fmt.Errorf("record %d: no frame source", i)
		}
		if i > 0 && ingest < prev {
			return false, nil
		}
		prev = ingest
	}
	return true, nil
}

func (rd *Reader) recordIndexAt(pos uint32) (recordIndex, error) {
	if rd.indexBytes != nil {
		return recordIndexAt(rd.indexBytes, pos)
	}
	if pos >= uint32(len(rd.index)) { //nolint:gosec // G115: index length fits uint32 record counts
		return recordIndex{}, chunk.ErrNoMoreRecords
	}
	return rd.index[pos], nil
}

// IngestMonotonicInMergeOrder reports whether ingest timestamps are
// non-decreasing in merge order without decoding attrs or raw payloads.
func (rd *Reader) IngestMonotonicInMergeOrder() (bool, error) {
	if rd.meta.RecordCount == 0 {
		return true, nil
	}
	if rd.indexBytes == nil {
		var prev time.Time
		for i := range rd.meta.RecordCount {
			rec, err := rd.ReadRecord(i)
			if err != nil {
				return false, err
			}
			if i > 0 && rec.IngestTS.Before(prev) {
				return false, nil
			}
			prev = rec.IngestTS
		}
		return true, nil
	}
	var readAt func(int64, []byte) error
	if rd.mmapData == nil && rd.file != nil {
		f := rd.file
		readAt = func(off int64, buf []byte) error {
			_, err := f.ReadAt(buf, off)
			return err
		}
	}
	return ingestMonotonicFromIndex(rd.indexBytes, rd.recordsBaseOff, rd.meta.RecordCount, rd.mmapData, readAt)
}
