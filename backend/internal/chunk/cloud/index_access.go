package cloud

import (
	"encoding/binary"

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

func (rd *Reader) recordIndexAt(pos uint32) (recordIndex, error) {
	if rd.indexBytes != nil {
		return recordIndexAt(rd.indexBytes, pos)
	}
	if pos >= uint32(len(rd.index)) { //nolint:gosec // G115: index length fits uint32 record counts
		return recordIndex{}, chunk.ErrNoMoreRecords
	}
	return rd.index[pos], nil
}
