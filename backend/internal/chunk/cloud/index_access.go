package cloud

import (
	"encoding/binary"

	"gastrolog/internal/chunk"
)

// recordIndexAt decodes the record-index entry for pos from the raw index
// bytes (a slice into the blob mapping).
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
