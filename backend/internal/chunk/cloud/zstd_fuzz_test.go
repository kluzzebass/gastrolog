package cloud

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"gastrolog/internal/chunk"
)

// decodeDictFromBuf decodes dictionary entries from a byte buffer. It is
// the heap-decoding counterpart of chunk.NewMmapStringDict, kept only as
// the fuzz target below — production readers parse the dict in place from
// the blob mapping (gastrolog-2v9d67).
func decodeDictFromBuf(buf []byte, dictEntries uint32) (*chunk.StringDict, error) {
	dict := chunk.NewStringDict()
	off := 0
	for range dictEntries {
		if off+2 > len(buf) {
			return nil, errors.New("truncated dict buffer")
		}
		strLen := int(binary.LittleEndian.Uint16(buf[off:]))
		off += 2
		if off+strLen > len(buf) {
			return nil, errors.New("truncated dict entry")
		}
		if _, err := dict.Add(string(buf[off : off+strLen])); err != nil {
			return nil, fmt.Errorf("add dict entry: %w", err)
		}
		off += strLen
	}
	return dict, nil
}

// FuzzDecodeDictFromBuf verifies that decodeDictFromBuf never panics on
// arbitrary byte buffers with arbitrary entry counts. This function parses
// the string dictionary section of a GLCB blob.
func FuzzDecodeDictFromBuf(f *testing.F) {
	f.Add([]byte{}, uint32(0))
	f.Add([]byte{}, uint32(1))
	f.Add([]byte{0x05, 0x00, 'h', 'e', 'l', 'l', 'o'}, uint32(1))
	f.Add([]byte{0x03, 0x00, 'f', 'o', 'o', 0x03, 0x00, 'b', 'a', 'r'}, uint32(2))
	f.Add([]byte{0xff, 0xff}, uint32(1)) // huge string length
	f.Add([]byte{0x00, 0x00}, uint32(1)) // zero-length string
	f.Add([]byte{0x01, 0x00, 'a', 0x01, 0x00, 'b'}, uint32(2))

	f.Fuzz(func(t *testing.T, buf []byte, entries uint32) {
		// Cap entries to prevent OOM from massive slice allocation.
		if entries > 100_000 {
			return
		}
		_, _ = decodeDictFromBuf(buf, entries)
	})
}
