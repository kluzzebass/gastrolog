package kv

import (
	"encoding/binary"
	"testing"

	"gastrolog/internal/format"
)

// makeKVHeader builds a valid file header for a given type byte with zero entries.
func makeKVHeader(typ byte) []byte {
	buf := make([]byte, headerSize)
	h := format.Header{Type: typ, Version: currentVersion, Flags: format.FlagComplete}
	h.EncodeInto(buf)
	buf[format.HeaderSize] = statusComplete // status byte
	binary.LittleEndian.PutUint32(buf[format.HeaderSize+statusSize:], 0)
	return buf
}

func FuzzDecodeKeyIndex(f *testing.F) {
	f.Add(makeKVHeader(format.TypeKVKeyIndex))
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	// Valid header with capped status
	capped := makeKVHeader(format.TypeKVKeyIndex)
	capped[format.HeaderSize] = statusCapped
	f.Add(capped)

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = decodeKeyIndex(data)
	})
}

func FuzzDecodeValueIndex(f *testing.F) {
	f.Add(makeKVHeader(format.TypeKVValueIndex))
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = decodeValueIndex(data)
	})
}

func FuzzDecodeKVIndex(f *testing.F) {
	f.Add(makeKVHeader(format.TypeKVIndex))
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	// Valid header with invalid status byte
	badStatus := makeKVHeader(format.TypeKVIndex)
	badStatus[format.HeaderSize] = 0x99
	f.Add(badStatus)

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = decodeKVIndex(data)
	})
}
