package token

import (
	"encoding/binary"
	"testing"

	"gastrolog/internal/format"
)

func FuzzDecodeTokenIndex(f *testing.F) {
	// Valid empty token index: header + keyCount=0
	valid := make([]byte, headerSize)
	h := format.Header{Type: format.TypeTokenIndex, Version: currentVersion, Flags: format.FlagComplete}
	h.EncodeInto(valid)
	binary.LittleEndian.PutUint32(valid[format.HeaderSize:], 0)
	f.Add(valid)

	// Valid token index with one entry: token "hi", 1 position at offset 0
	// Key table: tokenLen(2) + "hi"(2) + postingOffset(4) + postingCount(4) = 12
	// Posting blob: 1 position(4) = 4
	buf := make([]byte, headerSize+12+4)
	h.EncodeInto(buf)
	binary.LittleEndian.PutUint32(buf[format.HeaderSize:], 1)
	cursor := headerSize
	binary.LittleEndian.PutUint16(buf[cursor:], 2) // tokenLen=2
	cursor += tokenLenSize
	copy(buf[cursor:], "hi")
	cursor += 2
	binary.LittleEndian.PutUint32(buf[cursor:], 0) // postingOffset=0
	cursor += postingOffsetSize
	binary.LittleEndian.PutUint32(buf[cursor:], 1) // postingCount=1
	cursor += postingCountSize
	binary.LittleEndian.PutUint32(buf[cursor:], 7) // position=7
	f.Add(buf)

	// Edge cases
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	// Header with huge key count
	huge := make([]byte, headerSize)
	h.EncodeInto(huge)
	binary.LittleEndian.PutUint32(huge[format.HeaderSize:], 0xFFFFFFFF)
	f.Add(huge)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must never panic on any input.
		_, _ = decodeIndex(data)
	})
}
