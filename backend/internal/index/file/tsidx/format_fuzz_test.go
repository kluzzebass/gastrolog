package tsidx

import (
	"encoding/binary"
	"testing"

	"gastrolog/internal/format"
)

func FuzzDecodeIngestIndex(f *testing.F) {
	// Valid empty ingest index: header + count=0
	valid := make([]byte, headerSize)
	h := format.Header{Type: format.TypeIngestIndex, Version: currentVersion, Flags: format.FlagComplete}
	h.EncodeInto(valid)
	binary.LittleEndian.PutUint32(valid[format.HeaderSize:], 0)
	f.Add(valid)

	// Valid ingest index with one entry
	oneEntry := make([]byte, headerSize+entrySize)
	h.EncodeInto(oneEntry)
	binary.LittleEndian.PutUint32(oneEntry[format.HeaderSize:], 1)
	binary.LittleEndian.PutUint64(oneEntry[headerSize:], 1000)
	binary.LittleEndian.PutUint32(oneEntry[headerSize+8:], 42)
	f.Add(oneEntry)

	// Edge cases
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	// Header present but count claims more entries than data holds
	truncated := make([]byte, headerSize)
	h.EncodeInto(truncated)
	binary.LittleEndian.PutUint32(truncated[format.HeaderSize:], 100)
	f.Add(truncated)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must never panic on any input.
		_, _ = decodeIndex(data, format.TypeIngestIndex)
	})
}

func FuzzDecodeSourceIndex(f *testing.F) {
	valid := make([]byte, headerSize)
	h := format.Header{Type: format.TypeSourceIndex, Version: currentVersion, Flags: format.FlagComplete}
	h.EncodeInto(valid)
	binary.LittleEndian.PutUint32(valid[format.HeaderSize:], 0)
	f.Add(valid)

	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeIndex(data, format.TypeSourceIndex)
	})
}
