package tsidx

import (
	"encoding/binary"
	"testing"
)

// FuzzDecodeRawEntries hits the embedded ITSI/STSI section decoder with
// random byte sequences to make sure it never panics — only returns
// errors on malformed input. The on-disk shape is `[ts:i64][pos:u32]`
// × N (no header), so the decoder's only validation is "len(data) is a
// multiple of entrySize."
func FuzzDecodeRawEntries(f *testing.F) {
	// Empty section (legal — zero entries).
	f.Add([]byte{})

	// One well-formed entry: 12 bytes.
	oneEntry := make([]byte, entrySize)
	binary.LittleEndian.PutUint64(oneEntry[0:8], 1000)
	binary.LittleEndian.PutUint32(oneEntry[8:12], 42)
	f.Add(oneEntry)

	// Two entries.
	twoEntries := make([]byte, 2*entrySize)
	binary.LittleEndian.PutUint64(twoEntries[0:8], 100)
	binary.LittleEndian.PutUint32(twoEntries[8:12], 0)
	binary.LittleEndian.PutUint64(twoEntries[12:20], 200)
	binary.LittleEndian.PutUint32(twoEntries[20:24], 1)
	f.Add(twoEntries)

	// Truncated / non-multiple inputs that should produce errors but
	// must never panic.
	f.Add([]byte{0x00})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeRawEntries(data)
	})
}
