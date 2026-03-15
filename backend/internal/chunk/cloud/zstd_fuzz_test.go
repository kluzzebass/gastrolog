package cloud

import "testing"

// FuzzDecodeDictFromBuf verifies that decodeDictFromBuf never panics on
// arbitrary byte buffers with arbitrary entry counts. This function parses
// the string dictionary section of a cloud blob header.
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
