package server

import "testing"

func FuzzProtoToResumeToken(f *testing.F) {
	// Seed corpus: empty, minimal protobuf-like bytes, garbage.
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x0a, 0x00})                                     // empty length-delimited field 1
	f.Add([]byte{0x0a, 0x02, 0x0a, 0x00})                         // nested empty
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}) // all high bits
	f.Add([]byte{0x12, 0x04, 0x08, 0x80, 0x80, 0x01})             // field 2 with varint
	f.Add([]byte("not a protobuf at all"))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must not panic on any input. Errors are fine.
		_, _ = ProtoToResumeToken(data)
	})
}

func FuzzVaultTokenToPositions(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x0a, 0x00})
	f.Add([]byte{0xff, 0xfe, 0xfd})
	f.Add([]byte("garbage input"))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must not panic on any input. Errors are fine.
		_, _ = VaultTokenToPositions(data)
	})
}
