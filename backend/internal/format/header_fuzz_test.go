package format

import "testing"

func FuzzDecode(f *testing.F) {
	// Seed corpus: valid header, short inputs, bad signature, all zeros.
	f.Add([]byte{Signature, TypeCloudBlob, 0x01, 0x00})
	f.Add([]byte{Signature, TypeRawLog, 0x01, FlagSealed})
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x00, 0x00})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{Signature, 0x00, 0x00, 0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must never panic. Errors are expected.
		_, _ = Decode(data)
	})
}

func FuzzDecodeAndValidate(f *testing.F) {
	f.Add([]byte{Signature, TypeCloudBlob, 0x01, 0x00}, byte(TypeCloudBlob), byte(0x01))
	f.Add([]byte{}, byte(0x00), byte(0x00))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff}, byte(0x42), byte(0x01))
	f.Add([]byte{Signature, TypeRawLog, 0x02, FlagCompressed}, byte(TypeRawLog), byte(0x02))

	f.Fuzz(func(t *testing.T, data []byte, expectedType, expectedVersion byte) {
		_, _ = DecodeAndValidate(data, expectedType, expectedVersion)
	})
}
