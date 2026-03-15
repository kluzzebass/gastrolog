package cloud

import (
	"os"
	"testing"

	"gastrolog/internal/chunk"
)

func FuzzNewReader(f *testing.F) {
	// Seed corpus: empty, tiny, header-sized, and slightly larger blobs.
	f.Add([]byte{})
	f.Add(make([]byte, 4))
	f.Add(make([]byte, 96))
	f.Add(make([]byte, 256))

	// A plausible but still invalid header (correct signature + type + version).
	hdr := make([]byte, 200)
	hdr[0] = 'i'  // signature
	hdr[1] = 'g'  // TypeCloudBlob
	hdr[2] = 0x01 // formatVersion
	f.Add(hdr)

	f.Fuzz(func(t *testing.T, data []byte) {
		tmp, err := os.CreateTemp(t.TempDir(), "fuzz-blob-*.glcb")
		if err != nil {
			t.Fatal(err)
		}
		defer tmp.Close()

		if _, err := tmp.Write(data); err != nil {
			t.Fatal(err)
		}
		if _, err := tmp.Seek(0, 0); err != nil {
			t.Fatal(err)
		}

		rd, err := NewReader(tmp)
		if err != nil {
			return // errors are expected
		}
		rd.Close()
	})
}

func FuzzDecodeFrame(f *testing.F) {
	// Minimum valid frame is 58 bytes (3×8 timestamps + 16 ingesterID +
	// 4 ingestSeq + 2 attrCount=0 + 4 rawLen=0).
	minFrame := make([]byte, 58)
	// attrCount = 0 at offset 44, rawLen = 0 at offset 46 — already zero.
	f.Add(minFrame)
	f.Add([]byte{})
	f.Add(make([]byte, 10))
	f.Add(make([]byte, 100))

	f.Fuzz(func(t *testing.T, data []byte) {
		dict := chunk.NewStringDict()
		// Add a few entries so dict lookups can succeed for some inputs.
		dict.Add("key")   //nolint:errcheck
		dict.Add("value") //nolint:errcheck

		// Must never panic.
		_, _ = decodeFrame(data, dict)
	})
}
