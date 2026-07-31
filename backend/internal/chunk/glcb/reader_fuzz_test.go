package glcb

import (
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/chunk"
)

// FuzzOpenMappedBlob throws arbitrary bytes at the production GLCB open
// path (OpenMappedBlob + Reader + first record read); it must reject
// garbage with errors, never panic.
func FuzzOpenMappedBlob(f *testing.F) {
	// Seed corpus: empty, tiny, header-sized, and slightly larger blobs.
	f.Add([]byte{})
	f.Add(make([]byte, 4))
	f.Add(make([]byte, headerSize))
	f.Add(make([]byte, 256))

	// A plausible but still invalid header (correct signature + type + version).
	hdr := make([]byte, 200)
	hdr[0] = 'i'  // signature
	hdr[1] = 'g'  // TypeGLCB
	hdr[2] = 0x01 // formatVersion
	f.Add(hdr)

	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "fuzz-blob.glcb")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}

		blob, err := OpenMappedBlob(path)
		if err != nil {
			return // errors are expected
		}
		if rd, err := blob.Reader(); err == nil {
			_, _ = rd.ReadRecord(0)
			_ = rd.Close()
		}
		_ = blob.Close()
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
