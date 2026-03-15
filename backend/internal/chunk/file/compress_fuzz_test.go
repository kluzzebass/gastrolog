package file

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/format"
)

// FuzzOpenSeekableReader verifies that openSeekableReader never panics when
// given a file with a valid format header but arbitrary compressed data.
// This exercises the seekable zstd library's handling of malformed frame tables.
func FuzzOpenSeekableReader(f *testing.F) {
	// Seed: just a header, no data.
	hdr := format.Header{Type: format.TypeRawLog, Version: RawLogVersion, Flags: format.FlagSealed | format.FlagCompressed}
	hdrBytes := hdr.Encode()
	f.Add(hdrBytes[:], []byte{})

	// Seed: header + garbage compressed section.
	f.Add(hdrBytes[:], []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x00, 0x00})

	// Seed: header + random bytes.
	f.Add(hdrBytes[:], []byte{0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa})

	// Seed: header + large garbage.
	big := make([]byte, 512)
	for i := range big {
		big[i] = byte(i % 251)
	}
	f.Add(hdrBytes[:], big)

	f.Fuzz(func(t *testing.T, header []byte, body []byte) {
		if len(header) < format.HeaderSize {
			return
		}

		dir := t.TempDir()
		path := filepath.Join(dir, "fuzz.log")

		combined := make([]byte, len(header)+len(body))
		copy(combined, header)
		copy(combined[len(header):], body)

		if err := os.WriteFile(path, combined, 0o644); err != nil {
			t.Fatal(err)
		}

		r, file, err := openSeekableReader(path)
		if err != nil {
			// Expected for malformed data.
			return
		}
		defer file.Close()
		defer r.Close()

		// Try reading — should not panic.
		buf := make([]byte, 4096)
		_, _ = r.ReadAt(buf, 0)
		_, _ = io.ReadAll(r)
	})
}

// FuzzIsCompressed verifies that isCompressed never panics on arbitrary file contents.
func FuzzIsCompressed(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x69}) // just signature
	hdr := format.Header{Type: format.TypeRawLog, Version: RawLogVersion, Flags: format.FlagCompressed}
	hdrBytes := hdr.Encode()
	f.Add(hdrBytes[:])

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		path := filepath.Join(dir, "fuzz.log")

		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}

		_, _ = isCompressed(path)
	})
}
