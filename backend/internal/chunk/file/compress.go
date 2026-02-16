package file

import (
	"os"
	"path/filepath"

	"gastrolog/internal/format"

	"github.com/klauspost/compress/zstd"
)

// zstdDec is a package-level decoder, concurrent-safe, always available for reads.
var zstdDec *zstd.Decoder

func init() {
	var err error
	zstdDec, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	if err != nil {
		panic("zstd: init decoder: " + err.Error())
	}
}

// compressFile reads a file with a format header, compresses the data section
// with zstd, and atomically replaces the original via temp-file-then-rename.
// The FlagCompressed bit is OR'd into the header flags.
func compressFile(path string, enc *zstd.Encoder, mode os.FileMode) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) < format.HeaderSize {
		return format.ErrHeaderTooSmall
	}

	header := data[:format.HeaderSize]
	body := data[format.HeaderSize:]

	compressed := enc.EncodeAll(body, nil)

	// Build new header with FlagCompressed set.
	newHeader := make([]byte, format.HeaderSize)
	copy(newHeader, header)
	newHeader[3] |= format.FlagCompressed

	// Write to temp file, then rename (atomic).
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".compress-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()

	if _, err := tmp.Write(newHeader); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if _, err := tmp.Write(compressed); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	return os.Rename(tmpPath, path)
}

// readFileData reads a file, validates the header, and returns the data section.
// If FlagCompressed is set, the data is decompressed and (decompressed, true, nil) is returned.
// If not compressed, (nil, false, nil) is returned so the caller can mmap the file instead.
func readFileData(path string) ([]byte, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false, err
	}
	if len(data) < format.HeaderSize {
		return nil, false, format.ErrHeaderTooSmall
	}

	header, err := format.Decode(data[:format.HeaderSize])
	if err != nil {
		return nil, false, err
	}

	if header.Flags&format.FlagCompressed != 0 {
		body := data[format.HeaderSize:]
		decompressed, err := zstdDec.DecodeAll(body, nil)
		if err != nil {
			return nil, false, err
		}
		return decompressed, true, nil
	}

	return nil, false, nil
}
