package file

import (
	"io"
	"os"
	"path/filepath"

	"gastrolog/internal/format"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/klauspost/compress/zstd"
)

// seekableFrameSize is the uncompressed frame size for seekable zstd compression.
// Each frame is independently compressed, enabling random access at frame granularity.
// 256KB balances compression ratio vs. read amplification for typical log records.
const seekableFrameSize = 256 << 10 // 256 KB

// zstdDec is a package-level decoder, concurrent-safe, always available for reads.
var zstdDec *zstd.Decoder

func init() {
	var err error
	zstdDec, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	if err != nil {
		panic("zstd: init decoder: " + err.Error())
	}
}

// compressFile streams a file with a format header through seekable zstd
// compression, atomically replacing the original via temp-file-then-rename.
// The file is read in seekableFrameSize chunks to avoid loading it entirely
// into memory. The FlagCompressed bit is OR'd into the header flags.
func compressFile(path string, enc *zstd.Encoder, mode os.FileMode) error {
	src, err := os.Open(filepath.Clean(path))
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()

	// Read and validate header.
	var hdr [format.HeaderSize]byte
	if _, err := io.ReadFull(src, hdr[:]); err != nil {
		return format.ErrHeaderTooSmall
	}

	// Build new header with FlagCompressed set.
	newHeader := make([]byte, format.HeaderSize)
	copy(newHeader, hdr[:])
	newHeader[3] |= format.FlagCompressed

	// Write to temp file, then rename (atomic).
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".compress-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()

	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}

	// Write header.
	if _, err := tmp.Write(newHeader); err != nil {
		cleanup()
		return err
	}

	// Stream body as seekable zstd: read in fixed-size chunks, each becoming
	// an independent zstd frame for random access. Only one chunk buffer is
	// live at a time instead of the entire file.
	sw, err := seekable.NewWriter(tmp, enc)
	if err != nil {
		cleanup()
		return err
	}
	buf := make([]byte, seekableFrameSize)
	for {
		n, err := io.ReadFull(src, buf)
		if n > 0 {
			if _, werr := sw.Write(buf[:n]); werr != nil {
				cleanup()
				return werr
			}
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			cleanup()
			return err
		}
	}
	if err := sw.Close(); err != nil {
		cleanup()
		return err
	}

	if err := tmp.Chmod(mode); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath) //nolint:gosec // G703: tmpPath is from os.CreateTemp, not user input
		return err
	}

	return os.Rename(tmpPath, path) //nolint:gosec // G703: both paths are internal, not user input
}

// isCompressed reads a file's header and returns whether FlagCompressed is set.
func isCompressed(path string) (bool, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()

	var hdr [format.HeaderSize]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return false, err
	}
	h, err := format.Decode(hdr[:])
	if err != nil {
		return false, err
	}
	return h.Flags&format.FlagCompressed != 0, nil
}

// openSeekableReader opens a compressed data file and returns a seekable reader
// for the data section (after the header). The reader supports ReadAt for random
// access — only the frame(s) covering the requested byte range are decompressed.
// Caller must close both the returned reader and the file.
func openSeekableReader(path string) (seekable.Reader, *os.File, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}

	// Present only the compressed data (after header) to the seekable reader
	// via SectionReader. This ensures the reader's Seek(0, SeekStart) maps to
	// the first compressed frame, not our format header.
	section := io.NewSectionReader(f, int64(format.HeaderSize), info.Size()-int64(format.HeaderSize))
	r, err := seekable.NewReader(section, zstdDec)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return r, f, nil
}
