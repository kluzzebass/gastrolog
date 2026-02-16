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

// compressFile reads a file with a format header, compresses the data section
// using seekable zstd (enabling random access by uncompressed offset), and
// atomically replaces the original via temp-file-then-rename.
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

	cleanup := func() {
		tmp.Close()
		os.Remove(tmpPath)
	}

	// Write header.
	if _, err := tmp.Write(newHeader); err != nil {
		cleanup()
		return err
	}

	// Write body as seekable zstd: split into fixed-size frames for random access.
	// Each Write() creates an independent zstd frame; Close() appends the seek table.
	sw, err := seekable.NewWriter(tmp, enc)
	if err != nil {
		cleanup()
		return err
	}
	for off := 0; off < len(body); off += seekableFrameSize {
		end := min(off+seekableFrameSize, len(body))
		if _, err := sw.Write(body[off:end]); err != nil {
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
		os.Remove(tmpPath)
		return err
	}

	return os.Rename(tmpPath, path)
}

// isCompressed reads a file's header and returns whether FlagCompressed is set.
func isCompressed(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

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
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	// Present only the compressed data (after header) to the seekable reader
	// via SectionReader. This ensures the reader's Seek(0, SeekStart) maps to
	// the first compressed frame, not our format header.
	section := io.NewSectionReader(f, int64(format.HeaderSize), info.Size()-int64(format.HeaderSize))
	r, err := seekable.NewReader(section, zstdDec)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return r, f, nil
}
