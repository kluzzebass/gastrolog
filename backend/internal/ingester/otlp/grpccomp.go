package otlp

import (
	"io"

	"github.com/klauspost/compress/zstd"

	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/gzip" // registers gzip compressor
)

func init() {
	encoding.RegisterCompressor(&zstdCompressor{})
}

// zstdCompressor implements grpc/encoding.Compressor for zstd.
type zstdCompressor struct{}

func (c *zstdCompressor) Name() string { return "zstd" }

func (c *zstdCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedDefault))
}

func (c *zstdCompressor) Decompress(r io.Reader) (io.Reader, error) {
	return zstd.NewReader(r, zstd.WithDecoderMaxMemory(10<<20))
}
