// Package bodyutil provides HTTP body decompression for ingesters.
package bodyutil

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// zstdDec is a concurrent-safe zstd decoder.
var zstdDec *zstd.Decoder

func init() {
	var err error
	zstdDec, err = zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(0),
		zstd.WithDecoderMaxMemory(10<<20), // 10 MB
	)
	if err != nil {
		panic("bodyutil: init zstd decoder: " + err.Error())
	}
}

// ReadBody reads and decompresses an HTTP request body based on the
// Content-Encoding header value. Supports gzip, zstd, and identity.
// The returned bytes are limited to maxBytes of decompressed output.
func ReadBody(body io.Reader, contentEncoding string, maxBytes int64) ([]byte, error) {
	switch contentEncoding {
	case "zstd":
		compressed, err := io.ReadAll(io.LimitReader(body, maxBytes))
		if err != nil {
			return nil, fmt.Errorf("read compressed body: %w", err)
		}
		decompressed, err := zstdDec.DecodeAll(compressed, nil)
		if err != nil {
			return nil, fmt.Errorf("decompress zstd body: %w", err)
		}
		return decompressed, nil

	case "gzip":
		gz, err := gzip.NewReader(body)
		if err != nil {
			return nil, fmt.Errorf("open gzip reader: %w", err)
		}
		defer func() { _ = gz.Close() }()
		return io.ReadAll(io.LimitReader(gz, maxBytes))

	case "", "identity":
		return io.ReadAll(io.LimitReader(body, maxBytes))

	default:
		return nil, fmt.Errorf("unsupported Content-Encoding: %q", contentEncoding)
	}
}
