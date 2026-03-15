package bodyutil

import (
	"bytes"
	"compress/gzip"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func FuzzReadBody(f *testing.F) {
	// Valid gzip data.
	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	_, _ = gw.Write([]byte("hello gzip world"))
	_ = gw.Close()
	f.Add(gzBuf.Bytes(), "gzip")

	// Valid zstd data.
	enc, _ := zstd.NewWriter(nil)
	zstdData := enc.EncodeAll([]byte("hello zstd world"), nil)
	f.Add(zstdData, "zstd")

	// Identity / no encoding.
	f.Add([]byte("plain text body"), "")
	f.Add([]byte("plain text body"), "identity")

	// Unsupported encoding.
	f.Add([]byte("data"), "br")
	f.Add([]byte("data"), "deflate")

	// Garbage data with valid encoding headers.
	f.Add([]byte{0xff, 0xfe, 0x00, 0x01, 0x02}, "gzip")
	f.Add([]byte{0xff, 0xfe, 0x00, 0x01, 0x02}, "zstd")

	// Empty.
	f.Add([]byte{}, "gzip")
	f.Add([]byte{}, "zstd")
	f.Add([]byte{}, "")

	f.Fuzz(func(t *testing.T, data []byte, encoding string) {
		// Must not panic. Errors are expected for malformed input.
		result, err := ReadBody(bytes.NewReader(data), encoding, 1<<20)
		_ = result
		_ = err
	})
}
