// Package bodyutil provides HTTP body decompression for ingesters.
package bodyutil

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// zstdWindowBytes bounds the decoder's back-reference window. It is not an
// output bound — a stream whose frames all fit this window still expands
// without limit, which is what readBounded is for.
const zstdWindowBytes = 10 << 20

// zstdPool holds stream decoders. Decoding through a reader instead of
// DecodeAll is what makes the output boundable, and a streaming decoder is
// single-use, so they are pooled rather than rebuilt per request.
var zstdPool = sync.Pool{
	New: func() any {
		dec, err := zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxMemory(zstdWindowBytes),
		)
		if err != nil {
			panic("bodyutil: init zstd decoder: " + err.Error())
		}
		return dec
	},
}

// ErrBodyTooLarge reports a body that reached maxBytes. Callers surface it
// to the sender: a body quietly truncated at the limit parses as malformed
// further down, which hides the real reason a batch went missing.
var ErrBodyTooLarge = errors.New("bodyutil: body exceeds size limit")

// ReadBody reads and decompresses an HTTP request body based on the
// Content-Encoding header value. Supports gzip, zstd, and identity.
//
// maxBytes bounds the DECOMPRESSED output, not the bytes on the wire.
// Bounding the compressed input alone is no bound at all: a few kilobytes of
// crafted zstd expand to gigabytes. A body that reaches the limit is
// rejected with ErrBodyTooLarge rather than truncated.
func ReadBody(body io.Reader, contentEncoding string, maxBytes int64) ([]byte, error) {
	switch contentEncoding {
	case "zstd":
		compressed, err := readBounded(body, maxBytes)
		if err != nil {
			return nil, fmt.Errorf("read compressed body: %w", err)
		}
		return decodeZstd(compressed, maxBytes)

	case "gzip":
		gz, err := gzip.NewReader(body)
		if err != nil {
			return nil, fmt.Errorf("open gzip reader: %w", err)
		}
		defer func() { _ = gz.Close() }()
		out, err := readBounded(gz, maxBytes)
		if err != nil {
			return nil, fmt.Errorf("decompress gzip body: %w", err)
		}
		return out, nil

	case "", "identity":
		return readBounded(body, maxBytes)

	default:
		return nil, fmt.Errorf("unsupported Content-Encoding: %q", contentEncoding)
	}
}

func decodeZstd(compressed []byte, maxBytes int64) ([]byte, error) {
	dec, _ := zstdPool.Get().(*zstd.Decoder)
	defer func() {
		// Release the input before pooling so a rejected body is not
		// pinned by an idle decoder.
		_ = dec.Reset(nil)
		zstdPool.Put(dec)
	}()

	if err := dec.Reset(bytes.NewReader(compressed)); err != nil {
		return nil, fmt.Errorf("open zstd reader: %w", err)
	}
	out, err := readBounded(dec, maxBytes)
	if err != nil {
		return nil, fmt.Errorf("decompress zstd body: %w", err)
	}
	return out, nil
}

// readBounded reads up to maxBytes, returning ErrBodyTooLarge if the source
// has more. It reads one byte past the limit to tell "exactly at the limit"
// from "over it".
func readBounded(r io.Reader, maxBytes int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxBytes+1)) //ok:io-readall bounded by maxBytes+1; the caller's request-body ceiling
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("%w: exceeds %d bytes", ErrBodyTooLarge, maxBytes)
	}
	return data, nil
}
