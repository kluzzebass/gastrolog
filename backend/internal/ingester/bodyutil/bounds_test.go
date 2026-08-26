package bodyutil

import (
	"bytes"
	"compress/gzip"
	"errors"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

const testMaxBytes = 1 << 20

// TestZstdBombIsRejected proves a small zstd payload that expands past the
// caller's ceiling is refused. Bounding the compressed input alone is no
// bound at all: the payload below is a few hundred bytes on the wire and
// 64 MiB decompressed.
func TestZstdBombIsRejected(t *testing.T) {
	t.Parallel()
	bomb := zstdCompress(t, make([]byte, 64<<20))

	if len(bomb) > testMaxBytes {
		t.Fatalf("the bomb is not a bomb: %d compressed bytes already exceed the limit", len(bomb))
	}

	out, err := ReadBody(bytes.NewReader(bomb), "zstd", testMaxBytes)
	if err == nil {
		t.Fatalf("decompressed %d bytes past a %d ceiling", len(out), testMaxBytes)
	}
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Errorf("rejection does not identify the ceiling: %v", err)
	}
}

// TestGzipBombIsRejected proves the same for gzip, and that reaching the
// ceiling is an error rather than a silent truncation — a truncated body
// fails to parse later, hiding why the batch went missing.
func TestGzipBombIsRejected(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(make([]byte, 8<<20)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	out, err := ReadBody(bytes.NewReader(buf.Bytes()), "gzip", testMaxBytes)
	if err == nil {
		t.Fatalf("decompressed %d bytes past a %d ceiling", len(out), testMaxBytes)
	}
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Errorf("rejection does not identify the ceiling: %v", err)
	}
}

func TestOversizeIdentityBodyIsRejected(t *testing.T) {
	t.Parallel()
	body := strings.NewReader(strings.Repeat("x", testMaxBytes+1))

	if _, err := ReadBody(body, "", testMaxBytes); !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("expected a size rejection, got %v", err)
	}
}

// TestLegitimateBodiesRoundTrip proves the bounds did not change what a
// conforming sender gets: each encoding returns its exact payload, including
// one sized right at the ceiling.
func TestLegitimateBodiesRoundTrip(t *testing.T) {
	t.Parallel()
	payload := []byte(strings.Repeat("log line\n", 1000))

	cases := []struct {
		encoding string
		body     []byte
	}{
		{"", payload},
		{"identity", payload},
		{"gzip", gzipCompress(t, payload)},
		{"zstd", zstdCompress(t, payload)},
	}
	for _, tc := range cases {
		t.Run("encoding="+tc.encoding, func(t *testing.T) {
			got, err := ReadBody(bytes.NewReader(tc.body), tc.encoding, testMaxBytes)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !bytes.Equal(got, payload) {
				t.Errorf("payload altered: got %d bytes, want %d", len(got), len(payload))
			}
		})
	}

	exact := bytes.Repeat([]byte("x"), testMaxBytes)
	got, err := ReadBody(bytes.NewReader(exact), "identity", testMaxBytes)
	if err != nil {
		t.Fatalf("a body exactly at the ceiling was rejected: %v", err)
	}
	if len(got) != testMaxBytes {
		t.Errorf("expected %d bytes, got %d", testMaxBytes, len(got))
	}
}

func zstdCompress(t *testing.T, data []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("zstd writer: %v", err)
	}
	defer func() { _ = enc.Close() }()
	return enc.EncodeAll(data, nil)
}

func gzipCompress(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(data); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}
