package fluentfwd

import (
	"bytes"
	"compress/gzip"
	"strings"
	"testing"
)

// TestGunzip_BelowCap is a sanity check that legitimate small payloads
// decompress correctly.
func TestGunzip_BelowCap(t *testing.T) {
	t.Parallel()

	plain := []byte("hello fluent forward")
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(plain); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	got, err := gunzip(buf.Bytes())
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Errorf("gunzip got %q, want %q", got, plain)
	}
}

// TestGunzip_NearCap verifies that a payload near (but under) the cap
// decompresses correctly. Uses 50 MiB of compressible repeated data,
// well below the 100 MiB cap.
func TestGunzip_NearCap(t *testing.T) {
	t.Parallel()

	const size = 50 << 20 // 50 MiB
	plain := []byte(strings.Repeat("a", size))

	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(plain); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	got, err := gunzip(buf.Bytes())
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	if len(got) != size {
		t.Errorf("gunzip returned %d bytes, want %d", len(got), size)
	}
}

// TestGunzip_GzipBomb is the regression test for gastrolog-e3qug. A small
// gzip blob that decompresses to MORE than the cap (200 MiB > 100 MiB cap)
// must be rejected with an error rather than allocating the full 200 MiB.
// Without the fix this test would either OOM the process or take seconds
// of memory churn.
func TestGunzip_GzipBomb(t *testing.T) {
	t.Parallel()

	const size = 200 << 20 // 200 MiB > 100 MiB cap
	plain := []byte(strings.Repeat("a", size))

	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(plain); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	// Sanity: the compressed payload of 200 MiB of 'a' is tiny
	// (well under 1 MiB), confirming the gzip-bomb shape.
	if buf.Len() > 1<<20 {
		t.Logf("WARNING: compressed payload was %d bytes — bomb test less dramatic than expected", buf.Len())
	}

	_, err := gunzip(buf.Bytes())
	if err == nil {
		t.Fatal("gunzip should have rejected oversized payload")
	}
	if !strings.Contains(err.Error(), "decompressed payload exceeds") {
		t.Errorf("error should mention size limit, got: %v", err)
	}
}

// TestGunzip_MalformedGzip verifies that garbage input returns an error
// rather than panicking.
func TestGunzip_MalformedGzip(t *testing.T) {
	t.Parallel()

	_, err := gunzip([]byte("not actually gzip data at all"))
	if err == nil {
		t.Fatal("gunzip should have errored on garbage input")
	}
}
