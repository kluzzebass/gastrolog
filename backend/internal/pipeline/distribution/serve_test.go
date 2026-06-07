package distribution_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/pipeline/distribution"
)

func TestStreamSegmentCopiesBytes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.dat")
	payload := []byte("segment-bytes-for-pull")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := distribution.StreamSegment(path, &buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), payload) {
		t.Fatalf("got %q, want %q", buf.Bytes(), payload)
	}
}

func TestStreamSegmentMissingFile(t *testing.T) {
	t.Parallel()
	err := distribution.StreamSegment(filepath.Join(t.TempDir(), "missing"), &bytes.Buffer{})
	if !errors.Is(err, distribution.ErrSegmentGone) {
		t.Fatalf("StreamSegment() = %v, want ErrSegmentGone", err)
	}
}
