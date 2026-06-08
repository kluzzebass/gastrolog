package record_test

import (
	"testing"

	"gastrolog/internal/record"
)

func TestDecodeAttributesRoundTrip(t *testing.T) {
	t.Parallel()
	attrs := record.Attributes{"b": "2", "a": "1", "env": "prod"}
	enc, err := attrs.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, n, err := record.DecodeAttributes(enc)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(enc) {
		t.Fatalf("consumed %d, want %d", n, len(enc))
	}
	if got["a"] != "1" || got["env"] != "prod" {
		t.Fatalf("got %v", got)
	}

	empty, n, err := record.DecodeAttributes([]byte{0, 0})
	if err != nil || n != 2 || len(empty) != 0 {
		t.Fatalf("empty decode: attrs=%v n=%d err=%v", empty, n, err)
	}
}

// TestDecodeAttributesEmptyWithTrailingData guards the segment-frame use:
// an empty attribute blob is followed by the rest of the frame body (raw +
// CRC). DecodeAttributes must consume exactly the 2-byte header and report
// n=2, leaving the trailing bytes for the caller — not reject the buffer. This
// mirrors the count>0 path and is what makes records with no attributes
// decodable from a segment frame.
func TestDecodeAttributesEmptyWithTrailingData(t *testing.T) {
	t.Parallel()
	buf := []byte{0, 0, 0xDE, 0xAD, 0xBE, 0xEF} // empty attrs + trailing frame bytes
	attrs, n, err := record.DecodeAttributes(buf)
	if err != nil {
		t.Fatalf("decode empty attrs with trailing data: %v", err)
	}
	if n != 2 {
		t.Fatalf("consumed %d, want 2 (header only)", n)
	}
	if len(attrs) != 0 {
		t.Fatalf("attrs = %v, want empty", attrs)
	}
}

func TestDecodeAttributesInvalid(t *testing.T) {
	t.Parallel()
	if _, _, err := record.DecodeAttributes([]byte{0}); err == nil {
		t.Fatal("expected error for truncated blob")
	}
}
