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

func TestDecodeAttributesInvalid(t *testing.T) {
	t.Parallel()
	if _, _, err := record.DecodeAttributes([]byte{0}); err == nil {
		t.Fatal("expected error for truncated blob")
	}
}
