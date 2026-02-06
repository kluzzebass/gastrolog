package chunk

import (
	"testing"
	"time"
)

func TestNewChunkIDUnique(t *testing.T) {
	a := NewChunkID()
	time.Sleep(time.Microsecond)
	b := NewChunkID()
	if a == b {
		t.Fatal("expected distinct IDs")
	}
}

func TestChunkIDStringRoundTrip(t *testing.T) {
	id := NewChunkID()
	s := id.String()
	parsed, err := ParseChunkID(s)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed != id {
		t.Fatalf("expected %s, got %s", id, parsed)
	}
}

func TestChunkIDStringLength(t *testing.T) {
	id := NewChunkID()
	s := id.String()
	if len(s) != 13 {
		t.Fatalf("expected 13-char string, got %d: %q", len(s), s)
	}
}

func TestChunkIDSortOrder(t *testing.T) {
	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 15, 12, 30, 0, 0, time.UTC)

	id1 := ChunkIDFromTime(t1)
	id2 := ChunkIDFromTime(t2)

	if id1.String() >= id2.String() {
		t.Fatalf("expected %s < %s", id1, id2)
	}
}

func TestChunkIDTimeRoundTrip(t *testing.T) {
	orig := time.Date(2026, 2, 6, 14, 23, 45, 123456000, time.UTC)
	id := ChunkIDFromTime(orig)
	got := id.Time()

	// Truncate to microseconds for comparison.
	want := orig.Truncate(time.Microsecond)
	if !got.Equal(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestParseChunkIDValid(t *testing.T) {
	// Create a known ID and verify round-trip.
	known := ChunkIDFromTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	s := known.String()
	parsed, err := ParseChunkID(s)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed != known {
		t.Fatalf("expected %s, got %s", known, parsed)
	}
}

func TestParseChunkIDInvalid(t *testing.T) {
	cases := []string{
		"",
		"short",
		"toolongstring!!",  // 15 chars
		"0000000000000000", // 16 chars
		"!!!!!!!!!!!!!",    // 13 chars but invalid base32hex
	}
	for _, input := range cases {
		_, err := ParseChunkID(input)
		if err == nil {
			t.Fatalf("expected error for %q, got nil", input)
		}
	}
}

func TestChunkIDZero(t *testing.T) {
	zero := ChunkID{}
	s := zero.String()
	if len(s) != 13 {
		t.Fatalf("expected 13 chars, got %d: %q", len(s), s)
	}
	parsed, err := ParseChunkID(s)
	if err != nil {
		t.Fatalf("parse zero: %v", err)
	}
	if parsed != zero {
		t.Fatalf("expected zero ID, got %s", parsed)
	}
}

func TestChunkIDBase32HexCharset(t *testing.T) {
	id := NewChunkID()
	s := id.String()
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'v')) {
			t.Fatalf("unexpected character %q in %q", string(c), s)
		}
	}
}
