package chunk

import (
	"testing"
	"time"
)

func TestNewChunkIDUnique(t *testing.T) {
	t.Parallel()
	a := NewChunkID()
	b := NewChunkID()
	if a == b {
		t.Fatal("expected distinct IDs")
	}
}

func TestChunkIDStringRoundTrip(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	id := NewChunkID()
	s := id.String()
	if len(s) != 26 {
		t.Fatalf("expected 26-char string, got %d: %q", len(s), s)
	}
}

func TestChunkIDMonotonicity(t *testing.T) {
	t.Parallel()
	// UUIDv7 IDs should be monotonically increasing.
	ids := make([]ChunkID, 100)
	for i := range ids {
		ids[i] = NewChunkID()
	}
	for i := 1; i < len(ids); i++ {
		if ids[i].String() <= ids[i-1].String() {
			t.Fatalf("ID %d (%s) <= ID %d (%s)", i, ids[i], i-1, ids[i-1])
		}
	}
}

func TestChunkIDTimeExtraction(t *testing.T) {
	t.Parallel()
	before := time.Now().Truncate(time.Millisecond)
	id := NewChunkID()
	after := time.Now().Truncate(time.Millisecond).Add(time.Millisecond)

	got := id.Time()
	if got.Before(before) || got.After(after) {
		t.Fatalf("time %v outside expected range [%v, %v]", got, before, after)
	}
}

func TestParseChunkIDValid(t *testing.T) {
	t.Parallel()
	known := NewChunkID()
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
	t.Parallel()
	cases := []string{
		"",
		"short",
		"toolongstringfortesting!!!!!", // too long
		"!!!!!!!!!!!!!!!!!!!!!!!!!!", // 26 chars but invalid base32hex
	}
	for _, input := range cases {
		_, err := ParseChunkID(input)
		if err == nil {
			t.Fatalf("expected error for %q, got nil", input)
		}
	}
}

func TestChunkIDZero(t *testing.T) {
	t.Parallel()
	zero := ChunkID{}
	s := zero.String()
	if len(s) != 26 {
		t.Fatalf("expected 26 chars, got %d: %q", len(s), s)
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
	t.Parallel()
	id := NewChunkID()
	s := id.String()
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'v')) {
			t.Fatalf("unexpected character %q in %q", string(c), s)
		}
	}
}
