package chunk

import (
	"testing"

	"github.com/google/uuid"
)

func TestNewChunkIDIsV7(t *testing.T) {
	id := NewChunkID()
	uid := uuid.UUID(id)
	if uid.Version() != 7 {
		t.Fatalf("expected UUID v7, got v%d", uid.Version())
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

func TestChunkIDStringFormat(t *testing.T) {
	id := NewChunkID()
	s := id.String()
	// UUID string format: 8-4-4-4-12 = 36 chars.
	if len(s) != 36 {
		t.Fatalf("expected 36-char string, got %d: %q", len(s), s)
	}
}

func TestParseChunkIDValid(t *testing.T) {
	known := "01234567-89ab-cdef-0123-456789abcdef"
	id, err := ParseChunkID(known)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if id.String() != known {
		t.Fatalf("expected %s, got %s", known, id.String())
	}
}

func TestParseChunkIDInvalid(t *testing.T) {
	cases := []string{
		"",
		"not-a-uuid",
		"01234567-89ab-cdef-0123",
		"01234567-89ab-cdef-0123-456789abcdef-extra",
		"zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz",
	}
	for _, input := range cases {
		_, err := ParseChunkID(input)
		if err == nil {
			t.Fatalf("expected error for %q, got nil", input)
		}
	}
}

func TestChunkIDZero(t *testing.T) {
	zero := ChunkID(uuid.UUID{})
	s := zero.String()
	parsed, err := ParseChunkID(s)
	if err != nil {
		t.Fatalf("parse zero: %v", err)
	}
	if parsed != zero {
		t.Fatalf("expected zero ID, got %s", parsed)
	}
}

func TestNewChunkIDUnique(t *testing.T) {
	a := NewChunkID()
	b := NewChunkID()
	if a == b {
		t.Fatal("expected distinct IDs")
	}
}
