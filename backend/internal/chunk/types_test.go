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

func TestNewSourceIDIsV7(t *testing.T) {
	id := NewSourceID()
	uid := uuid.UUID(id)
	if uid.Version() != 7 {
		t.Fatalf("expected UUID v7, got v%d", uid.Version())
	}
}
