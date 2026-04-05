package server

import (
	"testing"

	"github.com/google/uuid"
)

func TestResolveCertID_ExistingID(t *testing.T) {
	t.Parallel()
	existing := uuid.Must(uuid.NewV7())
	got, err := resolveCertID(existing, "anything")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != existing {
		t.Fatalf("expected existing ID %s, got %s", existing, got)
	}
}

func TestResolveCertID_ValidReqID(t *testing.T) {
	t.Parallel()
	reqID := uuid.Must(uuid.NewV7())
	got, err := resolveCertID(uuid.Nil, reqID.String())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != reqID {
		t.Fatalf("expected %s, got %s", reqID, got)
	}
}

func TestResolveCertID_MalformedReqID(t *testing.T) {
	t.Parallel()
	_, err := resolveCertID(uuid.Nil, "not-a-uuid")
	if err == nil {
		t.Fatal("expected error for malformed UUID, got nil")
	}
}

func TestResolveCertID_EmptyReqID(t *testing.T) {
	t.Parallel()
	got, err := resolveCertID(uuid.Nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == uuid.Nil {
		t.Fatal("expected a new UUIDv7, got nil UUID")
	}
}
