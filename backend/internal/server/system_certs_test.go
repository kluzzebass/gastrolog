package server

import (
	"gastrolog/internal/glid"
	"testing"
)

func TestResolveCertID_ExistingID(t *testing.T) {
	t.Parallel()
	existing := glid.New()
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
	reqID := glid.New()
	got, err := resolveCertID(glid.Nil, reqID.String())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != reqID {
		t.Fatalf("expected %s, got %s", reqID, got)
	}
}

func TestResolveCertID_MalformedReqID(t *testing.T) {
	t.Parallel()
	_, err := resolveCertID(glid.Nil, "not-a-uuid")
	if err == nil {
		t.Fatal("expected error for malformed UUID, got nil")
	}
}

func TestResolveCertID_EmptyReqID(t *testing.T) {
	t.Parallel()
	got, err := resolveCertID(glid.Nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == glid.Nil {
		t.Fatal("expected a new UUIDv7, got nil UUID")
	}
}
