package auth

import (
	"strings"
	"testing"
)

func TestHashPassword(t *testing.T) {
	hash, err := HashPassword("testpassword")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}

	// Should be a valid PHC string.
	if !strings.HasPrefix(hash, "$argon2id$") {
		t.Errorf("expected PHC format, got %q", hash)
	}

	parts := strings.Split(hash, "$")
	if len(parts) != 6 {
		t.Fatalf("expected 6 parts, got %d: %q", len(parts), hash)
	}
}

func TestHashPasswordUniqueSalts(t *testing.T) {
	h1, err := HashPassword("same-password")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	h2, err := HashPassword("same-password")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}

	if h1 == h2 {
		t.Error("two hashes of the same password should differ (unique salts)")
	}
}

func TestVerifyPasswordCorrect(t *testing.T) {
	hash, err := HashPassword("correcthorse")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}

	ok, err := VerifyPassword("correcthorse", hash)
	if err != nil {
		t.Fatalf("VerifyPassword: %v", err)
	}
	if !ok {
		t.Error("expected password to verify correctly")
	}
}

func TestVerifyPasswordWrong(t *testing.T) {
	hash, err := HashPassword("correcthorse")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}

	ok, err := VerifyPassword("wrongpassword", hash)
	if err != nil {
		t.Fatalf("VerifyPassword: %v", err)
	}
	if ok {
		t.Error("expected wrong password to fail verification")
	}
}

func TestVerifyPasswordInvalidFormat(t *testing.T) {
	_, err := VerifyPassword("test", "not-a-valid-hash")
	if err == nil {
		t.Error("expected error for invalid hash format")
	}
}
