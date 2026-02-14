package auth

import (
	"testing"
	"time"
)

func TestIssueAndVerify(t *testing.T) {
	ts := NewTokenService([]byte("test-secret-key-for-testing-only"), 7*24*time.Hour)

	token, expiresAt, err := ts.Issue("user-001", "alice", "admin")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if token == "" {
		t.Fatal("expected non-empty token")
	}
	if expiresAt.Before(time.Now()) {
		t.Error("expected expiration in the future")
	}

	claims, err := ts.Verify(token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.UserID != "user-001" {
		t.Errorf("UserID: expected %q, got %q", "user-001", claims.UserID)
	}
	if claims.Username() != "alice" {
		t.Errorf("Username: expected %q, got %q", "alice", claims.Username())
	}
	if claims.Role != "admin" {
		t.Errorf("Role: expected %q, got %q", "admin", claims.Role)
	}
	if claims.Subject != "alice" {
		t.Errorf("Subject: expected %q, got %q", "alice", claims.Subject)
	}
}

func TestVerifyExpiredToken(t *testing.T) {
	// Token that expired 1 hour ago.
	ts := NewTokenService([]byte("test-secret"), -1*time.Hour)

	token, _, err := ts.Issue("user-002", "bob", "user")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	_, err = ts.Verify(token)
	if err == nil {
		t.Fatal("expected error for expired token")
	}
}

func TestVerifyWrongSecret(t *testing.T) {
	ts1 := NewTokenService([]byte("secret-one"), 7*24*time.Hour)
	ts2 := NewTokenService([]byte("secret-two"), 7*24*time.Hour)

	token, _, err := ts1.Issue("user-003", "carol", "user")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	_, err = ts2.Verify(token)
	if err == nil {
		t.Fatal("expected error verifying with wrong secret")
	}
}

func TestVerifyInvalidToken(t *testing.T) {
	ts := NewTokenService([]byte("secret"), 7*24*time.Hour)

	_, err := ts.Verify("not-a-valid-token")
	if err == nil {
		t.Fatal("expected error for invalid token")
	}
}
