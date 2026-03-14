package auth

import (
	"testing"
	"time"
)

func TestSetSecret_InvalidatesOldTokens(t *testing.T) {
	secret1 := []byte("original-secret-key-32bytes!!!!!")
	secret2 := []byte("rotated--secret-key-32bytes!!!!!")

	ts := NewTokenService(secret1, time.Hour)
	token, _, err := ts.Issue("uid", "admin", "admin")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := ts.Verify(token); err != nil {
		t.Fatalf("verify with original secret should pass: %v", err)
	}

	ts.SetSecret(secret2)

	if _, err := ts.Verify(token); err == nil {
		t.Fatal("verify with rotated secret should FAIL but passed")
	} else {
		t.Logf("correctly rejected: %v", err)
	}
}
