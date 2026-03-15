package auth

import (
	"testing"
	"time"
)

// FuzzJWTVerify feeds random token strings to the JWT verifier.
// It must return an error for invalid tokens and never panic.
func FuzzJWTVerify(f *testing.F) {
	ts := NewTokenService([]byte("fuzz-secret-key"), 1*time.Hour)

	// Seed with a legitimately issued token.
	validToken, _, _ := ts.Issue("user-id-1", "admin", "admin")
	f.Add(validToken)
	f.Add("")
	f.Add("not.a.jwt")
	f.Add("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.invalid")
	f.Add("eyJhbGciOiJub25lIn0.eyJzdWIiOiJ0ZXN0In0.")
	f.Add("a]]]]][[[.b.c")
	// Three base64url segments but garbage content.
	f.Add("AAAA.BBBB.CCCC")

	f.Fuzz(func(t *testing.T, tokenString string) {
		// Verify may succeed or fail; must never panic.
		_, _ = ts.Verify(tokenString)
	})
}
