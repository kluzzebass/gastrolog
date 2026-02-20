package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

// GenerateRefreshToken creates a new opaque refresh token and its SHA-256 hash.
// The token is 32 random bytes encoded as base64url (no padding).
// The hash is the hex-encoded SHA-256 of the raw token string.
func GenerateRefreshToken() (token string, hash string, err error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", fmt.Errorf("generate refresh token: %w", err)
	}
	token = base64.RawURLEncoding.EncodeToString(b)
	hash = HashRefreshToken(token)
	return token, hash, nil
}

// HashRefreshToken returns the hex-encoded SHA-256 hash of a refresh token string.
func HashRefreshToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}
