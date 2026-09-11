// Package auth provides password hashing and JWT token management.
package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"

	"golang.org/x/crypto/argon2"
)

// Argon2id parameters following OWASP recommendations.
const (
	argonMemory  = 64 * 1024 // 64 MB
	argonTime    = 3         // 3 iterations
	argonThreads = 4         // 4 parallel lanes
	argonKeyLen  = 32        // 32-byte derived key
	argonSaltLen = 16        // 16-byte random salt
)

// HashPassword hashes a password using argon2id and returns a PHC-format string:
// $argon2id$v=19$m=65536,t=3,p=4$<salt>$<hash>
func HashPassword(password string) (string, error) {
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate salt: %w", err)
	}

	hash := argon2.IDKey([]byte(password), salt, argonTime, argonMemory, argonThreads, argonKeyLen)

	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version,
		argonMemory, argonTime, argonThreads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(hash),
	), nil
}

// VerifyPassword checks a password against an argon2id PHC-format hash.
func VerifyPassword(password, encoded string) (bool, error) {
	salt, hash, memory, time, threads, keyLen, err := parsePHC(encoded)
	if err != nil {
		return false, err
	}

	candidate := argon2.IDKey([]byte(password), salt, time, memory, threads, keyLen)
	return subtle.ConstantTimeCompare(hash, candidate) == 1, nil
}

// decoyHash is an argon2id hash of a value no password equals, generated
// once at startup with the same parameters HashPassword uses.
//
// It exists so a login for an unknown user can do the same work as a login
// for a known one. Skipping the verification when there is no user to verify
// against answers a wrong username faster than a wrong password, which tells
// an unauthenticated caller which usernames exist.
var decoyHash = sync.OnceValue(func() string {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		// Falling back to a fixed value costs nothing an attacker can use:
		// the decoy's only job is to cost the same as a real verification.
		secret = []byte("decoy password, matched by nothing")
	}
	encoded, err := HashPassword(string(secret))
	if err != nil {
		return ""
	}
	return encoded
})

// VerifyAgainstDecoy spends the work a password verification costs, against
// a hash nothing matches. Call it where a real verification would otherwise
// be skipped, so the answer takes the same time either way.
func VerifyAgainstDecoy(password string) {
	encoded := decoyHash()
	if encoded == "" {
		return
	}
	_, _ = VerifyPassword(password, encoded)
}

// parsePHC parses an argon2id PHC string format.
func parsePHC(encoded string) (salt, hash []byte, memory, time uint32, threads uint8, keyLen uint32, err error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 {
		return nil, nil, 0, 0, 0, 0, fmt.Errorf("invalid PHC format: expected 6 parts, got %d", len(parts))
	}

	if parts[1] != "argon2id" {
		return nil, nil, 0, 0, 0, 0, fmt.Errorf("unsupported algorithm: %s", parts[1])
	}

	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return nil, nil, 0, 0, 0, 0, fmt.Errorf("parse version: %w", err)
	}

	var m, t uint32
	var p uint8
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &m, &t, &p); err != nil {
		return nil, nil, 0, 0, 0, 0, fmt.Errorf("parse params: %w", err)
	}
	if t == 0 || p == 0 || m == 0 {
		return nil, nil, 0, 0, 0, 0, fmt.Errorf("invalid argon2 params: t=%d, p=%d, m=%d (all must be > 0)", t, p, m)
	}

	salt, err = base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return nil, nil, 0, 0, 0, 0, fmt.Errorf("decode salt: %w", err)
	}

	hash, err = base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return nil, nil, 0, 0, 0, 0, fmt.Errorf("decode hash: %w", err)
	}

	if len(salt) == 0 || len(hash) == 0 {
		return nil, nil, 0, 0, 0, 0, fmt.Errorf("invalid argon2 salt/hash: salt=%d hash=%d bytes (both must be > 0)", len(salt), len(hash))
	}
	return salt, hash, m, t, p, uint32(len(hash)), nil //nolint:gosec // G115: hash length is always 32 bytes
}
