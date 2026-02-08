// Package auth provides password hashing and JWT token management.
package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"

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

	salt, err = base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return nil, nil, 0, 0, 0, 0, fmt.Errorf("decode salt: %w", err)
	}

	hash, err = base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return nil, nil, 0, 0, 0, 0, fmt.Errorf("decode hash: %w", err)
	}

	return salt, hash, m, t, p, uint32(len(hash)), nil
}
