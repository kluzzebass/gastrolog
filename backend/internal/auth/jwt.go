package auth

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Claims holds the JWT claims for a GastroLog token.
// Username is stored in the standard "sub" (Subject) claim.
// UserID stores the user's UUID for ID-based lookups.
type Claims struct {
	Role   string `json:"role"`
	UserID string `json:"uid,omitempty"`
	// SessionID names the refresh-token row this token was issued from.
	// Deleting that row ends the session, this token included.
	SessionID string `json:"sid,omitempty"`
	// IssuedAtNano is the issue time in Unix nanoseconds. The registered
	// "iat" claim serializes to whole seconds, too coarse to rank a token
	// against a revocation timestamp recorded in the same second.
	IssuedAtNano int64 `json:"iat_ns,omitempty"`
	jwt.RegisteredClaims
}

// Username returns the subject (username) from the token.
func (c *Claims) Username() string {
	return c.Subject
}

// IssuedAtPrecise returns the issue time at full precision. It reports false
// for a token carrying no such claim, which revocation treats as unrankable.
func (c *Claims) IssuedAtPrecise() (time.Time, bool) {
	if c.IssuedAtNano == 0 {
		return time.Time{}, false
	}
	return time.Unix(0, c.IssuedAtNano).UTC(), true
}

// secretDuration bundles the signing secret and token lifetime for atomic swap.
type secretDuration struct {
	secret   []byte
	duration time.Duration
}

// TokenService issues and verifies JWT tokens. The secret can be swapped
// at runtime via SetSecret to support key regeneration without restart.
type TokenService struct {
	state atomic.Pointer[secretDuration]
}

// NewTokenService creates a token service with the given HMAC secret and
// token lifetime.
func NewTokenService(secret []byte, duration time.Duration) *TokenService {
	ts := &TokenService{}
	ts.state.Store(&secretDuration{secret: secret, duration: duration})
	return ts
}

// SetSecret atomically replaces the signing secret. All tokens issued with
// the previous secret become immediately unverifiable.
func (ts *TokenService) SetSecret(secret []byte) {
	old := ts.state.Load()
	ts.state.Store(&secretDuration{secret: secret, duration: old.duration})
}

// Issue creates a signed JWT for the given user, bound to the session
// identified by sessionID.
func (ts *TokenService) Issue(userID, username, role, sessionID string) (string, time.Time, error) {
	sd := ts.state.Load()
	now := time.Now().UTC()
	expiresAt := now.Add(sd.duration)

	claims := Claims{
		Role:         role,
		UserID:       userID,
		SessionID:    sessionID,
		IssuedAtNano: now.UnixNano(),
		Subject:      username,
		IssuedAt:     jwt.NewNumericDate(now),
		ExpiresAt:    jwt.NewNumericDate(expiresAt),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(sd.secret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sign token: %w", err)
	}

	return signed, expiresAt, nil
}

// Verify parses and validates a JWT, returning the claims if valid.
func (ts *TokenService) Verify(tokenString string) (*Claims, error) {
	sd := ts.state.Load()
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return sd.secret, nil
	})
	if err != nil {
		return nil, fmt.Errorf("parse token: %w", err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token claims")
	}

	return claims, nil
}
