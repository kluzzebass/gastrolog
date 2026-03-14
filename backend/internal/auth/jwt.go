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
	jwt.RegisteredClaims
}

// Username returns the subject (username) from the token.
func (c *Claims) Username() string {
	return c.Subject
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

// Issue creates a signed JWT for the given user.
func (ts *TokenService) Issue(userID, username, role string) (string, time.Time, error) {
	sd := ts.state.Load()
	now := time.Now().UTC()
	expiresAt := now.Add(sd.duration)

	claims := Claims{
		Role:   role,
		UserID: userID,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   username,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
		},
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
