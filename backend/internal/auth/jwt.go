package auth

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Claims holds the JWT claims for a GastroLog token.
// Username is stored in the standard "sub" (Subject) claim.
type Claims struct {
	Role string `json:"role"`
	jwt.RegisteredClaims
}

// Username returns the subject (username) from the token.
func (c *Claims) Username() string {
	return c.Subject
}

// TokenService issues and verifies JWT tokens.
type TokenService struct {
	secret   []byte
	duration time.Duration
}

// NewTokenService creates a token service with the given HMAC secret and
// token lifetime.
func NewTokenService(secret []byte, duration time.Duration) *TokenService {
	return &TokenService{
		secret:   secret,
		duration: duration,
	}
}

// Issue creates a signed JWT for the given user.
func (ts *TokenService) Issue(username, role string) (string, time.Time, error) {
	now := time.Now().UTC()
	expiresAt := now.Add(ts.duration)

	claims := Claims{
		Role: role,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   username,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(ts.secret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sign token: %w", err)
	}

	return signed, expiresAt, nil
}

// Verify parses and validates a JWT, returning the claims if valid.
func (ts *TokenService) Verify(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return ts.secret, nil
	})
	if err != nil {
		return nil, fmt.Errorf("parse token: %w", err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("invalid token claims")
	}

	return claims, nil
}
