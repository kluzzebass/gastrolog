package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
)

// TokenValidator checks whether a token is still valid after JWT verification.
// This is used for server-side token revocation: logout ends the token's
// session, and password change, rename, or role change invalidates every token
// the user holds; a deleted user is rejected the same way.
type TokenValidator interface {
	IsTokenValid(ctx context.Context, claims *Claims) (bool, error)
}

// HeaderGetter reads a request header by name. Connect request headers and
// http.Header both satisfy it.
type HeaderGetter interface{ Get(string) string }

// Verifier decides whether the caller behind a request satisfies an
// authorization level. Every entry point authorizes through it — the Connect
// interceptor and the plain HTTP routes alike — so signature, expiry,
// revocation and the admin comparison have one implementation. A role claim is
// only worth trusting because revocation rejects a token minted before the
// user's role changed, so the two checks must never come apart.
type Verifier struct {
	tokens    *TokenService
	validator TokenValidator
}

// NewVerifier creates a Verifier. validator may be nil, which skips the
// server-side revocation check.
func NewVerifier(tokens *TokenService, validator TokenValidator) *Verifier {
	return &Verifier{tokens: tokens, validator: validator}
}

// Authorize returns the claims of a caller allowed to proceed at level. The
// error carries the code the caller should see: Unauthenticated for a missing,
// unverifiable or revoked token, PermissionDenied for a role that is too low.
// PUBLIC returns whatever claims the request carries, nil included.
//
// The switch is total: a level this build does not recognize is denied, so
// adding a level to the schema without teaching this function about it makes
// the RPCs carrying it unreachable rather than merely authenticated.
func (v *Verifier) Authorize(ctx context.Context, level apiv1.AuthLevel, headers HeaderGetter) (*Claims, error) {
	switch level {
	case apiv1.AuthLevel_AUTH_LEVEL_PUBLIC:
		return v.bestEffortClaims(ctx, headers), nil
	case apiv1.AuthLevel_AUTH_LEVEL_AUTHENTICATED:
		return v.verifiedClaims(ctx, headers)
	case apiv1.AuthLevel_AUTH_LEVEL_ADMIN:
		claims, err := v.verifiedClaims(ctx, headers)
		if err != nil {
			return nil, err
		}
		if claims.Role != "admin" {
			return nil, connect.NewError(connect.CodePermissionDenied, errors.New("admin role required"))
		}
		return claims, nil
	case apiv1.AuthLevel_AUTH_LEVEL_UNSPECIFIED:
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("no authorization level declared"))
	default:
		return nil, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("unrecognized authorization level %d", int32(level)))
	}
}

// verifiedClaims extracts a Bearer token from headers, verifies its signature,
// and checks server-side revocation. Returns the parsed claims or a Connect error.
func (v *Verifier) verifiedClaims(ctx context.Context, headers HeaderGetter) (*Claims, error) {
	authHeader := headers.Get("Authorization")
	if authHeader == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("missing authorization header"))
	}
	token, ok := strings.CutPrefix(authHeader, "Bearer ")
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authorization header must use Bearer scheme"))
	}
	claims, err := v.tokens.Verify(token)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("invalid token: %w", err))
	}
	if v.validator != nil {
		valid, err := v.validator.IsTokenValid(ctx, claims)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("validate token: %w", err))
		}
		if !valid {
			return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("token has been revoked"))
		}
	}
	return claims, nil
}

// bestEffortClaims returns the caller's claims when the request carries a
// usable token, and nil on any failure (missing header, bad token, expired,
// revoked) — the caller proceeds as anonymous.
func (v *Verifier) bestEffortClaims(ctx context.Context, headers HeaderGetter) *Claims {
	claims, err := v.verifiedClaims(ctx, headers)
	if err != nil {
		return nil
	}
	return claims
}
