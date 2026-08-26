package server

import (
	"context"
	"fmt"
	"gastrolog/internal/glid"
	"net/http"
	"time"

	"connectrpc.com/connect"

	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/auth"
	"gastrolog/internal/system"
)

// tokenValidator adapts system.Store to auth.TokenValidator.
type tokenValidator struct {
	cfgStore system.Store
}

// apiVerifier returns the authorization verifier for API callers. The Connect
// interceptor and the plain HTTP routes both authorize through it, so a role
// claim is checked against the same revocation state everywhere: a demoted,
// logged-out or deleted user is rejected on every entry point at once.
func (s *Server) apiVerifier() *auth.Verifier {
	return auth.NewVerifier(s.tokens, &tokenValidator{cfgStore: s.cfgStore})
}

func (tv *tokenValidator) IsTokenValid(ctx context.Context, userID string, issuedAt time.Time) (bool, error) {
	uid, err := glid.ParseUUID(userID)
	if err != nil {
		return false, fmt.Errorf("parse user ID %q: %w", userID, err)
	}
	user, err := tv.cfgStore.GetUser(ctx, uid)
	if err != nil {
		return false, err
	}
	if user == nil {
		return false, nil // deleted user
	}
	if !user.TokenInvalidatedAt.IsZero() && !issuedAt.After(user.TokenInvalidatedAt) {
		return false, nil // token issued before invalidation
	}
	return true, nil
}

// writeAuthError maps an authorization error onto the HTTP status a route that
// does not speak Connect should return. The caller of a denied request learns
// only whether they need to authenticate or need a different role.
func writeAuthError(w http.ResponseWriter, err error) {
	code := connect.CodeOf(err)
	if code == connect.CodeUnauthenticated {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if code == connect.CodePermissionDenied {
		http.Error(w, "admin role required", http.StatusForbidden)
		return
	}
	http.Error(w, "authorization check failed", http.StatusInternalServerError)
}

// Client creates a set of Connect clients for the given base URL.
type Client struct {
	Query     gastrologv1connect.QueryServiceClient
	Vault     gastrologv1connect.VaultServiceClient
	System    gastrologv1connect.SystemServiceClient
	Lifecycle gastrologv1connect.LifecycleServiceClient
	Auth      gastrologv1connect.AuthServiceClient
	Job       gastrologv1connect.JobServiceClient
}

// NewClient creates Connect clients for the given base URL.
func NewClient(baseURL string, opts ...connect.ClientOption) *Client {
	return &Client{
		Query:     gastrologv1connect.NewQueryServiceClient(http.DefaultClient, baseURL, opts...),
		Vault:     gastrologv1connect.NewVaultServiceClient(http.DefaultClient, baseURL, opts...),
		System:    gastrologv1connect.NewSystemServiceClient(http.DefaultClient, baseURL, opts...),
		Lifecycle: gastrologv1connect.NewLifecycleServiceClient(http.DefaultClient, baseURL, opts...),
		Auth:      gastrologv1connect.NewAuthServiceClient(http.DefaultClient, baseURL, opts...),
		Job:       gastrologv1connect.NewJobServiceClient(http.DefaultClient, baseURL, opts...),
	}
}

// NewClientWithHTTP creates Connect clients with a custom HTTP client.
func NewClientWithHTTP(httpClient connect.HTTPClient, baseURL string, opts ...connect.ClientOption) *Client {
	return &Client{
		Query:     gastrologv1connect.NewQueryServiceClient(httpClient, baseURL, opts...),
		Vault:     gastrologv1connect.NewVaultServiceClient(httpClient, baseURL, opts...),
		System:    gastrologv1connect.NewSystemServiceClient(httpClient, baseURL, opts...),
		Lifecycle: gastrologv1connect.NewLifecycleServiceClient(httpClient, baseURL, opts...),
		Auth:      gastrologv1connect.NewAuthServiceClient(httpClient, baseURL, opts...),
		Job:       gastrologv1connect.NewJobServiceClient(httpClient, baseURL, opts...),
	}
}
