package server

import (
	"context"
	"fmt"
	"gastrolog/internal/glid"
	"net/http"

	"connectrpc.com/connect"

	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/auth"
	"gastrolog/internal/system"
)

// tokenValidator adapts system.Store to auth.TokenValidator.
type tokenValidator struct {
	cfgStore system.Store
}

func (tv *tokenValidator) IsTokenValid(ctx context.Context, claims *auth.Claims) (bool, error) {
	uid, err := glid.ParseUUID(claims.UserID)
	if err != nil {
		return false, fmt.Errorf("parse user ID %q: %w", claims.UserID, err)
	}
	issuedAt, ok := claims.IssuedAtPrecise()
	if !ok {
		return false, nil // no issue time to rank against an invalidation
	}
	user, err := tv.cfgStore.GetUser(ctx, uid)
	if err != nil {
		return false, err
	}
	if user == nil {
		return false, nil // deleted user
	}
	// An invalidation covers every token already in existence at that instant,
	// so an issue time equal to it counts as invalidated.
	if !user.TokenInvalidatedAt.IsZero() && !issuedAt.After(user.TokenInvalidatedAt) {
		return false, nil
	}
	return tv.sessionLive(ctx, claims.SessionID, uid)
}

// sessionLive reports whether the session a token names still exists. Logout
// deletes the session's refresh-token row, and that is what stops the access
// token issued from it.
func (tv *tokenValidator) sessionLive(ctx context.Context, sessionID string, userID glid.GLID) (bool, error) {
	if sessionID == "" {
		return false, nil // a token naming no session cannot be logged out
	}
	sid, err := glid.ParseUUID(sessionID)
	if err != nil {
		// A claim that is not an ID names no session, so the token is
		// rejected — this is a bad token, not a failure to check one.
		return false, nil //nolint:nilerr // rejection, not an error to report
	}
	rt, err := tv.cfgStore.GetRefreshToken(ctx, sid)
	if err != nil {
		return false, err
	}
	if rt == nil {
		// A miss on local state is not proof the session is gone: any node
		// serves any request, and this one may not have applied the login
		// that opened the session. Catch up before rejecting, or a freshly
		// logged-in user is bounced back to the login page.
		if err := tv.cfgStore.Barrier(ctx); err != nil {
			return false, fmt.Errorf("confirm session %s: %w", sid, err)
		}
		rt, err = tv.cfgStore.GetRefreshToken(ctx, sid)
		if err != nil {
			return false, err
		}
	}
	if rt == nil || rt.UserID != userID {
		return false, nil
	}
	return true, nil
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
