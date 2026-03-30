package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/config"
)

// tokenValidator adapts config.Store to auth.TokenValidator.
type tokenValidator struct {
	cfgStore config.Store
}

func (tv *tokenValidator) IsTokenValid(ctx context.Context, userID string, issuedAt time.Time) (bool, error) {
	uid, err := uuid.Parse(userID)
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

// Client creates a set of Connect clients for the given base URL.
type Client struct {
	Query     gastrologv1connect.QueryServiceClient
	Vault     gastrologv1connect.VaultServiceClient
	Config    gastrologv1connect.ConfigServiceClient
	Lifecycle gastrologv1connect.LifecycleServiceClient
	Auth      gastrologv1connect.AuthServiceClient
	Job       gastrologv1connect.JobServiceClient
}

// NewClient creates Connect clients for the given base URL.
func NewClient(baseURL string, opts ...connect.ClientOption) *Client {
	return &Client{
		Query:     gastrologv1connect.NewQueryServiceClient(http.DefaultClient, baseURL, opts...),
		Vault:     gastrologv1connect.NewVaultServiceClient(http.DefaultClient, baseURL, opts...),
		Config:    gastrologv1connect.NewConfigServiceClient(http.DefaultClient, baseURL, opts...),
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
		Config:    gastrologv1connect.NewConfigServiceClient(httpClient, baseURL, opts...),
		Lifecycle: gastrologv1connect.NewLifecycleServiceClient(httpClient, baseURL, opts...),
		Auth:      gastrologv1connect.NewAuthServiceClient(httpClient, baseURL, opts...),
		Job:       gastrologv1connect.NewJobServiceClient(httpClient, baseURL, opts...),
	}
}
