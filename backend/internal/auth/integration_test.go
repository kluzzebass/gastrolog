package auth_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/auth"
	"gastrolog/internal/config"
	configmem "gastrolog/internal/config/memory"
)

// TestIntegration_RegisterThenDeny proves that after a user is created,
// unauthenticated requests to protected endpoints are denied.
func TestIntegration_RegisterThenDeny(t *testing.T) {
	cfgStore := configmem.NewStore()
	tokens := auth.NewTokenService([]byte("test-secret-key-32-bytes-long!!"), 7*24*time.Hour)
	interceptor := auth.NewAuthInterceptor(tokens, cfgStore)
	opts := connect.WithInterceptors(interceptor)

	mux := http.NewServeMux()
	mux.Handle(gastrologv1connect.NewQueryServiceHandler(&stubQueryService{}, opts))

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	client := gastrologv1connect.NewQueryServiceClient(http.DefaultClient, ts.URL)

	// Before any users: non-public endpoints should be denied.
	_, err := client.Explain(context.Background(), connect.NewRequest(&apiv1.ExplainRequest{}))
	if err == nil {
		t.Fatal("expected non-public endpoint to be denied before any users exist")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}

	// Create a user in the store.
	now := time.Now().UTC()
	hash, err := auth.HashPassword("testpassword")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	err = cfgStore.CreateUser(context.Background(), config.User{
		Username:     "admin",
		PasswordHash: hash,
		Role:         "admin",
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	// Now the same request without a token should be denied.
	_, err = client.Explain(context.Background(), connect.NewRequest(&apiv1.ExplainRequest{}))
	if err == nil {
		t.Fatal("expected Unauthenticated after user exists, but request succeeded")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}

	// With a valid token, it should pass.
	token, _, err := tokens.Issue("uid-admin", "admin", "admin")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	authedClient := gastrologv1connect.NewQueryServiceClient(http.DefaultClient, ts.URL, withBearer(token))
	_, err = authedClient.Explain(context.Background(), connect.NewRequest(&apiv1.ExplainRequest{}))
	if err != nil {
		t.Fatalf("expected authenticated request to pass, got: %v", err)
	}
}
