package server_test

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
	configmem "gastrolog/internal/config/memory"
	"gastrolog/internal/server"
)

// TestIntegration_RegisterThenDenyQuery registers a user via the Register RPC,
// then verifies that an unauthenticated query request is denied.
func TestIntegration_RegisterThenDenyQuery(t *testing.T) {
	cfgStore := configmem.NewStore()
	tokens := auth.NewTokenService([]byte("test-secret-key-32-bytes-long!!"), 7*24*time.Hour)
	interceptor := auth.NewAuthInterceptor(tokens, cfgStore, nil)
	opts := connect.WithInterceptors(interceptor)

	authServer := server.NewAuthServer(cfgStore, tokens)

	mux := http.NewServeMux()
	mux.Handle(gastrologv1connect.NewAuthServiceHandler(authServer, opts))
	mux.Handle(gastrologv1connect.NewQueryServiceHandler(&stubQueryForIntegration{}, opts))

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	authClient := gastrologv1connect.NewAuthServiceClient(http.DefaultClient, ts.URL)
	queryClient := gastrologv1connect.NewQueryServiceClient(http.DefaultClient, ts.URL)

	// Before registration: non-public query should be denied.
	_, err := queryClient.Explain(context.Background(), connect.NewRequest(&apiv1.ExplainRequest{}))
	if err == nil {
		t.Fatal("expected query to be denied before any users exist")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}

	// Register a user.
	regResp, err := authClient.Register(context.Background(), connect.NewRequest(&apiv1.RegisterRequest{
		Username: "admin",
		Password: "password123",
	}))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if regResp.Msg.Token == nil || regResp.Msg.Token.Token == "" {
		t.Fatal("expected token from Register")
	}

	// After registration: unauthenticated query should be DENIED.
	_, err = queryClient.Explain(context.Background(), connect.NewRequest(&apiv1.ExplainRequest{}))
	if err == nil {
		t.Fatal("BUG: unauthenticated request succeeded after user registration")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}

	// With the token from registration, it should work.
	authedClient := gastrologv1connect.NewQueryServiceClient(
		http.DefaultClient, ts.URL,
		withBearerToken(regResp.Msg.Token.Token),
	)
	_, err = authedClient.Explain(context.Background(), connect.NewRequest(&apiv1.ExplainRequest{}))
	if err != nil {
		t.Fatalf("authenticated request should succeed, got: %v", err)
	}
}

type stubQueryForIntegration struct {
	gastrologv1connect.UnimplementedQueryServiceHandler
}

func (s *stubQueryForIntegration) Explain(
	ctx context.Context,
	req *connect.Request[apiv1.ExplainRequest],
) (*connect.Response[apiv1.ExplainResponse], error) {
	return connect.NewResponse(&apiv1.ExplainResponse{}), nil
}

func withBearerToken(token string) connect.ClientOption {
	return connect.WithInterceptors(&bearerTokenInterceptor{token: token})
}

type bearerTokenInterceptor struct {
	token string
}

func (b *bearerTokenInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set("Authorization", "Bearer "+b.token)
		return next(ctx, req)
	}
}

func (b *bearerTokenInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (b *bearerTokenInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}
