package auth_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/auth"
)

// mockCounter implements auth.UserCounter for testing.
type mockCounter struct {
	count int
	err   error
}

func (m *mockCounter) CountUsers(ctx context.Context) (int, error) {
	return m.count, m.err
}

// stubAuthService is a minimal AuthService that records whether it was called.
type stubAuthService struct {
	gastrologv1connect.UnimplementedAuthServiceHandler
	called bool
}

func (s *stubAuthService) GetAuthStatus(
	ctx context.Context,
	req *connect.Request[apiv1.GetAuthStatusRequest],
) (*connect.Response[apiv1.GetAuthStatusResponse], error) {
	s.called = true
	return connect.NewResponse(&apiv1.GetAuthStatusResponse{NeedsSetup: false}), nil
}

func (s *stubAuthService) Register(
	ctx context.Context,
	req *connect.Request[apiv1.RegisterRequest],
) (*connect.Response[apiv1.RegisterResponse], error) {
	s.called = true
	return connect.NewResponse(&apiv1.RegisterResponse{}), nil
}

func (s *stubAuthService) CreateUser(
	ctx context.Context,
	req *connect.Request[apiv1.CreateUserRequest],
) (*connect.Response[apiv1.CreateUserResponse], error) {
	s.called = true
	if req.Msg.Username == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("username required"))
	}
	return connect.NewResponse(&apiv1.CreateUserResponse{}), nil
}

func (s *stubAuthService) Login(
	ctx context.Context,
	req *connect.Request[apiv1.LoginRequest],
) (*connect.Response[apiv1.LoginResponse], error) {
	s.called = true
	return connect.NewResponse(&apiv1.LoginResponse{}), nil
}

func (s *stubAuthService) ChangePassword(
	ctx context.Context,
	req *connect.Request[apiv1.ChangePasswordRequest],
) (*connect.Response[apiv1.ChangePasswordResponse], error) {
	s.called = true
	// Verify claims were attached to context.
	claims := auth.ClaimsFromContext(ctx)
	if claims != nil {
		return connect.NewResponse(&apiv1.ChangePasswordResponse{}), nil
	}
	return connect.NewResponse(&apiv1.ChangePasswordResponse{}), nil
}

// stubConfigService is a minimal ConfigService for testing admin endpoints.
type stubConfigService struct {
	gastrologv1connect.UnimplementedConfigServiceHandler
}

func (s *stubConfigService) GetConfig(
	ctx context.Context,
	req *connect.Request[apiv1.GetConfigRequest],
) (*connect.Response[apiv1.GetConfigResponse], error) {
	return connect.NewResponse(&apiv1.GetConfigResponse{}), nil
}

// stubQueryService for testing authenticated endpoints.
type stubQueryService struct {
	gastrologv1connect.UnimplementedQueryServiceHandler
}

func (s *stubQueryService) Explain(
	ctx context.Context,
	req *connect.Request[apiv1.ExplainRequest],
) (*connect.Response[apiv1.ExplainResponse], error) {
	return connect.NewResponse(&apiv1.ExplainResponse{}), nil
}

type testSetup struct {
	authClient   gastrologv1connect.AuthServiceClient
	configClient gastrologv1connect.ConfigServiceClient
	queryClient  gastrologv1connect.QueryServiceClient
	server       *httptest.Server
}

func newTestSetup(t *testing.T, counter *mockCounter) *testSetup {
	t.Helper()

	tokens := auth.NewTokenService([]byte("test-secret-key-32-bytes-long!!"), 7*24*time.Hour)
	interceptor := auth.NewAuthInterceptor(tokens, counter, nil)
	opts := connect.WithInterceptors(interceptor)

	mux := http.NewServeMux()
	mux.Handle(gastrologv1connect.NewAuthServiceHandler(&stubAuthService{}, opts))
	mux.Handle(gastrologv1connect.NewConfigServiceHandler(&stubConfigService{}, opts))
	mux.Handle(gastrologv1connect.NewQueryServiceHandler(&stubQueryService{}, opts))

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return &testSetup{
		authClient:   gastrologv1connect.NewAuthServiceClient(http.DefaultClient, ts.URL),
		configClient: gastrologv1connect.NewConfigServiceClient(http.DefaultClient, ts.URL),
		queryClient:  gastrologv1connect.NewQueryServiceClient(http.DefaultClient, ts.URL),
		server:       ts,
	}
}

func withBearer(token string) connect.ClientOption {
	return connect.WithInterceptors(&bearerInterceptor{token: token})
}

type bearerInterceptor struct {
	token string
}

func (b *bearerInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set("Authorization", "Bearer "+b.token)
		return next(ctx, req)
	}
}

func (b *bearerInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (b *bearerInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

func TestPublicEndpoint_NoToken(t *testing.T) {
	s := newTestSetup(t, &mockCounter{count: 5})

	_, err := s.authClient.Login(context.Background(), connect.NewRequest(&apiv1.LoginRequest{}))
	if err != nil {
		t.Fatalf("public endpoint should pass without token: %v", err)
	}
}

func TestAuthenticatedEndpoint_MissingToken(t *testing.T) {
	s := newTestSetup(t, &mockCounter{count: 1})

	_, err := s.queryClient.Explain(context.Background(), connect.NewRequest(&apiv1.ExplainRequest{}))
	if err == nil {
		t.Fatal("expected error for missing token")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}
}

func TestAuthenticatedEndpoint_InvalidToken(t *testing.T) {
	s := newTestSetup(t, &mockCounter{count: 1})

	client := gastrologv1connect.NewQueryServiceClient(http.DefaultClient, s.server.URL, withBearer("invalid-token"))
	_, err := client.Explain(context.Background(), connect.NewRequest(&apiv1.ExplainRequest{}))
	if err == nil {
		t.Fatal("expected error for invalid token")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}
}

func TestAuthenticatedEndpoint_ValidToken(t *testing.T) {
	tokens := auth.NewTokenService([]byte("test-secret-key-32-bytes-long!!"), 7*24*time.Hour)
	token, _, err := tokens.Issue("uid-alice", "alice", "user")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	s := newTestSetup(t, &mockCounter{count: 1})
	client := gastrologv1connect.NewQueryServiceClient(http.DefaultClient, s.server.URL, withBearer(token))

	_, err = client.Explain(context.Background(), connect.NewRequest(&apiv1.ExplainRequest{}))
	if err != nil {
		t.Fatalf("valid token should pass: %v", err)
	}
}

func TestAdminEndpoint_NonAdminToken(t *testing.T) {
	tokens := auth.NewTokenService([]byte("test-secret-key-32-bytes-long!!"), 7*24*time.Hour)
	token, _, err := tokens.Issue("uid-alice", "alice", "user")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	s := newTestSetup(t, &mockCounter{count: 1})
	client := gastrologv1connect.NewConfigServiceClient(http.DefaultClient, s.server.URL, withBearer(token))

	_, err = client.GetConfig(context.Background(), connect.NewRequest(&apiv1.GetConfigRequest{}))
	if err == nil {
		t.Fatal("expected error for non-admin on admin endpoint")
	}
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", connect.CodeOf(err))
	}
}

func TestAdminEndpoint_AdminToken(t *testing.T) {
	tokens := auth.NewTokenService([]byte("test-secret-key-32-bytes-long!!"), 7*24*time.Hour)
	token, _, err := tokens.Issue("uid-admin", "admin", "admin")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	s := newTestSetup(t, &mockCounter{count: 1})
	client := gastrologv1connect.NewConfigServiceClient(http.DefaultClient, s.server.URL, withBearer(token))

	_, err = client.GetConfig(context.Background(), connect.NewRequest(&apiv1.GetConfigRequest{}))
	if err != nil {
		t.Fatalf("admin should access admin endpoint: %v", err)
	}
}

func TestFirstBoot_NonPublicDenied(t *testing.T) {
	s := newTestSetup(t, &mockCounter{count: 0})

	// Non-public endpoints should be denied even during first-boot.
	_, err := s.configClient.GetConfig(context.Background(), connect.NewRequest(&apiv1.GetConfigRequest{}))
	if err == nil {
		t.Fatal("expected non-public endpoint to be denied during first-boot")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}
}

func TestFirstBoot_PublicAllowed(t *testing.T) {
	s := newTestSetup(t, &mockCounter{count: 0})

	// Public endpoints should still work during first-boot.
	_, err := s.authClient.Login(context.Background(), connect.NewRequest(&apiv1.LoginRequest{}))
	if err != nil {
		t.Fatalf("public endpoint should pass during first-boot: %v", err)
	}
}

func TestCountUsersError_FailsClosed(t *testing.T) {
	s := newTestSetup(t, &mockCounter{err: fmt.Errorf("db error")})

	_, err := s.queryClient.Explain(context.Background(), connect.NewRequest(&apiv1.ExplainRequest{}))
	if err == nil {
		t.Fatal("expected error when CountUsers fails")
	}
	if connect.CodeOf(err) != connect.CodeInternal {
		t.Errorf("expected Internal, got %v", connect.CodeOf(err))
	}
}

func TestRegister_FirstBoot_Allowed(t *testing.T) {
	s := newTestSetup(t, &mockCounter{count: 0})

	_, err := s.authClient.Register(context.Background(), connect.NewRequest(&apiv1.RegisterRequest{}))
	if err != nil {
		t.Fatalf("Register should be allowed during first-boot: %v", err)
	}
}

func TestRegister_AfterSetup_PublicButSelfGuards(t *testing.T) {
	// Register is public at the interceptor level — the handler itself
	// rejects calls after the first user (tested in server tests).
	// Here we verify the interceptor doesn't block it.
	s := newTestSetup(t, &mockCounter{count: 1})
	_, err := s.authClient.Register(context.Background(), connect.NewRequest(&apiv1.RegisterRequest{}))
	// The stub handler succeeds — interceptor didn't block it.
	if err != nil {
		t.Fatalf("Register should pass through interceptor (handler guards): %v", err)
	}
}

func TestCreateUser_RequiresAdmin(t *testing.T) {
	tokens := auth.NewTokenService([]byte("test-secret-key-32-bytes-long!!"), 7*24*time.Hour)

	// No token → Unauthenticated.
	s := newTestSetup(t, &mockCounter{count: 1})
	_, err := s.authClient.CreateUser(context.Background(), connect.NewRequest(&apiv1.CreateUserRequest{}))
	if err == nil {
		t.Fatal("CreateUser without token should fail")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}

	// Non-admin token → PermissionDenied.
	userToken, _, _ := tokens.Issue("uid-alice", "alice", "user")
	userClient := gastrologv1connect.NewAuthServiceClient(http.DefaultClient, s.server.URL, withBearer(userToken))
	_, err = userClient.CreateUser(context.Background(), connect.NewRequest(&apiv1.CreateUserRequest{}))
	if err == nil {
		t.Fatal("CreateUser with non-admin token should fail")
	}
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", connect.CodeOf(err))
	}

	// Admin token → allowed (will fail on validation, not auth).
	adminToken, _, _ := tokens.Issue("uid-admin", "admin", "admin")
	adminClient := gastrologv1connect.NewAuthServiceClient(http.DefaultClient, s.server.URL, withBearer(adminToken))
	_, err = adminClient.CreateUser(context.Background(), connect.NewRequest(&apiv1.CreateUserRequest{}))
	if err == nil {
		t.Fatal("CreateUser with empty request should fail on validation")
	}
	// Should get past auth and hit validation (InvalidArgument), not auth errors.
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", connect.CodeOf(err))
	}
}
