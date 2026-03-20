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

func newAuthTestClient(t *testing.T) (gastrologv1connect.AuthServiceClient, *configmem.Store) {
	t.Helper()

	cfgStore := configmem.NewStore()
	tokens := auth.NewTokenService([]byte("test-secret-32-bytes-long-key!!"), 7*24*time.Hour)
	authServer := server.NewAuthServer(cfgStore, tokens, nil, false)

	_, handler := gastrologv1connect.NewAuthServiceHandler(authServer)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	client := gastrologv1connect.NewAuthServiceClient(http.DefaultClient, ts.URL)
	return client, cfgStore
}

// alwaysValidTokenValidator satisfies auth.TokenValidator for tests.
type alwaysValidTokenValidator struct{}

func (alwaysValidTokenValidator) IsTokenValid(context.Context, string, time.Time) (bool, error) {
	return true, nil
}

// newAuthTestClientWithInterceptor returns a client whose requests pass through
// the auth interceptor, allowing Logout (which reads JWT claims) to work.
func newAuthTestClientWithInterceptor(t *testing.T) (gastrologv1connect.AuthServiceClient, *configmem.Store, *auth.TokenService) {
	t.Helper()

	cfgStore := configmem.NewStore()
	tokens := auth.NewTokenService([]byte("test-secret-32-bytes-long-key!!"), 7*24*time.Hour)
	authServer := server.NewAuthServer(cfgStore, tokens, nil, false)
	interceptor := auth.NewAuthInterceptor(tokens, cfgStore, alwaysValidTokenValidator{})

	_, handler := gastrologv1connect.NewAuthServiceHandler(authServer,
		connect.WithInterceptors(interceptor),
	)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	client := gastrologv1connect.NewAuthServiceClient(http.DefaultClient, ts.URL)
	return client, cfgStore, tokens
}

func TestGetAuthStatus_NeedsSetup(t *testing.T) {
	t.Parallel()
	client, _ := newAuthTestClient(t)
	ctx := context.Background()

	resp, err := client.GetAuthStatus(ctx, connect.NewRequest(&apiv1.GetAuthStatusRequest{}))
	if err != nil {
		t.Fatalf("GetAuthStatus: %v", err)
	}
	if !resp.Msg.NeedsSetup {
		t.Error("expected needs_setup=true when no users exist")
	}
}

func TestRegister_FirstUserIsAdmin(t *testing.T) {
	t.Parallel()
	client, _ := newAuthTestClient(t)
	ctx := context.Background()

	resp, err := client.Register(ctx, connect.NewRequest(&apiv1.RegisterRequest{
		Username: "admin",
		Password: "password123",
	}))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if resp.Msg.Token == nil {
		t.Fatal("expected token in response")
	}
	if resp.Msg.Token.Token == "" {
		t.Error("expected non-empty token")
	}
	if resp.Msg.Token.ExpiresAt == 0 {
		t.Error("expected non-zero expiration")
	}

	// After registration, needs_setup should be false.
	statusResp, err := client.GetAuthStatus(ctx, connect.NewRequest(&apiv1.GetAuthStatusRequest{}))
	if err != nil {
		t.Fatalf("GetAuthStatus: %v", err)
	}
	if statusResp.Msg.NeedsSetup {
		t.Error("expected needs_setup=false after registration")
	}
}

func TestRegister_AfterFirstUser(t *testing.T) {
	t.Parallel()
	client, _ := newAuthTestClient(t)
	ctx := context.Background()

	_, err := client.Register(ctx, connect.NewRequest(&apiv1.RegisterRequest{
		Username: "alice",
		Password: "password123",
	}))
	if err != nil {
		t.Fatalf("Register first user: %v", err)
	}

	// Second Register should fail with FailedPrecondition.
	_, err = client.Register(ctx, connect.NewRequest(&apiv1.RegisterRequest{
		Username: "bob",
		Password: "password456",
	}))
	if err == nil {
		t.Fatal("expected error for register after first user")
	}
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("expected FailedPrecondition, got %v", connect.CodeOf(err))
	}
}

func TestRegister_InvalidUsername(t *testing.T) {
	t.Parallel()
	client, _ := newAuthTestClient(t)
	ctx := context.Background()

	_, err := client.Register(ctx, connect.NewRequest(&apiv1.RegisterRequest{
		Username: "ab", // too short
		Password: "password123",
	}))
	if err == nil {
		t.Fatal("expected error for short username")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", connect.CodeOf(err))
	}
}

func TestRegister_ShortPassword(t *testing.T) {
	t.Parallel()
	client, _ := newAuthTestClient(t)
	ctx := context.Background()

	_, err := client.Register(ctx, connect.NewRequest(&apiv1.RegisterRequest{
		Username: "validuser",
		Password: "short",
	}))
	if err == nil {
		t.Fatal("expected error for short password")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", connect.CodeOf(err))
	}
}

func TestLogin_Success(t *testing.T) {
	t.Parallel()
	client, _ := newAuthTestClient(t)
	ctx := context.Background()

	// Register first.
	_, err := client.Register(ctx, connect.NewRequest(&apiv1.RegisterRequest{
		Username: "bob",
		Password: "password123",
	}))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Login.
	resp, err := client.Login(ctx, connect.NewRequest(&apiv1.LoginRequest{
		Username: "bob",
		Password: "password123",
	}))
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if resp.Msg.Token == nil || resp.Msg.Token.Token == "" {
		t.Error("expected token in login response")
	}
}

func TestLogin_WrongPassword(t *testing.T) {
	t.Parallel()
	client, _ := newAuthTestClient(t)
	ctx := context.Background()

	_, err := client.Register(ctx, connect.NewRequest(&apiv1.RegisterRequest{
		Username: "carol",
		Password: "password123",
	}))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	_, err = client.Login(ctx, connect.NewRequest(&apiv1.LoginRequest{
		Username: "carol",
		Password: "wrongpassword",
	}))
	if err == nil {
		t.Fatal("expected error for wrong password")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}
}

func TestLogin_NonExistentUser(t *testing.T) {
	t.Parallel()
	client, _ := newAuthTestClient(t)
	ctx := context.Background()

	_, err := client.Login(ctx, connect.NewRequest(&apiv1.LoginRequest{
		Username: "ghost",
		Password: "password123",
	}))
	if err == nil {
		t.Fatal("expected error for non-existent user")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}
}

func TestChangePassword_Success(t *testing.T) {
	t.Parallel()
	client, _ := newAuthTestClient(t)
	ctx := context.Background()

	_, err := client.Register(ctx, connect.NewRequest(&apiv1.RegisterRequest{
		Username: "dave",
		Password: "oldpassword1",
	}))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	_, err = client.ChangePassword(ctx, connect.NewRequest(&apiv1.ChangePasswordRequest{
		Username:    "dave",
		OldPassword: "oldpassword1",
		NewPassword: "newpassword1",
	}))
	if err != nil {
		t.Fatalf("ChangePassword: %v", err)
	}

	// Old password should no longer work.
	_, err = client.Login(ctx, connect.NewRequest(&apiv1.LoginRequest{
		Username: "dave",
		Password: "oldpassword1",
	}))
	if err == nil {
		t.Error("expected old password to fail after change")
	}

	// New password should work.
	_, err = client.Login(ctx, connect.NewRequest(&apiv1.LoginRequest{
		Username: "dave",
		Password: "newpassword1",
	}))
	if err != nil {
		t.Fatalf("Login with new password: %v", err)
	}
}

func TestChangePassword_WrongOldPassword(t *testing.T) {
	t.Parallel()
	client, _ := newAuthTestClient(t)
	ctx := context.Background()

	_, err := client.Register(ctx, connect.NewRequest(&apiv1.RegisterRequest{
		Username: "eve",
		Password: "password123",
	}))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	_, err = client.ChangePassword(ctx, connect.NewRequest(&apiv1.ChangePasswordRequest{
		Username:    "eve",
		OldPassword: "wrongpassword",
		NewPassword: "newpassword1",
	}))
	if err == nil {
		t.Fatal("expected error for wrong old password")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}
}

func TestLogout_RevokesOnlyCurrentSession(t *testing.T) {
	t.Parallel()
	client, cfgStore, _ := newAuthTestClientWithInterceptor(t)
	ctx := context.Background()

	// Register a user.
	_, err := client.Register(ctx, connect.NewRequest(&apiv1.RegisterRequest{
		Username: "logout-test",
		Password: "password123",
	}))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Login from "session A".
	respA, err := client.Login(ctx, connect.NewRequest(&apiv1.LoginRequest{
		Username: "logout-test",
		Password: "password123",
	}))
	if err != nil {
		t.Fatalf("Login A: %v", err)
	}
	tokenA := respA.Msg.Token.Token
	refreshA := respA.Msg.RefreshToken

	// Login from "session B".
	respB, err := client.Login(ctx, connect.NewRequest(&apiv1.LoginRequest{
		Username: "logout-test",
		Password: "password123",
	}))
	if err != nil {
		t.Fatalf("Login B: %v", err)
	}
	refreshB := respB.Msg.RefreshToken

	// Verify both refresh tokens exist in the store.
	hashA := auth.HashRefreshToken(refreshA)
	hashB := auth.HashRefreshToken(refreshB)
	if tok, _ := cfgStore.GetRefreshTokenByHash(ctx, hashA); tok == nil {
		t.Fatal("expected refresh token A to exist before logout")
	}
	if tok, _ := cfgStore.GetRefreshTokenByHash(ctx, hashB); tok == nil {
		t.Fatal("expected refresh token B to exist before logout")
	}

	// Logout session A — sending its refresh token.
	logoutReq := connect.NewRequest(&apiv1.LogoutRequest{
		RefreshToken: refreshA,
	})
	logoutReq.Header().Set("Authorization", "Bearer "+tokenA)
	_, err = client.Logout(ctx, logoutReq)
	if err != nil {
		t.Fatalf("Logout: %v", err)
	}

	// Session A's refresh token should be gone.
	if tok, _ := cfgStore.GetRefreshTokenByHash(ctx, hashA); tok != nil {
		t.Error("expected refresh token A to be deleted after logout")
	}

	// Session B's refresh token should still exist.
	if tok, _ := cfgStore.GetRefreshTokenByHash(ctx, hashB); tok == nil {
		t.Error("expected refresh token B to survive logout of session A")
	}
}
