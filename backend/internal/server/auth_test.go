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
	authServer := server.NewAuthServer(cfgStore, tokens)

	_, handler := gastrologv1connect.NewAuthServiceHandler(authServer)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	client := gastrologv1connect.NewAuthServiceClient(http.DefaultClient, ts.URL)
	return client, cfgStore
}

func TestGetAuthStatus_NeedsSetup(t *testing.T) {
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

func TestRegister_DuplicateUsername(t *testing.T) {
	client, _ := newAuthTestClient(t)
	ctx := context.Background()

	_, err := client.Register(ctx, connect.NewRequest(&apiv1.RegisterRequest{
		Username: "alice",
		Password: "password123",
	}))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	_, err = client.Register(ctx, connect.NewRequest(&apiv1.RegisterRequest{
		Username: "alice",
		Password: "differentpassword",
	}))
	if err == nil {
		t.Fatal("expected error for duplicate username")
	}
	if connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Errorf("expected AlreadyExists, got %v", connect.CodeOf(err))
	}
}

func TestRegister_InvalidUsername(t *testing.T) {
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
