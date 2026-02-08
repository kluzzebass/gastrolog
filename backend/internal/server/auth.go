package server

import (
	"context"
	"fmt"
	"regexp"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/auth"
	"gastrolog/internal/config"
)

var usernameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{3,64}$`)

// AuthServer implements the AuthService.
type AuthServer struct {
	cfgStore config.Store
	tokens   *auth.TokenService
}

var _ gastrologv1connect.AuthServiceHandler = (*AuthServer)(nil)

// NewAuthServer creates a new AuthServer.
func NewAuthServer(cfgStore config.Store, tokens *auth.TokenService) *AuthServer {
	return &AuthServer{
		cfgStore: cfgStore,
		tokens:   tokens,
	}
}

// Register creates a new user account and returns a token.
func (s *AuthServer) Register(
	ctx context.Context,
	req *connect.Request[apiv1.RegisterRequest],
) (*connect.Response[apiv1.RegisterResponse], error) {
	username := req.Msg.Username
	password := req.Msg.Password

	// Validate username.
	if !usernameRe.MatchString(username) {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("username must be 3-64 characters, alphanumeric, underscores, or hyphens"))
	}

	// Validate password.
	if utf8.RuneCountInString(password) < 8 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("password must be at least 8 characters"))
	}

	// Check if username is taken.
	existing, err := s.cfgStore.GetUser(ctx, username)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("check user: %w", err))
	}
	if existing != nil {
		return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("username %q is already taken", username))
	}

	// Determine role: first user is admin.
	count, err := s.cfgStore.CountUsers(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("count users: %w", err))
	}
	role := "user"
	if count == 0 {
		role = "admin"
	}

	// Hash password.
	hash, err := auth.HashPassword(password)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("hash password: %w", err))
	}

	// Create user.
	now := time.Now().UTC()
	user := config.User{
		Username:     username,
		PasswordHash: hash,
		Role:         role,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := s.cfgStore.CreateUser(ctx, user); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("create user: %w", err))
	}

	// Issue token.
	token, expiresAt, err := s.tokens.Issue(username, role)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("issue token: %w", err))
	}

	return connect.NewResponse(&apiv1.RegisterResponse{
		Token: &apiv1.Token{
			Token:     token,
			ExpiresAt: expiresAt.Unix(),
		},
	}), nil
}

// Login authenticates a user and returns a token.
func (s *AuthServer) Login(
	ctx context.Context,
	req *connect.Request[apiv1.LoginRequest],
) (*connect.Response[apiv1.LoginResponse], error) {
	username := req.Msg.Username
	password := req.Msg.Password

	user, err := s.cfgStore.GetUser(ctx, username)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("get user: %w", err))
	}
	if user == nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("invalid credentials"))
	}

	ok, err := auth.VerifyPassword(password, user.PasswordHash)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("verify password: %w", err))
	}
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("invalid credentials"))
	}

	token, expiresAt, err := s.tokens.Issue(username, user.Role)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("issue token: %w", err))
	}

	return connect.NewResponse(&apiv1.LoginResponse{
		Token: &apiv1.Token{
			Token:     token,
			ExpiresAt: expiresAt.Unix(),
		},
	}), nil
}

// ChangePassword updates a user's password.
func (s *AuthServer) ChangePassword(
	ctx context.Context,
	req *connect.Request[apiv1.ChangePasswordRequest],
) (*connect.Response[apiv1.ChangePasswordResponse], error) {
	username := req.Msg.Username
	oldPassword := req.Msg.OldPassword
	newPassword := req.Msg.NewPassword

	// Validate new password.
	if utf8.RuneCountInString(newPassword) < 8 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("new password must be at least 8 characters"))
	}

	// Verify old password.
	user, err := s.cfgStore.GetUser(ctx, username)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("get user: %w", err))
	}
	if user == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("user not found"))
	}

	ok, err := auth.VerifyPassword(oldPassword, user.PasswordHash)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("verify password: %w", err))
	}
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("old password is incorrect"))
	}

	// Hash new password.
	hash, err := auth.HashPassword(newPassword)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("hash password: %w", err))
	}

	if err := s.cfgStore.UpdatePassword(ctx, username, hash); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("update password: %w", err))
	}

	return connect.NewResponse(&apiv1.ChangePasswordResponse{}), nil
}

// GetAuthStatus returns whether the system needs initial user setup.
func (s *AuthServer) GetAuthStatus(
	ctx context.Context,
	req *connect.Request[apiv1.GetAuthStatusRequest],
) (*connect.Response[apiv1.GetAuthStatusResponse], error) {
	count, err := s.cfgStore.CountUsers(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("count users: %w", err))
	}

	return connect.NewResponse(&apiv1.GetAuthStatusResponse{
		NeedsSetup: count == 0,
	}), nil
}
