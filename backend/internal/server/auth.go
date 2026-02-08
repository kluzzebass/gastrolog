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

// Register creates the first user account during initial setup.
// Returns FailedPrecondition if any users already exist.
func (s *AuthServer) Register(
	ctx context.Context,
	req *connect.Request[apiv1.RegisterRequest],
) (*connect.Response[apiv1.RegisterResponse], error) {
	// Register is first-user-only. After bootstrap, use CreateUser.
	count, err := s.cfgStore.CountUsers(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("count users: %w", err))
	}
	if count > 0 {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("registration is disabled; use the admin API to create users"))
	}

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

	// Hash password.
	hash, err := auth.HashPassword(password)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("hash password: %w", err))
	}

	// Create first user as admin.
	now := time.Now().UTC()
	user := config.User{
		Username:     username,
		PasswordHash: hash,
		Role:         "admin",
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := s.cfgStore.CreateUser(ctx, user); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("create user: %w", err))
	}

	// Issue token.
	token, expiresAt, err := s.tokens.Issue(username, "admin")
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
	// Prefer username from JWT claims (set by auth interceptor).
	// Fall back to request field for first-boot or unauthenticated contexts.
	username := req.Msg.Username
	if claims := auth.ClaimsFromContext(ctx); claims != nil {
		username = claims.Username()
	}
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

// CreateUser creates a new user account. Admin only.
func (s *AuthServer) CreateUser(
	ctx context.Context,
	req *connect.Request[apiv1.CreateUserRequest],
) (*connect.Response[apiv1.CreateUserResponse], error) {
	username := req.Msg.Username
	password := req.Msg.Password
	role := req.Msg.Role

	if !usernameRe.MatchString(username) {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("username must be 3-64 characters, alphanumeric, underscores, or hyphens"))
	}

	if utf8.RuneCountInString(password) < 8 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("password must be at least 8 characters"))
	}

	if role != "admin" && role != "user" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("role must be \"admin\" or \"user\""))
	}

	existing, err := s.cfgStore.GetUser(ctx, username)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("check user: %w", err))
	}
	if existing != nil {
		return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("username %q is already taken", username))
	}

	hash, err := auth.HashPassword(password)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("hash password: %w", err))
	}

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

	return connect.NewResponse(&apiv1.CreateUserResponse{
		User: userToProto(user),
	}), nil
}

// ListUsers returns all user accounts. Admin only.
func (s *AuthServer) ListUsers(
	ctx context.Context,
	req *connect.Request[apiv1.ListUsersRequest],
) (*connect.Response[apiv1.ListUsersResponse], error) {
	users, err := s.cfgStore.ListUsers(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("list users: %w", err))
	}

	infos := make([]*apiv1.UserInfo, len(users))
	for i, u := range users {
		infos[i] = userToProto(u)
	}

	return connect.NewResponse(&apiv1.ListUsersResponse{
		Users: infos,
	}), nil
}

// UpdateUserRole changes a user's role. Admin only.
func (s *AuthServer) UpdateUserRole(
	ctx context.Context,
	req *connect.Request[apiv1.UpdateUserRoleRequest],
) (*connect.Response[apiv1.UpdateUserRoleResponse], error) {
	username := req.Msg.Username
	role := req.Msg.Role

	if role != "admin" && role != "user" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("role must be \"admin\" or \"user\""))
	}

	if err := s.cfgStore.UpdateUserRole(ctx, username, role); err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("update role: %w", err))
	}

	user, err := s.cfgStore.GetUser(ctx, username)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("get user: %w", err))
	}

	return connect.NewResponse(&apiv1.UpdateUserRoleResponse{
		User: userToProto(*user),
	}), nil
}

// ResetPassword sets a new password for a user. Admin only.
func (s *AuthServer) ResetPassword(
	ctx context.Context,
	req *connect.Request[apiv1.ResetPasswordRequest],
) (*connect.Response[apiv1.ResetPasswordResponse], error) {
	username := req.Msg.Username
	newPassword := req.Msg.NewPassword

	if utf8.RuneCountInString(newPassword) < 8 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("password must be at least 8 characters"))
	}

	user, err := s.cfgStore.GetUser(ctx, username)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("get user: %w", err))
	}
	if user == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("user %q not found", username))
	}

	hash, err := auth.HashPassword(newPassword)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("hash password: %w", err))
	}

	if err := s.cfgStore.UpdatePassword(ctx, username, hash); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("update password: %w", err))
	}

	return connect.NewResponse(&apiv1.ResetPasswordResponse{}), nil
}

// DeleteUser removes a user account. Admin only.
// An admin cannot delete their own account.
func (s *AuthServer) DeleteUser(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteUserRequest],
) (*connect.Response[apiv1.DeleteUserResponse], error) {
	username := req.Msg.Username

	// Prevent self-deletion.
	if claims := auth.ClaimsFromContext(ctx); claims != nil && claims.Username() == username {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("cannot delete your own account"))
	}

	if err := s.cfgStore.DeleteUser(ctx, username); err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("delete user: %w", err))
	}

	return connect.NewResponse(&apiv1.DeleteUserResponse{}), nil
}

// userToProto converts a config.User to a proto UserInfo, stripping the password hash.
func userToProto(u config.User) *apiv1.UserInfo {
	return &apiv1.UserInfo{
		Username:  u.Username,
		Role:      u.Role,
		CreatedAt: u.CreatedAt.Unix(),
		UpdatedAt: u.UpdatedAt.Unix(),
	}
}
