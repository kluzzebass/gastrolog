package server

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	"github.com/google/uuid"

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

// animalNoises is the set of recognised animal sounds for password validation.
var animalNoises = []string{
	"moo", "woof", "bark", "meow", "oink", "quack", "baa", "neigh",
	"roar", "hiss", "chirp", "tweet", "cluck", "ribbit", "buzz",
	"howl", "purr", "squeak", "growl", "caw", "gobble",
}

// passwordPolicy holds the password complexity rules loaded from server config.
type passwordPolicy struct {
	MinLength             int
	RequireMixedCase      bool
	RequireDigit          bool
	RequireSpecial        bool
	MaxConsecutiveRepeats int
	ForbidAnimalNoise    bool
}

// loadPasswordPolicy reads the password policy from server config.
func (s *AuthServer) loadPasswordPolicy(ctx context.Context) passwordPolicy {
	p := passwordPolicy{MinLength: 8}
	raw, err := s.cfgStore.GetSetting(ctx, "server")
	if err != nil || raw == nil {
		return p
	}
	var sc config.ServerConfig
	if err := json.Unmarshal([]byte(*raw), &sc); err != nil {
		return p
	}
	if sc.Auth.MinPasswordLength > 0 {
		p.MinLength = sc.Auth.MinPasswordLength
	}
	p.RequireMixedCase = sc.Auth.RequireMixedCase
	p.RequireDigit = sc.Auth.RequireDigit
	p.RequireSpecial = sc.Auth.RequireSpecial
	p.MaxConsecutiveRepeats = sc.Auth.MaxConsecutiveRepeats
	p.ForbidAnimalNoise = sc.Auth.ForbidAnimalNoise
	return p
}

// validatePassword checks a password against the policy and returns a descriptive error.
func validatePassword(pw string, p passwordPolicy) error {
	if utf8.RuneCountInString(pw) < p.MinLength {
		return fmt.Errorf("password must be at least %d characters", p.MinLength)
	}
	if p.RequireMixedCase {
		hasLower := regexp.MustCompile(`[a-z]`).MatchString(pw)
		hasUpper := regexp.MustCompile(`[A-Z]`).MatchString(pw)
		if !hasLower || !hasUpper {
			return fmt.Errorf("password must contain both uppercase and lowercase letters")
		}
	}
	if p.RequireDigit {
		if !regexp.MustCompile(`[0-9]`).MatchString(pw) {
			return fmt.Errorf("password must contain at least one digit")
		}
	}
	if p.RequireSpecial {
		if !regexp.MustCompile(`[^a-zA-Z0-9]`).MatchString(pw) {
			return fmt.Errorf("password must contain at least one special character")
		}
	}
	if p.MaxConsecutiveRepeats > 0 {
		count := 1
		var prev rune
		for i, r := range pw {
			if i > 0 && r == prev {
				count++
				if count > p.MaxConsecutiveRepeats {
					return fmt.Errorf("password must not have more than %d identical characters in a row", p.MaxConsecutiveRepeats)
				}
			} else {
				count = 1
			}
			prev = r
		}
	}
	if p.ForbidAnimalNoise {
		lower := strings.ToLower(pw)
		for _, noise := range animalNoises {
			if strings.Contains(lower, noise) {
				return fmt.Errorf("password must not contain animal noises (e.g. moo, woof, meow)")
			}
		}
	}
	return nil
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
	if err := validatePassword(password, s.loadPasswordPolicy(ctx)); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Hash password.
	hash, err := auth.HashPassword(password)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("hash password: %w", err))
	}

	// Create first user as admin.
	userID := uuid.Must(uuid.NewV7())
	now := time.Now().UTC()
	user := config.User{
		ID:           userID,
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
	token, expiresAt, err := s.tokens.Issue(userID.String(), username, "admin")
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

	user, err := s.cfgStore.GetUserByUsername(ctx, username)
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

	token, expiresAt, err := s.tokens.Issue(user.ID.String(), username, user.Role)
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
	if err := validatePassword(newPassword, s.loadPasswordPolicy(ctx)); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Verify old password.
	user, err := s.cfgStore.GetUserByUsername(ctx, username)
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

	if err := s.cfgStore.UpdatePassword(ctx, user.ID, hash); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("update password: %w", err))
	}

	// Invalidate existing tokens so the user must re-login with the new password.
	if err := s.cfgStore.InvalidateTokens(ctx, user.ID, time.Now().UTC()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("invalidate tokens: %w", err))
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

	if err := validatePassword(password, s.loadPasswordPolicy(ctx)); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	if role != "admin" && role != "user" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("role must be \"admin\" or \"user\""))
	}

	existing, err := s.cfgStore.GetUserByUsername(ctx, username)
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
		ID:           uuid.Must(uuid.NewV7()),
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
	userID, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}
	role := req.Msg.Role

	if role != "admin" && role != "user" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("role must be \"admin\" or \"user\""))
	}

	user, err := s.cfgStore.GetUser(ctx, userID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("get user: %w", err))
	}
	if user == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("user %q not found", userID))
	}

	if err := s.cfgStore.UpdateUserRole(ctx, userID, role); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("update role: %w", err))
	}

	// Invalidate existing tokens so the user's role claim gets refreshed on re-login.
	if err := s.cfgStore.InvalidateTokens(ctx, userID, time.Now().UTC()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("invalidate tokens: %w", err))
	}

	// Re-fetch to get the updated user.
	user, err = s.cfgStore.GetUser(ctx, userID)
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
	userID, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}
	newPassword := req.Msg.NewPassword

	if err := validatePassword(newPassword, s.loadPasswordPolicy(ctx)); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	user, err := s.cfgStore.GetUser(ctx, userID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("get user: %w", err))
	}
	if user == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("user %q not found", userID))
	}

	hash, err := auth.HashPassword(newPassword)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("hash password: %w", err))
	}

	if err := s.cfgStore.UpdatePassword(ctx, userID, hash); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("update password: %w", err))
	}

	// Invalidate existing tokens so the user must re-login with the new password.
	if err := s.cfgStore.InvalidateTokens(ctx, userID, time.Now().UTC()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("invalidate tokens: %w", err))
	}

	return connect.NewResponse(&apiv1.ResetPasswordResponse{}), nil
}

// DeleteUser removes a user account. Admin only.
// An admin cannot delete their own account.
func (s *AuthServer) DeleteUser(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteUserRequest],
) (*connect.Response[apiv1.DeleteUserResponse], error) {
	userID, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Prevent self-deletion.
	if claims := auth.ClaimsFromContext(ctx); claims != nil && claims.UserID == userID.String() {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("cannot delete your own account"))
	}

	if err := s.cfgStore.DeleteUser(ctx, userID); err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("delete user: %w", err))
	}

	return connect.NewResponse(&apiv1.DeleteUserResponse{}), nil
}

// Logout invalidates the current user's token by setting TokenInvalidatedAt to now.
func (s *AuthServer) Logout(
	ctx context.Context,
	req *connect.Request[apiv1.LogoutRequest],
) (*connect.Response[apiv1.LogoutResponse], error) {
	claims := auth.ClaimsFromContext(ctx)
	if claims == nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("no claims in context"))
	}

	userID, connErr := parseUUID(claims.UserID)
	if connErr != nil {
		return nil, connErr
	}

	now := time.Now().UTC()
	if err := s.cfgStore.InvalidateTokens(ctx, userID, now); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("invalidate tokens: %w", err))
	}

	return connect.NewResponse(&apiv1.LogoutResponse{}), nil
}

// userToProto converts a config.User to a proto UserInfo, stripping the password hash.
func userToProto(u config.User) *apiv1.UserInfo {
	return &apiv1.UserInfo{
		Id:        u.ID.String(),
		Username:  u.Username,
		Role:      u.Role,
		CreatedAt: u.CreatedAt.Unix(),
		UpdatedAt: u.UpdatedAt.Unix(),
	}
}
