package auth

import (
	"context"
	"fmt"
	"strings"

	"connectrpc.com/connect"

	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
)

// UserCounter provides user count for first-boot detection.
// config.Store satisfies this interface.
type UserCounter interface {
	CountUsers(ctx context.Context) (int, error)
}

// AuthInterceptor is a Connect interceptor that validates JWT tokens
// and enforces access levels per endpoint.
type AuthInterceptor struct {
	tokens  *TokenService
	counter UserCounter
	public  map[string]bool
	admin   map[string]bool
}

// NewAuthInterceptor creates an interceptor with the standard access level
// configuration. Public endpoints require no auth. Admin endpoints require
// role=admin. Everything else requires a valid token.
func NewAuthInterceptor(tokens *TokenService, counter UserCounter) *AuthInterceptor {
	return &AuthInterceptor{
		tokens:  tokens,
		counter: counter,
		public: map[string]bool{
			gastrologv1connect.LifecycleServiceHealthProcedure:      true,
			gastrologv1connect.AuthServiceGetAuthStatusProcedure:    true,
			gastrologv1connect.AuthServiceLoginProcedure:            true,
			gastrologv1connect.AuthServiceRegisterProcedure:         true, // self-guards after first user
			gastrologv1connect.ConfigServiceGetServerConfigProcedure: true, // password policy needed on register page
		},
		admin: map[string]bool{
			// User management (admin-only)
			gastrologv1connect.AuthServiceCreateUserProcedure:     true,
			gastrologv1connect.AuthServiceListUsersProcedure:      true,
			gastrologv1connect.AuthServiceUpdateUserRoleProcedure: true,
			gastrologv1connect.AuthServiceResetPasswordProcedure:  true,
			gastrologv1connect.AuthServiceDeleteUserProcedure:     true,
			// Lifecycle
			gastrologv1connect.LifecycleServiceShutdownProcedure: true,
			// StoreService
			gastrologv1connect.StoreServiceListStoresProcedure:   true,
			gastrologv1connect.StoreServiceGetStoreProcedure:     true,
			gastrologv1connect.StoreServiceListChunksProcedure:   true,
			gastrologv1connect.StoreServiceGetChunkProcedure:     true,
			gastrologv1connect.StoreServiceGetIndexesProcedure:   true,
			gastrologv1connect.StoreServiceAnalyzeChunkProcedure: true,
			gastrologv1connect.StoreServiceGetStatsProcedure:     true,
			// ConfigService
			gastrologv1connect.ConfigServiceGetConfigProcedure:             true,
			gastrologv1connect.ConfigServiceListIngestersProcedure:         true,
			gastrologv1connect.ConfigServiceGetIngesterStatusProcedure:     true,
			gastrologv1connect.ConfigServicePutFilterProcedure:             true,
			gastrologv1connect.ConfigServiceDeleteFilterProcedure:          true,
			gastrologv1connect.ConfigServicePutRotationPolicyProcedure:     true,
			gastrologv1connect.ConfigServiceDeleteRotationPolicyProcedure:  true,
			gastrologv1connect.ConfigServicePutRetentionPolicyProcedure:    true,
			gastrologv1connect.ConfigServiceDeleteRetentionPolicyProcedure: true,
			gastrologv1connect.ConfigServicePutStoreProcedure:              true,
			gastrologv1connect.ConfigServiceDeleteStoreProcedure:           true,
			gastrologv1connect.ConfigServicePutIngesterProcedure:           true,
			gastrologv1connect.ConfigServiceDeleteIngesterProcedure:        true,
			gastrologv1connect.ConfigServicePutServerConfigProcedure:       true,
		},
	}
}

// WrapUnary implements connect.Interceptor for unary RPCs.
func (i *AuthInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		ctx, err := i.authenticate(ctx, req.Spec().Procedure, req.Header())
		if err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

// WrapStreamingHandler implements connect.Interceptor for server-side streaming RPCs.
func (i *AuthInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		ctx, err := i.authenticate(ctx, conn.Spec().Procedure, conn.RequestHeader())
		if err != nil {
			return err
		}
		return next(ctx, conn)
	}
}

// WrapStreamingClient is a no-op for server-side interceptors.
func (i *AuthInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

// authenticate checks the token and access level for a procedure.
// Returns the (possibly enriched) context or a Connect error.
func (i *AuthInterceptor) authenticate(ctx context.Context, procedure string, headers interface{ Get(string) string }) (context.Context, error) {
	// Public endpoints need no auth.
	if i.public[procedure] {
		return ctx, nil
	}

	// First-boot: if no users exist, allow Register (so the first admin
	// can be created) but block everything else.
	count, err := i.counter.CountUsers(ctx)
	if err != nil {
		return ctx, connect.NewError(connect.CodeInternal, fmt.Errorf("check user count: %w", err))
	}
	if count == 0 {
		if procedure == gastrologv1connect.AuthServiceRegisterProcedure {
			return ctx, nil
		}
		return ctx, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("no users registered; call Register to create the first user"))
	}

	// Extract Bearer token.
	authHeader := headers.Get("Authorization")
	if authHeader == "" {
		return ctx, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("missing authorization header"))
	}
	token, ok := strings.CutPrefix(authHeader, "Bearer ")
	if !ok {
		return ctx, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("authorization header must use Bearer scheme"))
	}

	// Verify token.
	claims, err := i.tokens.Verify(token)
	if err != nil {
		return ctx, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("invalid token: %w", err))
	}

	// Admin check.
	if i.admin[procedure] && claims.Role != "admin" {
		return ctx, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("admin role required"))
	}

	return WithClaims(ctx, claims), nil
}
