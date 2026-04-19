package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/golang-jwt/jwt/v5"

	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
)

// NoAuthInterceptor is a Connect interceptor that injects synthetic admin
// claims into every request, bypassing all authentication.
type NoAuthInterceptor struct{}

func noAuthClaims() *Claims {
	return &Claims{
		Role:   "admin",
		UserID: "00000000-0000-0000-0000-000000000000",
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "admin",
			ExpiresAt: jwt.NewNumericDate(time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)),
		},
	}
}

// WrapUnary implements connect.Interceptor for unary RPCs.
func (i *NoAuthInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		return next(WithClaims(ctx, noAuthClaims()), req)
	}
}

// WrapStreamingHandler implements connect.Interceptor for server-side streaming RPCs.
func (i *NoAuthInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		return next(WithClaims(ctx, noAuthClaims()), conn)
	}
}

// WrapStreamingClient is a no-op for server-side interceptors.
func (i *NoAuthInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

// UserCounter provides user count for first-boot detection.
// system.Store satisfies this interface.
type UserCounter interface {
	CountUsers(ctx context.Context) (int, error)
}

// TokenValidator checks whether a token is still valid after JWT verification.
// This is used for server-side token revocation (e.g. after logout, password
// change, or role change).
type TokenValidator interface {
	IsTokenValid(ctx context.Context, userID string, issuedAt time.Time) (bool, error)
}

// AuthInterceptor is a Connect interceptor that validates JWT tokens
// and enforces access levels per endpoint.
type AuthInterceptor struct {
	tokens    *TokenService
	counter   UserCounter
	validator TokenValidator
	public    map[string]bool
	admin     map[string]bool
}

// NewAuthInterceptor creates an interceptor with the standard access level
// configuration. Public endpoints require no auth. Admin endpoints require
// role=admin. Everything else requires a valid token.
// validator may be nil (token revocation is skipped).
func NewAuthInterceptor(tokens *TokenService, counter UserCounter, validator TokenValidator) *AuthInterceptor {
	return &AuthInterceptor{
		tokens:    tokens,
		counter:   counter,
		validator: validator,
		public: map[string]bool{
			gastrologv1connect.LifecycleServiceHealthProcedure:       true,
			gastrologv1connect.AuthServiceGetAuthStatusProcedure:     true,
			gastrologv1connect.AuthServiceLoginProcedure:             true,
			gastrologv1connect.AuthServiceRegisterProcedure:          true, // self-guards after first user
			gastrologv1connect.AuthServiceRefreshTokenProcedure:      true, // uses opaque token, not JWT
			gastrologv1connect.SystemServiceGetSettingsProcedure: true, // password policy needed on register page
		},
		admin: map[string]bool{
			// User management
			gastrologv1connect.AuthServiceCreateUserProcedure:     true,
			gastrologv1connect.AuthServiceListUsersProcedure:      true,
			gastrologv1connect.AuthServiceUpdateUserRoleProcedure: true,
			gastrologv1connect.AuthServiceResetPasswordProcedure:  true,
			gastrologv1connect.AuthServiceDeleteUserProcedure:     true,
			gastrologv1connect.AuthServiceRenameUserProcedure:     true,
			// Lifecycle + cluster
			gastrologv1connect.LifecycleServiceShutdownProcedure:        true,
			gastrologv1connect.LifecycleServiceGetClusterStatusProcedure: true,
			gastrologv1connect.LifecycleServiceSetNodeSuffrageProcedure: true,
			gastrologv1connect.LifecycleServiceJoinClusterProcedure:     true,
			gastrologv1connect.LifecycleServiceRemoveNodeProcedure:          true,
			gastrologv1connect.LifecycleServiceWatchSystemStatusProcedure: true,
			// VaultService (inspector + operations)
			gastrologv1connect.VaultServiceListVaultsProcedure:    true,
			gastrologv1connect.VaultServiceGetVaultProcedure:      true,
			gastrologv1connect.VaultServiceListChunksProcedure:    true,
			gastrologv1connect.VaultServiceGetChunkProcedure:      true,
			gastrologv1connect.VaultServiceGetIndexesProcedure:    true,
			gastrologv1connect.VaultServiceAnalyzeChunkProcedure:  true,
			gastrologv1connect.VaultServiceGetStatsProcedure:      true,
			gastrologv1connect.VaultServiceReindexVaultProcedure:  true,
			gastrologv1connect.VaultServiceValidateVaultProcedure: true,
			gastrologv1connect.VaultServiceMigrateVaultProcedure:  true,
			gastrologv1connect.VaultServiceExportVaultProcedure:   true,
			gastrologv1connect.VaultServiceImportRecordsProcedure: true,
			gastrologv1connect.VaultServiceMergeVaultsProcedure:   true,
			gastrologv1connect.VaultServiceSealVaultProcedure:     true,
			// ConfigService — mutations
			gastrologv1connect.SystemServiceGetSystemProcedure:             true,
			gastrologv1connect.SystemServiceListIngestersProcedure:         true,
			gastrologv1connect.SystemServiceGetIngesterStatusProcedure:     true,
			gastrologv1connect.SystemServicePutFilterProcedure:             true,
			gastrologv1connect.SystemServiceDeleteFilterProcedure:          true,
			gastrologv1connect.SystemServicePutRotationPolicyProcedure:     true,
			gastrologv1connect.SystemServiceDeleteRotationPolicyProcedure:  true,
			gastrologv1connect.SystemServicePutRetentionPolicyProcedure:    true,
			gastrologv1connect.SystemServiceDeleteRetentionPolicyProcedure: true,
			gastrologv1connect.SystemServicePutVaultProcedure:              true,
			gastrologv1connect.SystemServiceDeleteVaultProcedure:           true,
			gastrologv1connect.SystemServicePutIngesterProcedure:           true,
			gastrologv1connect.SystemServiceDeleteIngesterProcedure:        true,
			gastrologv1connect.SystemServicePutServiceSettingsProcedure:   true,
			gastrologv1connect.SystemServicePutLookupSettingsProcedure:    true,
			gastrologv1connect.SystemServicePutMaxMindSettingsProcedure:   true,
			gastrologv1connect.SystemServicePutSetupSettingsProcedure:     true,
			gastrologv1connect.SystemServiceRegenerateJwtSecretProcedure:   true,
			gastrologv1connect.SystemServicePutNodeConfigProcedure:         true,
			gastrologv1connect.SystemServicePutRouteProcedure:              true,
			gastrologv1connect.SystemServiceDeleteRouteProcedure:           true,
			gastrologv1connect.SystemServicePauseVaultProcedure:            true,
			gastrologv1connect.SystemServiceResumeVaultProcedure:           true,
			gastrologv1connect.SystemServiceTriggerIngesterProcedure:       true,
			// ConfigService — certificates
			gastrologv1connect.SystemServiceListCertificatesProcedure:  true,
			gastrologv1connect.SystemServiceGetCertificateProcedure:    true,
			gastrologv1connect.SystemServicePutCertificateProcedure:    true,
			gastrologv1connect.SystemServiceDeleteCertificateProcedure: true,
			// ConfigService — managed files
			gastrologv1connect.SystemServiceListManagedFilesProcedure:  true,
			gastrologv1connect.SystemServiceDeleteManagedFileProcedure: true,
			// QueryService — destructive
			gastrologv1connect.QueryServiceExportToVaultProcedure: true,
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
	// Public endpoints need no auth but still benefit from knowing WHO is
	// calling (e.g. GetSettings returns more data to authenticated users).
	// Best-effort: parse the token if present, ignore failures.
	if i.public[procedure] {
		return i.bestEffortClaims(ctx, headers), nil
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
		return ctx, connect.NewError(connect.CodeUnauthenticated, errors.New("no users registered; call Register to create the first user"))
	}

	// Extract, verify, and validate the token.
	claims, err := i.verifiedClaims(ctx, headers)
	if err != nil {
		return ctx, err
	}

	// Admin check.
	if i.admin[procedure] && claims.Role != "admin" {
		return ctx, connect.NewError(connect.CodePermissionDenied, errors.New("admin role required"))
	}

	return WithClaims(ctx, claims), nil
}

// verifiedClaims extracts a Bearer token from headers, verifies its signature,
// and checks server-side revocation. Returns the parsed claims or a Connect error.
func (i *AuthInterceptor) verifiedClaims(ctx context.Context, headers interface{ Get(string) string }) (*Claims, error) {
	authHeader := headers.Get("Authorization")
	if authHeader == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("missing authorization header"))
	}
	token, ok := strings.CutPrefix(authHeader, "Bearer ")
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authorization header must use Bearer scheme"))
	}
	claims, err := i.tokens.Verify(token)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("invalid token: %w", err))
	}
	if i.validator != nil && claims.IssuedAt != nil {
		valid, err := i.validator.IsTokenValid(ctx, claims.UserID, claims.IssuedAt.Time)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("validate token: %w", err))
		}
		if !valid {
			return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("token has been revoked"))
		}
	}
	return claims, nil
}

// bestEffortClaims tries to extract and verify claims from the Authorization
// header. On any failure (missing header, bad token, expired, revoked) it
// returns the original context unchanged — the caller proceeds as anonymous.
func (i *AuthInterceptor) bestEffortClaims(ctx context.Context, headers interface{ Get(string) string }) context.Context {
	claims, err := i.verifiedClaims(ctx, headers)
	if err != nil {
		return ctx
	}
	return WithClaims(ctx, claims)
}
