package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/golang-jwt/jwt/v5"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
)

// NoAuthInterceptor is a Connect interceptor that injects synthetic admin
// claims into every request, bypassing all authentication.
type NoAuthInterceptor struct{}

func noAuthClaims() *Claims {
	return &Claims{
		Role:      "admin",
		UserID:    "00000000-0000-0000-0000-000000000000",
		Subject:   "admin",
		ExpiresAt: jwt.NewNumericDate(time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)),
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

// AuthInterceptor is a Connect interceptor that enforces the authorization
// level each RPC declares in its proto.
type AuthInterceptor struct {
	verifier *Verifier
	counter  UserCounter
	levels   map[string]apiv1.AuthLevel
}

// NewAuthInterceptor creates an interceptor that enforces the level each RPC
// declares through the auth_level method option. A procedure that declares no
// level is denied. The verifier is shared with any non-Connect route that
// authorizes callers, so both apply the same checks.
func NewAuthInterceptor(verifier *Verifier, counter UserCounter) *AuthInterceptor {
	return &AuthInterceptor{
		verifier: verifier,
		counter:  counter,
		levels:   procedureLevels(),
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

// authenticate checks the declared level for a procedure and resolves the
// caller. Returns the (possibly enriched) context or a Connect error.
func (i *AuthInterceptor) authenticate(ctx context.Context, procedure string, headers HeaderGetter) (context.Context, error) {
	// Authorization is part of an RPC's contract: a procedure that declares no
	// level is denied, so an RPC added without one is unreachable rather than
	// open to everyone.
	level, declared := i.levels[procedure]
	if !declared {
		return ctx, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("procedure %s declares no authorization level", procedure))
	}

	// First-boot: if no users exist, allow Register (so the first admin can be
	// created) but block everything else. Public procedures skip the gate —
	// they are what a browser needs before anyone can log in.
	if level != apiv1.AuthLevel_AUTH_LEVEL_PUBLIC {
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
	}

	// A public procedure resolves whoever is calling without requiring it: a
	// handler such as GetSettings returns more to a known caller.
	claims, err := i.verifier.Authorize(ctx, level, headers)
	if err != nil {
		return ctx, err
	}
	if claims == nil {
		return ctx, nil
	}
	return WithClaims(ctx, claims), nil
}
