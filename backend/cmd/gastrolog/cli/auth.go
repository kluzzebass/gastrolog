package cli

import (
	"context"
	"os"

	"connectrpc.com/connect"
)

// authInterceptor adds a Bearer token to every outgoing request.
type authInterceptor struct {
	token string
}

func newAuthInterceptor(token string) *authInterceptor {
	return &authInterceptor{token: token}
}

func (a *authInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set("Authorization", "Bearer "+a.token)
		return next(ctx, req)
	}
}

func (a *authInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (a *authInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// envToken reads the token from GASTROLOG_TOKEN if set.
func envToken() string {
	return os.Getenv("GASTROLOG_TOKEN")
}
