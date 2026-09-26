package cli

import (
	"context"
	"os"

	"connectrpc.com/connect"
)

// authInterceptor adds a Bearer token to every outgoing request.
//
// Connect splits unary and streaming into separate methods, so a token
// attached in one is absent from the other. Both are implemented here: the
// streaming half is what `gastrolog query` uses, and an interceptor that
// covers only unary calls leaves every streaming command talking to an
// authenticated cluster with no credentials.
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
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		conn.RequestHeader().Set("Authorization", "Bearer "+a.token)
		return conn
	}
}

// WrapStreamingHandler is the server half of the interceptor contract and
// stays a pass-through: this interceptor is installed on CLI clients, which
// never handle an inbound stream.
func (a *authInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// envToken reads the token from GASTROLOG_TOKEN if set.
func envToken() string {
	return os.Getenv("GASTROLOG_TOKEN")
}
