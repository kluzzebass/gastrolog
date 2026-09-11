package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
)

// newRPCErrorLogInterceptor returns an outermost Connect interceptor that logs
// errors returned to API clients (unary and streaming handler completion).
// Canceled and DeadlineExceeded are skipped as normal client disconnects.
func newRPCErrorLogInterceptor(logger *slog.Logger) connect.Interceptor {
	if logger == nil {
		return noopConnectInterceptor{}
	}
	return &rpcErrorLogInterceptor{logger: compRPCErrors.Apply(logger)}
}

type rpcErrorLogInterceptor struct {
	logger *slog.Logger
}

func (e *rpcErrorLogInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		resp, err := next(ctx, req)
		if err != nil {
			return resp, logClientRPCError(e.logger, req.Spec().Procedure, err)
		}
		return resp, nil
	}
}

func (e *rpcErrorLogInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		err := next(ctx, conn)
		if err != nil {
			return logClientRPCError(e.logger, conn.Spec().Procedure, err)
		}
		return nil
	}
}

func (e *rpcErrorLogInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

type noopConnectInterceptor struct{}

func (noopConnectInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc { return next }

func (noopConnectInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

func (noopConnectInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

// logClientRPCError records the failure and returns what the caller should
// see. Every code but Internal is a statement about the request, so it goes
// back as written; Internal is a statement about this server, and its text
// carries whatever the failing layer happened to say — paths, driver
// messages, peer addresses — to a caller who may not be authenticated. Those
// go back as a reference the operator can find in the log.
func logClientRPCError(logger *slog.Logger, procedure string, err error) error {
	if err == nil {
		return nil
	}
	ce, isConnect := errors.AsType[*connect.Error](err)
	if isConnect && (ce.Code() == connect.CodeCanceled || ce.Code() == connect.CodeDeadlineExceeded) {
		// Normal client disconnect / timeout; avoid log noise.
		return err
	}
	if isConnect && ce.Code() != connect.CodeInternal {
		if logger != nil {
			logger.Warn("rpc error response",
				"procedure", procedure,
				"code", ce.Code().String(),
				"message", ce.Message(),
			)
		}
		return err
	}

	ref := errorReference()
	if logger != nil {
		code := "non_connect"
		if isConnect {
			code = ce.Code().String()
		}
		logger.Warn("rpc error response",
			"procedure", procedure,
			"code", code,
			"ref", ref,
			"error", err,
		)
	}
	return connect.NewError(connect.CodeInternal,
		fmt.Errorf("internal error (ref %s); the detail is in this node's log", ref))
}

// errorReference is a short handle shared by the client's error and the log
// line describing it, so an operator handed the one can find the other.
func errorReference() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "unreferenced"
	}
	return hex.EncodeToString(b[:])
}

var _ connect.Interceptor = (*rpcErrorLogInterceptor)(nil)
var _ connect.Interceptor = noopConnectInterceptor{}
