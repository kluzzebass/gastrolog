package server

import (
	"context"
	"errors"
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
	return &rpcErrorLogInterceptor{logger: logger.With("component", "rpc_errors")}
}

type rpcErrorLogInterceptor struct {
	logger *slog.Logger
}

func (e *rpcErrorLogInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		resp, err := next(ctx, req)
		if err != nil {
			logClientRPCError(e.logger, req.Spec().Procedure, err)
		}
		return resp, err
	}
}

func (e *rpcErrorLogInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		err := next(ctx, conn)
		if err != nil {
			logClientRPCError(e.logger, conn.Spec().Procedure, err)
		}
		return err
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

func logClientRPCError(logger *slog.Logger, procedure string, err error) {
	if logger == nil || err == nil {
		return
	}
	var ce *connect.Error
	if errors.As(err, &ce) {
		// Normal client disconnect / timeout; avoid log noise.
		if ce.Code() == connect.CodeCanceled || ce.Code() == connect.CodeDeadlineExceeded {
			return
		}
		logger.Warn("rpc error response",
			"procedure", procedure,
			"code", ce.Code().String(),
			"message", ce.Message(),
		)
		return
	}
	logger.Warn("rpc error response", "procedure", procedure, "code", "non_connect", "error", err)
}

var _ connect.Interceptor = (*rpcErrorLogInterceptor)(nil)
var _ connect.Interceptor = noopConnectInterceptor{}
