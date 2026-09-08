package otlp

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gastrolog/internal/logging/logtest"
)

// TestGRPCInterceptorAnswersAPanicWithInternal proves a panic inside a gRPC
// handler costs the caller its one request. grpc-go does not recover handler
// panics itself, so without this interceptor the panic ends the process,
// taking every vault and Raft group on the node with it.
func TestGRPCInterceptorAnswersAPanicWithInternal(t *testing.T) {
	t.Parallel()

	logger, logs := logtest.New()
	ing := New(Config{ID: "test-otlp", Logger: logger})

	info := &grpc.UnaryServerInfo{FullMethod: "/opentelemetry.proto.collector.logs.v1.LogsService/Export"}
	resp, err := ing.recoverUnary(t.Context(), nil, info, func(_ context.Context, _ any) (any, error) {
		panic("record decode")
	})

	if err == nil {
		t.Fatal("the panic was swallowed and reported as success")
	}
	if got := status.Code(err); got != codes.Internal {
		t.Errorf("expected Internal, got %s: %v", got, err)
	}
	if resp != nil {
		t.Errorf("expected no response alongside the error, got %v", resp)
	}
	if !logs.Contains("recovered panic", "OTLP gRPC handler", "Export", "stack") {
		t.Errorf("panic was contained but not reported with a stack: %s", logs)
	}
}

// TestGRPCInterceptorPassesThroughNormalCalls proves the guard does not
// change what a working handler returns.
func TestGRPCInterceptorPassesThroughNormalCalls(t *testing.T) {
	t.Parallel()

	logger, logs := logtest.New()
	ing := New(Config{ID: "test-otlp", Logger: logger})

	info := &grpc.UnaryServerInfo{FullMethod: "/test/Method"}
	resp, err := ing.recoverUnary(t.Context(), "req", info, func(_ context.Context, req any) (any, error) {
		return req, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "req" {
		t.Errorf("guard altered the response: %v", resp)
	}
	if logs.Contains("recovered panic") {
		t.Errorf("logged a panic that never happened: %s", logs)
	}
}
