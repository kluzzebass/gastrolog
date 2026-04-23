package server

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
)

func TestRPCErrorLogInterceptor_LogsConnectError(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	icept := newRPCErrorLogInterceptor(logger)
	next := icept.WrapUnary(func(context.Context, connect.AnyRequest) (connect.AnyResponse, error) {
		return nil, connect.NewError(connect.CodeInternal, errors.New("boom"))
	})

	req := connect.NewRequest(&apiv1.GetChunkRequest{})
	_, err := next(context.Background(), req)
	if err == nil {
		t.Fatal("expected error")
	}
	out := strings.ToLower(buf.String())
	if !strings.Contains(out, "rpc error response") || !strings.Contains(out, "code=internal") {
		t.Fatalf("log output missing expected fields:\n%s", buf.String())
	}
}

func TestRPCErrorLogInterceptor_SkipsCanceled(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	icept := newRPCErrorLogInterceptor(logger)
	next := icept.WrapUnary(func(context.Context, connect.AnyRequest) (connect.AnyResponse, error) {
		return nil, connect.NewError(connect.CodeCanceled, errors.New("client gone"))
	})

	req := connect.NewRequest(&apiv1.SearchRequest{})
	_, _ = next(context.Background(), req)
	if buf.Len() > 0 {
		t.Fatalf("expected no log for canceled, got:\n%s", buf.String())
	}
}
