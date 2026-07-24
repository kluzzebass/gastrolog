package cluster

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// fixedSizeHandler is an internal-mux stand-in that returns a Connect-unary
// 200 response whose body is exactly n bytes of raw proto payload. The
// forwardRPCStreamHandler reads the body verbatim, so the bytes need not be a
// valid proto message for the size-limit behaviour under test.
type fixedSizeHandler struct{ n int }

func (h fixedSizeHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/proto")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(bytes.Repeat([]byte{'x'}, h.n))
}

// captureRPCStream is a grpc.ServerStream stub that delivers a single request
// frame via RecvMsg and captures every frame written via SendMsg.
type captureRPCStream struct {
	ctx  context.Context
	req  *gastrologv1.ForwardRPCFrame
	recv bool
	sent []*gastrologv1.ForwardRPCFrame
}

func (s *captureRPCStream) SetHeader(metadata.MD) error  { return nil }
func (s *captureRPCStream) SendHeader(metadata.MD) error { return nil }
func (s *captureRPCStream) SetTrailer(metadata.MD)       {}
func (s *captureRPCStream) Context() context.Context     { return s.ctx }

func (s *captureRPCStream) SendMsg(m any) error {
	frame, ok := m.(*gastrologv1.ForwardRPCFrame)
	if !ok {
		return io.ErrUnexpectedEOF
	}
	// The handler constructs a fresh frame per send, so capturing the pointer
	// is safe — it is never mutated after SendMsg returns.
	s.sent = append(s.sent, frame)
	return nil
}

func (s *captureRPCStream) RecvMsg(m any) error {
	if s.recv {
		return io.EOF
	}
	s.recv = true
	proto.Merge(m.(*gastrologv1.ForwardRPCFrame), s.req)
	return nil
}

// dispatchForward runs forwardRPCStreamHandler against an internal mux that
// returns a response of respBytes bytes, returning the single captured frame.
func dispatchForward(t *testing.T, respBytes int) *gastrologv1.ForwardRPCFrame {
	t.Helper()

	srv := &Server{}
	srv.SetInternalHandler(fixedSizeHandler{n: respBytes})

	stream := &captureRPCStream{
		ctx: context.Background(),
		req: &gastrologv1.ForwardRPCFrame{Procedure: "/test.Service/Method"},
	}

	if err := forwardRPCStreamHandler(srv, stream); err != nil {
		t.Fatalf("forwardRPCStreamHandler returned transport error: %v", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("expected exactly one response frame, got %d", len(stream.sent))
	}
	return stream.sent[0]
}

// TestForwardRPCOverLimitErrorsExplicitly builds a forwarded response one byte
// over the limit and asserts the handler returns an explicit ResourceExhausted
// error frame naming the limit, rather than a silently truncated payload.
func TestForwardRPCOverLimitErrorsExplicitly(t *testing.T) {
	frame := dispatchForward(t, forwardRPCMaxResponseBytes+1)

	if frame.GetErrorCode() != uint32(codes.ResourceExhausted) {
		t.Fatalf("expected error_code %d (ResourceExhausted), got %d (msg=%q, payload=%d bytes)",
			codes.ResourceExhausted, frame.GetErrorCode(), frame.GetErrorMessage(), len(frame.GetPayload()))
	}
	if len(frame.GetPayload()) != 0 {
		t.Errorf("over-limit error frame must carry no payload, got %d bytes (silent truncation)", len(frame.GetPayload()))
	}
	if !bytes.Contains([]byte(frame.GetErrorMessage()), []byte("forwardRPCMaxResponseBytes")) {
		t.Errorf("error message should name the limit constant, got %q", frame.GetErrorMessage())
	}
}

// TestForwardRPCAtLimitSucceeds pins the inclusive boundary: a response of
// exactly the limit is passed through intact.
func TestForwardRPCAtLimitSucceeds(t *testing.T) {
	frame := dispatchForward(t, forwardRPCMaxResponseBytes)

	if frame.GetErrorCode() != 0 {
		t.Fatalf("at-limit response must not error, got code %d msg=%q", frame.GetErrorCode(), frame.GetErrorMessage())
	}
	if len(frame.GetPayload()) != forwardRPCMaxResponseBytes {
		t.Errorf("expected %d payload bytes, got %d", forwardRPCMaxResponseBytes, len(frame.GetPayload()))
	}
}

// TestForwardRPCUnderLimitUnchanged verifies ordinary under-limit responses
// round-trip byte-for-byte with no error.
func TestForwardRPCUnderLimitUnchanged(t *testing.T) {
	const n = 1024
	frame := dispatchForward(t, n)

	if frame.GetErrorCode() != 0 {
		t.Fatalf("under-limit response must not error, got code %d msg=%q", frame.GetErrorCode(), frame.GetErrorMessage())
	}
	want := bytes.Repeat([]byte{'x'}, n)
	if !bytes.Equal(frame.GetPayload(), want) {
		t.Errorf("payload mismatch: got %d bytes, want %d", len(frame.GetPayload()), n)
	}
}
