package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"

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
	frame := dispatchForward(t, ForwardRPCMaxResponseBytes+1)

	if frame.GetErrorCode() != uint32(codes.ResourceExhausted) {
		t.Fatalf("expected error_code %d (ResourceExhausted), got %d (msg=%q, payload=%d bytes)",
			codes.ResourceExhausted, frame.GetErrorCode(), frame.GetErrorMessage(), len(frame.GetPayload()))
	}
	if len(frame.GetPayload()) != 0 {
		t.Errorf("over-limit error frame must carry no payload, got %d bytes (silent truncation)", len(frame.GetPayload()))
	}
	if !bytes.Contains([]byte(frame.GetErrorMessage()), []byte("ForwardRPCMaxResponseBytes")) {
		t.Errorf("error message should name the limit constant, got %q", frame.GetErrorMessage())
	}
}

// TestForwardRPCAtLimitSucceeds pins the inclusive boundary: a response of
// exactly the limit is passed through intact.
func TestForwardRPCAtLimitSucceeds(t *testing.T) {
	frame := dispatchForward(t, ForwardRPCMaxResponseBytes)

	if frame.GetErrorCode() != 0 {
		t.Fatalf("at-limit response must not error, got code %d msg=%q", frame.GetErrorCode(), frame.GetErrorMessage())
	}
	if len(frame.GetPayload()) != ForwardRPCMaxResponseBytes {
		t.Errorf("expected %d payload bytes, got %d", ForwardRPCMaxResponseBytes, len(frame.GetPayload()))
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

// forwardOverGRPC forwards one request to a real cluster gRPC server whose
// internal mux is handler, through the PeerConnManager client path.
func forwardOverGRPC(t *testing.T, handler http.Handler, reqPayload []byte) ([]byte, uint32, string) {
	t.Helper()
	peer, err := New(Config{ClusterAddr: "127.0.0.1:0", NodeID: "owner"})
	if err != nil {
		t.Fatal(err)
	}
	peer.Transport()
	peer.SetInternalHandler(handler)
	if err := peer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(peer.Stop)
	peers := NewStaticPeerConns("caller", func(id string) (string, bool) {
		return peer.Addr(), id == "owner"
	})
	t.Cleanup(func() { _ = peers.Close() })

	payload, code, msg, err := ForwardRPC(context.Background(), peers, "owner", "/test.Service/Method", reqPayload)
	if err != nil {
		t.Fatalf("ForwardRPC transport error: %v", err)
	}
	return payload, code, msg
}

// TestForwardRPCResponseLimitOverGRPC pins the response limit end to end:
// every payload up to and including ForwardRPCMaxResponseBytes reaches the
// caller intact over a real gRPC stream, and one byte more is refused by name.
func TestForwardRPCResponseLimitOverGRPC(t *testing.T) {
	for _, n := range []int{
		ForwardRPCMaxResponseBytes - 1<<10,
		ForwardRPCMaxResponseBytes - 16,
		ForwardRPCMaxResponseBytes - 5,
		ForwardRPCMaxResponseBytes - 1,
		ForwardRPCMaxResponseBytes,
	} {
		t.Run(fmt.Sprintf("%d", n), func(t *testing.T) {
			payload, code, msg := forwardOverGRPC(t, fixedSizeHandler{n: n}, nil)
			if code != 0 {
				t.Fatalf("code = %d, msg = %q; want success", code, msg)
			}
			if !bytes.Equal(payload, bytes.Repeat([]byte{'x'}, n)) {
				t.Fatalf("payload is %d bytes, want %d intact bytes", len(payload), n)
			}
		})
	}
	t.Run("over", func(t *testing.T) {
		payload, code, msg := forwardOverGRPC(t, fixedSizeHandler{n: ForwardRPCMaxResponseBytes + 1}, nil)
		if code != uint32(codes.ResourceExhausted) {
			t.Fatalf("code = %d, want ResourceExhausted (msg %q)", code, msg)
		}
		if !strings.Contains(msg, "ForwardRPCMaxResponseBytes") {
			t.Errorf("message %q does not name the limit", msg)
		}
		if len(payload) != 0 {
			t.Errorf("refusal carried a %d-byte payload", len(payload))
		}
	})
}

// TestForwardRPCMaxFrameBytesMatchesEncoding pins the computed bound to the
// encoder: the largest payload frame and the largest error frame both
// marshal to at most forwardRPCMaxFrameBytes, and one of them to exactly it.
func TestForwardRPCMaxFrameBytesMatchesEncoding(t *testing.T) {
	payloadFrame := proto.Size(&gastrologv1.ForwardRPCFrame{
		Payload: bytes.Repeat([]byte{'x'}, ForwardRPCMaxResponseBytes),
	})
	errorFrame := proto.Size(&gastrologv1.ForwardRPCFrame{
		ErrorCode:    math.MaxUint32,
		ErrorMessage: strings.Repeat("x", ForwardRPCMaxResponseBytes),
	})
	if got := max(payloadFrame, errorFrame); got != forwardRPCMaxFrameBytes {
		t.Fatalf("largest response frame encodes to %d bytes (payload %d, error %d), bound is %d",
			got, payloadFrame, errorFrame, forwardRPCMaxFrameBytes)
	}
}

// TestForwardRPCErrorMessageAtLimitOverGRPC: an error message of exactly
// ForwardRPCMaxResponseBytes, the largest frame the handler sends, reaches
// the caller whole.
func TestForwardRPCErrorMessageAtLimitOverGRPC(t *testing.T) {
	body := strings.Repeat("e", ForwardRPCMaxResponseBytes)
	payload, code, msg := forwardOverGRPC(t,
		rawErrorHandler{status: http.StatusServiceUnavailable, contentType: "text/plain", body: body}, nil)
	if code != uint32(codes.Unavailable) {
		t.Fatalf("code = %d, want Unavailable", code)
	}
	if msg != body {
		t.Fatalf("message is %d bytes, want the %d-byte body", len(msg), len(body))
	}
	if len(payload) != 0 {
		t.Errorf("error response carried a %d-byte payload", len(payload))
	}
}

// TestForwardRPCDecodedErrorMessageOverLimitIsNamed: a Connect error body
// within the limit whose message decodes past it — invalid UTF-8 triples in
// size — is refused by name instead of overflowing the frame.
func TestForwardRPCDecodedErrorMessageOverLimitIsNamed(t *testing.T) {
	const prefix = `{"code":"failed_precondition","message":"`
	const suffix = `"}`
	body := prefix + strings.Repeat("\xff", ForwardRPCMaxResponseBytes-len(prefix)-len(suffix)) + suffix
	payload, code, msg := forwardOverGRPC(t,
		rawErrorHandler{status: http.StatusBadRequest, contentType: "application/json", body: body}, nil)
	if connect.Code(code) != connect.CodeFailedPrecondition {
		t.Fatalf("code = %v, want failed_precondition (message %.120q)", connect.Code(code), msg)
	}
	want := fmt.Sprintf(
		"upstream error: HTTP 400 with a decoded error message exceeding ForwardRPCMaxResponseBytes limit of %d bytes",
		ForwardRPCMaxResponseBytes)
	if msg != want {
		t.Errorf("message = %.120q, want %q", msg, want)
	}
	if len(payload) != 0 {
		t.Errorf("error response carried a %d-byte payload", len(payload))
	}
}

// echoVaultService answers SealVault with the length of the vault name it
// received, so the caller can tell the request arrived whole.
type echoVaultService struct {
	gastrologv1connect.UnimplementedVaultServiceHandler
}

func (echoVaultService) SealVault(
	_ context.Context, req *connect.Request[gastrologv1.SealVaultRequest],
) (*connect.Response[gastrologv1.SealVaultResponse], error) {
	return connect.NewResponse(&gastrologv1.SealVaultResponse{SealedCount: int32(len(req.Msg.GetVault()))}), nil
}

// sealVaultRequestOfSize returns a SealVaultRequest that marshals to exactly
// n bytes, and the length of its vault name.
func sealVaultRequestOfSize(t *testing.T, n int) ([]byte, int) {
	t.Helper()
	for name := n; name > 0; name-- {
		b, err := proto.Marshal(&gastrologv1.SealVaultRequest{Vault: strings.Repeat("v", name)})
		if err != nil {
			t.Fatal(err)
		}
		if len(b) == n {
			return b, name
		}
		if len(b) < n {
			break
		}
	}
	t.Fatalf("no SealVaultRequest marshals to exactly %d bytes", n)
	return nil, 0
}

// TestForwardRPCRequestLimitOverGRPC pins the request direction against a
// peer mux that reads at most ForwardRPCMaxResponseBytes, as every internal
// mux does: requests up to the limit cross the real gRPC stream and reach
// the handler whole; one byte more is refused by the peer's mux.
func TestForwardRPCRequestLimitOverGRPC(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(gastrologv1connect.NewVaultServiceHandler(echoVaultService{},
		connect.WithReadMaxBytes(ForwardRPCMaxResponseBytes)))
	peer, err := New(Config{ClusterAddr: "127.0.0.1:0", NodeID: "owner"})
	if err != nil {
		t.Fatal(err)
	}
	peer.Transport()
	peer.SetInternalHandler(mux)
	if err := peer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(peer.Stop)
	peers := NewStaticPeerConns("caller", func(id string) (string, bool) {
		return peer.Addr(), id == "owner"
	})
	t.Cleanup(func() { _ = peers.Close() })

	forward := func(t *testing.T, req []byte) ([]byte, uint32, string) {
		t.Helper()
		payload, code, msg, err := ForwardRPC(context.Background(), peers, "owner",
			gastrologv1connect.VaultServiceSealVaultProcedure, req)
		if err != nil {
			t.Fatalf("ForwardRPC transport error: %v", err)
		}
		return payload, code, msg
	}

	for _, n := range []int{
		ForwardRPCMaxResponseBytes - 1<<10,
		ForwardRPCMaxResponseBytes - 1,
		ForwardRPCMaxResponseBytes,
	} {
		t.Run(fmt.Sprintf("%d", n), func(t *testing.T) {
			req, nameLen := sealVaultRequestOfSize(t, n)
			payload, code, msg := forward(t, req)
			if code != 0 {
				t.Fatalf("code = %v, msg = %q; want success", connect.Code(code), msg)
			}
			var resp gastrologv1.SealVaultResponse
			if err := proto.Unmarshal(payload, &resp); err != nil {
				t.Fatal(err)
			}
			if int(resp.GetSealedCount()) != nameLen {
				t.Fatalf("handler saw a %d-byte vault name, want %d", resp.GetSealedCount(), nameLen)
			}
		})
	}
	t.Run("over", func(t *testing.T) {
		req, _ := sealVaultRequestOfSize(t, ForwardRPCMaxResponseBytes+1)
		_, code, msg := forward(t, req)
		if connect.Code(code) != connect.CodeResourceExhausted {
			t.Fatalf("code = %v, want resource_exhausted (msg %q)", connect.Code(code), msg)
		}
	})
}
