package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// forwardRPCStreamHandler implements the ForwardRPC transport. The underlying
// gRPC method is a bidirectional stream, but the contract is strictly UNARY:
// the client sends exactly one request frame (procedure + serialized request),
// the handler dispatches it through the internal Connect mux, and exactly one
// response frame is sent back (payload, or error_code + error_message).
//
// Server-streaming forwards are NOT supported on this path and never were: the
// routing interceptor only ever calls ForwardUnary, and large streamed
// responses (search) travel over the dedicated ForwardSearch RPC, not here.
// A forwarded response larger than forwardRPCMaxResponseBytes cannot round-trip
// and is rejected with a ResourceExhausted error frame rather than silently
// truncated.
func forwardRPCStreamHandler(srv any, stream grpc.ServerStream) error {
	s, ok := srv.(*Server)
	if !ok {
		return status.Error(codes.Internal, "invalid server type")
	}

	if s.internalHandler == nil {
		return status.Error(codes.Unavailable, "internal handler not configured")
	}

	// Read the request frame.
	var frame gastrologv1.ForwardRPCFrame
	if err := stream.RecvMsg(&frame); err != nil {
		return status.Errorf(codes.InvalidArgument, "recv request frame: %v", err)
	}
	if frame.Procedure == "" {
		return status.Error(codes.InvalidArgument, "procedure is required")
	}

	// Build an HTTP request targeting the internal Connect mux.
	// Connect unary protocol: POST with raw proto bytes (no envelope framing).
	// Envelope framing is only used for streaming RPCs.
	req, err := http.NewRequestWithContext(stream.Context(), "POST", frame.Procedure, bytes.NewReader(frame.Payload))
	if err != nil {
		return status.Errorf(codes.Internal, "build request: %v", err)
	}
	// Connect unary uses "application/proto" (not "application/connect+proto"
	// which is for streaming). See connectUnaryContentTypePrefix in the
	// Connect source: "application/" + codec name.
	req.Header.Set("Content-Type", "application/proto")
	req.Header.Set("Connect-Protocol-Version", "1")

	// Dispatch through the internal mux.
	rec := httptest.NewRecorder()
	s.internalHandler.ServeHTTP(rec, req)

	resp := rec.Result()
	defer func() { _ = resp.Body.Close() }()

	// Check for HTTP-level errors.
	if resp.StatusCode != http.StatusOK {
		ct := resp.Header.Get("Content-Type")
		return sendErrorFrame(stream, resp, ct)
	}

	// Connect unary response: body is raw proto bytes (no envelope).
	return unaryResponseFrame(stream, resp.Body)
}

// forwardRPCMaxResponseBytes bounds a single forwarded unary response. It
// mirrors the internal Connect mux's WithReadMaxBytes: a response larger than
// this cannot be read back by the forwarding client, so the frame protocol
// refuses it explicitly instead of silently truncating. gastrolog-4qhej will
// add transport compression; the size check below runs on the uncompressed
// body, the point at which compression would naturally be applied.
const forwardRPCMaxResponseBytes = 4 << 20

// unaryResponseFrame reads a raw proto response body and sends it as a single
// ForwardRPCFrame. Connect unary responses are NOT envelope-framed — the body
// is raw proto bytes. Responses exceeding forwardRPCMaxResponseBytes are
// rejected with a ResourceExhausted error frame naming the limit, rather than
// truncated to a corrupt payload.
func unaryResponseFrame(stream grpc.ServerStream, body io.Reader) error {
	// Read one byte past the limit so we can distinguish "exactly at the
	// limit" (allowed) from "over the limit" (rejected).
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(io.LimitReader(body, forwardRPCMaxResponseBytes+1)); err != nil {
		return status.Errorf(codes.Internal, "read response body: %v", err)
	}
	if buf.Len() > forwardRPCMaxResponseBytes {
		return stream.SendMsg(&gastrologv1.ForwardRPCFrame{
			ErrorCode: uint32(codes.ResourceExhausted),
			ErrorMessage: fmt.Sprintf(
				"forwarded response exceeds forwardRPCMaxResponseBytes limit of %d bytes",
				forwardRPCMaxResponseBytes),
		})
	}

	return stream.SendMsg(&gastrologv1.ForwardRPCFrame{
		Payload: buf.Bytes(),
	})
}

// sendErrorFrame extracts a Connect error from the HTTP response and sends
// it as a ForwardRPCFrame with the error code.
func sendErrorFrame(stream grpc.ServerStream, resp *http.Response, _ string) error {
	// Read error body into a fixed 4KB stack buffer — error messages are short.
	var buf [4 << 10]byte
	n, _ := io.ReadFull(resp.Body, buf[:])
	errBody := string(buf[:n])

	code := httpStatusToConnectCode(resp.StatusCode)
	msg := fmt.Sprintf("upstream error: HTTP %d", resp.StatusCode)
	if n > 0 {
		msg = errBody
	}

	return stream.SendMsg(&gastrologv1.ForwardRPCFrame{
		ErrorCode:    code,
		ErrorMessage: msg,
	})
}

// httpStatusToConnectCode maps an HTTP status code to a Connect error code.
func httpStatusToConnectCode(httpStatus int) uint32 {
	switch httpStatus {
	case http.StatusBadRequest:
		return 3 // InvalidArgument
	case http.StatusUnauthorized:
		return 16 // Unauthenticated
	case http.StatusForbidden:
		return 7 // PermissionDenied
	case http.StatusNotFound:
		return 5 // NotFound
	case http.StatusConflict:
		return 6 // AlreadyExists
	case http.StatusTooManyRequests:
		return 8 // ResourceExhausted
	case http.StatusNotImplemented:
		return 12 // Unimplemented
	case http.StatusServiceUnavailable:
		return 14 // Unavailable
	case http.StatusGatewayTimeout:
		return 4 // DeadlineExceeded
	default:
		return 2 // Unknown
	}
}

const forwardRPCPurpose = PurposeFwdRPC

// ForwardRPC opens a ForwardRPC stream to a remote node, sends a single
// request frame, and returns the single serialized response payload. Used by
// the routing interceptor's Forwarder. The contract is unary-only; there is no
// server-streaming variant (large streamed responses use ForwardSearch).
func ForwardRPC(ctx context.Context, peers *PeerConnManager, nodeID, procedure string, reqPayload []byte) ([]byte, uint32, string, error) {
	// Bound the call so a paused remote (SIGSTOP, GC stall, …) can't wedge
	// the caller forever. Forwarded unary RPCs are small request/response
	// pairs; unaryCallTimeout is plenty of headroom. See gastrolog-4rp6i.
	ctx, cancel := context.WithTimeout(ctx, unaryCallTimeout)
	defer cancel()

	h, stream, err := peers.OpenServiceStream(ctx, nodeID, forwardRPCPurpose,
		&grpc.StreamDesc{
			StreamName:    "ForwardRPC",
			ServerStreams: true,
			ClientStreams: true,
		},
		"/gastrolog.v1.ClusterService/ForwardRPC",
	)
	if err != nil {
		return nil, 14, "", fmt.Errorf("open ForwardRPC stream to %s: %w", nodeID, err)
	}
	defer h.Release()

	// Send the request frame.
	frame := &gastrologv1.ForwardRPCFrame{
		Procedure: procedure,
		Payload:   reqPayload,
	}
	if err := stream.SendMsg(frame); err != nil {
		h.Invalidate(err)
		return nil, 14, "", fmt.Errorf("send request to %s: %w", nodeID, err)
	}
	if err := stream.CloseSend(); err != nil {
		return nil, 14, "", fmt.Errorf("close send to %s: %w", nodeID, err)
	}

	// Read the response frame(s) — for unary, just one.
	resp := &gastrologv1.ForwardRPCFrame{}
	if err := stream.RecvMsg(resp); err != nil {
		h.Invalidate(err)
		return nil, 14, "", fmt.Errorf("recv response from %s: %w", nodeID, err)
	}

	if resp.ErrorCode != 0 {
		return nil, resp.ErrorCode, resp.ErrorMessage, nil
	}
	return resp.Payload, 0, "", nil
}
