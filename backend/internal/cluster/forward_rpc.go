package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"connectrpc.com/connect"
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
// A forwarded response larger than ForwardRPCMaxResponseBytes cannot round-trip
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
		return sendErrorFrame(stream, resp)
	}

	// Connect unary response: body is raw proto bytes (no envelope).
	return unaryResponseFrame(stream, resp.Body)
}

// ForwardRPCMaxResponseBytes bounds a single forwarded unary response, and is
// the read limit the Connect mux applies to every request body: a response
// larger than this could not be read back by the forwarding client, so the
// frame protocol refuses it explicitly instead of silently truncating. The
// size check below runs on the uncompressed body, where planned zstd
// transport compression will hook in — revisit this limit when that lands.
const ForwardRPCMaxResponseBytes = 4 << 20

// unaryResponseFrame reads a raw proto response body and sends it as a single
// ForwardRPCFrame. Connect unary responses are NOT envelope-framed — the body
// is raw proto bytes. Responses exceeding ForwardRPCMaxResponseBytes are
// rejected with a ResourceExhausted error frame naming the limit, rather than
// truncated to a corrupt payload.
func unaryResponseFrame(stream grpc.ServerStream, body io.Reader) error {
	// Read one byte past the limit so we can distinguish "exactly at the
	// limit" (allowed) from "over the limit" (rejected).
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(io.LimitReader(body, ForwardRPCMaxResponseBytes+1)); err != nil {
		return status.Errorf(codes.Internal, "read response body: %v", err)
	}
	if buf.Len() > ForwardRPCMaxResponseBytes {
		return stream.SendMsg(&gastrologv1.ForwardRPCFrame{
			ErrorCode: uint32(codes.ResourceExhausted),
			ErrorMessage: fmt.Sprintf(
				"forwarded response exceeds ForwardRPCMaxResponseBytes limit of %d bytes",
				ForwardRPCMaxResponseBytes),
		})
	}

	return stream.SendMsg(&gastrologv1.ForwardRPCFrame{
		Payload: buf.Bytes(),
	})
}

// sendErrorFrame sends a non-200 internal-mux response as a ForwardRPCFrame
// carrying the handler's Connect code and bare message, so the caller can
// rebuild the same error the handler returned.
func sendErrorFrame(stream grpc.ServerStream, resp *http.Response) error {
	code, msg, err := decodeForwardedError(resp)
	if err != nil {
		return status.Errorf(codes.Internal, "read error body: %v", err)
	}
	return stream.SendMsg(&gastrologv1.ForwardRPCFrame{
		ErrorCode:    uint32(code),
		ErrorMessage: msg,
	})
}

// decodeForwardedError extracts the code and message of a failed internal-mux
// response. The code comes from the Connect JSON body because the HTTP status
// is shared by several codes (400 is failed_precondition, invalid_argument
// and out_of_range); the status is the fallback for a non-Connect body.
func decodeForwardedError(resp *http.Response) (connect.Code, string, error) {
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(io.LimitReader(resp.Body, ForwardRPCMaxResponseBytes+1)); err != nil {
		return 0, "", err
	}
	fallback := httpStatusToConnectCode(resp.StatusCode)
	if buf.Len() > ForwardRPCMaxResponseBytes {
		return fallback, fmt.Sprintf(
			"upstream error: HTTP %d with an error body exceeding ForwardRPCMaxResponseBytes limit of %d bytes",
			resp.StatusCode, ForwardRPCMaxResponseBytes), nil
	}
	if code, msg, ok := decodeConnectWireError(buf.Bytes(), fallback); ok {
		return code, msg, nil
	}
	if text := strings.TrimSpace(buf.String()); text != "" {
		return fallback, text, nil
	}
	return fallback, fmt.Sprintf("upstream error: HTTP %d", resp.StatusCode), nil
}

// decodeConnectWireError decodes a Connect unary error body. Like a Connect
// client, it keeps the message of a JSON error whose code it does not
// recognise and substitutes fallback for the code. ok is false when the body
// is not a JSON object carrying a recognised code or a message.
func decodeConnectWireError(body []byte, fallback connect.Code) (connect.Code, string, bool) {
	var wire struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return 0, "", false
	}
	var code connect.Code
	if err := code.UnmarshalText([]byte(wire.Code)); err != nil || code < connect.CodeCanceled || code > connect.CodeUnauthenticated {
		if wire.Message == "" {
			return 0, "", false
		}
		code = fallback
	}
	return code, wire.Message, true
}

// httpStatusToConnectCode maps an HTTP status code to a Connect error code.
func httpStatusToConnectCode(httpStatus int) connect.Code {
	switch httpStatus {
	case http.StatusBadRequest:
		return connect.CodeInvalidArgument
	case http.StatusUnauthorized:
		return connect.CodeUnauthenticated
	case http.StatusForbidden:
		return connect.CodePermissionDenied
	case http.StatusNotFound:
		return connect.CodeNotFound
	case http.StatusConflict:
		return connect.CodeAlreadyExists
	case http.StatusTooManyRequests:
		return connect.CodeResourceExhausted
	case http.StatusNotImplemented:
		return connect.CodeUnimplemented
	case http.StatusServiceUnavailable:
		return connect.CodeUnavailable
	case http.StatusGatewayTimeout:
		return connect.CodeDeadlineExceeded
	default:
		return connect.CodeUnknown
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
	// pairs; unaryCallTimeout is plenty of headroom.
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
