package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// forwardRPCStreamHandler implements the bidirectional ForwardRPC stream.
// The client sends a single frame (procedure + serialized request), and the
// handler dispatches through the internal Connect mux, returning one or more
// response frames.
//
// For unary RPCs: one response frame with end_stream=true.
// For server-streaming RPCs: multiple payload frames, last has end_stream=true.
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

// unaryResponseFrame reads a raw proto response body and sends it as a
// ForwardRPCFrame with end_stream=true. Connect unary responses are NOT
// envelope-framed — the body is raw proto bytes.
func unaryResponseFrame(stream grpc.ServerStream, body io.Reader) error {
	// Read the entire response body (bounded by Connect's ReadMaxBytes).
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(io.LimitReader(body, 4<<20)); err != nil {
		return status.Errorf(codes.Internal, "read response body: %v", err)
	}

	return stream.SendMsg(&gastrologv1.ForwardRPCFrame{
		Payload:   buf.Bytes(),
		EndStream: true,
	})
}

// sendErrorFrame extracts a Connect error from the HTTP response and sends
// it as a ForwardRPCFrame with the error code.
func sendErrorFrame(stream grpc.ServerStream, resp *http.Response, ct string) error {
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
		EndStream:    true,
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

// ForwardRPC opens a ForwardRPC bidirectional stream to a remote node and
// sends a single request, returning the serialized response payload(s).
// Used by the routing interceptor's Forwarder.
//
// For unary RPCs, returns a single payload. For server-streaming RPCs,
// the caller should use ForwardRPCStream instead.
func ForwardRPC(ctx context.Context, peers *PeerConns, nodeID, procedure string, reqPayload []byte) ([]byte, uint32, string, error) {
	conn, err := peers.Conn(nodeID)
	if err != nil {
		return nil, 14, "", fmt.Errorf("dial node %s: %w", nodeID, err)
	}

	stream, err := conn.NewStream(ctx,
		&grpc.StreamDesc{
			StreamName:    "ForwardRPC",
			ServerStreams: true,
			ClientStreams: true,
		},
		"/gastrolog.v1.ClusterService/ForwardRPC",
	)
	if err != nil {
		peers.Invalidate(nodeID)
		return nil, 14, "", fmt.Errorf("open ForwardRPC stream to %s: %w", nodeID, err)
	}

	// Send the request frame.
	frame := &gastrologv1.ForwardRPCFrame{
		Procedure: procedure,
		Payload:   reqPayload,
	}
	if err := stream.SendMsg(frame); err != nil {
		peers.Invalidate(nodeID)
		return nil, 14, "", fmt.Errorf("send request to %s: %w", nodeID, err)
	}
	if err := stream.CloseSend(); err != nil {
		return nil, 14, "", fmt.Errorf("close send to %s: %w", nodeID, err)
	}

	// Read the response frame(s) — for unary, just one.
	resp := &gastrologv1.ForwardRPCFrame{}
	if err := stream.RecvMsg(resp); err != nil {
		peers.Invalidate(nodeID)
		return nil, 14, "", fmt.Errorf("recv response from %s: %w", nodeID, err)
	}

	if resp.ErrorCode != 0 {
		return nil, resp.ErrorCode, resp.ErrorMessage, nil
	}
	return resp.Payload, 0, "", nil
}

// ForwardRPCStreamSender wraps a ForwardRPC gRPC stream for reading
// server-streaming response frames.
type ForwardRPCStreamSender struct {
	stream grpc.ClientStream
	peers  *PeerConns
	nodeID string
}

// Recv reads the next response frame from the stream. Returns io.EOF when
// the server signals end_stream or the gRPC stream ends.
func (s *ForwardRPCStreamSender) Recv() (*gastrologv1.ForwardRPCFrame, error) {
	frame := &gastrologv1.ForwardRPCFrame{}
	if err := s.stream.RecvMsg(frame); err != nil {
		if !errors.Is(err, io.EOF) {
			s.peers.Invalidate(s.nodeID)
		}
		return nil, err
	}
	if frame.ErrorCode != 0 {
		return frame, nil
	}
	if frame.EndStream && len(frame.Payload) == 0 {
		return nil, io.EOF
	}
	return frame, nil
}

// Close signals we are done reading (the gRPC stream will be cancelled).
func (s *ForwardRPCStreamSender) Close() {
	// Context cancellation handles cleanup; nothing explicit needed.
}

// ForwardRPCStream opens a ForwardRPC stream for a server-streaming RPC.
// Returns a ForwardRPCStreamSender that yields response frames.
func ForwardRPCStream(ctx context.Context, peers *PeerConns, nodeID, procedure string, reqPayload []byte) (*ForwardRPCStreamSender, error) {
	conn, err := peers.Conn(nodeID)
	if err != nil {
		return nil, fmt.Errorf("dial node %s: %w", nodeID, err)
	}

	stream, err := conn.NewStream(ctx,
		&grpc.StreamDesc{
			StreamName:    "ForwardRPC",
			ServerStreams: true,
			ClientStreams: true,
		},
		"/gastrolog.v1.ClusterService/ForwardRPC",
	)
	if err != nil {
		peers.Invalidate(nodeID)
		return nil, fmt.Errorf("open ForwardRPC stream to %s: %w", nodeID, err)
	}

	frame := &gastrologv1.ForwardRPCFrame{
		Procedure: procedure,
		Payload:   reqPayload,
	}
	if err := stream.SendMsg(frame); err != nil {
		peers.Invalidate(nodeID)
		return nil, fmt.Errorf("send request to %s: %w", nodeID, err)
	}
	if err := stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("close send to %s: %w", nodeID, err)
	}

	return &ForwardRPCStreamSender{
		stream: stream,
		peers:  peers,
		nodeID: nodeID,
	}, nil
}

