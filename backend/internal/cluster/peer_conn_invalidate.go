package cluster

import (
	"context"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const invalidateGracePeriod = 5 * time.Second

// shouldInvalidate reports whether err indicates the underlying gRPC
// connection itself is broken and should be discarded. A status the peer
// returned travelled over the connection, so whatever its code it says
// nothing against the connection.
func shouldInvalidate(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if _, ok := errors.AsType[*peerStatusError](err); ok {
		return false
	}
	return status.Code(err) == codes.Unavailable
}

// peerStatusTrailerKey marks a call's trailer as written by the cluster
// server's handler chain. gRPC reports a broken transport and a handler's own
// status with the same codes, so the marker is what tells the caller the peer
// answered.
const peerStatusTrailerKey = "x-peer-status"

func peerStatusTrailer() metadata.MD { return metadata.Pairs(peerStatusTrailerKey, "1") }

func peerAnswered(trailer metadata.MD) bool { return len(trailer.Get(peerStatusTrailerKey)) > 0 }

// peerStatusError is a failed call's status as the peer returned it. It
// carries the status unchanged, so callers read its code and message as if
// unwrapped.
type peerStatusError struct{ err error }

func (e *peerStatusError) Error() string              { return e.err.Error() }
func (e *peerStatusError) Unwrap() error              { return e.err }
func (e *peerStatusError) GRPCStatus() *status.Status { return status.Convert(e.err) }

func markPeerStatus(err error, trailer metadata.MD) error {
	if err == nil || errors.Is(err, io.EOF) || !peerAnswered(trailer) {
		return err
	}
	return &peerStatusError{err: err}
}

func peerStatusUnaryServerInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	resp, err := handler(ctx, req)
	if err != nil {
		_ = grpc.SetTrailer(ctx, peerStatusTrailer())
	}
	return resp, err
}

func peerStatusStreamServerInterceptor(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	err := handler(srv, ss)
	if err != nil {
		ss.SetTrailer(peerStatusTrailer())
	}
	return err
}

func peerStatusUnaryClientInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	var trailer metadata.MD
	err := invoker(ctx, method, req, reply, cc, append(opts, grpc.Trailer(&trailer))...)
	return markPeerStatus(err, trailer)
}

func peerStatusStreamClientInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	cs, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		return nil, err
	}
	return &peerStatusClientStream{ClientStream: cs}, nil
}

// peerStatusClientStream marks a receive error that ended in a peer's status.
// SendMsg needs no marking: a status the peer returned surfaces there as
// io.EOF, with the status itself left for RecvMsg.
type peerStatusClientStream struct{ grpc.ClientStream }

func (s *peerStatusClientStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err == nil || errors.Is(err, io.EOF) {
		return err
	}
	return markPeerStatus(err, s.Trailer())
}
