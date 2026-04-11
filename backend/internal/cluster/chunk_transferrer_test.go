package cluster

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"gastrolog/internal/chunk"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// hangingGRPCServer stands up a gRPC server on a loopback port whose unknown
// service handler blocks until the stream's context is done. Any RPC against
// this server will hang until the *client* gives up via context deadline.
//
// Returns the *grpc.ClientConn pointing at the server and a teardown.
func hangingGRPCServer(t *testing.T) (*grpc.ClientConn, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ interface{}, stream grpc.ServerStream) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	}))
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		_ = lis.Close()
		t.Fatalf("dial: %v", err)
	}
	return conn, func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	}
}

// emptyIterator is a chunk.RecordIterator that immediately yields
// ErrNoMoreRecords. Streaming methods that use it will go straight to
// CloseSend + RecvMsg, where they'll block waiting for the (never-coming)
// server response.
func emptyIterator() chunk.RecordIterator {
	return func() (chunk.Record, error) {
		return chunk.Record{}, chunk.ErrNoMoreRecords
	}
}

// isDeadlineExceeded reports whether the error is a DeadlineExceeded gRPC
// status (or context.DeadlineExceeded, depending on which layer fired).
func isDeadlineExceeded(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if s, ok := status.FromError(err); ok && s.Code() == codes.DeadlineExceeded {
		return true
	}
	// Wrapped errors: walk via errors.As over the gRPC status
	for unwrapped := err; unwrapped != nil; unwrapped = errors.Unwrap(unwrapped) {
		if s, ok := status.FromError(unwrapped); ok && s.Code() == codes.DeadlineExceeded {
			return true
		}
	}
	return false
}

// TestStreamToTierTimeoutOnHangingPeer is the regression test for the silent
// SIGSTOP wedge (gastrolog-4rp6i). Before the fix, StreamToTier passed the
// caller's context.Background() straight through to RecvMsg, which would
// block forever waiting for an ack from a paused remote process. The fix
// wraps the context with streamCallTimeout. The assertion: the call returns
// a DeadlineExceeded error within streamCallTimeout + a small margin, never
// blocks indefinitely.
func TestStreamToTierTimeoutOnHangingPeer(t *testing.T) {
	t.Parallel()
	conn, cleanup := hangingGRPCServer(t)
	defer cleanup()

	ct := &ChunkTransferrer{
		peers: &PeerConns{conns: map[string]*grpc.ClientConn{"hung": conn}},
	}

	start := time.Now()
	err := ct.StreamToTier(context.Background(), "hung", uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), emptyIterator())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("StreamToTier returned nil against a hanging server — should have timed out")
	}
	if !isDeadlineExceeded(err) {
		t.Fatalf("StreamToTier returned %v (%T), want DeadlineExceeded", err, err)
	}
	// Allow 2s of margin above streamCallTimeout for goroutine scheduling.
	if elapsed > streamCallTimeout+2*time.Second {
		t.Errorf("StreamToTier took %v, expected ≤ %v", elapsed, streamCallTimeout+2*time.Second)
	}
	// Also require the call took at least streamCallTimeout - it shouldn't
	// have failed instantly (which would suggest the timeout wasn't applied
	// and some other error path returned early).
	if elapsed < streamCallTimeout-time.Second {
		t.Errorf("StreamToTier returned suspiciously fast in %v (< %v) — was the timeout applied?",
			elapsed, streamCallTimeout-time.Second)
	}
}

