package cluster

import (
	"context"
	"errors"
	"gastrolog/internal/glid"
	"net"
	"testing"
	"time"

	"gastrolog/internal/chunk"

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
// block forever waiting for an ack from a paused remote process. StreamToTier
// now uses CatchupTimeout for healthy multi-record transitions; this test uses
// a short parent context so the call still fails fast against a hung peer.
func TestStreamToTierTimeoutOnHangingPeer(t *testing.T) {
	t.Parallel()
	conn, cleanup := hangingGRPCServer(t)
	defer cleanup()

	ct := &ChunkTransferrer{
		peers: &PeerConns{conns: map[string]*grpc.ClientConn{"hung": conn}},
	}

	const hangBudget = 400 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), hangBudget)
	defer cancel()

	start := time.Now()
	err := ct.StreamToTier(ctx, "hung", glid.New(), glid.New(), emptyIterator())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("StreamToTier returned nil against a hanging server — should have timed out")
	}
	if !isDeadlineExceeded(err) {
		t.Fatalf("StreamToTier returned %v (%T), want DeadlineExceeded", err, err)
	}
	if elapsed > hangBudget+1500*time.Millisecond {
		t.Errorf("StreamToTier took %v, expected ≤ ~%v + margin", elapsed, hangBudget)
	}
	if elapsed < 50*time.Millisecond {
		t.Errorf("StreamToTier returned suspiciously fast in %v — was the deadline applied?", elapsed)
	}
}
