package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
)

// blockingServerStream is a grpc.ServerStream stub whose RecvMsg blocks
// forever until stop is closed. Used to verify that recvOrShutdown
// unblocks callers stuck in Recv when the cluster server cancels its
// stopCtx — without relying on a real gRPC server wiring.
type blockingServerStream struct {
	ctx  context.Context
	stop chan struct{}
}

func (b *blockingServerStream) SetHeader(metadata.MD) error  { return nil }
func (b *blockingServerStream) SendHeader(metadata.MD) error { return nil }
func (b *blockingServerStream) SetTrailer(metadata.MD)       {}
func (b *blockingServerStream) Context() context.Context     { return b.ctx }
func (b *blockingServerStream) SendMsg(any) error            { return nil }

// RecvMsg blocks until stop closes. This mirrors the real behaviour of
// tierReplicationStreamHandler's RecvMsg when no peer is sending: the
// handler is parked in Recv indefinitely.
func (b *blockingServerStream) RecvMsg(any) error {
	<-b.stop
	return errors.New("stream closed")
}

// TestRecvOrShutdownReturnsQuicklyOnCancel is the regression test for
// the cluster-server half of gastrolog-1e5ke. Before this change,
// long-running stream handlers (tier replication, stream forward
// records, forward import records) sat in stream.RecvMsg forever, and
// cluster.Server.Stop()'s GracefulStop had to wait the full 5-second
// fallback timeout because the handlers would not return. Fixing this
// involved wrapping RecvMsg in recvOrShutdown, which selects on the
// server's stopCtx — when Stop cancels stopCtx, recvOrShutdown returns
// errShuttingDown immediately and the handler exits cleanly.
//
// This test drives that mechanism directly: create a Server, spawn a
// goroutine that calls recvOrShutdown on a blocking stream, cancel
// stopCtx, assert the call returns within a tight bound. No gRPC
// server, no network, no flakiness — just the core cancellation
// contract.
func TestRecvOrShutdownReturnsQuicklyOnCancel(t *testing.T) {
	t.Parallel()

	s := &Server{}
	s.stopCtx, s.stopCancel = context.WithCancel(context.Background())

	stream := &blockingServerStream{
		ctx:  context.Background(),
		stop: make(chan struct{}),
	}
	defer close(stream.stop)

	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		done <- result{err: s.recvOrShutdown(stream, nil)}
	}()

	// Cancel stopCtx: recvOrShutdown's select should fire the stopCtx
	// case and return errShuttingDown immediately.
	s.stopCancel()

	select {
	case r := <-done:
		if !errors.Is(r.err, errShuttingDown) {
			t.Errorf("recvOrShutdown returned %v, want errShuttingDown", r.err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("recvOrShutdown did not return within 500ms of stopCancel")
	}
}

// TestRecvOrShutdownFastPathWhenAlreadyCancelled verifies that once
// stopCtx is cancelled, subsequent recvOrShutdown calls short-circuit
// without spawning a goroutine. This matters because the handler loop
// can call recvOrShutdown many times in quick succession after cancel,
// and spawning a goroutine per call would be wasteful (and would leak
// if RecvMsg itself had become non-responsive for unrelated reasons).
func TestRecvOrShutdownFastPathWhenAlreadyCancelled(t *testing.T) {
	t.Parallel()

	s := &Server{}
	s.stopCtx, s.stopCancel = context.WithCancel(context.Background())
	s.stopCancel() // cancel before first call

	stream := &blockingServerStream{
		ctx:  context.Background(),
		stop: make(chan struct{}),
	}
	defer close(stream.stop)

	start := time.Now()
	err := s.recvOrShutdown(stream, nil)
	elapsed := time.Since(start)

	if !errors.Is(err, errShuttingDown) {
		t.Errorf("err = %v, want errShuttingDown", err)
	}
	// Fast-path should take microseconds at most. A generous bound
	// tolerates scheduler noise without accepting goroutine spawn cost.
	if elapsed > 10*time.Millisecond {
		t.Errorf("fast path took %v, want <10ms (suggests goroutine spawn)", elapsed)
	}
}

// TestRecvOrShutdownDelegatesWhenRunning verifies the happy path: when
// stopCtx is not cancelled, recvOrShutdown simply returns whatever
// RecvMsg returns. This guards against regressions that would break
// normal stream operation.
func TestRecvOrShutdownDelegatesWhenRunning(t *testing.T) {
	t.Parallel()

	s := &Server{}
	s.stopCtx, s.stopCancel = context.WithCancel(context.Background())

	stream := &blockingServerStream{
		ctx:  context.Background(),
		stop: make(chan struct{}),
	}

	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		done <- result{err: s.recvOrShutdown(stream, nil)}
	}()

	// Let RecvMsg finish normally by closing the stream's stop channel.
	// The helper should return whatever RecvMsg returned.
	close(stream.stop)

	select {
	case r := <-done:
		if r.err == nil || r.err.Error() != "stream closed" {
			t.Errorf("recvOrShutdown err = %v, want stream-closed error", r.err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("recvOrShutdown did not return within 500ms after RecvMsg completed")
	}
}
