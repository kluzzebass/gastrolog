package cluster

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/glid"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// startWaitVaultReadyServer stands up a real gRPC ClusterService with the
// WaitVaultReady executor set to exec, and returns a ChunkTransferrer wired to
// it over a real connection plus a teardown. Exercises the full
// ForwardWaitVaultReady round-trip: client InvokeService, the server handler,
// and its context/error classification.
func startWaitVaultReadyServer(t *testing.T, exec WaitVaultReadyExecutor) (*ChunkTransferrer, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gsrv := grpc.NewServer()
	gsrv.RegisterService(&clusterServiceDesc, &Server{waitVaultReadyExecutor: exec})
	go func() { _ = gsrv.Serve(lis) }()

	mgr := NewStaticPeerConns("local", func(id string) (string, bool) {
		if id == "node-target" {
			return lis.Addr().String(), true
		}
		return "", false
	})
	ct := &ChunkTransferrer{peers: mgr}
	return ct, func() {
		_ = mgr.Close()
		gsrv.Stop()
		_ = lis.Close()
	}
}

// TestWaitVaultReadyRPC_AlreadyReady: executor returns immediately, the RPC
// returns nil.
func TestWaitVaultReadyRPC_AlreadyReady(t *testing.T) {
	t.Parallel()
	ct, cleanup := startWaitVaultReadyServer(t, func(context.Context, glid.GLID) error {
		return nil
	})
	defer cleanup()

	if err := ct.WaitVaultReady(context.Background(), "node-target", glid.New()); err != nil {
		t.Fatalf("WaitVaultReady: %v", err)
	}
}

// TestWaitVaultReadyRPC_BecomesReady: the executor blocks until an explicit
// signal, modeling the target's vault-ready broadcast. The RPC returns nil
// once the transition is triggered — no polling on the client side.
func TestWaitVaultReadyRPC_BecomesReady(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	ct, cleanup := startWaitVaultReadyServer(t, func(ctx context.Context, _ glid.GLID) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	defer cleanup()

	errCh := make(chan error, 1)
	go func() {
		errCh <- ct.WaitVaultReady(context.Background(), "node-target", glid.New())
	}()

	close(release) // drive the readiness transition

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("WaitVaultReady after readiness: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("WaitVaultReady did not return after readiness signal")
	}
}

// TestWaitVaultReadyRPC_CtxCancel: a never-ready target with the caller
// cancelling its context tears down the blocking RPC and returns an error
// carrying context cancellation, not a hang.
func TestWaitVaultReadyRPC_CtxCancel(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	var once sync.Once
	ct, cleanup := startWaitVaultReadyServer(t, func(ctx context.Context, _ glid.GLID) error {
		once.Do(func() { close(entered) })
		<-ctx.Done() // never ready
		return ctx.Err()
	})
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- ct.WaitVaultReady(ctx, "node-target", glid.New())
	}()

	<-entered // server is blocked in the wait
	cancel()  // caller gives up

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error after ctx cancel, got nil")
		}
		if st, ok := status.FromError(errors.Unwrap(err)); ok {
			if st.Code() != codes.Canceled {
				t.Fatalf("expected Canceled, got %v", st.Code())
			}
		}
	case <-time.After(10 * time.Second):
		t.Fatal("WaitVaultReady did not return after ctx cancel")
	}
}

// TestWaitVaultReadyRPC_ExecutorUnset: a server without the executor wired
// returns Unavailable rather than panicking.
func TestWaitVaultReadyRPC_ExecutorUnset(t *testing.T) {
	t.Parallel()
	ct, cleanup := startWaitVaultReadyServer(t, nil)
	defer cleanup()

	err := ct.WaitVaultReady(context.Background(), "node-target", glid.New())
	if err == nil {
		t.Fatal("expected error when executor unset, got nil")
	}
	if st, ok := status.FromError(errors.Unwrap(err)); ok {
		if st.Code() != codes.Unavailable {
			t.Fatalf("expected Unavailable, got %v", st.Code())
		}
	}
}
