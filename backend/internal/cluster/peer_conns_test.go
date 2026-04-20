package cluster

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// errTransport is a sentinel error that causes Invalidate to actually drop
// the connection. We use this in tests that exercise the deferred-close
// machinery — see shouldInvalidate for the gating logic.
var errTransport = status.Error(codes.Unavailable, "test transport failure")

// dialLocal stands up a minimal gRPC server on a loopback port and returns a
// real *grpc.ClientConn to it plus a teardown. We need a real conn (not a
// fake) because we're testing connectivity-state transitions and Close()
// semantics on actual gRPC machinery.
func dialLocal(t *testing.T) (*grpc.ClientConn, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
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

// TestInvalidateDeferredClose verifies that Invalidate removes the conn from
// the cache immediately but defers the actual Close — concurrent goroutines
// holding the same *ClientConn must be able to keep using it during the
// grace period without seeing "client connection is closing".
func TestInvalidateDeferredClose(t *testing.T) {
	conn, cleanup := dialLocal(t)
	defer cleanup()

	p := &PeerConns{conns: map[string]*grpc.ClientConn{"node-x": conn}}

	p.Invalidate("node-x", errTransport)

	// Cache entry must be gone immediately so the next Conn() will re-dial.
	p.mu.Lock()
	_, exists := p.conns["node-x"]
	p.mu.Unlock()
	if exists {
		t.Fatalf("Invalidate did not remove cache entry")
	}

	// The underlying conn must NOT be closed yet — concurrent in-flight RPCs
	// would otherwise fail with "client connection is closing".
	if state := conn.GetState(); state == connectivity.Shutdown {
		t.Fatalf("Invalidate closed the conn synchronously, state=%v", state)
	}

	// After the grace period the deferred Close should have fired.
	deadline := time.Now().Add(invalidateGracePeriod + 2*time.Second)
	for time.Now().Before(deadline) {
		if conn.GetState() == connectivity.Shutdown {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("expected conn to be Shutdown after grace period, got %v", conn.GetState())
}

// TestInvalidateMissingNoOp verifies Invalidate is safe to call for a node
// that isn't cached — the per-RPC error paths in chunk_transferrer call
// Invalidate even on errors that may already have invalidated.
func TestInvalidateMissingNoOp(t *testing.T) {
	p := &PeerConns{conns: map[string]*grpc.ClientConn{}}
	p.Invalidate("does-not-exist", errTransport) // must not panic
}

// TestResetDeferredClose verifies Reset behaves like Invalidate for all
// cached conns: cache cleared synchronously, Close deferred.
func TestResetDeferredClose(t *testing.T) {
	c1, cleanup1 := dialLocal(t)
	defer cleanup1()
	c2, cleanup2 := dialLocal(t)
	defer cleanup2()

	p := &PeerConns{conns: map[string]*grpc.ClientConn{
		"node-a": c1,
		"node-b": c2,
	}}

	p.Reset(nil)

	p.mu.Lock()
	cacheLen := len(p.conns)
	p.mu.Unlock()
	if cacheLen != 0 {
		t.Fatalf("Reset did not clear cache, len=%d", cacheLen)
	}

	if c1.GetState() == connectivity.Shutdown || c2.GetState() == connectivity.Shutdown {
		t.Fatalf("Reset closed conns synchronously")
	}

	deadline := time.Now().Add(invalidateGracePeriod + 2*time.Second)
	for time.Now().Before(deadline) {
		if c1.GetState() == connectivity.Shutdown && c2.GetState() == connectivity.Shutdown {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("expected both conns to be Shutdown after grace period, got %v / %v",
		c1.GetState(), c2.GetState())
}

// TestConnRejectsShutdownCachedConn verifies that if a cached conn enters
// connectivity.Shutdown for any reason (e.g., the deferred-close goroutine
// fires), the next Conn() call discards it. Without raft we can't exercise
// the re-dial path, but we can verify the discard.
func TestConnRejectsShutdownCachedConn(t *testing.T) {
	conn, cleanup := dialLocal(t)
	defer cleanup()

	// Force the conn to Shutdown state.
	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for conn.GetState() != connectivity.Shutdown && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if conn.GetState() != connectivity.Shutdown {
		t.Fatalf("conn did not reach Shutdown, state=%v", conn.GetState())
	}

	p := &PeerConns{conns: map[string]*grpc.ClientConn{"node-x": conn}}

	// Conn() will hit the state check, discard the entry, then fall through
	// to resolveAddr which fails because raft is nil. We assert (a) the
	// cache was cleared and (b) the failure is from the dial path, not from
	// returning a dead conn.
	defer func() {
		if r := recover(); r != nil {
			// resolveAddr nil-deref is acceptable for this isolated test —
			// we only care that the cached conn was discarded first.
			p.mu.Lock()
			_, stillCached := p.conns["node-x"]
			p.mu.Unlock()
			if stillCached {
				t.Errorf("Conn() returned/kept Shutdown conn instead of discarding")
			}
		}
	}()
	_, _ = p.Conn("node-x")

	p.mu.Lock()
	_, stillCached := p.conns["node-x"]
	p.mu.Unlock()
	if stillCached {
		t.Errorf("Conn() did not discard Shutdown cached conn")
	}
}

// TestInvalidateConcurrentUsersNotDisrupted is the regression test for the
// race that caused the warm→cold "client connection is closing" cascade.
// Many goroutines retrieve the same conn from the cache and run RPCs against
// it; one of them calls Invalidate mid-flight; the others must not see
// "client connection is closing".
func TestInvalidateConcurrentUsersNotDisrupted(t *testing.T) {
	conn, cleanup := dialLocal(t)
	defer cleanup()

	p := &PeerConns{conns: map[string]*grpc.ClientConn{"node-x": conn}}

	const goroutines = 32
	var wg sync.WaitGroup
	var disrupted atomic.Int32
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// Simulate "got conn from cache, doing work" — the work is
			// holding the conn handle and watching its state for the
			// duration of the would-be RPC.
			p.mu.Lock()
			c := p.conns["node-x"]
			p.mu.Unlock()
			if c == nil {
				return // already invalidated, fresh dial would happen
			}
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				// If Invalidate had closed the conn synchronously the state
				// would flip to Shutdown while we still hold the handle.
				if c.GetState() == connectivity.Shutdown {
					disrupted.Add(1)
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	close(start)
	time.Sleep(50 * time.Millisecond)
	p.Invalidate("node-x", errTransport)
	wg.Wait()

	if disrupted.Load() > 0 {
		t.Fatalf("%d concurrent users saw the conn enter Shutdown during their work — Invalidate is still synchronous", disrupted.Load())
	}
}
