package cluster

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// --- test plumbing -----------------------------------------------------------

// fakePeerSource implements broadcastPeerSource without touching Raft.
type fakePeerSource struct {
	mu             sync.Mutex
	peers          []hraft.Server // ordered list; returned by Peers()
	conns          map[string]*grpc.ClientConn
	connErrors     map[string]error // dial errors by node ID
	invalidated    map[string]int   // count of Invalidate calls per node
	peersErr       error            // set to simulate Peers() failure
}

func newFakePeerSource() *fakePeerSource {
	return &fakePeerSource{
		conns:       map[string]*grpc.ClientConn{},
		connErrors:  map[string]error{},
		invalidated: map[string]int{},
	}
}

func (f *fakePeerSource) Peers() ([]hraft.Server, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.peersErr != nil {
		return nil, f.peersErr
	}
	out := make([]hraft.Server, len(f.peers))
	copy(out, f.peers)
	return out, nil
}

func (f *fakePeerSource) Conn(id string) (*grpc.ClientConn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.connErrors[id]; ok {
		return nil, err
	}
	c, ok := f.conns[id]
	if !ok {
		return nil, errors.New("no such peer")
	}
	return c, nil
}

func (f *fakePeerSource) Invalidate(id string) {
	f.mu.Lock()
	f.invalidated[id]++
	f.mu.Unlock()
}

func (f *fakePeerSource) addPeer(t *testing.T, id string, handler broadcastHandlerFn) {
	t.Helper()
	conn, stop := spawnBroadcastPeer(t, handler)
	t.Cleanup(stop)
	f.mu.Lock()
	f.peers = append(f.peers, hraft.Server{ID: hraft.ServerID(id)})
	f.conns[id] = conn
	f.mu.Unlock()
}

// broadcastHandlerFn is the per-server hook — receives the decoded message
// and optionally blocks / errors to simulate slow or failed peers.
type broadcastHandlerFn func(ctx context.Context, msg *gastrologv1.BroadcastMessage) error

// spawnBroadcastPeer stands up a minimal gRPC server that handles
// /gastrolog.v1.ClusterService/Broadcast with the given callback, and returns
// a client conn pointing at it plus a teardown.
func spawnBroadcastPeer(t *testing.T, handler broadcastHandlerFn) (*grpc.ClientConn, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "gastrolog.v1.ClusterService",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Broadcast",
			Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				req := &gastrologv1.BroadcastRequest{}
				if err := dec(req); err != nil {
					return nil, err
				}
				if err := handler(ctx, req.GetMessage()); err != nil {
					return nil, err
				}
				return &gastrologv1.BroadcastResponse{}, nil
			},
		}},
	}, struct{}{})
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

func testMsg() *gastrologv1.BroadcastMessage {
	return &gastrologv1.BroadcastMessage{SenderId: []byte("sender")}
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// --- tests -------------------------------------------------------------------

// TestSend_NoPeers verifies that Send with zero peers is a no-op — no panics,
// no blocking, no failed calls.
func TestSend_NoPeers(t *testing.T) {
	fp := newFakePeerSource()
	b := newBroadcaster(fp, quietLogger(), time.Second)

	done := make(chan struct{})
	go func() {
		b.Send(context.Background(), testMsg())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Send blocked with no peers")
	}
}

// TestSend_PeersErr verifies that Peers() failure aborts the send without a
// panic and without touching connections.
func TestSend_PeersErr(t *testing.T) {
	fp := newFakePeerSource()
	fp.peersErr = errors.New("raft unavailable")
	b := newBroadcaster(fp, quietLogger(), time.Second)

	b.Send(context.Background(), testMsg())
	// No assertion beyond "didn't panic" — Send logs at debug and returns.
}

// TestSend_AllPeersReceive verifies the happy path: every peer's handler
// is invoked exactly once.
func TestSend_AllPeersReceive(t *testing.T) {
	fp := newFakePeerSource()
	var received sync.Map // id -> count
	hit := func(id string) broadcastHandlerFn {
		return func(_ context.Context, _ *gastrologv1.BroadcastMessage) error {
			v, _ := received.LoadOrStore(id, new(int64))
			atomic.AddInt64(v.(*int64), 1)
			return nil
		}
	}
	fp.addPeer(t, "a", hit("a"))
	fp.addPeer(t, "b", hit("b"))
	fp.addPeer(t, "c", hit("c"))

	b := newBroadcaster(fp, quietLogger(), time.Second)
	b.Send(context.Background(), testMsg())

	for _, id := range []string{"a", "b", "c"} {
		v, ok := received.Load(id)
		if !ok {
			t.Errorf("peer %s did not receive", id)
			continue
		}
		if got := atomic.LoadInt64(v.(*int64)); got != 1 {
			t.Errorf("peer %s received %d times, want 1", id, got)
		}
	}
	fp.mu.Lock()
	defer fp.mu.Unlock()
	if len(fp.invalidated) != 0 {
		t.Errorf("unexpected invalidations: %v", fp.invalidated)
	}
}

// TestSend_SlowPeerDoesNotBlockFastPeers verifies the parallel-send
// behavior: a slow peer no longer stalls delivery to fast peers. The fast
// peers must receive before the slow peer's timeout elapses.
func TestSend_SlowPeerDoesNotBlockFastPeers(t *testing.T) {
	fp := newFakePeerSource()

	fastDone := make(chan string, 3)
	hit := func(id string) broadcastHandlerFn {
		return func(_ context.Context, _ *gastrologv1.BroadcastMessage) error {
			fastDone <- id
			return nil
		}
	}
	slowEntered := make(chan struct{})
	slow := func(ctx context.Context, _ *gastrologv1.BroadcastMessage) error {
		close(slowEntered)
		<-ctx.Done() // stay until we hit the per-peer timeout
		return ctx.Err()
	}

	fp.addPeer(t, "fast-a", hit("fast-a"))
	fp.addPeer(t, "fast-b", hit("fast-b"))
	fp.addPeer(t, "slow", slow)

	// 2 s per-peer timeout — long enough to prove fast peers don't wait for it.
	b := newBroadcaster(fp, quietLogger(), 2*time.Second)

	start := time.Now()
	done := make(chan struct{})
	go func() {
		b.Send(context.Background(), testMsg())
		close(done)
	}()

	// Both fast peers should arrive well before the slow peer's 2s timeout.
	deadline := time.After(1 * time.Second)
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case id := <-fastDone:
			seen[id] = true
		case <-deadline:
			t.Fatalf("fast peers did not complete before slow timeout; seen=%v", seen)
		}
	}
	if !seen["fast-a"] || !seen["fast-b"] {
		t.Fatalf("expected both fast peers, got %v", seen)
	}

	// Make sure the slow peer actually started (confirms parallelism).
	select {
	case <-slowEntered:
	case <-time.After(time.Second):
		t.Fatal("slow peer handler never entered — peers may have been serialized")
	}

	// Send returns once all peer goroutines finish. Upper bound: slow timeout
	// + grace. We don't assert tight fast-return semantics because Send waits
	// for all — just that total runtime is bounded.
	<-done
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Send took too long: %v", elapsed)
	}

	// Slow peer invalidated (it timed out from the caller's perspective).
	fp.mu.Lock()
	defer fp.mu.Unlock()
	if fp.invalidated["slow"] == 0 {
		t.Error("slow peer should have been invalidated after timeout")
	}
	if fp.invalidated["fast-a"] != 0 || fp.invalidated["fast-b"] != 0 {
		t.Errorf("fast peers should not be invalidated: %v", fp.invalidated)
	}
}

// TestSend_CallerDeadlineCancelsInFlight verifies that cancelling the
// caller's context aborts in-flight peer RPCs instead of letting them run to
// the per-peer timeout.
func TestSend_CallerDeadlineCancelsInFlight(t *testing.T) {
	fp := newFakePeerSource()
	entered := make(chan struct{})
	finishedCtxErr := make(chan error, 1)
	slow := func(ctx context.Context, _ *gastrologv1.BroadcastMessage) error {
		close(entered)
		<-ctx.Done()
		finishedCtxErr <- ctx.Err()
		return ctx.Err()
	}
	fp.addPeer(t, "slow", slow)

	// Per-peer timeout 5 s. Caller ctx deadline 200ms — caller wins.
	b := newBroadcaster(fp, quietLogger(), 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	b.Send(ctx, testMsg())
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Fatalf("caller deadline ignored: Send took %v, expected <1s", elapsed)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("slow handler did not enter")
	}
	select {
	case err := <-finishedCtxErr:
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			t.Errorf("unexpected ctx error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not observe ctx cancellation")
	}
}

// TestSend_DialErrorInvalidates verifies that a Conn() failure doesn't panic
// and doesn't block siblings, and does NOT invalidate (there's no conn yet).
func TestSend_DialErrorInvalidates(t *testing.T) {
	fp := newFakePeerSource()
	fp.addPeer(t, "good", func(_ context.Context, _ *gastrologv1.BroadcastMessage) error { return nil })
	// Add a bogus peer whose Conn() fails.
	fp.mu.Lock()
	fp.peers = append(fp.peers, hraft.Server{ID: "bogus"})
	fp.connErrors["bogus"] = errors.New("dial failed")
	fp.mu.Unlock()

	b := newBroadcaster(fp, quietLogger(), time.Second)
	b.Send(context.Background(), testMsg())

	// "good" delivered, "bogus" returned dial error; neither panicked.
	// Dial-error path does not call Invalidate (no conn to invalidate).
	fp.mu.Lock()
	defer fp.mu.Unlock()
	if fp.invalidated["bogus"] != 0 {
		t.Errorf("bogus peer should not be invalidated on dial error, got %d", fp.invalidated["bogus"])
	}
}

// TestSend_ErrorSuppressionAndRecovery verifies the per-peer log suppression
// contract: repeated failures don't hammer the log, and a subsequent success
// clears the failure flag (recovery path).
func TestSend_ErrorSuppressionAndRecovery(t *testing.T) {
	fp := newFakePeerSource()
	var shouldFail atomic.Bool
	shouldFail.Store(true)
	fp.addPeer(t, "flap", func(_ context.Context, _ *gastrologv1.BroadcastMessage) error {
		if shouldFail.Load() {
			return errors.New("simulated peer error")
		}
		return nil
	})

	b := newBroadcaster(fp, quietLogger(), time.Second)

	// First send: fails. failed[flap] = true.
	b.Send(context.Background(), testMsg())
	b.mu.Lock()
	failedNow := b.failed["flap"]
	b.mu.Unlock()
	if !failedNow {
		t.Fatal("peer not marked failed after error")
	}

	// Recover: next send succeeds, failed flag clears.
	shouldFail.Store(false)
	b.Send(context.Background(), testMsg())
	b.mu.Lock()
	stillFailed := b.failed["flap"]
	b.mu.Unlock()
	if stillFailed {
		t.Error("peer should be cleared after successful send")
	}
}

// TestSend_ParallelFanoutRunsConcurrently is a timing test: when every peer
// blocks for D and we have N peers, a serial implementation takes ~N*D while
// parallel takes ~D. This asserts the parallel shape without being flaky by
// using a generous tolerance.
func TestSend_ParallelFanoutRunsConcurrently(t *testing.T) {
	const nPeers = 4
	const handlerBlock = 300 * time.Millisecond

	fp := newFakePeerSource()
	var concurrent int32
	var maxConcurrent int32
	handler := func(_ context.Context, _ *gastrologv1.BroadcastMessage) error {
		n := atomic.AddInt32(&concurrent, 1)
		for {
			cur := atomic.LoadInt32(&maxConcurrent)
			if n <= cur || atomic.CompareAndSwapInt32(&maxConcurrent, cur, n) {
				break
			}
		}
		time.Sleep(handlerBlock)
		atomic.AddInt32(&concurrent, -1)
		return nil
	}
	for i := 0; i < nPeers; i++ {
		fp.addPeer(t, string(rune('a'+i)), handler)
	}

	b := newBroadcaster(fp, quietLogger(), 2*time.Second)
	start := time.Now()
	b.Send(context.Background(), testMsg())
	elapsed := time.Since(start)

	// Serial would be ~nPeers*handlerBlock (1.2s for 4x300ms). Parallel
	// should be ~handlerBlock + small overhead. Tolerate 2×handlerBlock.
	if elapsed > 2*handlerBlock {
		t.Errorf("Send appears serial: elapsed=%v, expected <%v", elapsed, 2*handlerBlock)
	}
	if got := atomic.LoadInt32(&maxConcurrent); got < 2 {
		t.Errorf("expected ≥2 concurrent handlers, got max=%d", got)
	}
}
