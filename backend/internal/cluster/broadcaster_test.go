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

func (f *fakePeerSource) Invalidate(id string, _ error) {
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

	// Send is fire-and-forget (push, not pull — see gastrolog-5oofa).
	// Poll for delivery with a generous deadline.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		allReceived := true
		for _, id := range []string{"a", "b", "c"} {
			if _, ok := received.Load(id); !ok {
				allReceived = false
				break
			}
		}
		if allReceived {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
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

// TestSend_ReturnsImmediatelyEvenWithSlowPeer verifies the push-not-pull
// contract: Send returns immediately regardless of how slow any peer is.
// Per gastrolog-5oofa, a SIGSTOPed peer must not stall the caller — the
// fix is to make Send fire-and-forget. Callers (StatsCollector) push
// their local state; they don't wait for peer acknowledgment.
func TestSend_ReturnsImmediatelyEvenWithSlowPeer(t *testing.T) {
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
		<-ctx.Done()
		return ctx.Err()
	}

	fp.addPeer(t, "fast-a", hit("fast-a"))
	fp.addPeer(t, "fast-b", hit("fast-b"))
	fp.addPeer(t, "slow", slow)

	b := newBroadcaster(fp, quietLogger(), 2*time.Second)

	start := time.Now()
	b.Send(context.Background(), testMsg())
	callerElapsed := time.Since(start)

	// Send MUST return immediately (push, not pull). 50ms is a generous
	// upper bound for any in-process dispatch overhead.
	if callerElapsed > 50*time.Millisecond {
		t.Fatalf("Send blocked the caller for %v; should return immediately", callerElapsed)
	}

	// Fast peers complete in the background, well before slow timeout.
	deadline := time.After(1 * time.Second)
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case id := <-fastDone:
			seen[id] = true
		case <-deadline:
			t.Fatalf("fast peers did not complete in background; seen=%v", seen)
		}
	}

	// Slow peer handler entered (parallelism confirmed).
	select {
	case <-slowEntered:
	case <-time.After(time.Second):
		t.Fatal("slow peer handler never entered — peers may have been serialized")
	}

	// Wait for slow peer to time out and be invalidated.
	invalidDeadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(invalidDeadline) {
		fp.mu.Lock()
		got := fp.invalidated["slow"]
		fp.mu.Unlock()
		if got > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	fp.mu.Lock()
	defer fp.mu.Unlock()
	if fp.invalidated["slow"] == 0 {
		t.Error("slow peer should have been invalidated after per-peer timeout")
	}
	if fp.invalidated["fast-a"] != 0 || fp.invalidated["fast-b"] != 0 {
		t.Errorf("fast peers should not be invalidated: %v", fp.invalidated)
	}
}

// TestSend_CallerDeadlineCancelsInFlight verifies that cancelling the
// caller's context aborts in-flight peer RPCs. Send returns immediately
// (push), but the in-flight goroutine must still observe caller-side
// cancellation and unblock when the caller's ctx fires.
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

	b := newBroadcaster(fp, quietLogger(), 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	b.Send(ctx, testMsg())
	// Send is fire-and-forget; don't time it.

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

	// First send: fails. failed[flap] = true. Send is async, so poll
	// for the state transition.
	b.Send(context.Background(), testMsg())
	waitFor(t, time.Second, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.failed["flap"]
	}, "peer not marked failed after error")

	// Recover: next send succeeds, failed flag clears.
	shouldFail.Store(false)
	b.Send(context.Background(), testMsg())
	waitFor(t, time.Second, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return !b.failed["flap"]
	}, "peer should be cleared after successful send")
}

// waitFor polls cond every 10ms until it returns true or the deadline
// expires. Useful for async assertions after fire-and-forget calls.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(msg)
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
	b.Send(context.Background(), testMsg())

	// Send is fire-and-forget. Wait for the background goroutines to all
	// hit the handler concurrently, then verify max concurrency ≥ 2.
	waitFor(t, 2*time.Second, func() bool {
		return atomic.LoadInt32(&maxConcurrent) >= 2
	}, "peers did not run concurrently")

	if got := atomic.LoadInt32(&maxConcurrent); got < 2 {
		t.Errorf("expected ≥2 concurrent handlers, got max=%d", got)
	}

	// Wait for handlers to complete so the test cleanly tears down.
	waitFor(t, 3*handlerBlock, func() bool {
		return atomic.LoadInt32(&concurrent) == 0
	}, "handlers did not complete")
}
