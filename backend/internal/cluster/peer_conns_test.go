package cluster

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var errTransport = status.Error(codes.Unavailable, "test transport failure")

func dialLocal(t *testing.T) (addr string, conn *grpc.ClientConn, cleanup func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()

	conn, err = grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		_ = lis.Close()
		t.Fatalf("dial: %v", err)
	}
	return lis.Addr().String(), conn, func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	}
}

func testManager(t *testing.T, peerAddr string) *PeerConnManager {
	t.Helper()
	return NewStaticPeerConns("local", func(id string) (string, bool) {
		if id == "node-x" {
			return peerAddr, true
		}
		return "", false
	})
}

func TestInvalidateDeferredClose(t *testing.T) {
	addr, _, cleanup := dialLocal(t)
	defer cleanup()

	mgr := testManager(t, addr)
	h, err := mgr.AcquireService("node-x", "test")
	if err != nil {
		t.Fatalf("AcquireService: %v", err)
	}
	conn := h.GRPC()
	h.Release()

	mgr.Invalidate("node-x", errTransport)

	h2, err := mgr.AcquireService("node-x", "test-redial")
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if h2.GRPC() == conn {
		t.Fatal("Invalidate did not drop cached conn")
	}
	h2.Release()

	if conn.GetState() == connectivity.Shutdown {
		t.Fatal("Invalidate closed conn synchronously")
	}

	deadline := time.Now().Add(invalidateGracePeriod + 2*time.Second)
	for time.Now().Before(deadline) {
		if conn.GetState() == connectivity.Shutdown {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("expected conn Shutdown after grace period, got %v", conn.GetState())
}

func TestInvalidateMissingNoOp(t *testing.T) {
	mgr := NewStaticPeerConns("local", func(string) (string, bool) { return "", false })
	mgr.Invalidate("does-not-exist", errTransport)
}

func TestResetClearsCache(t *testing.T) {
	addr, _, cleanup := dialLocal(t)
	defer cleanup()

	mgr := testManager(t, addr)
	h, err := mgr.AcquireService("node-x", "test")
	if err != nil {
		t.Fatalf("AcquireService: %v", err)
	}
	h.Release()

	mgr.Reset(nil)
	if snaps := mgr.Snapshot(); len(snaps) != 0 {
		t.Fatalf("Reset left %d cached connections", len(snaps))
	}
}

func TestInvalidateConcurrentUsersNotDisrupted(t *testing.T) {
	addr, _, cleanup := dialLocal(t)
	defer cleanup()

	mgr := testManager(t, addr)
	h, err := mgr.AcquireService("node-x", "test")
	if err != nil {
		t.Fatalf("AcquireService: %v", err)
	}
	conn := h.GRPC()
	h.Release()

	const goroutines = 32
	start := make(chan struct{})
	var disrupted atomic.Int32
	done := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		go func() {
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if conn.GetState() == connectivity.Shutdown {
					disrupted.Add(1)
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	close(start)
	time.Sleep(50 * time.Millisecond)
	mgr.Invalidate("node-x", errTransport)
	close(done)
	time.Sleep(invalidateGracePeriod + 100*time.Millisecond)

	if disrupted.Load() > 0 {
		t.Fatalf("%d goroutines saw Shutdown during grace window", disrupted.Load())
	}
}

type noopFSM struct{}

func (*noopFSM) Apply(*hraft.Log) any                 { return nil }
func (*noopFSM) Snapshot() (hraft.FSMSnapshot, error) { return &noopSnapshot{}, nil }
func (*noopFSM) Restore(io.ReadCloser) error          { return nil }

type noopSnapshot struct{}

func (*noopSnapshot) Persist(s hraft.SnapshotSink) error { return s.Close() }
func (*noopSnapshot) Release()                           {}
