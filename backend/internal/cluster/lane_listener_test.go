package cluster

import (
	"net"
	"testing"

	"gastrolog/internal/multiraft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestSNIDemuxVirtualListenersHaveAddr(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	reg := multiraft.NewInboundLaneRegistry(ln.Addr())
	demux := newSNIDemuxListener(ln, reg)
	t.Cleanup(func() { _ = demux.Close() })

	vl := demux.ServiceListener()
	if vl.Addr() == nil {
		t.Fatal("service listener Addr() is nil")
	}
	if got, want := vl.Addr().String(), ln.Addr().String(); got != want {
		t.Fatalf("service listener addr = %q, want %q", got, want)
	}

	groupLn := reg.Listener("config")
	if groupLn.Addr() == nil {
		t.Fatal("group listener Addr() is nil")
	}
}

func TestSNIDemuxGrpcServeDoesNotPanic(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	reg := multiraft.NewInboundLaneRegistry(ln.Addr())
	demux := newSNIDemuxListener(ln, reg)
	t.Cleanup(func() { _ = demux.Close() })

	srv := grpc.NewServer()
	t.Cleanup(func() { srv.Stop() })

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(demux.ServiceListener()) }()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("grpc Serve returned error: %v", err)
		}
	default:
	}

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
}
