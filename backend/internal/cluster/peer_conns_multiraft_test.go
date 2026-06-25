package cluster

import (
	"io"
	"net"
	"testing"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

// TestPeerConnsMultiraftShareConn verifies multiraft and ClusterService callers
// receive the same *grpc.ClientConn for a peer when wired through SetPeerConnPool.
func TestPeerConnsMultiraftShareConn(t *testing.T) {
	t.Parallel()

	const (
		localID  = "node-a"
		peerID   = "node-b"
		peerAddr = "127.0.0.1:4566"
	)

	conf := hraft.DefaultConfig()
	conf.LocalID = hraft.ServerID(localID)
	conf.LogOutput = io.Discard

	_, trans := hraft.NewInmemTransport(hraft.ServerAddress(localID))
	r, err := hraft.NewRaft(conf, &noopFSM{}, hraft.NewInmemStore(), hraft.NewInmemStore(), hraft.NewInmemSnapshotStore(), trans)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	t.Cleanup(func() { _ = r.Shutdown().Error() })

	if err := r.BootstrapCluster(hraft.Configuration{Servers: []hraft.Server{
		{ID: localID, Address: "127.0.0.1:4565"},
		{ID: peerID, Address: hraft.ServerAddress(peerAddr)},
	}}).Error(); err != nil {
		t.Fatalf("BootstrapCluster: %v", err)
	}

	lis, err := net.Listen("tcp", peerAddr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})

	pool := NewPeerConns(r, nil, localID)
	viaNode, err := pool.Conn(peerID)
	if err != nil {
		t.Fatalf("Conn(peer): %v", err)
	}
	viaAddr, err := pool.ConnForAddress(hraft.ServerAddress(peerAddr))
	if err != nil {
		t.Fatalf("ConnForAddress: %v", err)
	}
	if viaNode != viaAddr {
		t.Fatal("Conn and ConnForAddress returned different ClientConns")
	}
}

// TestStaticPeerConnsConnForAddress verifies reverse lookup for harness pools.
func TestStaticPeerConnsConnForAddress(t *testing.T) {
	t.Parallel()

	const (
		localID  = "node-a"
		peerID   = "node-b"
		peerAddr = "127.0.0.1:4567"
	)

	resolve := func(nodeID string) (string, bool) {
		switch nodeID {
		case localID:
			return "127.0.0.1:4565", true
		case peerID:
			return peerAddr, true
		default:
			return "", false
		}
	}

	lis, err := net.Listen("tcp", peerAddr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})

	pool := NewStaticPeerConns(localID, resolve)
	pool.SetStaticPeerIDs([]string{localID, peerID})

	viaNode, err := pool.Conn(peerID)
	if err != nil {
		t.Fatalf("Conn(peer): %v", err)
	}
	viaAddr, err := pool.ConnForAddress(hraft.ServerAddress(peerAddr))
	if err != nil {
		t.Fatalf("ConnForAddress: %v", err)
	}
	if viaNode != viaAddr {
		t.Fatal("static pool: Conn and ConnForAddress returned different ClientConns")
	}
}

type noopFSM struct{}

func (*noopFSM) Apply(*hraft.Log) any                      { return nil }
func (*noopFSM) Snapshot() (hraft.FSMSnapshot, error)        { return &noopSnapshot{}, nil }
func (*noopFSM) Restore(io.ReadCloser) error                 { return nil }

type noopSnapshot struct{}

func (*noopSnapshot) Persist(s hraft.SnapshotSink) error { return s.Close() }
func (*noopSnapshot) Release()                         {}
