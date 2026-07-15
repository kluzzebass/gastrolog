package cluster

import (
	"io"
	"net"
	"testing"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

func TestPeerConnsMultiraftLaneIsolation(t *testing.T) {
	t.Parallel()

	const (
		localID  = "node-a"
		peerID   = "node-b"
		peerAddr = "127.0.0.1:4566"
		groupID  = "config"
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

	mgr := NewPeerConns(r, nil, localID)
	svc, err := mgr.AcquireService(peerID, "test")
	if err != nil {
		t.Fatalf("AcquireService: %v", err)
	}
	raftH, err := mgr.AcquireRaftPeer(peerID, groupID, "test")
	if err != nil {
		t.Fatalf("AcquireRaft: %v", err)
	}
	if svc.GRPC() == raftH.GRPC() {
		t.Fatal("service and raft lanes returned the same ClientConn")
	}
	svc.Release()
	raftH.Release()
}

func TestServicePoolMaxPerPeer(t *testing.T) {
	t.Parallel()

	const peerAddr = "127.0.0.1:4568"
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

	mgr := NewStaticPeerConns("local", func(id string) (string, bool) {
		if id == "peer" {
			return peerAddr, true
		}
		return "", false
	})
	mgr.SetServicePoolMaxPerPeer(2)

	var conns []PeerConnHandle
	for i := 0; i < 2; i++ {
		h, err := mgr.AcquireService("peer", "test")
		if err != nil {
			t.Fatalf("AcquireService %d: %v", i, err)
		}
		conns = append(conns, h)
	}
	if conns[0].GRPC() == conns[1].GRPC() {
		t.Fatal("expected two distinct pool connections under concurrent acquire")
	}
	for _, h := range conns {
		h.Release()
	}
}
