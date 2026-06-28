package cluster

import (
	"context"
	"net"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"google.golang.org/grpc"
)

// TestPeerConnManager_RaftLaneTracksBytes verifies multiraft-style raft-lane
// dials stamp x-gastrolog-node-id, increment per-connection catalog bytes, and
// mirror outbound totals into PeerByteMetrics (gastrolog-5uyy6).
func TestPeerConnManager_RaftLaneTracksBytes(t *testing.T) {
	t.Parallel()

	const (
		localID  = "node-a"
		peerID   = "node-b"
		groupID  = "vault/test/ctl"
		peerAddr = "127.0.0.1:0"
	)

	clientMetrics := NewPeerByteMetrics()
	serverMetrics := NewPeerByteMetrics()

	lis, err := net.Listen("tcp", peerAddr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.StatsHandler(newServerStatsHandler(serverMetrics)))
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "gastrolog.v1.MultiRaftTransportService",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "AppendEntries",
			Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				if peerIDFromIncoming(ctx) == "" {
					t.Error("raft lane RPC missing x-gastrolog-node-id header")
				}
				req := &gastrologv1.MultiRaftAppendEntriesRequest{}
				if err := dec(req); err != nil {
					return nil, err
				}
				return &gastrologv1.MultiRaftAppendEntriesResponse{}, nil
			},
		}},
	}, struct{}{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})

	mgr := NewPeerConnManager(PeerConnManagerConfig{
		NodeID:        localID,
		ByteMetrics:   clientMetrics,
		StaticResolve: func(id string) (string, bool) {
			if id == peerID {
				return lis.Addr().String(), true
			}
			return "", false
		},
	})

	raftH, err := mgr.AcquireRaftPeer(peerID, groupID, "multiraft-test")
	if err != nil {
		t.Fatalf("AcquireRaftPeer: %v", err)
	}
	defer raftH.Release()

	req := &gastrologv1.MultiRaftAppendEntriesRequest{GroupId: []byte(groupID)}
	resp := &gastrologv1.MultiRaftAppendEntriesResponse{}
	if err := raftH.GRPC().Invoke(context.Background(), "/gastrolog.v1.MultiRaftTransportService/AppendEntries", req, resp); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	// Invoke again without explicit metadata — PeerConnManager interceptor should stamp it.
	req2 := &gastrologv1.MultiRaftAppendEntriesRequest{GroupId: []byte(groupID)}
	resp2 := &gastrologv1.MultiRaftAppendEntriesResponse{}
	if err := raftH.GRPC().Invoke(context.Background(), "/gastrolog.v1.MultiRaftTransportService/AppendEntries", req2, resp2); err != nil {
		t.Fatalf("invoke without ctx metadata: %v", err)
	}

	snaps := mgr.Snapshot()
	var raftSnap *PeerConnSnapshot
	for i := range snaps {
		if snaps[i].Lane == "raft" && snaps[i].GroupID == groupID {
			raftSnap = &snaps[i]
			break
		}
	}
	if raftSnap == nil {
		t.Fatalf("no raft lane snapshot; got %+v", snaps)
	}
	if raftSnap.BytesSent == 0 {
		t.Errorf("raft conn BytesSent = 0, want > 0")
	}
	if raftSnap.BytesRecv == 0 {
		t.Errorf("raft conn BytesRecv = 0, want > 0")
	}

	clientSnap := clientMetrics.Snapshot()
	if len(clientSnap) != 1 || clientSnap[0].Peer != peerID {
		t.Fatalf("client PeerByteMetrics: %+v", clientSnap)
	}
	if clientSnap[0].Sent == 0 || clientSnap[0].Received == 0 {
		t.Errorf("client aggregate sent=%d recv=%d, want both > 0", clientSnap[0].Sent, clientSnap[0].Received)
	}

	serverSnap := serverMetrics.Snapshot()
	if len(serverSnap) != 1 || serverSnap[0].Peer != localID {
		t.Fatalf("server PeerByteMetrics: %+v", serverSnap)
	}
	if serverSnap[0].Received == 0 || serverSnap[0].Sent == 0 {
		t.Errorf("server aggregate recv=%d sent=%d, want both > 0", serverSnap[0].Received, serverSnap[0].Sent)
	}
}

// TestPeerConnManager_ServiceLaneMirrorsByteMetrics ensures the aggregate hook
// applies to service lanes too, not only raft.
func TestPeerConnManager_ServiceLaneMirrorsByteMetrics(t *testing.T) {
	t.Parallel()

	clientMetrics := NewPeerByteMetrics()

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
			Handler: func(_ any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				req := &gastrologv1.BroadcastRequest{}
				if err := dec(req); err != nil {
					return nil, err
				}
				return &gastrologv1.BroadcastResponse{}, nil
			},
		}},
	}, struct{}{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})

	mgr := NewPeerConnManager(PeerConnManagerConfig{
		NodeID:      "local",
		ByteMetrics: clientMetrics,
		StaticResolve: func(id string) (string, bool) {
			if id == "peer" {
				return lis.Addr().String(), true
			}
			return "", false
		},
	})

	h, err := mgr.AcquireService("peer", PurposeBroadcast)
	if err != nil {
		t.Fatalf("AcquireService: %v", err)
	}
	defer h.Release()

	req := &gastrologv1.BroadcastRequest{}
	resp := &gastrologv1.BroadcastResponse{}
	if err := h.GRPC().Invoke(context.Background(), "/gastrolog.v1.ClusterService/Broadcast", req, resp); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	snap := clientMetrics.Snapshot()
	if len(snap) != 1 || snap[0].Peer != "peer" || snap[0].Sent == 0 {
		t.Fatalf("expected aggregate outbound bytes for peer, got %+v", snap)
	}
}
