package cluster

import (
	"context"
	"net"
	"sync"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
)

func TestPeerByteMetrics_EmptySnapshot(t *testing.T) {
	m := NewPeerByteMetrics()
	if got := m.Snapshot(); len(got) != 0 {
		t.Fatalf("expected empty snapshot, got %d entries", len(got))
	}
}

func TestPeerByteMetrics_TrackAndSnapshot(t *testing.T) {
	m := NewPeerByteMetrics()
	m.TrackSent("node-a", 100)
	m.TrackSent("node-a", 50)
	m.TrackReceived("node-a", 200)
	m.TrackSent("node-b", 10)

	got := m.Snapshot()
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d: %+v", len(got), got)
	}
	// Snapshot sorts by peer.
	if got[0].Peer != "node-a" {
		t.Errorf("expected first peer node-a, got %s", got[0].Peer)
	}
	if got[0].Sent != 150 || got[0].Received != 200 {
		t.Errorf("node-a: sent=%d recv=%d, want 150/200", got[0].Sent, got[0].Received)
	}
	if got[1].Peer != "node-b" {
		t.Errorf("expected second peer node-b, got %s", got[1].Peer)
	}
	if got[1].Sent != 10 || got[1].Received != 0 {
		t.Errorf("node-b: sent=%d recv=%d, want 10/0", got[1].Sent, got[1].Received)
	}
}

func TestPeerByteMetrics_IgnoresZeroAndNegative(t *testing.T) {
	m := NewPeerByteMetrics()
	m.TrackSent("node-a", 0)
	m.TrackSent("node-a", -10)
	m.TrackReceived("node-a", -5)
	m.TrackSent("", 100)

	if got := m.Snapshot(); len(got) != 0 {
		t.Fatalf("expected empty snapshot, got %+v", got)
	}
}

func TestPeerByteMetrics_ConcurrentTracking(t *testing.T) {
	m := NewPeerByteMetrics()
	const goroutines = 8
	const perGoroutine = 1000
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				m.TrackSent("node-a", 1)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				m.TrackReceived("node-a", 1)
			}
		}()
	}
	wg.Wait()
	got := m.Snapshot()
	if len(got) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(got))
	}
	wantTotal := int64(goroutines * perGoroutine)
	if got[0].Sent != wantTotal {
		t.Errorf("sent = %d, want %d", got[0].Sent, wantTotal)
	}
	if got[0].Received != wantTotal {
		t.Errorf("received = %d, want %d", got[0].Received, wantTotal)
	}
}

func TestClientStatsHandler_TracksInAndOut(t *testing.T) {
	m := NewPeerByteMetrics()
	h := newClientStatsHandler("node-a", m)

	h.HandleRPC(context.Background(), &stats.OutPayload{WireLength: 250})
	h.HandleRPC(context.Background(), &stats.InPayload{WireLength: 400})
	// Unrelated stats events are ignored.
	h.HandleRPC(context.Background(), &stats.Begin{})
	h.HandleRPC(context.Background(), &stats.End{})

	got := m.Snapshot()
	if len(got) != 1 || got[0].Peer != "node-a" {
		t.Fatalf("unexpected snapshot: %+v", got)
	}
	if got[0].Sent != 250 || got[0].Received != 400 {
		t.Errorf("sent=%d recv=%d, want 250/400", got[0].Sent, got[0].Received)
	}
}

func TestServerStatsHandler_AttributesBytesFromMetadata(t *testing.T) {
	m := NewPeerByteMetrics()
	h := newServerStatsHandler(m)

	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(NodeIDMetadataKey, "node-a"))
	ctx = h.TagRPC(ctx, &stats.RPCTagInfo{})

	h.HandleRPC(ctx, &stats.InPayload{WireLength: 500})
	h.HandleRPC(ctx, &stats.OutPayload{WireLength: 150})

	got := m.Snapshot()
	if len(got) != 1 || got[0].Peer != "node-a" {
		t.Fatalf("unexpected snapshot: %+v", got)
	}
	// Server side reverses: inbound payload is from the peer, outbound
	// payload is response back to the peer.
	if got[0].Received != 500 || got[0].Sent != 150 {
		t.Errorf("recv=%d sent=%d, want 500/150", got[0].Received, got[0].Sent)
	}
}

func TestServerStatsHandler_MissingMetadataIsNoOp(t *testing.T) {
	m := NewPeerByteMetrics()
	h := newServerStatsHandler(m)

	// No x-gastrolog-node-id header — can't attribute, don't track.
	ctx := h.TagRPC(context.Background(), &stats.RPCTagInfo{})

	h.HandleRPC(ctx, &stats.InPayload{WireLength: 500})

	if got := m.Snapshot(); len(got) != 0 {
		t.Fatalf("expected empty snapshot when peer metadata absent, got %+v", got)
	}
}

// TestPeerBytes_EndToEnd exercises the full gRPC flow: a server with the
// server stats handler, a client with both the node-ID-stamping interceptor
// and the client stats handler, doing an actual Broadcast RPC. Verifies
// that both sides populate their counters and that the server attributes
// inbound bytes to the right peer via the metadata header.
func TestPeerBytes_EndToEnd(t *testing.T) {
	clientMetrics := NewPeerByteMetrics()
	serverMetrics := NewPeerByteMetrics()

	// Server.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.StatsHandler(newServerStatsHandler(serverMetrics)))
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
				return &gastrologv1.BroadcastResponse{}, nil
			},
		}},
	}, struct{}{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop(); _ = lis.Close() })

	// Client. Mimics the interceptors + stats handler that PeerConns
	// installs in production.
	stampNodeID := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(metadata.AppendToOutgoingContext(ctx, NodeIDMetadataKey, "node-client"), method, req, reply, cc, opts...)
	}
	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(stampNodeID),
		grpc.WithStatsHandler(newClientStatsHandler("node-server", clientMetrics)),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	req := &gastrologv1.BroadcastRequest{}
	resp := &gastrologv1.BroadcastResponse{}
	if err := conn.Invoke(context.Background(), "/gastrolog.v1.ClusterService/Broadcast", req, resp); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	clientSnap := clientMetrics.Snapshot()
	if len(clientSnap) != 1 || clientSnap[0].Peer != "node-server" {
		t.Fatalf("client snapshot: %+v", clientSnap)
	}
	if clientSnap[0].Sent == 0 {
		t.Errorf("client Sent = 0, expected > 0 after Invoke")
	}
	if clientSnap[0].Received == 0 {
		t.Errorf("client Received = 0, expected > 0 after Invoke")
	}

	serverSnap := serverMetrics.Snapshot()
	if len(serverSnap) != 1 || serverSnap[0].Peer != "node-client" {
		t.Fatalf("server snapshot: %+v", serverSnap)
	}
	if serverSnap[0].Received == 0 {
		t.Errorf("server Received = 0, expected > 0 after Invoke")
	}
	if serverSnap[0].Sent == 0 {
		t.Errorf("server Sent = 0, expected > 0 (response) after Invoke")
	}
}
