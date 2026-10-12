package cluster

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/query"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// startPeer starts a real cluster gRPC server for node id, configured by
// setup.
func startPeer(t *testing.T, id string, setup func(*Server)) *Server {
	t.Helper()
	peer, err := New(Config{ClusterAddr: "127.0.0.1:0", NodeID: id})
	if err != nil {
		t.Fatal(err)
	}
	peer.Transport()
	setup(peer)
	if err := peer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(peer.Stop)
	return peer
}

// callerOf returns a SearchForwarder and PeerConnManager on node "caller"
// that reach every peer over loopback.
func callerOf(t *testing.T, peers map[string]*Server) (*SearchForwarder, *PeerConnManager) {
	t.Helper()
	conns := NewStaticPeerConns("caller", func(id string) (string, bool) {
		p, ok := peers[id]
		if !ok {
			return "", false
		}
		return p.Addr(), true
	})
	t.Cleanup(func() { _ = conns.Close() })
	return NewSearchForwarder(conns), conns
}

// serviceConnIDsTo returns the IDs of the pooled service-lane connections to
// one peer.
func serviceConnIDsTo(conns *PeerConnManager, peer string) []uint64 {
	var ids []uint64
	for _, s := range conns.Snapshot() {
		if s.Lane == LaneService.String() && s.PeerNodeID == peer {
			ids = append(ids, s.ConnID)
		}
	}
	return ids
}

func failingWithMemoryOverflow(s *Server) {
	s.SetSearchExecutor(searchExecutorFailing(
		fmt.Errorf("vault v: %w", &query.MemoryLimitError{Consumer: "sort buffer", Limit: query.MaxQueryMemoryBytes}), true))
}

func servingNothing(s *Server) { servingRecords(nil)(s) }

// startBlockingPeer starts a peer whose every search parks in the executor
// until the test ends, without watching its context: the handler never writes
// a status, so the only way the call ends is the transport going away. Each
// search announces itself on entered.
func startBlockingPeer(t *testing.T, id string, entered chan<- struct{}) *Server {
	t.Helper()
	release := make(chan struct{})
	peer := startPeer(t, id, func(s *Server) {
		s.SetSearchExecutor(func(context.Context, *gastrologv1.ForwardSearchRequest) (iter.Seq2[chunk.Record, error], func() []byte, *gastrologv1.TableResult, []*gastrologv1.HistogramBucket, error) {
			entered <- struct{}{}
			<-release
			return nil, nil, nil, nil, errors.New("released")
		})
	})
	// Registered after the peer's Stop so it runs first: Stop waits for the
	// parked handler.
	t.Cleanup(func() { close(release) })
	return peer
}

type searchCall struct {
	name string
	run  func(*SearchForwarder, string, *gastrologv1.ForwardSearchRequest) error
}

var searchCalls = []searchCall{
	{"Search", func(sf *SearchForwarder, node string, req *gastrologv1.ForwardSearchRequest) error {
		_, err := sf.Search(context.Background(), node, req)
		return err
	}},
	{"SearchStream", func(sf *SearchForwarder, node string, req *gastrologv1.ForwardSearchRequest) error {
		recCh, _, errCh, _ := sf.SearchStream(context.Background(), node, req)
		for range recCh {
		}
		return <-errCh
	}},
}

// A search the peer refuses or fails — over its memory budget, malformed, or
// with no executor wired yet — is the peer's answer about that one query. The
// connection that carried the answer is healthy and serves the next search.
func TestSearchPeerStatusKeepsConnection(t *testing.T) {
	cases := []struct {
		name  string
		setup func(*Server)
		req   *gastrologv1.ForwardSearchRequest
		want  codes.Code
	}{
		{"memory budget", failingWithMemoryOverflow, forwardSearchRequest(), codes.ResourceExhausted},
		{"invalid vault id", servingNothing, &gastrologv1.ForwardSearchRequest{VaultId: []byte{1}}, codes.InvalidArgument},
		{"query failure", func(s *Server) { s.SetSearchExecutor(searchExecutorFailing(errors.New("disk on fire"), false)) }, forwardSearchRequest(), codes.Internal},
		{"peer reports unavailable", func(*Server) {}, forwardSearchRequest(), codes.Unavailable},
	}
	for _, tc := range cases {
		for _, call := range searchCalls {
			t.Run(tc.name+"/"+call.name, func(t *testing.T) {
				sf, conns := callerOf(t, map[string]*Server{"owner": startPeer(t, "owner", tc.setup)})

				err := call.run(sf, "owner", tc.req)
				if got := status.Code(err); got != tc.want {
					t.Fatalf("code = %v, want %v (err: %v)", got, tc.want, err)
				}
				before := serviceConnIDsTo(conns, "owner")
				if len(before) != 1 {
					t.Fatalf("service connections after the peer's answer = %v, want the one that carried it", before)
				}
				if err := call.run(sf, "owner", tc.req); status.Code(err) != tc.want {
					t.Fatalf("second call: code = %v, want %v (err: %v)", status.Code(err), tc.want, err)
				}
				if after := serviceConnIDsTo(conns, "owner"); !slices.Equal(after, before) {
					t.Fatalf("service connections after the next call = %v, want %v reused", after, before)
				}
			})
		}
	}
}

// Unary forwards go through the same classification: a peer that answers a
// GetContext with a status keeps its connection.
func TestUnaryPeerStatusKeepsConnection(t *testing.T) {
	sf, conns := callerOf(t, map[string]*Server{"owner": startPeer(t, "owner", func(*Server) {})})
	req := &gastrologv1.ForwardGetContextRequest{
		VaultId: glid.New().ToProto(),
		ChunkId: glid.GLID(chunk.NewChunkID()).ToProto(),
	}
	for i := range 2 {
		_, err := sf.GetContext(context.Background(), "owner", req)
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("call %d: code = %v, want Unavailable (err: %v)", i, status.Code(err), err)
		}
		if ids := serviceConnIDsTo(conns, "owner"); len(ids) != 1 {
			t.Fatalf("call %d: service connections = %v, want the one that carried the answer", i, ids)
		}
	}
}

// A peer whose transport dies under a search never answers it. The caller
// sees Unavailable with no peer status behind it and drops the connection, so
// the next call dials afresh.
func TestSearchTransportFailureInvalidatesConnection(t *testing.T) {
	for _, call := range searchCalls {
		t.Run(call.name, func(t *testing.T) {
			entered := make(chan struct{}, 1)
			peer := startBlockingPeer(t, "owner", entered)
			sf, conns := callerOf(t, map[string]*Server{"owner": peer})

			done := make(chan error, 1)
			go func() { done <- call.run(sf, "owner", forwardSearchRequest()) }()
			<-entered
			if ids := serviceConnIDsTo(conns, "owner"); len(ids) != 1 {
				t.Fatalf("service connections during the search = %v, want one", ids)
			}
			peer.grpcSrv.Stop()

			err := <-done
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("code = %v, want Unavailable (err: %v)", status.Code(err), err)
			}
			if ids := serviceConnIDsTo(conns, "owner"); len(ids) != 0 {
				t.Fatalf("service connections after the transport died = %v, want none", ids)
			}
		})
	}
}

// Four peers behind one caller: the one whose transport dies loses its
// connection, and the three that answer with a status keep theirs.
func TestSearchStatusAndTransportFailuresAcrossPeers(t *testing.T) {
	for _, call := range searchCalls {
		t.Run(call.name, func(t *testing.T) {
			entered := make(chan struct{}, 1)
			broken := startBlockingPeer(t, "broken", entered)
			sf, conns := callerOf(t, map[string]*Server{
				"broken":    broken,
				"exhausted": startPeer(t, "exhausted", failingWithMemoryOverflow),
				"bare":      startPeer(t, "bare", func(*Server) {}),
				"healthy":   startPeer(t, "healthy", servingRecords(sizedRecords(3, 64))),
			})

			done := make(chan error, 1)
			go func() { done <- call.run(sf, "broken", forwardSearchRequest()) }()
			<-entered

			want := map[string]codes.Code{"exhausted": codes.ResourceExhausted, "bare": codes.Unavailable, "healthy": codes.OK}
			before := map[string][]uint64{}
			for node, code := range want {
				if err := call.run(sf, node, forwardSearchRequest()); status.Code(err) != code {
					t.Fatalf("%s: code = %v, want %v (err: %v)", node, status.Code(err), code, err)
				}
				before[node] = serviceConnIDsTo(conns, node)
				if len(before[node]) != 1 {
					t.Fatalf("%s: service connections = %v, want one", node, before[node])
				}
			}

			broken.grpcSrv.Stop()
			if err := <-done; status.Code(err) != codes.Unavailable {
				t.Fatalf("broken: code = %v, want Unavailable (err: %v)", status.Code(err), err)
			}
			if ids := serviceConnIDsTo(conns, "broken"); len(ids) != 0 {
				t.Fatalf("broken: service connections = %v, want none", ids)
			}
			for node := range want {
				if after := serviceConnIDsTo(conns, node); !slices.Equal(after, before[node]) {
					t.Fatalf("%s: service connections = %v, want %v kept", node, after, before[node])
				}
			}
		})
	}
}

// The marker carries a peer's status through unchanged: callers that read the
// code or message, or unwrap the error, see exactly what the peer returned.
func TestPeerStatusErrorIsTransparent(t *testing.T) {
	inner := status.Error(codes.Unavailable, "search executor not configured")
	answered := metadata.Pairs(peerStatusTrailerKey, "1")

	marked := markPeerStatus(inner, answered)
	if shouldInvalidate(marked) {
		t.Error("a status the peer returned invalidates its connection")
	}
	if !shouldInvalidate(markPeerStatus(inner, metadata.MD{})) {
		t.Error("an Unavailable with no peer status behind it keeps its connection")
	}
	if !shouldInvalidate(markPeerStatus(inner, nil)) {
		t.Error("an Unavailable with no trailer keeps its connection")
	}
	st, ok := status.FromError(marked)
	if !ok || st.Code() != codes.Unavailable || st.Message() != "search executor not configured" {
		t.Errorf("status = %v (ok %v), want the peer's code and message unchanged", st, ok)
	}
	if marked.Error() != inner.Error() || !errors.Is(marked, inner) {
		t.Errorf("marked error %q does not read as the peer's %q", marked, inner)
	}
	wrapped := fmt.Errorf("search stream from owner: %w", marked)
	if shouldInvalidate(wrapped) || status.Code(wrapped) != codes.Unavailable {
		t.Error("wrapping the marked error loses the classification or the code")
	}
	if markPeerStatus(nil, answered) != nil {
		t.Error("a successful call gained an error")
	}
}
