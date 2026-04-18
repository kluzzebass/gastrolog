package cluster

import (
	"context"
	"sort"
	"sync"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
)

// NodeIDMetadataKey is the gRPC metadata header used to carry the caller's
// node ID on every inter-node RPC. PeerConns installs an outgoing-context
// interceptor that stamps this header, and the server-side stats handler
// reads it to identify which peer a given RPC's bytes belong to.
//
// The shared cluster cert doesn't encode per-node identity (CN is
// "gastrolog-cluster" for every node), so peer identification has to come
// from an explicit signal rather than from mTLS.
const NodeIDMetadataKey = "x-gastrolog-node-id"

// PeerByteMetrics tracks cumulative gRPC wire bytes sent to and received
// from each cluster peer. Aggregates traffic across ALL inter-node RPCs —
// Raft, broadcast, tier replication, query forwarding, chunk streaming,
// and anything else that goes through the cluster transport.
//
// Counters are monotonic and reset only on process restart. Rate derivation
// is left to consumers (UI delta between broadcast ticks, or an external
// scraper if one is ever added back).
type PeerByteMetrics struct {
	mu   sync.Mutex
	sent map[string]int64
	recv map[string]int64
}

// NewPeerByteMetrics returns a zero-valued metrics tracker.
func NewPeerByteMetrics() *PeerByteMetrics {
	return &PeerByteMetrics{
		sent: make(map[string]int64),
		recv: make(map[string]int64),
	}
}

// TrackSent records bytes sent to a peer. A zero or negative n is a no-op.
func (m *PeerByteMetrics) TrackSent(peer string, n int) {
	if n <= 0 || peer == "" {
		return
	}
	m.mu.Lock()
	m.sent[peer] += int64(n)
	m.mu.Unlock()
}

// TrackReceived records bytes received from a peer. A zero or negative n
// is a no-op.
func (m *PeerByteMetrics) TrackReceived(peer string, n int) {
	if n <= 0 || peer == "" {
		return
	}
	m.mu.Lock()
	m.recv[peer] += int64(n)
	m.mu.Unlock()
}

// PeerByteCounter is a (peer, sent, received) triplet returned by Snapshot.
type PeerByteCounter struct {
	Peer     string
	Sent     int64
	Received int64
}

// Snapshot returns one counter per peer that has seen traffic in either
// direction. Sorted by peer ID for deterministic iteration order.
func (m *PeerByteMetrics) Snapshot() []PeerByteCounter {
	m.mu.Lock()
	peers := make(map[string]struct{}, len(m.sent)+len(m.recv))
	for p := range m.sent {
		peers[p] = struct{}{}
	}
	for p := range m.recv {
		peers[p] = struct{}{}
	}
	out := make([]PeerByteCounter, 0, len(peers))
	for p := range peers {
		out = append(out, PeerByteCounter{
			Peer:     p,
			Sent:     m.sent[p],
			Received: m.recv[p],
		})
	}
	m.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Peer < out[j].Peer })
	return out
}

// clientStatsHandler is a grpc/stats.Handler installed on each outbound
// PeerConn. One instance per dialed peer — the peer ID is baked in at
// construction time, so HandleRPC doesn't need to look it up on every
// event (hot path).
type clientStatsHandler struct {
	peer string
	m    *PeerByteMetrics
}

func newClientStatsHandler(peer string, m *PeerByteMetrics) *clientStatsHandler {
	return &clientStatsHandler{peer: peer, m: m}
}

func (h *clientStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (*clientStatsHandler) HandleConn(context.Context, stats.ConnStats) {}

func (h *clientStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (h *clientStatsHandler) HandleRPC(_ context.Context, rs stats.RPCStats) {
	switch s := rs.(type) {
	case *stats.OutPayload:
		h.m.TrackSent(h.peer, s.WireLength)
	case *stats.InPayload:
		h.m.TrackReceived(h.peer, s.WireLength)
	}
}

// serverStatsHandler is installed on the cluster gRPC server. One instance
// serves every inbound connection — the peer ID for each RPC is pulled from
// the x-gastrolog-node-id metadata header that the client-side interceptor
// stamps on outgoing contexts.
type serverStatsHandler struct {
	m *PeerByteMetrics
}

func newServerStatsHandler(m *PeerByteMetrics) *serverStatsHandler {
	return &serverStatsHandler{m: m}
}

type serverPeerCtxKey struct{}

func (h *serverStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (*serverStatsHandler) HandleConn(context.Context, stats.ConnStats) {}

func (h *serverStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	// Pull the peer node ID from the incoming metadata once per RPC and
	// stash it in the context so HandleRPC doesn't have to re-parse the
	// metadata on every InPayload / OutPayload event.
	peer := peerIDFromIncoming(ctx)
	return context.WithValue(ctx, serverPeerCtxKey{}, peer)
}

func (h *serverStatsHandler) HandleRPC(ctx context.Context, rs stats.RPCStats) {
	peer, _ := ctx.Value(serverPeerCtxKey{}).(string)
	if peer == "" {
		return
	}
	switch s := rs.(type) {
	case *stats.InPayload:
		h.m.TrackReceived(peer, s.WireLength)
	case *stats.OutPayload:
		h.m.TrackSent(peer, s.WireLength)
	}
}

// peerIDFromIncoming reads the x-gastrolog-node-id metadata header from
// an inbound gRPC context. Returns empty string when absent — callers
// skip tracking rather than misattribute.
func peerIDFromIncoming(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get(NodeIDMetadataKey)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}
