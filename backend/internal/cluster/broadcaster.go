package cluster

import (
	"context"
	"log/slog"
	"sync"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

// broadcastPeerSource is the subset of *PeerConns that Broadcaster needs.
// Extracted so tests can inject a fake without standing up a real Raft.
type broadcastPeerSource interface {
	Peers() ([]hraft.Server, error)
	Conn(nodeID string) (*grpc.ClientConn, error)
	Invalidate(nodeID string)
}

// Broadcaster fans out BroadcastMessages to all cluster peers via the
// Broadcast RPC. Unlike Forwarder (leader-only), Broadcaster maintains
// connections to every peer in the Raft configuration.
type Broadcaster struct {
	peers  broadcastPeerSource
	logger *slog.Logger

	// perPeerTimeout bounds each per-peer RPC so one unresponsive peer can't
	// stretch an individual broadcast send indefinitely. When the caller's
	// ctx carries a tighter deadline, that deadline wins.
	perPeerTimeout time.Duration

	mu      sync.Mutex
	failed  map[string]bool      // true = peer is unreachable (suppress repeated logs)
	sent    map[metricsKey]int64 // per (peer, type) success counter
	errored map[metricsKey]int64 // per (peer, type) failure counter
}

// metricsKey identifies a (peer, message-type) bucket for counters.
type metricsKey struct {
	peer string
	typ  string
}

// NewBroadcaster creates a Broadcaster that uses the shared PeerConns pool.
func NewBroadcaster(peers *PeerConns, logger *slog.Logger) *Broadcaster {
	return newBroadcaster(peers, logger, ForwardingTimeout)
}

// newBroadcaster is the internal constructor used by production and tests.
// Tests can inject a fake peer source and a custom per-peer timeout.
func newBroadcaster(peers broadcastPeerSource, logger *slog.Logger, perPeerTimeout time.Duration) *Broadcaster {
	return &Broadcaster{
		peers:          peers,
		logger:         logger,
		perPeerTimeout: perPeerTimeout,
		failed:         make(map[string]bool),
		sent:           make(map[metricsKey]int64),
		errored:        make(map[metricsKey]int64),
	}
}

// Send broadcasts a message to all peers in parallel. Best-effort: errors
// are logged but don't block delivery to other peers. A slow or unresponsive
// peer can no longer stall delivery to healthy peers — each peer's RPC runs
// in its own goroutine under its own per-peer timeout. Returns once every
// peer goroutine has finished (succeeded, failed, or timed out).
func (b *Broadcaster) Send(ctx context.Context, msg *gastrologv1.BroadcastMessage) {
	peers, err := b.peers.Peers()
	if err != nil {
		b.logger.Debug("broadcast: get peers", "error", err)
		return
	}
	if len(peers) == 0 {
		return
	}

	typ := PayloadType(msg)
	req := &gastrologv1.BroadcastRequest{Message: msg}
	var wg sync.WaitGroup
	wg.Add(len(peers))
	for _, p := range peers {
		go func(id string) {
			defer wg.Done()
			b.sendToPeer(ctx, id, typ, req)
		}(string(p.ID))
	}
	wg.Wait()
}

// sendToPeer handles one peer's delivery: dial → per-peer-timeout context →
// Invoke → record metrics for the outcome.
func (b *Broadcaster) sendToPeer(ctx context.Context, id, typ string, req *gastrologv1.BroadcastRequest) {
	conn, err := b.peers.Conn(id)
	if err != nil {
		b.logPeerError(id, "dial peer", err)
		b.recordError(id, typ)
		return
	}

	peerCtx, cancel := b.peerContext(ctx)
	defer cancel()

	if err := b.sendOne(peerCtx, conn, req); err != nil {
		b.logPeerError(id, "send", err)
		b.recordError(id, typ)
		b.peers.Invalidate(id)
		return
	}
	b.clearPeerError(id)
	b.recordSent(id, typ)
}

// peerContext derives a per-peer context from the caller's ctx. If the
// caller's ctx carries a deadline tighter than perPeerTimeout, the caller's
// deadline wins (context.WithTimeout preserves the earlier deadline of the
// parent). Cancellation propagates from parent to child, so a caller-side
// abort cancels in-flight peer RPCs immediately.
func (b *Broadcaster) peerContext(parent context.Context) (context.Context, context.CancelFunc) {
	if b.perPeerTimeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, b.perPeerTimeout)
}

// recordSent bumps the success counter for (peer, type).
func (b *Broadcaster) recordSent(peer, typ string) {
	b.mu.Lock()
	b.sent[metricsKey{peer: peer, typ: typ}]++
	b.mu.Unlock()
}

// recordError bumps the error counter for (peer, type).
func (b *Broadcaster) recordError(peer, typ string) {
	b.mu.Lock()
	b.errored[metricsKey{peer: peer, typ: typ}]++
	b.mu.Unlock()
}

// BroadcastCounter is a (peer, type, value) triplet used by the metrics
// writer. Exposed for consumers that want to emit Prometheus text.
type BroadcastCounter struct {
	Peer  string
	Type  string
	Value int64
}

// MetricsSnapshot returns the current send / error counters for all (peer,
// type) combinations that have seen traffic. Stable ordering left to the
// caller; the underlying maps are not sorted here.
func (b *Broadcaster) MetricsSnapshot() (sent, errors []BroadcastCounter) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sent = make([]BroadcastCounter, 0, len(b.sent))
	for k, v := range b.sent {
		sent = append(sent, BroadcastCounter{Peer: k.peer, Type: k.typ, Value: v})
	}
	errors = make([]BroadcastCounter, 0, len(b.errored))
	for k, v := range b.errored {
		errors = append(errors, BroadcastCounter{Peer: k.peer, Type: k.typ, Value: v})
	}
	return sent, errors
}

// PayloadType returns the broadcast payload's oneof variant as a stable
// label ("node_stats", "node_jobs", or "unknown"). Used for Prometheus
// metrics labels and log fields.
func PayloadType(msg *gastrologv1.BroadcastMessage) string {
	if msg == nil {
		return "unknown"
	}
	switch msg.Payload.(type) {
	case *gastrologv1.BroadcastMessage_NodeStats:
		return "node_stats"
	case *gastrologv1.BroadcastMessage_NodeJobs:
		return "node_jobs"
	default:
		return "unknown"
	}
}

// logPeerError logs the first error for a peer, then suppresses repeats.
func (b *Broadcaster) logPeerError(id string, action string, err error) {
	b.mu.Lock()
	alreadyFailed := b.failed[id]
	b.failed[id] = true
	b.mu.Unlock()

	if !alreadyFailed {
		b.logger.Debug("broadcast: "+action, "peer", id, "error", err)
	}
}

// clearPeerError marks a peer as healthy and logs recovery if it was down.
func (b *Broadcaster) clearPeerError(id string) {
	b.mu.Lock()
	wasFailed := b.failed[id]
	delete(b.failed, id)
	b.mu.Unlock()

	if wasFailed {
		b.logger.Info("broadcast: peer recovered", "peer", id)
	}
}

// sendOne sends a BroadcastRequest to a single peer via the manually-defined
// RPC path (same pattern as ForwardApplyClient).
func (b *Broadcaster) sendOne(ctx context.Context, conn *grpc.ClientConn, req *gastrologv1.BroadcastRequest) error {
	out := &gastrologv1.BroadcastResponse{}
	return conn.Invoke(ctx, "/gastrolog.v1.ClusterService/Broadcast", req, out)
}

// Close is a no-op — connection lifecycle is managed by PeerConns.
func (b *Broadcaster) Close() error { return nil }
