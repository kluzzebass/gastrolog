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
	Invalidate(nodeID string, err error)
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

	mu     sync.Mutex
	failed map[string]bool // true = peer is unreachable (suppress repeated logs)
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
	}
}

// Send pushes a message to every peer and returns immediately. Push,
// not pull: the caller is notifying peers of local state; it does not
// need — and should not wait for — per-peer acknowledgment.
//
// Each peer's delivery happens on its own goroutine with its own
// per-peer timeout (ForwardingTimeout by default). Errors are logged
// and the connection is invalidated, but never surface to the caller.
//
// This is why a SIGSTOP on one peer does NOT stall the caller: the
// paused peer's goroutine runs to its per-peer timeout asynchronously;
// meanwhile, the caller and other peers are unaffected. See
// gastrolog-5oofa.
func (b *Broadcaster) Send(ctx context.Context, msg *gastrologv1.BroadcastMessage) {
	peers, err := b.peers.Peers()
	if err != nil {
		b.logger.Debug("broadcast: get peers", "error", err)
		return
	}
	if len(peers) == 0 {
		return
	}

	req := &gastrologv1.BroadcastRequest{Message: msg}
	for _, p := range peers {
		go b.sendToPeer(ctx, string(p.ID), req)
	}
}

// sendToPeer handles one peer's delivery: dial → per-peer-timeout context →
// Invoke.
func (b *Broadcaster) sendToPeer(ctx context.Context, id string, req *gastrologv1.BroadcastRequest) {
	conn, err := b.peers.Conn(id)
	if err != nil {
		b.logPeerError(id, "dial peer", err)
		return
	}

	peerCtx, cancel := b.peerContext(ctx)
	defer cancel()

	if err := b.sendOne(peerCtx, conn, req); err != nil {
		b.logPeerError(id, "send", err)
		b.peers.Invalidate(id, err)
		return
	}
	b.clearPeerError(id)
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
