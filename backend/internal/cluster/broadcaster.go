package cluster

import (
	"context"
	"log/slog"
	"sync"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	hraft "github.com/hashicorp/raft"
)

// broadcastPeerSource is the subset of *PeerConnManager that Broadcaster needs.
// Extracted so tests can inject a fake without standing up a real Raft.
type broadcastPeerSource interface {
	Peers() ([]hraft.Server, error)
	InvokeService(ctx context.Context, peerNodeID, purpose, method string, req, resp any) error
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
func NewBroadcaster(peers *PeerConnManager, logger *slog.Logger) *Broadcaster {
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

// sendToPeer handles one peer's delivery: acquire → per-peer-timeout context →
// Invoke.
func (b *Broadcaster) sendToPeer(ctx context.Context, id string, req *gastrologv1.BroadcastRequest) {
	peerCtx, cancel := b.peerContext(ctx)
	defer cancel()

	resp := &gastrologv1.BroadcastResponse{}
	if err := b.peers.InvokeService(peerCtx, id, "broadcast",
		"/gastrolog.v1.ClusterService/Broadcast", req, resp); err != nil {
		b.logPeerError(id, "send", err)
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

// Close is a no-op — connection lifecycle is managed by PeerConnManager.
func (b *Broadcaster) Close() error { return nil }

// Delete drops the failure-suppression entry for a removed peer.
// Called from the peer-removal observer so the `failed` map doesn't
// accumulate dead entries forever. Naming aligns with the
// peerEvictor interface used elsewhere in cluster removal.
func (b *Broadcaster) Delete(peer string) {
	b.mu.Lock()
	delete(b.failed, peer)
	b.mu.Unlock()
}

// ReconcilePeers drops any failure-suppression entry whose peer is
// not in keep. Backstop for the observer path when hraft delivers a
// config change via snapshot install (no PeerObservation fires).
func (b *Broadcaster) ReconcilePeers(keep map[string]struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for p := range b.failed {
		if _, ok := keep[p]; !ok {
			delete(b.failed, p)
		}
	}
}
